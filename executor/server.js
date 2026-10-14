const express = require('express');
const { executeCode } = require('./executor');
const { submissionQueue } = require('./queue');
const http = require('http');
const { Server } = require('socket.io');
const { createClient } = require('redis');
const path = require('path');
const pool = require('./db');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const authRoutes = require('./routes/auth');
const adminRoutes = require('./routes/admin');
const { verifyToken } = require('./middleware/auth');
const { authorizeRoles } = require('./middleware/rbac');
const { analyzeCode } = require('./services/aiAnalysis');

const app = express();

const server = http.createServer(app);      //crate http server
const io = new Server(server, {             //create socket.io server and allow CORS from frontend
    cors: {
        origin: "http://localhost:5173",
        credentials: true,
        methods: ["GET", "POST"]
    }
});

app.use(express.json({ limit: '1mb' }));  // Limit request size
app.use(cookieParser());       

app.use((req, res, next) => {           //Provide access to frontend
    res.header("Access-Control-Allow-Origin", "http://localhost:5173");
    res.header("Access-Control-Allow-Credentials", "true");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

const executeLimiter = rateLimit({
    windowMs: 60 * 1000,    
    max: 10,                 //max attempt can be 10
    message: {
        error: 'Too many submissions. Wait 1 minute.'
    }
});

app.use('/api/auth', authRoutes);
app.use('/admin', verifyToken, authorizeRoles('admin'), adminRoutes);

//Redis Subscriber --- Server listens for worker results.
const subscriber = createClient({
    socket: {
        host: process.env.REDIS_HOST || 'redis',
        port: 6379
    }
});

subscriber.connect()
    .then(() => console.log('Redis Subscriber Connected'))
    .catch(err => console.error('Redis Subscriber Error', err));

// Lamport Logical Clock for API Gateway
let serverLamportClock = 0;

//subscribe to job results from worker.
//When worker publishes ---> Server emits to WebSocket Room.

subscriber.pSubscribe('job-*', (message, channel) => {
    const result = JSON.parse(message);
    const jobId = channel.replace('job-', '');

    // Lamport Rule 2: L_server = max(L_server, L_msg) + 1
    const incomingClock = result.lamportClock || 0;
    serverLamportClock = Math.max(serverLamportClock, incomingClock) + 1;

    const timeline = Array.isArray(result.lamportTimeline) ? [...result.lamportTimeline] : [];
    timeline.push({
        node: 'API Gateway',
        event: 'Websocket Relay',
        clock: serverLamportClock
    });

    result.lamportClock = serverLamportClock;
    result.lamportTimeline = timeline;

    console.log(`Received Result for job ${jobId} (Lamport Clock: ${serverLamportClock})`);
    console.log(`Status: ${result.status}`);

    //Emit to Websocket Room for this job.
    io.to(`job-${jobId}`).emit('job-result', result);

    console.log(`Emitted to Room: job-${jobId}`); 
});

//Make io accessible everywhere in the application.
app.set('io', io);

//-------  Websocket Connection Handler  -------------------
io.on('connection', (socket) => {
    console.log(`\n Client Connected: ${socket.id}`);

    //Client joins room for their specific job
    socket.on('join-job', (jobId) => {
       socket.join(`job-${jobId}`);
       console.log(`Client ${socket.id} join room: job-${jobId}`);
    });

    socket.on('disconnect', () => {
       console.log(`Client disconnected: ${socket.id}`);
    });
});

//REST API

app.get('/test', (req, res) => {
    res.sendFile(path.join(__dirname, 'test.html'));
});

app.get('/', (req, res) => {
     res.json({
        status: 'CodeArena running',
        version: '2.0 - Queue System'
    });
});

// Add Run route (no queue, instant)
app.post('/run', verifyToken, async (req, res) => {
    const { code, language, stdin } = req.body;

    if (!code || !language) {
        return res.status(400).json({
            error: 'code and language required'
        });
    }

    // Simple code length check
    if (code.length > 50000) {
        return res.status(400).json({
            error: 'Code too long (max 50,000 characters)'
        });
    }

    try {
        const { runOnce } = require('./executor');
        const result = await runOnce(code, language, stdin || '');
        res.json(result);
    } catch (err) {
        console.error('Run error:', err);
        res.status(500).json({ error: 'Execution failed' });
    }
});

app.post('/execute', verifyToken, executeLimiter, async (req, res) => {
    const { code, language, problemId, lamportClock } = req.body;
    const userId = req.user.id;

    if(!code || !language) {
        return res.status(400).json({
            error: 'code and language required'
        });
    }

    // Simple code length check
    if (code.length > 50000) {
        return res.status(400).json({
            error: 'Code too long (max 50,000 characters)'
        });
    }

    // Lamport Rule 2: L_server = max(L_server, L_client) + 1
    const clientClock = parseInt(lamportClock, 10) || 0;
    serverLamportClock = Math.max(serverLamportClock, clientClock) + 1;

    const initialTimeline = [
        { node: 'Client (Browser)', event: 'Submit Code', clock: clientClock },
        { node: 'API Gateway', event: 'Job Queued in Redis', clock: serverLamportClock }
    ];

    try {
        const submission = await pool.query(       //update in submission
            `INSERT INTO submissions
                (user_id, problem_id, code, language, status)
             VALUES ($1, $2, $3, $4, 'pending')
             RETURNING id`,
             [userId, problemId || 1, code, language]
        );

        const submissionId = submission.rows[0].id;
        
        const job = await submissionQueue.add('execute', {         //add job to Queue for worker
            submissionId,
            code,
            language,
            problemId: problemId || null,
            userId: userId,
            lamportClock: serverLamportClock,
            lamportTimeline: initialTimeline
        });

        console.log(`Job ${job.id} queued for submission ${submissionId} (Lamport Clock: ${serverLamportClock})`);

        res.json({       //Response with pending and submission to queue is successfull
            message: 'Submitted successfully',
            submissionId,
            jobId: job.id,
            status: 'pending',
            checkStatus: `user/status/${submissionId}`,
            lamportClock: serverLamportClock,
            lamportTimeline: initialTimeline
        });

    } catch (err) {
        console.error('Execute error:', err);
        res.status(500).json({ error: 'Submission failed' });
    }
});

//To get the status of the submission either successfull or unsuccessfull
app.get('/user/status/:submissionId', verifyToken, authorizeRoles('user'), async (req, res) => {
    const submissionId = parseInt(req.params.submissionId);
    const userId = req.user.id;
    
    console.log('Status check for submission:', submissionId);
    
    if (isNaN(submissionId)) {
        return res.status(400).json({ error: 'Invalid submission ID' });
    }
    
    try {
        const result = await pool.query(
            `SELECT id, language, status, output, exec_time_ms, submitted_at
             FROM submissions
             WHERE id = $1 AND user_id = $2`,
            [submissionId, userId]
        );

        console.log('Query result rows:', result.rows.length);
        
        if(result.rows.length === 0) {
            console.log('Submission not found');
            return res.status(404).json({ error: 'Submission not found' });
        }

        console.log('Returning submission:', result.rows[0]);
        res.json(result.rows[0]);

    } catch (err) {
        console.error('Error in /status:', err);
        res.status(500).json({ error: 'Server error' });
    }
});

//Get history of Data send for particular user.
app.get('/user/submissions', verifyToken, authorizeRoles('user'), async (req, res) => {
    const userId = req.user.id;

    try {
        const result = await pool.query(
            `SELECT
                s.id, s.language, s.status,
                s.exec_time_ms, s.submitted_at,
                p.title as problem
             FROM submissions s
             LEFT JOIN problems p ON s.problem_id = p.id
             WHERE s.user_id = $1
             ORDER BY s.submitted_at DESC
             LIMIT 20`,
            [userId]
        );

        res.json(result.rows);

    } catch (err) {
        res.status(500).json({ error: 'Server error' });
    }
});

// In server.js — public route, no auth needed
app.get('/problems', async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                p.id, p.title, p.description, p.difficulty,
                json_agg(
                    json_build_object(
                        'id', tc.id,
                        'input', tc.input,
                        'expected_output', tc.expected_output,
                        'is_hidden', tc.is_hidden
                    )
                ) FILTER (WHERE tc.id IS NOT NULL 
                          AND tc.is_hidden = false)
                as visible_test_cases
             FROM problems p
             LEFT JOIN test_cases tc ON p.id = tc.problem_id
             GROUP BY p.id
             ORDER BY p.difficulty, p.title`
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/me', verifyToken, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT id, username, email, role, created_at
             FROM users WHERE id = $1`,
            [req.user.id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        res.json(result.rows[0]);

    } catch (error) {
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/analyze', verifyToken, async (req, res) => {
    const {
        problemId,
        code,
        language,
        testResults,
        passedCount,
        totalCount
    } = req.body;

    if (!problemId || !code || !language) {
        return res.status(400).json({
            error: 'Missing required fields'
        });
    }

    try {
        // Get problem details
        const problemResult = await pool.query(
            'SELECT id, title, description, difficulty FROM problems WHERE id = $1',
            [problemId]
        );

        if (problemResult.rows.length === 0) {
            return res.status(404).json({
                error: 'Problem not found'
            });
        }

        // Get user's recent submissions
        const recentResult = await pool.query(
            `SELECT status, exec_time_ms FROM submissions
             WHERE user_id = $1 AND problem_id = $2
             ORDER BY submitted_at DESC LIMIT 3`,
            [req.user.id, problemId]
        );

        // Setup streaming response
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');

        // Get stream from Groq
        const stream = await analyzeCode({
            problem: problemResult.rows[0],
            code,
            language,
            testResults,
            passedCount,
            totalCount,
            recentSubmissions: recentResult.rows
        });

        // Stream chunks to client
        for await (const chunk of stream) {
            const text = chunk.choices[0]?.delta?.content || '';
            if (text) {
                res.write(`data: ${JSON.stringify({ text })}\n\n`);
            }
        }

        res.write('data: [DONE]\n\n');
        res.end();

    } catch (err) {
        console.error('AI Analysis error:', err);
        if (!res.headersSent) {
            res.status(500).json({ error: 'AI analysis failed' });
        }
    }
});


server.listen(3000, () => console.log('Server running on 3000'));