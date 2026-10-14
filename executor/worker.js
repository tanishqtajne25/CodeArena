require('dotenv').config();
const { Worker } = require('bullmq');
const { connection } = require('./queue');
const pool = require('./db');
const { executeCode } = require('./executor');
const { createClient } = require('redis');

// Redis Pub --- Worker publishes results.
const publisher = createClient({
    socket: {
        host: process.env.REDIS_HOST || 'localhost',
        port: 6379
    }
});

publisher.connect().then(() => console.log('Redis Publisher Connected')).catch(err => console.error('Redis Publisher Error', err));

console.log('worker started');

function sanitizeOutput(str) {
    if (!str) return '';
    return str.replace(/\0/g, '').trim();
}

// Lamport Logical Clock for Worker Node
let workerLamportClock = 0;

const worker = new Worker(
    'code-submission',  // Must match queue name exactly.

    async (job) => {
        const { submissionId, code, language, problemId, lamportClock, lamportTimeline } = job.data;

        // Lamport Rule 2: L_worker = max(L_worker, L_msg) + 1
        const incomingClock = parseInt(lamportClock, 10) || 0;
        workerLamportClock = Math.max(workerLamportClock, incomingClock) + 1;

        const timeline = Array.isArray(lamportTimeline) ? [...lamportTimeline] : [];
        timeline.push({
            node: 'Worker Daemon',
            event: 'Job Picked from Queue',
            clock: workerLamportClock
        });

        console.log(`\n Processing job ${job.id} (Lamport Clock: ${workerLamportClock})`);
        console.log('Submission ID:', submissionId);
        console.log('Language:', language);
        console.log('Problem ID:', problemId);

        //update status to running.
        await pool.query(
            `UPDATE submissions SET status = 'running' WHERE id=$1`,
            [submissionId]
        );

        // Lamport Rule 1: increment before sending event
        workerLamportClock = workerLamportClock + 1;
        timeline.push({
            node: 'Worker Daemon',
            event: 'Execution Started',
            clock: workerLamportClock
        });

        //Notify client: running
        await publisher.publish(
            `job-${job.id}`,
            JSON.stringify({
                submissionId,
                status: 'running',
                output: null,
                lamportClock: workerLamportClock,
                lamportTimeline: timeline
            })
        );

         // Fetch test cases if problemId exists
        let testCases = [];
        if (problemId) {
            const tcResult = await pool.query(
                `SELECT id, input, expected_output, is_hidden
                 FROM test_cases
                 WHERE problem_id = $1
                 ORDER BY id ASC`,
                [problemId]
            );
            testCases = tcResult.rows;
            console.log(`Found ${testCases.length} test cases`);
        }

        // Execute code
        const result = await executeCode(code, language, testCases);

        // Determine final status
        const finalStatus = testCases.length > 0
            ? result.status          
            : (result.success ? 'accepted' : 'error');

         // Sanitize and save to DB
        await pool.query(
            `UPDATE submissions
             SET status = $1,
                 output = $2,
                 exec_time_ms = $3
             WHERE id = $4`,
            [
                finalStatus,
                sanitizeOutput(
                    testCases.length > 0
                        ? `${result.passedCount}/${result.totalCount} passed`
                        : (result.output || result.error)
                ),
                result.executionTime ||
                    (result.results?.[0]?.executionTime ?? 0),
                submissionId
            ]
        );

        // Lamport Rule 1: internal execution event completed
        workerLamportClock = workerLamportClock + 1;
        timeline.push({
            node: 'Worker Daemon',
            event: 'Sandbox Execution Completed',
            clock: workerLamportClock
        });

        // Publish full result to Redis
        await publisher.publish(
            `job-${job.id}`,
            JSON.stringify({
                submissionId,
                status: finalStatus,
                // Test case results
                passedCount:  result.passedCount ?? null,
                totalCount:   result.totalCount ?? null,
                testResults:  result.results ?? null,
                // Simple run output
                output:       result.output ?? null,
                executionTime: result.executionTime ??
                    result.results?.[0]?.executionTime ?? 0,
                lamportClock: workerLamportClock,
                lamportTimeline: timeline
            })
        );

        console.log(`Job ${job.id} done: ${finalStatus} (Lamport Clock: ${workerLamportClock})`);
        return { submissionId, status: finalStatus, lamportClock: workerLamportClock };
    },
     {
        connection,
        concurrency: 2,
     }
);

worker.on('completed', (job, result) => {
    console.log(`\n Job ${job.id} completed successfully.`);
});

worker.on('failed', (job, err) => {
    console.error(`\n Job ${job.id} failed:`, err.message);
});

worker.on('error', (err) => {
    console.error('Worker error:', err.message);
});

console.log('Worker ready — waiting for jobs...\n');