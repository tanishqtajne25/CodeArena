import { useState, useEffect, useRef } from 'react';
import MonacoEditor from '@monaco-editor/react';    //Editor to write the code
import { io } from 'socket.io-client';              //For websocket connection to join with server
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../api/axios';

const LANGUAGES = [                              //To choose language (one can add)
    { value: 'python', label: 'Python' },
    { value: 'cpp', label: 'C++' },
];

const DEFAULT_CODE = {                          //Default code visible to user as per language selected
    python: `# Write your Python code here     
nums = [1, 2, 3, 4, 5]
print(f"Sum: {sum(nums)}")`,
    cpp: `#include<iostream>                     
using namespace std;
int main(){
    cout << "Hello from C++!" << endl;
    return 0;
}`
};

export default function Editor() {
    const [code, setCode] = useState(DEFAULT_CODE.python);
    const [language, setLanguage] = useState('python');
    const [submitting, setSubmitting] = useState(false);
    const [result, setResult] = useState(null);
    const [status, setStatus] = useState('');
    const socketRef = useRef(null);
    const [problems, setProblems] = useState([]);
    const [selectedProblem, setSelectedProblem] = useState(null);

    // Add to state:
    const [customInput, setCustomInput] = useState('');
    const [testResults, setTestResults] = useState(null);
    const [activeOutputTab, setActiveOutputTab] = useState('output');

    // Distributed Systems: Lamport Logical Clock state (Unit III)
    const [clientLamportClock, setClientLamportClock] = useState(0);
    const [lamportTimeline, setLamportTimeline] = useState([]);

    const [showSuccess, setShowSuccess] = useState(false);

    // AI analysis
    const [aiAnalysis, setAiAnalysis] = useState('');
    const [aiLoading, setAiLoading] = useState(false);


    const { user, logout } = useAuth();
    const navigate = useNavigate();

    // Setup Socket.io connection
    useEffect(() => {                                         //useEffect to avoid infinte call
        socketRef.current = io('http://15.206.170.214:3000', {
            withCredentials: true
        });

        socketRef.current.on('connect', () => {             
            console.log('Socket connected');
        });

        socketRef.current.on('job-result', (data) => {        //Get the job available in the websocket channel
            // Lamport Rule 2 on client receipt: L_client = max(L_client, L_incoming) + 1
            if (data.lamportClock !== undefined) {
                setClientLamportClock(prev => {
                    const updated = Math.max(prev, Number(data.lamportClock)) + 1;
                    if (data.lamportTimeline) {
                        const finalTimeline = [...data.lamportTimeline, {
                            node: 'Client (Browser)',
                            event: data.status === 'running' ? 'Status: Running Received' : 'Final Result Received',
                            clock: updated
                        }];
                        setLamportTimeline(finalTimeline);
                    }
                    return updated;
                });
            }

            if (data.status === 'running') {
             setStatus('running');
             return;    
           }
            setResult(data);
            setStatus(data.status);
            setSubmitting(false);

            if (data.testResults) {
               setTestResults(data.testResults);               
               setActiveOutputTab('testcases');
            }else {
               setActiveOutputTab('output');
            }

            if (data.status === 'accepted') {
               setShowSuccess(true);
               setTimeout(() => setShowSuccess(false), 3000);
            }
     });

        return () => {
            socketRef.current.disconnect();           //disconnect for saving from slow down website
        };
    }, []);             // run once when refresh page

    // Add useEffect to fetch problems:
    useEffect(() => {
      api.get('/problems')
        .then(res => {
            setProblems(res.data);
            if (res.data.length > 0) {
                setSelectedProblem(res.data[0]);
            }
        })
        .catch(err => console.error('Failed to fetch problems'));
    }, []);

    // Change code when language changes
    const handleLanguageChange = (e) => {
        setLanguage(e.target.value);
        setCode(DEFAULT_CODE[e.target.value]);
        setResult(null);
        setStatus('');
    };

    // Add Run function (instant, no queue):
    const handleRun = async () => {
        if (!code.trim()) return;
        setSubmitting(true);
        setResult(null);
        setStatus('running');
        setTestResults(null);

        try {
            const res = await api.post('/run', {        //no queue based run for RUN BUTTON
                code,
                language,
                stdin: customInput
            });
            setResult(res.data);
            setStatus(res.data.success ? 'accepted' : 'error');
            setActiveOutputTab('output');
        } catch (err) {
            setResult({
                output: err.response?.data?.error || 'Run failed'
        });
            setStatus('error');
        } finally {
            setSubmitting(false);
        }
    };

    const handleSubmit = async () => {
        if (!code.trim()) return;
        setSubmitting(true);
        setResult(null);
        setStatus('pending');

        // Lamport Rule 1: L_client = L_client + 1 before sending message
        const nextClock = clientLamportClock + 1;
        setClientLamportClock(nextClock);
        setLamportTimeline([
            { node: 'Client (Browser)', event: 'Submit Code', clock: nextClock }
        ]);

        try {
            const res = await api.post('/execute', {         //queue based FOR SUBMIT BUTTON
                code,
                language,
                problemId: selectedProblem?.id || null,
                lamportClock: nextClock
            });

            if (res.data.lamportTimeline) {
                setLamportTimeline(res.data.lamportTimeline);
            }
            if (res.data.lamportClock) {
                // Lamport Rule 2 on receiving HTTP response
                setClientLamportClock(prev => Math.max(prev, Number(res.data.lamportClock)));
            }

            const { jobId } = res.data;

            // Join WebSocket room for this job
            socketRef.current.emit('join-job', jobId);
            setStatus('running');

        } catch (err) {
            setSubmitting(false);
            setStatus('error');
            setResult({
                output: err.response?.data?.error || 'Submission failed'
            });
        }
    };

    const handleLogout = async () => {
        await logout();
        navigate('/login');
    };

    //Funciton for AI analysis on error.
    const getAiAnalysis = async () => {
        if (!testResults || !selectedProblem) return;
        setAiAnalysis('');
        setAiLoading(true);

        try {
         const response = await fetch(
            'http://15.206.170.214:3000/analyze',      
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                credentials: 'include',
                body: JSON.stringify({
                    problemId: selectedProblem.id,
                    code,
                    language,
                    testResults,
                    passedCount: result?.passedCount,
                    totalCount: result?.totalCount
                })
               }
           );

           // Read streaming response
           const reader = response.body.getReader();
           const decoder = new TextDecoder();

           while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const chunk = decoder.decode(value);
            const lines = chunk.split('\n');

            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    const data = line.slice(6);
                    if (data === '[DONE]') break;
                    try {
                        const parsed = JSON.parse(data);
                        setAiAnalysis(prev =>
                            prev + parsed.text
                        );
                    } catch (e) {}
                }
            }
          }
         } catch (err) {
            setAiAnalysis('AI analysis failed. Try again.');
         } finally {
           setAiLoading(false);
       }
    };

    return (
        <div style={styles.container}>

            {/* Navbar */}
            <div style={styles.navbar}>
                <div style={styles.logo}>
                    <span style={{ color: '#fff' }}>Code</span>
                    <span style={{ color: '#00ff88' }}>Arena</span>
                </div>
                <div style={styles.navRight}>

                    <span style={styles.username}>
                        👤 {user?.username}
                    </span>
                   
                    <button
                        onClick={() => navigate('/submissions')}
                        style={styles.navBtn}
                    >
                        My Submissions
                    </button>
                    <button
                        onClick={handleLogout}
                        style={styles.logoutBtn}
                    >
                        Logout
                    </button>
                </div>
            </div>

            {/* Main Content */}
            <div style={styles.main}>

                {/* Left — Problem Description */}
                <div style={styles.problemSection}>
                    {selectedProblem ? (
                        <div style={styles.problemContent}>
                            <div style={styles.problemHeader}>
                                <h2 style={styles.problemTitle}>
                                    {selectedProblem.title}
                                </h2>
                                <span style={{               //styling difficulty status using CSS
                                    ...styles.difficultyBadge,
                                    background: selectedProblem.difficulty === 'easy'
                                        ? 'rgba(0,255,136,0.1)'
                                        : selectedProblem.difficulty === 'medium'
                                        ? 'rgba(255,200,0,0.1)'
                                        : 'rgba(255,80,80,0.1)',
                                    color: selectedProblem.difficulty === 'easy'
                                        ? '#00ff88'
                                        : selectedProblem.difficulty === 'medium'
                                        ? '#ffc800'
                                        : '#ff6b6b',
                                    border: `1px solid ${
                                        selectedProblem.difficulty === 'easy'
                                        ? 'rgba(0,255,136,0.3)'
                                        : selectedProblem.difficulty === 'medium'
                                        ? 'rgba(255,200,0,0.3)'
                                        : 'rgba(255,80,80,0.3)'
                                    }`
                                }}>
                                    {selectedProblem.difficulty.toUpperCase()}
                                </span>
                            </div>

                            <div style={styles.problemDescription}>
                                {selectedProblem.description}
                            </div>
                            
                            //Show the visible test cases with input and output
                            {selectedProblem.visible_test_cases && 
                             selectedProblem.visible_test_cases.length > 0 && (
                                <div style={styles.examplesSection}>
                                    <h3 style={styles.examplesTitle}>Examples:</h3>
                                    {selectedProblem.visible_test_cases.map((tc, i) => (
                                        <div key={i} style={styles.exampleCard}>
                                            <div style={styles.exampleLabel}>
                                                Example {i + 1}
                                            </div>
                                            <div style={styles.exampleItem}>
                                                <span style={styles.exampleKey}>Input:</span>
                                                <pre style={styles.exampleValue}>
                                                    {tc.input || '(no input)'}
                                                </pre>
                                            </div>
                                            <div style={styles.exampleItem}>
                                                <span style={styles.exampleKey}>Output:</span>
                                                <pre style={styles.exampleValue}>
                                                    {tc.expected_output}
                                                </pre>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    ) : (
                        <div style={styles.noProblem}>
                            <p style={{ fontSize: '48px', marginBottom: '16px' }}>📝</p>
                            <p style={{ color: '#666' }}>Select a problem to get started</p>
                        </div>
                    )}
                </div>

                {/* Middle — Editor */}
                <div style={styles.editorSection}>

                    {/* Toolbar */}
                    <div style={styles.toolbar}>
                        <select
                            value={language}
                            onChange={handleLanguageChange}
                            style={styles.select}
                        >
                            {LANGUAGES.map(lang => (            //selected lanuage update
                                <option key={lang.value} value={lang.value}>
                                    {lang.label}
                                </option>
                            ))}
                        </select>

                        <select
                            value={selectedProblem?.id || ''}         //select problem form list
                            onChange={(e) => {
                               const p = problems.find(p => p.id === parseInt(e.target.value));
                               setSelectedProblem(p);
                            }}
                            style={styles.select}
                        >
                           {problems.map(p => (
                              <option key={p.id} value={p.id}>
                                {p.title} ({p.difficulty})
                              </option>
                            ))}
                        </select>

                        <button  //Run Button to test input
                            onClick={handleRun}
                            disabled={submitting}
                            style={{
                                ...styles.runBtn,
                                opacity: submitting ? 0.7 : 1
                            }}
                        >
                            ▶ Run                        
                        </button>

                        <button   //Submmit to test all test cases
                            onClick={handleSubmit}
                            disabled={submitting}
                            style={{
                                ...styles.submitBtn,
                                opacity: submitting ? 0.7 : 1
                            }}
                        >
                            ⬆ Submit
                        </button>
                    </div>

                    {/* Monaco Editor */}
                    <MonacoEditor
                        height="calc(100vh - 130px)"
                        language={language === 'cpp' ? 'cpp' : 'python'}
                        value={code}
                        onChange={(val) => setCode(val || '')}
                        theme="vs-dark"
                        options={{
                            fontSize: 15,
                            minimap: { enabled: false },
                            scrollBeyondLastLine: false,
                            automaticLayout: true,
                            tabSize: 4,
                            wordWrap: 'on',
                            lineNumbers: 'on',
                            renderLineHighlight: 'all',
                        }}
                    />
                </div>

                {/* Right — Output */}
<div style={styles.outputSection}>
    
    {/* Header */}
    <div style={styles.outputHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span>Output</span>
            <span style={{
                fontSize: '11px',
                padding: '3px 8px',
                borderRadius: '4px',
                background: 'rgba(0, 170, 255, 0.15)',
                color: '#00aaff',
                border: '1px solid rgba(0, 170, 255, 0.3)',
                fontFamily: 'monospace',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '4px'
            }}>
                🕒 Lamport Clock: <strong>L={clientLamportClock}</strong>
            </span>
        </div>
        {status && (             //styling status using CSS
            <span style={{
                ...styles.statusBadge,
                background: status === 'accepted'
                    ? 'rgba(0,255,136,0.2)'
                    : status === 'running'
                    ? 'rgba(0,170,255,0.2)'
                    : status === 'pending'
                    ? 'rgba(255,200,0,0.2)'
                    : 'rgba(255,80,80,0.2)',
                color: status === 'accepted'
                    ? '#00ff88'
                    : status === 'running'
                    ? '#00aaff'
                    : status === 'pending'
                    ? '#ffc800'
                    : '#ff5050',
                border: `1px solid ${
                    status === 'accepted' ? '#00ff88'
                    : status === 'running' ? '#00aaff'
                    : status === 'pending' ? '#ffc800'
                    : '#ff5050'}`
            }}>
                {status === 'accepted' ? '✅ Accepted'
                : status === 'running' ? '⚡ Running'
                : status === 'pending' ? '⏳ Pending'
                : '❌ Error'}
            </span>
        )}
    </div>

    {/* Tabs */}
    <div style={styles.outputTabs}>
        <button
            onClick={() => setActiveOutputTab('output')}
            style={{
                ...styles.outputTab,
                color: activeOutputTab === 'output'
                    ? '#00ff88' : '#666',
                borderBottom: activeOutputTab === 'output'
                    ? '2px solid #00ff88'
                    : '2px solid transparent'
            }}
        >
            Output
        </button>
        <button
            onClick={() => setActiveOutputTab('testcases')}
            style={{
                ...styles.outputTab,
                color: activeOutputTab === 'testcases'
                    ? '#00ff88' : '#666',
                borderBottom: activeOutputTab === 'testcases'
                    ? '2px solid #00ff88'
                    : '2px solid transparent'
            }}
        >
            Test Cases
        </button>
        <button
            onClick={() => setActiveOutputTab('lamport')}
            style={{
                ...styles.outputTab,
                color: activeOutputTab === 'lamport'
                    ? '#00aaff' : '#666',
                borderBottom: activeOutputTab === 'lamport'
                    ? '2px solid #00aaff'
                    : '2px solid transparent'
            }}
        >
            🕒 Lamport Clocks (DS)
        </button>
    </div>

    {/* Body */}
    <div style={styles.outputBody}>

        {/* OUTPUT TAB */}
        {activeOutputTab === 'output' && (
            <div>
                {/* Custom Input */}
                <p style={styles.outputLabel}>Custom Input</p>
                <textarea
                    value={customInput}
                    onChange={e => setCustomInput(e.target.value)}
                    placeholder="Enter custom input here..."
                    style={styles.customInput}
                    rows={3}
                />

                {/* Spinner */}
                {submitting && !result && (
                    <div style={styles.running}>
                        <div style={styles.spinner}>⚡</div>
                        <p>Executing your code...</p>
                    </div>
                )}

                {/* Result Output */}
                {result && !testResults && (
                    <div style={{ marginTop: '12px' }}>
                        {result.output && (
                            <div>
                                <p style={styles.outputLabel}>
                                    Output:
                                </p>
                                <pre style={styles.outputText}>
                                    {result.output}
                                </pre>
                            </div>
                        )}
                        {result.executionTime && (
                            <p style={styles.execTime}>
                                ⏱ {result.executionTime}ms
                            </p>
                        )}
                    </div>
                )}

                {/* Placeholder */}
                {!result && !submitting && (
                    <p style={{
                        ...styles.placeholder,
                        marginTop: '16px'
                    }}>
                        Click ▶ Run to test with custom input
                    </p>
                )}
            </div>
        )}

        {/* TEST CASES TAB */}
        {activeOutputTab === 'testcases' && (
            <div>
                {/* Spinner */}
                {submitting && (
                    <div style={styles.running}>
                        <div style={styles.spinner}>⚡</div>
                        <p>Running test cases...</p>
                    </div>
                )}

                {/* No results yet */}
                {!testResults && !submitting && (
                    <p style={styles.placeholder}>
                        Click ⬆ Submit to run test cases
                    </p>
                )}

                {/* Test Case Results */}
                {testResults && (
                    <div>
                        {/* Score */}
                        <p style={{
                            color: result?.status === 'accepted'
                                ? '#00ff88' : '#ff6b6b',
                            fontWeight: 'bold',
                            fontSize: '16px',
                            marginBottom: '16px'
                        }}>
                            {result?.passedCount}/{result?.totalCount}
                            {' '}Test Cases Passed
                        </p>

                        {/* Visible Test Cases */}
                        {testResults
                            .filter(tc => !tc.hidden)
                            .map((tc, i) => (
                            <div key={i} style={{
                                ...styles.testCase,
                                borderColor: tc.passed
                                    ? 'rgba(0,255,136,0.3)'
                                    : 'rgba(255,80,80,0.3)'
                            }}>
                                {/* Test Case Header */}
                                <div style={styles.testCaseHeader}>
                                    <span style={{
                                        color: tc.passed
                                            ? '#00ff88' : '#ff6b6b',
                                        fontWeight: 'bold',
                                        fontSize: '13px'
                                    }}>
                                        {tc.passed ? '✅' : '❌'}
                                        {' '}Test {i + 1}
                                    </span>
                                    <span style={{
                                        color: '#666',
                                        fontSize: '12px'
                                    }}>
                                        {tc.executionTime}ms
                                    </span>
                                </div>

                                {/* Input */}
                                {tc.input && (
                                    <div style={styles.tcRow}>
                                        <span style={styles.tcLabel}>
                                            Input:
                                        </span>
                                        <code style={styles.tcValue}>
                                            {tc.input}
                                        </code>
                                    </div>
                                )}

                                {/* Expected */}
                                <div style={styles.tcRow}>              
                                    <span style={styles.tcLabel}>
                                        Expected:
                                    </span>
                                    <code style={styles.tcValue}>
                                        {tc.expectedOutput}
                                    </code>
                                </div>

                                {/* Got (only on fail) */}
                                {!tc.passed && (                   
                                    <div style={styles.tcRow}>
                                        <span style={{
                                            ...styles.tcLabel,
                                            color: '#ff6b6b'
                                        }}>
                                            Got:
                                        </span>
                                        <code style={{
                                            ...styles.tcValue,
                                            color: '#ff6b6b'
                                        }}>
                                            {tc.actualOutput || tc.error}
                                        </code>
                                    </div>
                                )}
                            </div>
                        ))}

                        {/* Hidden Test Cases Count */}
                        {testResults.filter(tc => tc.hidden).length > 0 && (      //Show on screen how many hidden test cases passed
                            <div style={styles.hiddenInfo}>
                                🔒{' '}
                                {testResults
                                    .filter(tc => tc.hidden && tc.passed)
                                    .length}
                                /
                                {testResults
                                    .filter(tc => tc.hidden)
                                    .length}
                                {' '}hidden test cases passed
                            </div>
                        )}

                        {/* AI BUTTON GOES HERE */}
                {result?.status !== 'accepted' && (
                    <div style={{ marginTop: '16px' }}>
                        <button
                            onClick={getAiAnalysis}
                            disabled={aiLoading}
                            style={styles.aiBtn}
                        >
                            {aiLoading
                                ? '🤖 Analyzing...'
                                : '🤖 Get AI Help'}
                        </button>

                        {(aiAnalysis || aiLoading) && (          //AI analysis
                            <div style={styles.aiBox}>
                                <p style={styles.aiTitle}>
                                    🤖 AI Analysis
                                </p>
                                {aiLoading && !aiAnalysis && (
                                    <p style={{
                                        color: '#666',
                                        fontSize: '13px'
                                    }}>
                                        Analyzing your code...
                                    </p>
                                )}
                                <pre style={styles.aiText}>
                                    {aiAnalysis}
                                    {aiLoading && (
                                        <span style={styles.cursor}>
                                            ▋
                                        </span>
                                    )}
                                </pre>
                             </div>
                        )}
                     </div>
                   )}
                    </div>
                )}
            </div>
        )}

        {/* LAMPORT LOGICAL CLOCKS TAB (UNIT III) */}
        {activeOutputTab === 'lamport' && (
            <div style={{ padding: '4px 0' }}>
                <div style={{
                    padding: '12px 14px',
                    borderRadius: '8px',
                    background: 'rgba(0, 170, 255, 0.08)',
                    border: '1px solid rgba(0, 170, 255, 0.25)',
                    marginBottom: '16px'
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                            <span style={{ fontSize: '11px', textTransform: 'uppercase', letterSpacing: '1px', color: '#00aaff', fontWeight: 'bold' }}>
                                Syllabus Unit III: Synchronization
                            </span>
                            <h3 style={{ margin: '4px 0 0', color: '#fff', fontSize: '16px' }}>
                                Lamport's Logical Clock Algorithm
                            </h3>
                        </div>
                        <div style={{
                            padding: '6px 12px',
                            borderRadius: '6px',
                            background: '#0d1829',
                            border: '1px solid #00aaff',
                            color: '#00aaff',
                            fontSize: '18px',
                            fontWeight: 'bold',
                            fontFamily: 'monospace'
                        }}>
                            L = {clientLamportClock}
                        </div>
                    </div>
                    <p style={{ margin: '8px 0 0', fontSize: '12px', color: '#aaa', lineHeight: '1.5' }}>
                        Tracks causal event ordering across asynchronous nodes without relying on synchronized physical clocks.
                    </p>
                </div>

                {/* Causal Chain Timeline */}
                <p style={styles.outputLabel}>Distributed Causal Timeline (a → b)</p>
                {lamportTimeline.length === 0 ? (
                    <p style={styles.placeholder}>
                        Click ⬆ Submit to trigger the distributed Lamport clock event chain.
                    </p>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '16px' }}>
                        {lamportTimeline.map((item, index) => (
                            <div key={index} style={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                padding: '10px 12px',
                                borderRadius: '6px',
                                background: '#18181b',
                                border: '1px solid #27272a',
                                borderLeft: '3px solid #00aaff'
                            }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                    <span style={{
                                        display: 'inline-flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        width: '22px',
                                        height: '22px',
                                        borderRadius: '50%',
                                        background: 'rgba(0, 170, 255, 0.2)',
                                        color: '#00aaff',
                                        fontSize: '11px',
                                        fontWeight: 'bold'
                                    }}>
                                        {index + 1}
                                    </span>
                                    <div>
                                        <div style={{ color: '#fff', fontSize: '13px', fontWeight: '500' }}>
                                            {item.event}
                                        </div>
                                        <div style={{ color: '#71717a', fontSize: '11px' }}>
                                            Node: <span style={{ color: '#a1a1aa' }}>{item.node}</span>
                                        </div>
                                    </div>
                                </div>
                                <span style={{
                                    fontFamily: 'monospace',
                                    fontSize: '13px',
                                    padding: '2px 8px',
                                    borderRadius: '4px',
                                    background: 'rgba(0, 170, 255, 0.15)',
                                    color: '#00aaff',
                                    border: '1px solid rgba(0, 170, 255, 0.3)'
                                }}>
                                    Clock: L={item.clock}
                                </span>
                            </div>
                        ))}
                    </div>
                )}

                {/* Mathematical Proof Card */}
                <div style={{
                    padding: '12px 14px',
                    borderRadius: '8px',
                    background: '#131316',
                    border: '1px solid #27272a'
                }}>
                    <p style={{ margin: '0 0 6px', color: '#e4e4e7', fontSize: '13px', fontWeight: 'bold' }}>
                        📐 Algorithm Rules Applied:
                    </p>
                    <ul style={{ margin: 0, paddingLeft: '18px', color: '#a1a1aa', fontSize: '12px', lineHeight: '1.6' }}>
                        <li>
                            <strong style={{ color: '#00aaff' }}>Rule 1 (Internal Event / Pre-Send):</strong> Before generating an event or sending message <em>m</em>, process <em>i</em> increments: <code>L_i = L_i + 1</code>.
                        </li>
                        <li>
                            <strong style={{ color: '#00aaff' }}>Rule 2 (Message Reception):</strong> Upon receiving <em>(m, L_msg)</em>, process <em>j</em> updates its clock: <code>L_j = max(L_j, L_msg) + 1</code>.
                        </li>
                        <li>
                            <strong style={{ color: '#00ff88' }}>Clock Condition:</strong> For any two events <em>a</em> and <em>b</em>, if <em>a → b</em> (causally precedes), then <em>L(a) &lt; L(b)</em>.
                        </li>
                    </ul>
                </div>
            </div>
        )}

            </div>
        </div>
       </div>
            {showSuccess && (                      //Showing success after passing all test cases
            <div style={styles.successOverlay}>
                <div style={styles.successBox}>
                            <div style={{ fontSize: '48px' }}>🎉</div>
                    <h2 style={{ color: '#00ff88', margin: '8px 0' }}>
                        Accepted!
                    </h2>
                    <p style={{ color: '#aaa', fontSize: '14px' }}>
                        {result?.passedCount}/{result?.totalCount} test cases passed
                    </p>
                        </div>
            </div>
            )}
        </div>
        );
    }

const styles = {
    container: {
        height: '100vh',
        display: 'flex',
        flexDirection: 'column',
        background: '#0f0f23',
        overflow: 'hidden',
    },
    navbar: {
        height: '56px',
        background: 'rgba(255,255,255,0.05)',
        borderBottom: '1px solid rgba(0,255,136,0.2)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0 24px',
        flexShrink: 0,
    },
    logo: {
        fontSize: '22px',
        fontWeight: 'bold',
    },
    navRight: {
        display: 'flex',
        alignItems: 'center',
        gap: '16px',
    },
    username: {
        color: '#ffffff',
        fontSize: '18px',
        fontWeight: '700',
        letterSpacing: '1px',
        textShadow: '0 0 10px rgba(0,255,255,0.7)',
        margin: '20px',
        border: '3px solid #00ffff',
        borderRadius: '14px',
        background: 'linear-gradient(135deg, #ff00ff85, #00ffff61, #00ff9977)',
        boxShadow: `
            0 0 10px #00ffff3e,
            0 0 20px #ff00ff48,
            0 0 40px #00ff9956
        `,
        textShadow: '0 0 8px rgba(255,255,255,0.8)',
        display: 'inline-block'
    },
    navBtn: {
        background: 'transparent',
        border: '1px solid rgba(0,255,136,0.4)',
        borderRadius: '6px',
        padding: '6px 14px',
        color: '#00ff88',
        cursor: 'pointer',
        fontSize: '13px',
    },
    logoutBtn: {
        background: 'transparent',
        border: '1px solid rgba(255,80,80,0.4)',
        borderRadius: '6px',
        padding: '6px 14px',
        color: '#ff6b6b',
        cursor: 'pointer',
        fontSize: '13px',
    },
    main: {
        display: 'flex',
        flex: 1,
        overflow: 'hidden',
    },
    problemSection: {
        width: '400px',
        display: 'flex',
        flexDirection: 'column',
        background: '#0a0a1a',
        borderRight: '1px solid rgba(255,255,255,0.1)',
        overflowY: 'auto',
    },
    problemContent: {
        padding: '24px',
    },
    problemHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        marginBottom: '20px',
        gap: '12px',
    },
    problemTitle: {
        fontSize: '20px',
        fontWeight: 'bold',
        color: '#ffffff',
        margin: 0,
        lineHeight: '1.4',
    },
    difficultyBadge: {
        padding: '4px 12px',
        borderRadius: '20px',
        fontSize: '11px',
        fontWeight: 'bold',
        textTransform: 'uppercase',
        whiteSpace: 'nowrap',
    },
    problemDescription: {
        color: '#ccc',
        fontSize: '14px',
        lineHeight: '1.7',
        marginBottom: '24px',
        whiteSpace: 'pre-wrap',
    },
    examplesSection: {
        marginTop: '24px',
    },
    examplesTitle: {
        color: '#aaa',
        fontSize: '14px',
        fontWeight: '600',
        marginBottom: '12px',
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
    },
    exampleCard: {
        background: 'rgba(255,255,255,0.03)',
        border: '1px solid rgba(255,255,255,0.08)',
        borderRadius: '8px',
        padding: '12px',
        marginBottom: '12px',
    },
    exampleLabel: {
        color: '#00ff88',
        fontSize: '12px',
        fontWeight: 'bold',
        marginBottom: '8px',
    },
    exampleItem: {
        marginBottom: '8px',
    },
    exampleKey: {
        color: '#888',
        fontSize: '12px',
        display: 'block',
        marginBottom: '4px',
    },
    exampleValue: {
        background: 'rgba(0,0,0,0.3)',
        border: '1px solid rgba(255,255,255,0.1)',
        borderRadius: '4px',
        padding: '8px',
        color: '#ccc',
        fontSize: '13px',
        fontFamily: 'monospace',
        margin: 0,
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
    },
    noProblem: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        padding: '40px',
        textAlign: 'center',
    },
    editorSection: {
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        borderRight: '1px solid rgba(255,255,255,0.1)',
    },
    toolbar: {
        height: '48px',
        background: 'rgba(255,255,255,0.03)',
        borderBottom: '1px solid rgba(255,255,255,0.1)',
        display: 'flex',
        alignItems: 'center',
        padding: '0 16px',
        gap: '12px',
    },
    select: {
        background: 'rgba(255,255,255,0.07)',
        border: '1px solid rgba(255,255,255,0.15)',
        borderRadius: '6px',
        padding: '6px 12px',
        color: '#ffffff',
        fontSize: '13px',
        cursor: 'pointer',
    },
    submitBtn: {
        background: 'linear-gradient(135deg, #00ff88, #00cc6a)',
        border: 'none',
        borderRadius: '6px',
        padding: '8px 20px',
        color: '#000',
        fontWeight: 'bold',
        fontSize: '13px',
    },
    outputSection: {
        width: '380px',
        display: 'flex',
        flexDirection: 'column',
        background: '#0a0a1a',
    },
    outputHeader: {
        height: '48px',
        borderBottom: '1px solid rgba(255,255,255,0.1)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0 16px',
        color: '#aaa',
        fontSize: '13px',
        fontWeight: 'bold',
        background: 'rgba(255,255,255,0.03)',
    },
    statusBadge: {
        padding: '3px 10px',
        borderRadius: '20px',
        fontSize: '12px',
        fontWeight: 'bold',
    },
    outputBody: {
        flex: 1,
        padding: '16px',
        overflowY: 'auto',
    },
    placeholder: {
        color: '#444',
        fontSize: '14px',
        textAlign: 'center',
        marginTop: '40px',
    },
    running: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        marginTop: '40px',
        gap: '12px',
        color: '#00aaff',
    },
    spinner: {
        fontSize: '32px',
        animation: 'spin 1s linear infinite',
    },
    outputLabel: {
        color: '#888',
        fontSize: '12px',
        marginBottom: '8px',
        textTransform: 'uppercase',
        letterSpacing: '1px',
    },
    outputText: {
        color: '#00ff88',
        fontSize: '14px',
        fontFamily: 'monospace',
        background: 'rgba(0,255,136,0.05)',
        border: '1px solid rgba(0,255,136,0.1)',
        borderRadius: '8px',
        padding: '12px',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
    },
    execTime: {
        color: '#666',
        fontSize: '12px',
        marginTop: '12px',
    },
        runBtn: {
        background: 'rgba(0,170,255,0.15)',
        border: '1px solid rgba(0,170,255,0.4)',
        borderRadius: '6px',
        padding: '8px 16px',
        color: '#00aaff',
        fontWeight: 'bold',
        fontSize: '13px',
        cursor: 'pointer',
    },
    outputTabs: {
    display: 'flex',
    borderBottom: '1px solid rgba(255,255,255,0.1)',
    },
    outputTab: {
        padding: '10px 16px',
        background: 'transparent',
        border: 'none',
        cursor: 'pointer',
        fontSize: '13px',
        fontWeight: '600',
    },
    customInput: {
        width: '100%',
        background: 'rgba(255,255,255,0.05)',
        border: '1px solid rgba(255,255,255,0.1)',
        borderRadius: '6px',
            padding: '8px',
        color: '#fff',
        fontSize: '13px',
        resize: 'vertical',
        boxSizing: 'border-box',
        fontFamily: 'monospace',
    },
    testCase: {
        border: '1px solid',
        borderRadius: '8px',
        padding: '12px',
        marginBottom: '10px',
        background: 'rgba(255,255,255,0.02)',
    },
    testCaseHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        marginBottom: '8px',
    },
    tcRow: {
        display: 'flex',
        gap: '8px',
        marginTop: '6px',
        alignItems: 'flex-start',
    },
    tcLabel: {
        color: '#888',
        fontSize: '12px',
        minWidth: '70px',
        paddingTop: '2px',
    },
    tcValue: {
        background: 'rgba(255,255,255,0.05)',
        borderRadius: '4px',
        padding: '2px 8px',
        color: '#ccc',
        fontSize: '13px',
        fontFamily: 'monospace',
    },
    hiddenInfo: {
        background: 'rgba(255,200,0,0.05)',
        border: '1px solid rgba(255,200,0,0.2)',
        borderRadius: '8px',
        padding: '10px',
        color: '#ffc800',
        fontSize: '13px',
        textAlign: 'center',
        marginTop: '8px',
    },
    successOverlay: {
    position: 'fixed',
    top: 0, left: 0,
    width: '100%',
    height: '100%',
    background: 'rgba(0,0,0,0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000,
},
successBox: {
    background: '#1a1a2e',
    border: '1px solid rgba(0,255,136,0.4)',
    borderRadius: '16px',
    padding: '40px 60px',
    textAlign: 'center',
    animation: 'fadeIn 0.3s ease',
},
aiBtn: {
    background: 'linear-gradient(135deg, #6c47ff, #4c2fad)',
    border: 'none',
    borderRadius: '8px',
    padding: '10px 20px',
    color: '#fff',
    fontWeight: 'bold',
    cursor: 'pointer',
    fontSize: '13px',
    width: '100%',
},
aiBox: {
    marginTop: '12px',
    background: 'rgba(108,71,255,0.08)',
    border: '1px solid rgba(108,71,255,0.3)',
    borderRadius: '8px',
    padding: '16px',
},
aiTitle: {
    color: '#a78bfa',
    fontWeight: 'bold',
    fontSize: '13px',
    marginBottom: '10px',
},
aiText: {
    color: '#ccc',
    fontSize: '13px',
    lineHeight: 1.7,
    whiteSpace: 'pre-wrap',
    fontFamily: 'inherit',
},
cursor: {
    animation: 'blink 1s infinite',
    color: '#a78bfa',
},
};