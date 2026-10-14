require('dotenv').config();
const { analyzeCode } = require('./services/aiAnalysis');

async function testGroq() {
    console.log('Testing Groq API integration...');
    const rawKey = process.env.GROQ_API_KEY;
    console.log('Current GROQ_API_KEY from .env:', rawKey ? `${rawKey.substring(0, 6)}... (length: ${rawKey.length})` : 'NOT FOUND');

    try {
        console.log('Sending sample failing code to Groq (model: llama-3.1-8b-instant)...');
        const stream = await analyzeCode({
            problem: {
                id: 1,
                title: 'Reverse String',
                description: 'Given a string s, return the reversed string.',
                difficulty: 'Easy'
            },
            code: 's = input()\nprint("wrong")',
            language: 'python',
            testResults: [{
                input: 'hello',
                expectedOutput: 'olleh',
                actualOutput: 'wrong',
                passed: false,
                hidden: false
            }],
            passedCount: 0,
            totalCount: 1,
            recentSubmissions: []
        });

        console.log('\n--- GROQ STREAMING RESPONSE START ---');
        let fullResponse = '';
        for await (const chunk of stream) {
            const text = chunk.choices[0]?.delta?.content || '';
            process.stdout.write(text);
            fullResponse += text;
        }
        console.log('\n--- GROQ STREAMING RESPONSE END ---\n');
        console.log('SUCCESS: Groq API responded with', fullResponse.length, 'characters!');
    } catch (err) {
        console.error('\nFAILURE during Groq API call:');
        console.error('Error Message:', err.message);
        if (err.status) console.error('HTTP Status:', err.status);
    }
}

testGroq();
