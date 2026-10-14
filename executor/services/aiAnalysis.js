require('dotenv').config();
const Groq = require('groq-sdk');

function getGroqClient() {
    const key = process.env.GROQ_API_KEY;
    if (!key || key === 'dummy_groq_key') {
        throw new Error('Valid GROQ_API_KEY is not defined in executor/.env. Please add your Groq API key (starts with gsk_).');
    }
    return new Groq({ apiKey: key });
}


async function analyzeCode({
    problem,
    code,
    language,
    testResults,
    passedCount,
    totalCount,
    recentSubmissions
}) {
  
    // Advanced system prompt with role definition and constraints
    const systemPrompt = `You are an expert competitive programming mentor at CodeArena, specializing in debugging and algorithmic thinking.

<core_identity>
- You analyze code failures with precision
- You teach problem-solving patterns, not just fixes
- You adapt explanations to student's level
- You use Socratic method: guide, don't solve
</core_identity>

<analysis_framework>
1. Identify the root cause (logic, edge case, algorithm, syntax)
2. Explain the concept gap
3. Provide a directional hint with example pattern
4. Encourage incremental testing
</analysis_framework>

<constraints>
- Response length: 150-200 words
- Language: Simple, technical but accessible
- Tone: Encouraging yet precise
- Never give complete solutions
- Always reference specific test case data
- Use analogies for complex concepts
</constraints>

<output_structure>
Use markdown with clear sections:
## Root Cause
## Concept
## Hint
## Test Strategy
</output_structure>`;

    // Extract failed test cases with detailed context
    const failedCases = testResults
        ? testResults.filter(tc => !tc.passed && !tc.hidden)
        : [];

    const passedCases = testResults
        ? testResults.filter(tc => tc.passed && !tc.hidden)
        : [];

    // Analyze submission pattern for adaptive hints
    const recentContext = recentSubmissions?.length > 0
        ? `<submission_history>
           Recent attempts: ${recentSubmissions.length}
            ${recentSubmissions.slice(0, 3).map((s, i) =>
         `  ${i + 1}. Status: ${s.status} | Time: ${s.exec_time_ms}ms`
           ).join('\n')}
           Pattern: ${recentSubmissions.length > 2 ? 'Multiple attempts - needs conceptual guidance' : 'Early attempt - needs specific debugging'}
           </submission_history>`
            : '<submission_history>First attempt</submission_history>';

    // Determine error category for targeted analysis
    const errorCategory = categorizeError(failedCases, passedCases, totalCount);
    
    // Build few-shot examples for consistency
    const fewShotExamples = `<examples>
        Example 1 - Edge Case Issue:
        Input: "" (empty string)
        Expected: 0
        Got: Error
        Analysis: "Your code doesn't handle empty input. Consider: what should happen when there's nothing to process?"
        
        Example 2 - Logic Error:
        Input: [1,2,3]
        Expected: 6
        Got: 3
        Analysis: "You're returning early. The loop stops after first iteration. Think: how do you accumulate all values?"
        
        Example 3 - Algorithm Complexity:
        Status: Time Limit Exceeded
        Analysis: "Your O(n²) approach times out on large inputs. Hint: Can you solve this in one pass using a hash map?"
        </examples>`;
        
            // Enhanced user prompt with chain-of-thought structure
    const userPrompt = `<problem_context>
        Title: ${problem.title}
        Difficulty: ${problem.difficulty || 'medium'}
        Description: ${problem.description}
        </problem_context>
        
        <student_solution language="${language}">
        ${code}
        </student_solution>
        
        <execution_results>
        ✅ Passed: ${passedCount}/${totalCount} test cases
        ${passedCases.length > 0 ? `
        Passing test example:
          Input: ${passedCases[0]?.input || 'none'}
          Output: ${passedCases[0]?.expectedOutput}
          ✓ Correct` : ''}
        
        ${failedCases.length > 0
            ? `❌ First failing test:
          Input: ${failedCases[0]?.input || 'none'}
          Expected: ${failedCases[0]?.expectedOutput}
          Got: ${failedCases[0]?.actualOutput}
          ${failedCases[0]?.error ? `Error: ${failedCases[0].error}` : ''}`
            : '❌ All visible tests passed, but hidden tests failed (likely edge cases)'}
        </execution_results>
        
        ${recentContext}

        <error_classification>
        Category: ${errorCategory}
        </error_classification>
        
        ${fewShotExamples}
        
        <analysis_task>
        Using chain-of-thought reasoning:
        
        1. **Compare**: What's different between passing and failing inputs?
        2. **Identify**: What assumption in the code breaks for the failing case?
        3. **Explain**: What concept/pattern is missing?
        4. **Guide**: What's the minimal hint to lead them to the solution?
        
        Think step-by-step, then provide your analysis following the output structure.
        </analysis_task>

        <response_guidelines>
        - Reference the EXACT failing input/output
        - Use analogies if explaining complex concepts
        - Suggest a small test case they can trace manually
        - End with an encouraging note
        </response_guidelines>`;

    // Optimized model parameters for code analysis
    const groq = getGroqClient();
    const stream = await groq.chat.completions.create({
        model: "llama-3.1-8b-instant",
        messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user',   content: userPrompt   }
        ],
        temperature: 0.2,        // Lower for more deterministic, focused analysis
        max_tokens: 350,         // Slightly higher for detailed explanations
        top_p: 0.9,              // Nucleus sampling for quality
        frequency_penalty: 0.3,  // Reduce repetition
        presence_penalty: 0.1,   // Encourage diverse vocabulary
        stream: true
    });

    return stream;
}

// Helper function to categorize errors for targeted analysis
function categorizeError(failedCases, passedCases, totalCount) {
    if (failedCases.length === 0) {
        return 'HIDDEN_EDGE_CASES';
    }
    
    const firstFail = failedCases[0];
    
    // Check for common error patterns
    if (firstFail.error) {
        if (firstFail.error.includes('Time limit')) return 'TIME_COMPLEXITY';
        if (firstFail.error.includes('Memory')) return 'SPACE_COMPLEXITY';
        if (firstFail.error.includes('Runtime') || firstFail.error.includes('Error')) return 'RUNTIME_ERROR';
    }
    
    // Check for empty/null inputs
    if (!firstFail.input || firstFail.input.trim() === '') {
        return 'EMPTY_INPUT_HANDLING';
    }
    
    // Check if some tests pass (logic error vs complete failure)
    if (passedCases.length > 0) {
        return 'PARTIAL_LOGIC_ERROR';
    }
    
    // Check output pattern
    if (firstFail.actualOutput === '') {
        return 'NO_OUTPUT';
    }
    
    if (firstFail.actualOutput && firstFail.expectedOutput) {
        const actualLen = firstFail.actualOutput.length;
        const expectedLen = firstFail.expectedOutput.length;
        
        if (Math.abs(actualLen - expectedLen) > 10) {
            return 'OUTPUT_FORMAT_ERROR';
        }
    }
    
    return 'ALGORITHM_LOGIC_ERROR';
}

module.exports = { analyzeCode };