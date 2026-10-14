const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// 1. Load marked from frontend node_modules
const { marked } = require('c:/Users/HP/Desktop/DS/CodeArena/frontend/node_modules/marked');

const markdownPath = path.resolve('CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md');
let markdownContent = fs.readFileSync(markdownPath, 'utf8');

// 2. Preprocess custom blocks before marked
// Replace GitHub-style alerts: > [!NOTE], > [!TIP], > [!IMPORTANT], > [!WARNING], > [!CAUTION]
markdownContent = markdownContent.replace(
  /> \[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*\n((?:> .*\n?)*)/gi,
  (match, alertType, body) => {
    const cleanBody = body.replace(/^> ?/gm, '');
    const typeUpper = alertType.toUpperCase();
    return `<div class="callout callout-${typeUpper.toLowerCase()}"><div class="callout-header"><span class="callout-badge">${typeUpper}</span></div><div class="callout-content">\n\n${cleanBody}\n\n</div></div>\n`;
  }
);

// Preprocess mermaid blocks to <pre class="mermaid"> so marked doesn't mangle them
markdownContent = markdownContent.replace(/```mermaid\s*([\s\S]*?)```/g, (match, code) => {
  return `<div class="mermaid-container"><pre class="mermaid">\n${code.trim()}\n</pre></div>`;
});

// Configure marked
marked.setOptions({
  gfm: true,
  breaks: false
});

const parsedHtml = marked.parse(markdownContent);

// 3. Assemble full HTML document with styling
const fullHtml = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>CodeArena Distributed Systems Guide</title>
  <!-- KaTeX for math rendering -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/katex.min.css">
  <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/katex.min.js"></script>
  <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/contrib/auto-render.min.js"
    onload="renderMathInElement(document.body, {delimiters: [{left: '$$', right: '$$', display: true}, {left: '$', right: '$', display: false}]});"></script>

  <!-- Highlight.js for code syntax highlighting -->
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
  <script>document.addEventListener('DOMContentLoaded', () => hljs.highlightAll());</script>

  <!-- Mermaid.js for architecture diagrams -->
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script>
    mermaid.initialize({
      startOnLoad: true,
      theme: 'default',
      flowchart: { useMaxWidth: true, htmlLabels: true, curve: 'basis' }
    });
  </script>

  <style>
    @page {
      size: A4;
      margin: 20mm 15mm 20mm 15mm;
      @bottom-right {
        content: counter(page);
      }
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      font-size: 11pt;
      line-height: 1.6;
      color: #24292f;
      background-color: #ffffff;
      padding: 0;
      margin: 0;
    }

    h1, h2, h3, h4, h5, h6 {
      color: #0969da;
      font-weight: 600;
      line-height: 1.25;
      margin-top: 1.5em;
      margin-bottom: 0.5em;
      page-break-after: avoid;
      break-after: avoid;
    }

    h1 {
      font-size: 22pt;
      border-bottom: 2px solid #0969da;
      padding-bottom: 0.3em;
      margin-top: 0;
      color: #1f2328;
    }

    h2 {
      font-size: 15pt;
      border-bottom: 1px solid #d0d7de;
      padding-bottom: 0.25em;
      margin-top: 1.8em;
      color: #0969da;
    }

    h3 {
      font-size: 12.5pt;
      color: #1f2328;
    }

    h4 {
      font-size: 11.5pt;
      color: #57606a;
    }

    p, ul, ol {
      margin-top: 0.4em;
      margin-bottom: 0.8em;
    }

    li {
      margin-top: 0.25em;
      margin-bottom: 0.25em;
    }

    hr {
      height: 1px;
      padding: 0;
      margin: 2em 0;
      background-color: #d0d7de;
      border: 0;
    }

    code {
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
      font-size: 85%;
      background-color: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 4px;
      padding: 0.15em 0.35em;
      color: #24292e;
    }

    pre {
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
      font-size: 9pt;
      line-height: 1.45;
      background-color: #f6f8fa;
      border: 1px solid #d0d7de;
      border-radius: 6px;
      padding: 12px;
      overflow-x: auto;
      page-break-inside: avoid;
      break-inside: avoid;
      margin-top: 0.6em;
      margin-bottom: 1em;
    }

    pre code {
      font-size: inherit;
      background-color: transparent;
      border: none;
      padding: 0;
    }

    table {
      border-collapse: collapse;
      width: 100%;
      margin: 1.2em 0;
      font-size: 9.5pt;
      page-break-inside: avoid;
      break-inside: avoid;
    }

    table th, table td {
      border: 1px solid #d0d7de;
      padding: 7px 11px;
      text-align: left;
    }

    table th {
      background-color: #f6f8fa;
      font-weight: 600;
      color: #1f2328;
    }

    table tr:nth-child(2n) {
      background-color: #fcfdfe;
    }

    blockquote {
      margin: 0.8em 0;
      padding: 0 1em;
      color: #57606a;
      border-left: 0.25em solid #d0d7de;
    }

    .callout {
      border-left: 4px solid #0969da;
      background-color: #f6f8fa;
      border-radius: 0 6px 6px 0;
      padding: 10px 14px;
      margin: 1.2em 0;
      page-break-inside: avoid;
      break-inside: avoid;
    }

    .callout-header {
      margin-bottom: 6px;
    }

    .callout-badge {
      display: inline-block;
      font-size: 8pt;
      font-weight: bold;
      text-transform: uppercase;
      padding: 2px 6px;
      border-radius: 3px;
      background-color: #0969da;
      color: #ffffff;
      letter-spacing: 0.5px;
    }

    .callout-note { border-left-color: #0969da; background-color: #f0f7ff; }
    .callout-note .callout-badge { background-color: #0969da; }

    .callout-tip { border-left-color: #1a7f37; background-color: #f0fff4; }
    .callout-tip .callout-badge { background-color: #1a7f37; }

    .callout-important { border-left-color: #8250df; background-color: #fbf5ff; }
    .callout-important .callout-badge { background-color: #8250df; }

    .callout-warning { border-left-color: #9a6700; background-color: #fffbeb; }
    .callout-warning .callout-badge { background-color: #9a6700; }

    .callout-caution { border-left-color: #cf222e; background-color: #ffebe9; }
    .callout-caution .callout-badge { background-color: #cf222e; }

    .mermaid-container {
      text-align: center;
      margin: 1.5em 0;
      background: #ffffff;
      border: 1px solid #d0d7de;
      border-radius: 6px;
      padding: 16px;
      page-break-inside: avoid;
      break-inside: avoid;
    }

    .mermaid {
      background: transparent !important;
    }

    a {
      color: #0969da;
      text-decoration: none;
    }

    .cover-card {
      background: linear-gradient(135deg, #0969da 0%, #1f2328 100%);
      color: #ffffff;
      padding: 28px 24px;
      border-radius: 8px;
      margin-bottom: 2em;
    }

    .cover-card h1 {
      color: #ffffff;
      border-bottom: none;
      margin: 0 0 10px 0;
      font-size: 24pt;
    }

    .cover-card p {
      margin: 4px 0;
      color: #e1e4e8;
      font-size: 11pt;
    }

    .cover-badge {
      display: inline-block;
      background: rgba(255, 255, 255, 0.2);
      border: 1px solid rgba(255, 255, 255, 0.4);
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 9pt;
      margin-bottom: 12px;
      font-weight: 500;
    }
  </style>
</head>
<body>
  <div class="cover-card">
    <div class="cover-badge">B.Tech Computer Engineering • Distributed Systems Coursework</div>
    <h1>📚 CodeArena Distributed Systems Guide</h1>
    <p><strong>Comprehensive Curriculum Mapping, Architectural Blueprint & Viva Defense</strong></p>
    <p>Units I – IV • First Principles to Production Code • Lamport Clocks • Sandboxing • Distributed Queues</p>
  </div>

  ${parsedHtml}
</body>
</html>`;

const tempHtmlPath = path.resolve('temp_guide.html');
const outputPdfPath = path.resolve('CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.pdf');

fs.writeFileSync(tempHtmlPath, fullHtml, 'utf8');
console.log('HTML generated at:', tempHtmlPath);

// Execute Edge headless print to PDF
const edgePath = 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';
const cmd = `"${edgePath}" --headless=new --disable-gpu --virtual-time-budget=8000 --print-to-pdf="${outputPdfPath}" "${tempHtmlPath}"`;

console.log('Generating PDF via headless Edge...');
try {
  execSync(cmd, { stdio: 'inherit' });
  if (fs.existsSync(outputPdfPath)) {
    const stats = fs.statSync(outputPdfPath);
    console.log(`SUCCESS: PDF created at ${outputPdfPath}`);
    console.log(`File size: ${(stats.size / 1024).toFixed(2)} KB`);
  } else {
    console.error('PDF file was not found after execution.');
  }
} catch (err) {
  console.error('Failed to generate PDF:', err.message);
} finally {
  // Clean up temporary HTML
  if (fs.existsSync(tempHtmlPath)) {
    fs.unlinkSync(tempHtmlPath);
  }
}
