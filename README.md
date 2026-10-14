# 🚀 CodeArena

A distributed code execution platform and online judge — built from scratch with Docker sandboxing, Redis job queues, real-time WebSocket communication, Lamport logical clock synchronization, and AI-powered code analysis.

**Live Demo:** http://15.206.170.214  
**Distributed Systems Curriculum Guide:** [CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md](./CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md)

---

## 📖 Overview

CodeArena lets users write, submit, and execute code (Python, C++) against test cases in a secure, isolated environment — similar to the core infrastructure behind platforms like HackerRank, LeetCode, and Judge0. 

Every submission runs inside a sandboxed Docker container with strict resource limits (CPU caps, memory constraints, network isolation), gets processed asynchronously through a distributed job queue (Redis + BullMQ), and streams execution results to the client in real-time via WebSockets. The system also features Lamport Logical Clock synchronization across all distributed nodes for causal event ordering ($a \rightarrow b$) and contextual AI feedback for failed test cases.

---

## ✨ Features

- **Sandboxed Code Execution** — Python & C++ code runs in ephemeral, isolated Docker containers with memory limits (128MB–256MB), CPU limits (0.5 cores), and complete network isolation.
- **Distributed Job Queue** — Redis + BullMQ decouples the API gateway from execution workers, enabling graceful queuing and horizontal scalability.
- **Real-Time Bidirectional Results** — WebSocket + Redis Pub/Sub delivers instant test case pass/fail feedback without client-side polling.
- **🕒 Lamport Logical Clock Synchronization** — Implements Lamport's timestamp algorithm across Client, Server, and Worker nodes, complete with an in-browser live causal timeline visualization and invariant verification.
- **Judge System** — Automated evaluation against visible and hidden test cases with execution time, memory usage, and diff breakdowns.
- **AI Code Analysis** — Groq (Llama-3) powered contextual diagnostics on failed submissions with hints and edge-case suggestions.
- **Production-Grade Auth & Security** — JWT with refresh token rotation, Redis-based token blacklisting on logout, bcrypt hashing (12 rounds), and Role-Based Access Control (RBAC).
- **Comprehensive Admin Panel** — Dedicated dashboard to manage problems, test cases, submissions, and users.
- **Rate Limiting** — Redis-backed sliding window rate limiters protecting authentication and code execution endpoints.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client / Browser                           │
│           React 18 + Monaco Code Editor + Socket.io             │
│            Lamport Clock State: L_client (Rule 1 & 2)           │
└───────────────────────────────┬─────────────────────────────────┘
                                │ HTTP REST / WebSocket
┌───────────────────────────────▼─────────────────────────────────┐
│                    API Gateway (Node.js/Express)                │
│                 Port 3000 | Lamport Clock: L_server             │
│            Auth, Problem Catalog, Submission Producer           │
└───────────────────────────────┬─────────────────────────────────┘
                                │
               ┌────────────────┴────────────────┐
               │                                 │
┌──────────────▼─────────────┐    ┌──────────────▼─────────────┐
│  Redis (Port 6379)         │    │  PostgreSQL (Port 5432)    │
│  - BullMQ 'code-submission'│    │  - Users, Problems,        │
│  - Pub/Sub Channel 'job-*' │    │    Submissions, Test Cases │
│  - Token Blacklist & Limits│    │  - ACID Relational Store   │
└──────────────┬─────────────┘    └────────────────────────────┘
               │
┌──────────────▼─────────────────────────────────────────────────┐
│                   Worker Service (Node.js)                     │
│                  Lamport Clock: L_worker                        │
│            Picks jobs, manages Docker sandboxes                │
└───────────────────────────────┬─────────────────────────────────┘
                                │ Docker Engine API (npipe / sock)
┌───────────────────────────────▼─────────────────────────────────┐
│                     Ephemeral Sandboxes                         │
│            [codearena-python]     [codearena-cpp]               │
│          CPU: 0.5 | RAM: 128MB | Net: None | Auto-destroy       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | React 18, Vite, Monaco Editor, Tailwind CSS / Vanilla CSS, Socket.io Client |
| **Backend / Gateway** | Node.js, Express.js, Socket.io Server, dotenv |
| **Distributed Queue** | Redis 7, BullMQ |
| **Database** | PostgreSQL 15 / 16 (pg Pool with parameterized queries) |
| **Synchronization** | Lamport Logical Clocks (Causal Ordering, Invariant Validation) |
| **Execution Sandbox** | Docker, Docker Engine API (`dockerode`), Alpine / Debian Slim |
| **AI Analysis** | Groq API (Llama 3), Contextual Prompt Engineering |
| **Authentication** | JWT Access & Refresh Tokens, Redis Token Blacklisting, bcryptjs |

---

## 📂 Project Structure

```
CodeArena/
├── CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md  # Comprehensive B.Tech DS curriculum guide
├── README.md                               # Project documentation
├── SETUP_INSTRUCTIONS.md                   # Detailed setup reference
├── backend/
│   ├── docker-compose.yml                  # Container orchestration
│   └── sandboxes/                          # Sandbox Docker definitions
│       ├── python/Dockerfile               # Python execution sandbox
│       └── cpp/Dockerfile                  # C++ compilation & execution sandbox
├── executor/                               # Backend execution service & API
│   ├── server.js                           # Express API Gateway & WebSocket hub
│   ├── worker.js                           # BullMQ background job consumer
│   ├── executor.js                         # Dockerode container lifecycle manager
│   ├── queue.js                            # BullMQ queue & worker configuration
│   ├── db.js                               # PostgreSQL pool connection
│   ├── middleware/                         # JWT verification & RBAC guards
│   ├── routes/                             # Auth, Admin, and Problem routes
│   └── services/                           # AI analysis integration (Groq)
└── frontend/                               # React single-page application
    ├── src/
    │   ├── pages/                          # Editor (with Lamport UI), Admin, Login, etc.
    │   ├── context/                        # Auth context & state management
    │   └── api/                            # Axios interceptor & API client
    └── package.json
```

---

## 🚀 Running Locally

You can run CodeArena in two ways: **Hybrid / Local Mode (Recommended for Development)** or **Full Docker Mode**.

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Node.js](https://nodejs.org/) v18 or v20+
- [PostgreSQL](https://www.postgresql.org/) (running locally on port 5432)

---

### Method A: Local / Hybrid Mode (Recommended)

This mode runs Redis and execution sandboxes inside Docker, while running the Node.js services and Vite frontend natively for instant debugging.

#### 1. Start Docker Sandbox Images & Redis
```bash
# From the project root:
# 1. Build the execution sandbox containers
docker build -t codearena-python ./backend/sandboxes/python
docker build -t codearena-cpp ./backend/sandboxes/cpp

# 2. Start the Redis container
docker run -d --name codearena-redis -p 6379:6379 redis:7-alpine
# (On subsequent restarts, simply run: docker start codearena-redis)
```

#### 2. Configure Environment Variables
Create `executor/.env`:
```env
PORT=3000
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=codearena
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
REDIS_HOST=localhost
REDIS_PORT=6379
JWT_SECRET=your_jwt_secret_key_here
JWT_REFRESH_SECRET=your_jwt_refresh_secret_key_here
GROQ_API_KEY=your_groq_api_key_optional
```

#### 3. Start the API Gateway Server (Terminal 1)
```bash
cd executor
npm install
node server.js
```
*(Verify: `Server running on 3000`, `Redis Subscriber Connected`, `PostgreSQL connected`)*

#### 4. Start the Execution Worker (Terminal 2)
```bash
cd executor
node worker.js
```
*(Verify: `worker started`, `Worker ready — waiting for jobs...`)*

#### 5. Start the React Frontend (Terminal 3)
```bash
cd frontend
npm install
npm run dev
```
Open **`http://localhost:5173/`** in your browser.

---

### Method B: Full Docker Compose Mode

```bash
# Build sandbox images first
docker build -t codearena-python ./backend/sandboxes/python
docker build -t codearena-cpp ./backend/sandboxes/cpp

# Start all backend microservices
cd backend
docker compose up --build

# In a separate terminal, launch the frontend
cd frontend
npm install
npm run dev
```

---

## 🔒 Security Architecture

- **Sandbox Confinement:** Containers run with `--network none` (no outbound/inbound socket access), restricting malicious submissions from scanning local networks or initiating DoS attacks.
- **Resource Constraints:** Hard CPU quota (`CpuQuota: 50000`, `CpuPeriod: 100000`) and memory ceiling (`Memory: 256MB`) prevent fork bombs and memory exhaust attacks.
- **Ephemeral Lifecycles:** Containers are automatically destroyed (`AutoRemove: true` or `.remove({ force: true })`) immediately upon process completion or timeout.
- **Authentication:** Access tokens with 15-minute expirations paired with rotating refresh tokens stored in HTTP-only cookies; logged-out tokens are immediately blacklisted in Redis.
- **SQL Injection Prevention:** 100% parameterized queries via PostgreSQL `pg` pool.

---

## 📚 Academic & Curriculum Alignment

For students and evaluators reviewing this project for **Distributed Systems (B.Tech Computer Engineering)**:
- Consult [CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md](./CODEARENA_DISTRIBUTED_SYSTEMS_GUIDE.md) for in-depth coverage of:
  - **Unit I:** Distributed Models, Middleware, OS-level Virtualization vs Hypervisors.
  - **Unit II:** Inter-Process Communication, RPC vs MOM, WebSockets, At-least-once Delivery.
  - **Unit III:** Lamport’s Logical Clocks, Mathematical proofs, Distributed Mutual Exclusion, Deadlock detection.
  - **Unit IV:** Serverless execution paradigms, CAP Theorem trade-offs (PostgreSQL vs Redis).
  - **Oral / Viva Exam Defense:** Top 15 technical questions and answers with direct code pointers.

---

## 👥 Authors & Acknowledgments

- **Tanishq Tajne** ([GitHub](https://github.com/tanishqtajne25)) — Distributed Systems implementation, Lamport Logical Clock synchronization, pipeline optimization & curriculum mapping.
- **Piyush Waghmare** ([GitHub](https://github.com/piyushwaghmare)) — Original architecture, sandbox design, full-stack implementation, and AWS deployment.
