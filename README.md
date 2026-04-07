# SQL Query Debugger — OpenEnv Environment

An RL environment where an AI agent debugs broken SQL queries.
Given a faulty query, database schema, error message, and sample rows,
the agent must produce a corrected query that executes successfully
and returns the expected result set.

## Environment Description

Real-world motivation: Every data engineer, analyst, and backend developer
debugs SQL queries daily. This environment trains agents to identify and fix
common SQL mistakes — from simple typos to complex multi-table logic errors.

## Action Space

| Field | Type | Description |
|---|---|---|
| `fixed_query` | string | The corrected SQL query to execute |

## Observation Space

| Field | Type | Description |
|---|---|---|
| `broken_query` | string | The SQL query containing errors |
| `schema` | string | CREATE TABLE statements |
| `error_message` | string | Error from executing the broken query |
| `sample_rows` | string | Sample data as JSON string |
| `expected_output_hint` | string | Natural language description of correct output |
| `task_id` | string | Difficulty level of current task |
| `attempts_remaining` | integer | Fix attempts left in episode |
| `last_result` | string | Result rows from last query attempt |

## Tasks

### Task 1 — Syntax Fix (Easy)
Fix SQL syntax errors: misspelled keywords (SELCT, WERE, GRUP),
missing commas, wrong keyword order.
- Reward: F1 score between returned rows and expected rows
- Expected agent score: 0.7 — 1.0

### Task 2 — Logic Bug Fix (Medium)
Fix SQL logic errors: wrong GROUP BY column, incorrect WHERE condition,
wrong ORDER BY direction, misused LIMIT.
- Reward: F1 score between returned rows and expected rows
- Expected agent score: 0.4 — 0.8

### Task 3 — Multi-Table Optimization (Hard)
Fix complex multi-table queries: wrong JOIN conditions, missing GROUP BY,
incorrect self-joins, subquery errors, cartesian products.
- Reward: F1 score between returned rows and expected rows
- Expected agent score: 0.2 — 0.6

## Reward Function

Each step returns a reward between 0.0 and 1.0:
- Base reward = F1 score between agent query output and expected result set
- Early solve bonus = up to 0.1 extra for solving in fewer steps
- Score of 0.0 = query crashes or returns completely wrong rows
- Score of 1.0 = query returns exactly the expected result set

## Setup Instructions

### Local setup
```bash
git clone https://github.com/sharmagourav687526-sketch/sql-query-debugger.git
cd sql-query-debugger
pip install openenv-core fastapi uvicorn pydantic
```

### Run the server locally
```bash
cd sql-query-debugger
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

### Run the baseline inference script
```bash
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
export HF_TOKEN=your_token_here
python inference.py
```

### Docker
```bash
docker build -t sql-query-debugger -f server/Dockerfile .
docker run -p 8000:8000 sql-query-debugger
```

### Validate
```bash
openenv validate
```

## Baseline Scores

| Task | Difficulty | Baseline Score |
|---|---|---|
| syntax_fix | Easy | 0.72 |
| logic_bug | Medium | 0.51 |
| multi_table | Hard | 0.34 |
| **Average** | | **0.52** |

## Environment Details

- 20 pre-built scenarios across 3 difficulty levels
- Grader: SQLite execution + F1 score vs expected result set
- Max steps per episode: 5
- Scores always in range 0.0 — 1.0
- Fully deterministic graders — no randomness in scoring