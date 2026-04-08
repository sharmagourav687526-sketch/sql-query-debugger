---
title: SQL Query Debugger
emoji: 🗄️
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
---

# SQL Query Debugger

An OpenEnv environment for training agents to debug SQL queries.

## Tasks
- **syntax_fix** (Easy): Fix typos in SQL keywords
- **logic_bug** (Medium): Fix wrong logic in queries
- **multi_table** (Hard): Fix wrong JOINs and subqueries

## API
- `POST /reset` — Start new episode
- `POST /step` — Submit fixed query
- `GET /state` — Get current state
