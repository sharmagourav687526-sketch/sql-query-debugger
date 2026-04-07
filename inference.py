import asyncio
import os
import textwrap
from typing import List, Optional
from openai import OpenAI

from models import SqlQueryDebuggerAction, SqlQueryDebuggerObservation
from server.sql_query_debugger_environment import SqlQueryDebuggerEnvironment, SCENARIOS

# ── env vars ────────────────────────────────────────────────────────
API_KEY      = os.getenv("HF_TOKEN") or os.getenv("API_KEY", "dummy")
API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME   = os.getenv("MODEL_NAME",   "Qwen/Qwen2.5-72B-Instruct")
BENCHMARK    = os.getenv("BENCHMARK",    "sql_query_debugger")
MAX_STEPS    = 5
SUCCESS_THRESHOLD = 0.5

# ── logging helpers ─────────────────────────────────────────────────
def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)

def log_step(step: int, action: str, reward: float,
             done: bool, error: Optional[str]) -> None:
    err      = error if error else "null"
    done_val = str(done).lower()
    action_clean = action.replace("\n", " ").strip()[:120]
    print(
        f"[STEP]  step={step} action={action_clean} "
        f"reward={reward:.2f} done={done_val} error={err}",
        flush=True,
    )

def log_end(success: bool, steps: int,
            score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END]   success={str(success).lower()} steps={steps} "
        f"score={score:.3f} rewards={rewards_str}",
        flush=True,
    )

# ── agent prompt ─────────────────────────────────────────────────────
SYSTEM_PROMPT = textwrap.dedent("""
    You are an expert SQL debugger.
    You will be given a broken SQL query, the database schema, an error message,
    sample data rows, and a hint about what the correct output should be.
    Your job is to return ONLY the fixed SQL query — nothing else.
    No explanation, no markdown, no code blocks. Just the raw SQL query ending with a semicolon.
""").strip()

def build_user_prompt(obs: SqlQueryDebuggerObservation) -> str:
    return textwrap.dedent(f"""
        Task difficulty: {obs.task_id}
        
        Database schema:
        {obs.schema}
        
        Sample rows:
        {obs.sample_rows}
        
        Broken query:
        {obs.broken_query}
        
        Error message:
        {obs.error_message if obs.error_message else "No error — but output is wrong"}
        
        Expected output hint:
        {obs.expected_output_hint}
        
        Last attempt result:
        {obs.last_result if obs.last_result else "No attempt yet"}
        
        Attempts remaining: {obs.attempts_remaining}
        
        Return ONLY the fixed SQL query:
    """).strip()

def get_fixed_query(client: OpenAI, obs: SqlQueryDebuggerObservation) -> str:
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": build_user_prompt(obs)},
            ],
            temperature=0.2,
            max_tokens=256,
            stream=False,
        )
        query = (response.choices[0].message.content or "").strip()
        # strip markdown if model wraps in code block
        if query.startswith("```"):
            lines = query.split("\n")
            query = "\n".join(
                l for l in lines
                if not l.startswith("```")
            ).strip()
        return query if query else "SELECT 1;"
    except Exception as exc:
        print(f"[DEBUG] Model request failed: {exc}", flush=True)
        return "SELECT 1;"

# ── one episode ──────────────────────────────────────────────────────
async def run_episode(task_id: str) -> float:
    env     = SqlQueryDebuggerEnvironment()
    rewards: List[float] = []
    steps_taken = 0
    score   = 0.0
    success = False

    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        obs = env.reset(task_id=task_id)

        for step in range(1, MAX_STEPS + 1):
            if obs.done:
                break

            fixed_query = get_fixed_query(client, obs)
            action      = SqlQueryDebuggerAction(fixed_query=fixed_query)
            obs         = env.step(action)

            rewards.append(obs.reward or 0.0)
            steps_taken = step

            log_step(
                step   = step,
                action = fixed_query,
                reward = obs.reward or 0.0,
                done   = obs.done,
                error  = obs.error_message if obs.error_message else None,
            )

            if obs.done:
                break

        score   = min(max(sum(rewards) / MAX_STEPS, 0.0), 1.0)
        success = score >= SUCCESS_THRESHOLD

    finally:
        log_end(
            success = success,
            steps   = steps_taken,
            score   = score,
            rewards = rewards,
        )

    return score

# ── main: run all 3 tasks ────────────────────────────────────────────
async def main() -> None:
    task_ids   = ["syntax_fix", "logic_bug", "multi_table"]
    all_scores = []

    for task_id in task_ids:
        score = await run_episode(task_id)
        all_scores.append(score)
        print(f"[DEBUG] {task_id} score: {score:.3f}", flush=True)

    avg = sum(all_scores) / len(all_scores)
    print(f"[DEBUG] Average score across all tasks: {avg:.3f}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())S