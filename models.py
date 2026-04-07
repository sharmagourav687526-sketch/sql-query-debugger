from openenv.core.env_server.types import Action, Observation
from pydantic import Field
from typing import Optional


class SqlQueryDebuggerAction(Action):
    """What the agent does — submits a fixed SQL query."""

    fixed_query: str = Field(..., description="The corrected SQL query")


class SqlQueryDebuggerObservation(Observation):
    """What the agent sees each step."""

    broken_query: str = Field(
        default="", description="The SQL query containing errors"
    )
    schema: str = Field(
        default="", description="CREATE TABLE statements for the database"
    )
    error_message: str = Field(
        default="", description="Error from running the broken query"
    )
    sample_rows: str = Field(
        default="", description="Sample data from the tables as JSON string"
    )
    expected_output_hint: str = Field(
        default="", description="Natural language hint of what correct output looks like"
    )
    task_id: str = Field(
        default="", description="Which task: syntax_fix, logic_bug, multi_table"
    )
    attempts_remaining: int = Field(
        default=5, description="How many fix attempts left"
    )
    last_result: Optional[str] = Field(
        default=None, description="Result rows from agent's last query attempt"
    )