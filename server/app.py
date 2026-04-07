try:
    from openenv.core.env_server.http_server import create_app
except Exception as e:
    raise ImportError("openenv is required. Run: pip install openenv-core") from e

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import SqlQueryDebuggerAction, SqlQueryDebuggerObservation
from server.sql_query_debugger_environment import SqlQueryDebuggerEnvironment

app = create_app(
    SqlQueryDebuggerEnvironment,
    SqlQueryDebuggerAction,
    SqlQueryDebuggerObservation,
    env_name="sql_query_debugger",
    max_concurrent_envs=1,
)

def main(host: str = "0.0.0.0", port: int = 8000):
    import uvicorn
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    main()