import sqlite3
import json
import random
from uuid import uuid4
from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

try:
    from ..models import SqlQueryDebuggerAction, SqlQueryDebuggerObservation
except ImportError:
    from models import SqlQueryDebuggerAction, SqlQueryDebuggerObservation


SCENARIOS = [
    # ── EASY: syntax fixes ──────────────────────────────────────────
    {
        "id": "easy_1",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE employees (id INTEGER, name TEXT, salary REAL, dept TEXT);",
        "setup": [
            "INSERT INTO employees VALUES (1,'Alice',75000,'Engineering');",
            "INSERT INTO employees VALUES (2,'Bob',50000,'Marketing');",
            "INSERT INTO employees VALUES (3,'Carol',90000,'Engineering');",
        ],
        "broken_query": "SELCT name, salary FROM employees WHERE salary > 60000;",
        "fixed_query":  "SELECT name, salary FROM employees WHERE salary > 60000;",
        "error_message": "ParseError: near 'SELCT': syntax error",
        "expected_output_hint": "Should return Alice and Carol who earn more than 60000",
        "expected_rows": [("Alice", 75000.0), ("Carol", 90000.0)],
    },
    {
        "id": "easy_2",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE products (id INTEGER, name TEXT, price REAL, stock INTEGER);",
        "setup": [
            "INSERT INTO products VALUES (1,'Laptop',999.99,10);",
            "INSERT INTO products VALUES (2,'Mouse',29.99,50);",
            "INSERT INTO products VALUES (3,'Keyboard',79.99,30);",
        ],
        "broken_query": "SELECT name price FROM products WHERE stock > 20;",
        "fixed_query":  "SELECT name, price FROM products WHERE stock > 20;",
        "error_message": "",
        "expected_output_hint": "Should return Mouse and Keyboard with their prices",
        "expected_rows": [("Mouse", 29.99), ("Keyboard", 79.99)],
    },
    {
        "id": "easy_3",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE students (id INTEGER, name TEXT, grade INTEGER, subject TEXT);",
        "setup": [
            "INSERT INTO students VALUES (1,'Dan',85,'Math');",
            "INSERT INTO students VALUES (2,'Eve',92,'Science');",
            "INSERT INTO students VALUES (3,'Frank',78,'Math');",
        ],
        "broken_query": "SELECT name, grade FROM students WERE subject = 'Math';",
        "fixed_query":  "SELECT name, grade FROM students WHERE subject = 'Math';",
        "error_message": "ParseError: near 'WERE': syntax error",
        "expected_output_hint": "Should return Dan and Frank with their grades",
        "expected_rows": [("Dan", 85), ("Frank", 78)],
    },
    {
        "id": "easy_4",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE orders (id INTEGER, customer TEXT, amount REAL, status TEXT);",
        "setup": [
            "INSERT INTO orders VALUES (1,'Alice',250.0,'shipped');",
            "INSERT INTO orders VALUES (2,'Bob',89.0,'pending');",
            "INSERT INTO orders VALUES (3,'Carol',420.0,'shipped');",
        ],
        "broken_query": "SELECT customer, amount FROM orders WHERE status = 'shipped'",
        "fixed_query":  "SELECT customer, amount FROM orders WHERE status = 'shipped';",
        "error_message": "",
        "expected_output_hint": "Should return Alice and Carol with shipped order amounts",
        "expected_rows": [("Alice", 250.0), ("Carol", 420.0)],
    },
    {
        "id": "easy_5",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE inventory (id INTEGER, item TEXT, qty INTEGER, warehouse TEXT);",
        "setup": [
            "INSERT INTO inventory VALUES (1,'Bolts',500,'A');",
            "INSERT INTO inventory VALUES (2,'Nuts',300,'B');",
            "INSERT INTO inventory VALUES (3,'Screws',750,'A');",
        ],
        "broken_query": "SELECT item, qty FROM inventory WHERE warehouse = 'A' ORDR BY qty;",
        "fixed_query":  "SELECT item, qty FROM inventory WHERE warehouse = 'A' ORDER BY qty;",
        "error_message": "ParseError: near 'ORDR': syntax error",
        "expected_output_hint": "Should return Bolts and Screws ordered by quantity ascending",
        "expected_rows": [("Bolts", 500), ("Screws", 750)],
    },
    {
        "id": "easy_6",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE users (id INTEGER, username TEXT, age INTEGER, city TEXT);",
        "setup": [
            "INSERT INTO users VALUES (1,'alice',28,'Delhi');",
            "INSERT INTO users VALUES (2,'bob',35,'Mumbai');",
            "INSERT INTO users VALUES (3,'carol',22,'Delhi');",
        ],
        "broken_query": "SELECT username, age FORM users WHERE city = 'Delhi';",
        "fixed_query":  "SELECT username, age FROM users WHERE city = 'Delhi';",
        "error_message": "ParseError: near 'FORM': syntax error",
        "expected_output_hint": "Should return alice and carol from Delhi",
        "expected_rows": [("alice", 28), ("carol", 22)],
    },
    {
        "id": "easy_7",
        "task_id": "syntax_fix",
        "db_schema": "CREATE TABLE sales (id INTEGER, rep TEXT, amount REAL, region TEXT);",
        "setup": [
            "INSERT INTO sales VALUES (1,'Tom',15000,'North');",
            "INSERT INTO sales VALUES (2,'Sue',22000,'South');",
            "INSERT INTO sales VALUES (3,'Ray',18000,'North');",
        ],
        "broken_query": "SELECT rep, SUM(amount) FROM sales GRUP BY region;",
        "fixed_query":  "SELECT rep, SUM(amount) FROM sales GROUP BY region;",
        "error_message": "ParseError: near 'GRUP': syntax error",
        "expected_output_hint": "Should return total sales per region",
        "expected_rows": [("Tom", 33000.0), ("Sue", 22000.0)],
    },

    # ── MEDIUM: logic bugs ───────────────────────────────────────────
    {
        "id": "medium_1",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE employees (id INTEGER, name TEXT, salary REAL, dept TEXT);",
        "setup": [
            "INSERT INTO employees VALUES (1,'Alice',75000,'Engineering');",
            "INSERT INTO employees VALUES (2,'Bob',50000,'Marketing');",
            "INSERT INTO employees VALUES (3,'Carol',90000,'Engineering');",
            "INSERT INTO employees VALUES (4,'Dave',45000,'Marketing');",
        ],
        "broken_query": "SELECT dept, AVG(salary) FROM employees GROUP BY name;",
        "fixed_query":  "SELECT dept, AVG(salary) FROM employees GROUP BY dept;",
        "error_message": "",
        "expected_output_hint": "Should return average salary per department, not per person",
        "expected_rows": [("Engineering", 82500.0), ("Marketing", 47500.0)],
    },
    {
        "id": "medium_2",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE orders (id INTEGER, customer TEXT, amount REAL, status TEXT);",
        "setup": [
            "INSERT INTO orders VALUES (1,'Alice',250.0,'shipped');",
            "INSERT INTO orders VALUES (2,'Bob',89.0,'pending');",
            "INSERT INTO orders VALUES (3,'Carol',420.0,'shipped');",
            "INSERT INTO orders VALUES (4,'Dave',150.0,'cancelled');",
        ],
        "broken_query": "SELECT customer, amount FROM orders WHERE status != 'shipped';",
        "fixed_query":  "SELECT customer, amount FROM orders WHERE status = 'shipped';",
        "error_message": "",
        "expected_output_hint": "Should return only shipped orders — Alice and Carol",
        "expected_rows": [("Alice", 250.0), ("Carol", 420.0)],
    },
    {
        "id": "medium_3",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE products (id INTEGER, name TEXT, price REAL, category TEXT);",
        "setup": [
            "INSERT INTO products VALUES (1,'Laptop',999.99,'Electronics');",
            "INSERT INTO products VALUES (2,'Shirt',29.99,'Clothing');",
            "INSERT INTO products VALUES (3,'Phone',699.99,'Electronics');",
            "INSERT INTO products VALUES (4,'Jeans',59.99,'Clothing');",
        ],
        "broken_query": "SELECT name, price FROM products WHERE price > 100 AND category = 'Clothing';",
        "fixed_query":  "SELECT name, price FROM products WHERE price > 100 AND category = 'Electronics';",
        "error_message": "",
        "expected_output_hint": "Should return expensive electronics — Laptop and Phone",
        "expected_rows": [("Laptop", 999.99), ("Phone", 699.99)],
    },
    {
        "id": "medium_4",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE students (id INTEGER, name TEXT, score INTEGER, passed INTEGER);",
        "setup": [
            "INSERT INTO students VALUES (1,'Alice',85,1);",
            "INSERT INTO students VALUES (2,'Bob',45,0);",
            "INSERT INTO students VALUES (3,'Carol',72,1);",
            "INSERT INTO students VALUES (4,'Dave',38,0);",
        ],
        "broken_query": "SELECT COUNT(*) FROM students WHERE passed = 1 LIMIT 1;",
        "fixed_query":  "SELECT COUNT(*) FROM students WHERE passed = 1;",
        "error_message": "",
        "expected_output_hint": "Should return total count of passed students which is 2",
        "expected_rows": [(2,)],
    },
    {
        "id": "medium_5",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE employees (id INTEGER, name TEXT, salary REAL, dept TEXT);",
        "setup": [
            "INSERT INTO employees VALUES (1,'Alice',75000,'Engineering');",
            "INSERT INTO employees VALUES (2,'Bob',50000,'Marketing');",
            "INSERT INTO employees VALUES (3,'Carol',90000,'Engineering');",
        ],
        "broken_query": "SELECT name, salary FROM employees ORDER BY salary ASC LIMIT 1;",
        "fixed_query":  "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1;",
        "error_message": "",
        "expected_output_hint": "Should return the highest paid employee — Carol with 90000",
        "expected_rows": [("Carol", 90000.0)],
    },
    {
        "id": "medium_6",
        "task_id": "logic_bug",
        "db_schema": "CREATE TABLE sales (id INTEGER, rep TEXT, amount REAL, month INTEGER);",
        "setup": [
            "INSERT INTO sales VALUES (1,'Tom',15000,1);",
            "INSERT INTO sales VALUES (2,'Sue',22000,1);",
            "INSERT INTO sales VALUES (3,'Tom',18000,2);",
            "INSERT INTO sales VALUES (4,'Sue',25000,2);",
        ],
        "broken_query": "SELECT rep, amount FROM sales WHERE month = 1;",
        "fixed_query":  "SELECT rep, SUM(amount) FROM sales GROUP BY rep;",
        "error_message": "",
        "expected_output_hint": "Should return total sales per rep across all months",
        "expected_rows": [("Tom", 33000.0), ("Sue", 47000.0)],
    },

    # ── HARD: multi-table optimization ──────────────────────────────
    {
        "id": "hard_1",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary REAL);"
            "CREATE TABLE departments (id INTEGER, dept_name TEXT, budget REAL);"
        ),
        "setup": [
            "INSERT INTO departments VALUES (1,'Engineering',500000);",
            "INSERT INTO departments VALUES (2,'Marketing',200000);",
            "INSERT INTO employees VALUES (1,'Alice',1,75000);",
            "INSERT INTO employees VALUES (2,'Bob',2,50000);",
            "INSERT INTO employees VALUES (3,'Carol',1,90000);",
        ],
        "broken_query": "SELECT e.name, d.dept_name FROM employees e, departments d WHERE e.salary > 60000;",
        "fixed_query":  "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.salary > 60000;",
        "error_message": "",
        "expected_output_hint": "Should return Alice and Carol with their department names using proper JOIN",
        "expected_rows": [("Alice", "Engineering"), ("Carol", "Engineering")],
    },
    {
        "id": "hard_2",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount REAL);"
            "CREATE TABLE customers (id INTEGER, name TEXT, city TEXT);"
        ),
        "setup": [
            "INSERT INTO customers VALUES (1,'Alice','Delhi');",
            "INSERT INTO customers VALUES (2,'Bob','Mumbai');",
            "INSERT INTO orders VALUES (1,1,250.0);",
            "INSERT INTO orders VALUES (2,1,180.0);",
            "INSERT INTO orders VALUES (3,2,420.0);",
        ],
        "broken_query": "SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON c.id = o.id;",
        "fixed_query":  "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name;",
        "error_message": "",
        "expected_output_hint": "Should return total order amount per customer — Alice 430, Bob 420",
        "expected_rows": [("Alice", 430.0), ("Bob", 420.0)],
    },
    {
        "id": "hard_3",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary REAL);"
            "CREATE TABLE departments (id INTEGER, dept_name TEXT, budget REAL);"
        ),
        "setup": [
            "INSERT INTO departments VALUES (1,'Engineering',500000);",
            "INSERT INTO departments VALUES (2,'Marketing',200000);",
            "INSERT INTO employees VALUES (1,'Alice',1,75000);",
            "INSERT INTO employees VALUES (2,'Bob',2,50000);",
            "INSERT INTO employees VALUES (3,'Carol',1,90000);",
            "INSERT INTO employees VALUES (4,'Dave',2,45000);",
        ],
        "broken_query": "SELECT dept_name, COUNT(*) FROM departments GROUP BY dept_name;",
        "fixed_query":  "SELECT d.dept_name, COUNT(e.id) FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.dept_name;",
        "error_message": "",
        "expected_output_hint": "Should return headcount per department — Engineering 2, Marketing 2",
        "expected_rows": [("Engineering", 2), ("Marketing", 2)],
    },
    {
        "id": "hard_4",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE products (id INTEGER, name TEXT, category_id INTEGER, price REAL);"
            "CREATE TABLE categories (id INTEGER, cat_name TEXT);"
            "CREATE TABLE order_items (id INTEGER, product_id INTEGER, qty INTEGER);"
        ),
        "setup": [
            "INSERT INTO categories VALUES (1,'Electronics');",
            "INSERT INTO categories VALUES (2,'Clothing');",
            "INSERT INTO products VALUES (1,'Laptop',1,999.99);",
            "INSERT INTO products VALUES (2,'Shirt',2,29.99);",
            "INSERT INTO products VALUES (3,'Phone',1,699.99);",
            "INSERT INTO order_items VALUES (1,1,2);",
            "INSERT INTO order_items VALUES (2,3,5);",
            "INSERT INTO order_items VALUES (3,2,10);",
        ],
        "broken_query": "SELECT p.name, oi.qty FROM products p JOIN order_items oi ON p.id = oi.id;",
        "fixed_query":  "SELECT p.name, SUM(oi.qty) as total_qty FROM products p JOIN order_items oi ON p.id = oi.product_id GROUP BY p.name ORDER BY total_qty DESC;",
        "error_message": "",
        "expected_output_hint": "Should return total quantity ordered per product, highest first",
        "expected_rows": [("Shirt", 10), ("Phone", 5), ("Laptop", 2)],
    },
    {
        "id": "hard_5",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER, salary REAL);"
        ),
        "setup": [
            "INSERT INTO employees VALUES (1,'CEO',NULL,200000);",
            "INSERT INTO employees VALUES (2,'Alice',1,90000);",
            "INSERT INTO employees VALUES (3,'Bob',1,85000);",
            "INSERT INTO employees VALUES (4,'Carol',2,70000);",
        ],
        "broken_query": "SELECT e.name, m.name FROM employees e JOIN employees m ON e.id = m.manager_id;",
        "fixed_query":  "SELECT e.name, m.name as manager FROM employees e JOIN employees m ON e.manager_id = m.id WHERE e.manager_id IS NOT NULL;",
        "error_message": "",
        "expected_output_hint": "Should return each employee with their manager name — self join",
        "expected_rows": [("Alice", "CEO"), ("Bob", "CEO"), ("Carol", "Alice")],
    },
    {
        "id": "hard_6",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount REAL, status TEXT);"
            "CREATE TABLE customers (id INTEGER, name TEXT, tier TEXT);"
        ),
        "setup": [
            "INSERT INTO customers VALUES (1,'Alice','gold');",
            "INSERT INTO customers VALUES (2,'Bob','silver');",
            "INSERT INTO customers VALUES (3,'Carol','gold');",
            "INSERT INTO orders VALUES (1,1,500,'shipped');",
            "INSERT INTO orders VALUES (2,2,200,'shipped');",
            "INSERT INTO orders VALUES (3,3,800,'shipped');",
            "INSERT INTO orders VALUES (4,1,300,'pending');",
        ],
        "broken_query": "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name;",
        "fixed_query":  "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.tier = 'gold' AND o.status = 'shipped' GROUP BY c.name;",
        "error_message": "",
        "expected_output_hint": "Should return total shipped order amounts for gold tier customers only",
        "expected_rows": [("Alice", 500.0), ("Carol", 800.0)],
    },
    {
        "id": "hard_7",
        "task_id": "multi_table",
        "db_schema": (
            "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary REAL);"
            "CREATE TABLE departments (id INTEGER, dept_name TEXT, budget REAL);"
        ),
        "setup": [
            "INSERT INTO departments VALUES (1,'Engineering',500000);",
            "INSERT INTO departments VALUES (2,'Marketing',200000);",
            "INSERT INTO employees VALUES (1,'Alice',1,75000);",
            "INSERT INTO employees VALUES (2,'Bob',2,50000);",
            "INSERT INTO employees VALUES (3,'Carol',1,90000);",
            "INSERT INTO employees VALUES (4,'Dave',2,45000);",
        ],
        "broken_query": "SELECT dept_name FROM departments WHERE budget > AVG(budget);",
        "fixed_query":  "SELECT dept_name FROM departments WHERE budget > (SELECT AVG(budget) FROM departments);",
        "error_message": "misuse of aggregate function AVG()",
        "expected_output_hint": "Should return departments with above-average budget — Engineering only",
        "expected_rows": [("Engineering",)],
    },
]
# module-level globals — persist across all instances
_CURRENT_SCENARIO = None
_CURRENT_STEP = 0

def compute_f1(predicted_rows, expected_rows):
    if not expected_rows:
        return 1.0 if not predicted_rows else 0.0
    pred_set = [tuple(str(v) for v in row) for row in predicted_rows]
    exp_set  = [tuple(str(v) for v in row) for row in expected_rows]
    pred_multiset = {}
    for row in pred_set:
        pred_multiset[row] = pred_multiset.get(row, 0) + 1
    exp_multiset = {}
    for row in exp_set:
        exp_multiset[row] = exp_multiset.get(row, 0) + 1
    true_positives = 0
    for row, count in exp_multiset.items():
        true_positives += min(count, pred_multiset.get(row, 0))
    precision = true_positives / len(pred_set) if pred_set else 0.0
    recall    = true_positives / len(exp_set)  if exp_set  else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def run_query_safe(db_schema, setup_stmts, query):
    try:
        conn = sqlite3.connect(":memory:")
        cur  = conn.cursor()
        for stmt in db_schema.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        for stmt in setup_stmts:
            cur.execute(stmt)
        conn.commit()
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        return rows, ""
    except Exception as e:
        return [], str(e)


# module-level globals — survive across all instances
_CURRENT_SCENARIO = random.choice(SCENARIOS)
_CURRENT_STEP     = 0


class SqlQueryDebuggerEnvironment(Environment):

    SUPPORTS_CONCURRENT_SESSIONS: bool = False
    MAX_STEPS = 5

    def __init__(self):
        self._state = State(episode_id=str(uuid4()), step_count=0)

    def reset(self, task_id: str = None) -> SqlQueryDebuggerObservation:
        global _CURRENT_SCENARIO, _CURRENT_STEP
        self._state = State(episode_id=str(uuid4()), step_count=0)
        _CURRENT_STEP = 0

        pool = [s for s in SCENARIOS if s["task_id"] == task_id] if task_id else SCENARIOS
        _CURRENT_SCENARIO = random.choice(pool)

        sample_rows, _ = run_query_safe(
            _CURRENT_SCENARIO["db_schema"],
            _CURRENT_SCENARIO["setup"],
            "SELECT * FROM " + _CURRENT_SCENARIO["db_schema"]
                .split("CREATE TABLE ")[1].split(" ")[0] + " LIMIT 3;"
        )

        return SqlQueryDebuggerObservation(
            broken_query         = _CURRENT_SCENARIO["broken_query"],
            db_schema               = _CURRENT_SCENARIO["db_schema"],
            error_message        = _CURRENT_SCENARIO["error_message"],
            sample_rows          = json.dumps(sample_rows),
            expected_output_hint = _CURRENT_SCENARIO["expected_output_hint"],
            task_id              = _CURRENT_SCENARIO["task_id"],
            attempts_remaining   = self.MAX_STEPS,
            last_result          = None,
            done                 = False,
            reward               = 0.0,
        )

    def step(self, action: SqlQueryDebuggerAction) -> SqlQueryDebuggerObservation:
        global _CURRENT_SCENARIO, _CURRENT_STEP
        _CURRENT_STEP += 1
        self._state.step_count = _CURRENT_STEP
        attempts_left = self.MAX_STEPS - _CURRENT_STEP

        rows, error = run_query_safe(
            _CURRENT_SCENARIO["db_schema"],
            _CURRENT_SCENARIO["setup"],
            action.fixed_query,
        )

        f1     = compute_f1(rows, _CURRENT_SCENARIO["expected_rows"])
        done   = f1 >= 0.99 or attempts_left <= 0
        bonus  = 0.1 * (attempts_left / self.MAX_STEPS) if f1 >= 0.99 else 0.0
        reward = min(round(f1 + bonus, 4), 1.0)

        return SqlQueryDebuggerObservation(
            broken_query         = _CURRENT_SCENARIO["broken_query"],
            db_schema               = _CURRENT_SCENARIO["db_schema"],
            error_message        = error,
            sample_rows          = json.dumps(_CURRENT_SCENARIO["setup"]),
            expected_output_hint = _CURRENT_SCENARIO["expected_output_hint"],
            task_id              = _CURRENT_SCENARIO["task_id"],
            attempts_remaining   = max(attempts_left, 0),
            last_result          = json.dumps(rows),
            done                 = done,
            reward               = reward,
        )

    @property
    def state(self) -> State:
        return self._state