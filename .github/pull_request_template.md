## Description

Please include a detailed summary of the changes proposed in this Pull Request, highlighting the problem solved, design decisions, and architectural implications.

---

## 🛠️ Skywalker Local Gauntlet Checklist

Before submitting this PR, please check all boxes that apply. All checks must pass in order to merge.

- [ ] **Branch & Bump:** Developed on a dedicated `feature/` or `fix/` branch, and the version inside `pyproject.toml` has been incremented.
- [ ] **Linter (Ruff):** Running `uv run ruff check . --fix` reports no warnings or styling violations.
- [ ] **Formatter (Ruff):** Running `uv run ruff format .` is clean.
- [ ] **Static Type Check (MyPy):** Running `uv run mypy src` finishes with no typing errors.
- [ ] **Unit Tests (Pytest):** Running `uv run pytest` runs the full test suite with 100% success.

---

## 🧪 Functional Verification & Logs

Please outline the manual or functional tests run to verify these changes. Paste terminal outputs or log files proving successful command completion.

```bash
# Example verification run:
rds-cli [command] --args
```

---

## 🔒 Security & Privacy Review

- [ ] Interactive settings or secret credentials are not logged to stdout or stored in plain-text.
- [ ] Safe canonical file resolution checks have been applied to file inputs or directory paths.
- [ ] No placeholders, unfinished mocks, or empty `TODO` blocks have been committed.
