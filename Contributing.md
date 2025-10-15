# Contributing to FlowForge

First off, thanks for taking the time to contribute! 🎉

The following is a set of guidelines for contributing to FlowForge.

---

## 📌 How to Contribute

### Reporting Bugs

* Use GitHub Issues.
* Include reproduction steps, expected vs actual behavior, logs and version (`flowforge --version`).

### Suggesting Features

* Open a GitHub Issue labeled `enhancement`.
* Describe use case, not just solution.
* Reference similar tools or patterns if applicable.

### Pull Requests

* Fork the repo and create a feature branch (`feat/<short-name>`).
* Keep PRs focused (one change set per PR).
* Include tests where possible.
* Update docs (README/examples) if behavior changes.
* Run `pre-commit` hooks before pushing.
* Ensure CI passes (lint, typecheck, tests).

### Commit Messages

* Use [Conventional Commits](https://www.conventionalcommits.org/) when possible.

  * `feat: add postgres executor`
  * `fix: handle missing sources.yml`
  * `docs: update quickstart`
  * `chore: bump version`

### Development Setup

```bash
# 1) clone
 git clone https://github.com/<org>/<repo>.git && cd flowforge

# 2) create venv
 python -m venv .venv && source .venv/bin/activate

# 3) install
 pip install -e .
 pip install -r requirements-dev.txt  # if available

# 4) pre-commit
 pre-commit install
```

### Testing

```bash
pytest -q
make demo
```

---

## 🧑‍🤝‍🧑 Code of Conduct

This project adheres to the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

### Our Pledge

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, sex characteristics, gender identity and expression, level of experience, education, socio-economic status, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards

* Use welcoming and inclusive language.
* Respect different viewpoints and experiences.
* Gracefully accept constructive criticism.
* Focus on what is best for the community.
* Show empathy towards others.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting the maintainers at `<your-email@example.com>`.

---

## 📄 License

By contributing, you agree that your contributions will be licensed under the [Apache-2.0 License](LICENSE).
