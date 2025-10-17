# flowforge/errors.py

from __future__ import annotations

from collections.abc import Iterable


class FlowForgeError(Exception):
    """
    Base class for all FlowForge errors.

    Attributes:
        message: Human-readable error message.
        code: Optional short error code (e.g., 'CFG001', 'DAG002').
        hint: Optional human hint with remediation steps.
    """

    def __init__(self, message: str, *, code: str | None = None, hint: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.hint = hint

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message}\n\nHint:\n{self.hint}"
        return self.message


class DependencyNotFoundError(FlowForgeError):
    """Raised when a model depends on another model that does not exist."""

    def __init__(self, missing_map: dict[str, list[str]]):
        parts = []
        for depender, deps in missing_map.items():
            parts.append(f"{depender} → missing: {', '.join(sorted(deps))}")

        msg = (
            "❌ Missing model dependency.\n"
            + "\n".join(parts)
            + (
                "\n\nHints:\n"
                "• Check file names under models/ "
                "(node name = file stem, e.g. users.ff.sql → 'users.ff').\n"
                "• Ensure ref('…') matches the exact node name.\n"
                "• If it's a Python model, set @model(name='…')."
            )
        )

        super().__init__(msg)
        self.missing_map = missing_map


class ModelCycleError(FlowForgeError):
    """
    Raised when a cycle is detected in the model DAG.

    Args:
        affected_nodes: Nodes that couldn't be ordered due to the cycle.
    """

    def __init__(self, affected_nodes: Iterable[str]):
        affected = sorted(set(affected_nodes))
        msg = "Cycle detected in DAG. Affected models: " + ", ".join(affected)
        hint = (
            "Check for circular refs in your models:\n"
            "• Ensure A does not ref B while B (directly or indirectly) refs A.\n"
            "• Break the cycle by removing or refactoring one dependency.\n"
            "• If a ref is conditional in SQL, ensure the parse phase still sees the correct deps."
        )
        super().__init__(msg, code="DAG_CYCLE", hint=hint)
        self.affected_nodes = affected


class ModuleLoadError(FlowForgeError):
    """Raised when a Python model module cannot be loaded."""

    pass


class ProfileConfigError(FlowForgeError):
    """Profile/configuration error with a short, actionable hint."""

    def __init__(self, message: str):
        # keep to a single line for CLI readability
        super().__init__(message.replace("\n", " ").strip())
