"""@workflow and @step decorators, and the registry that maps names to code.

Workers look code up by name: a run's journal stores step names and `{workflow, version}`,
never Python objects. Versions are pinned per run (Phase 5 routes on them).
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    version: int
    fn: Callable[..., Any]


class Registry:
    def __init__(self) -> None:
        self._workflows: dict[tuple[str, int], WorkflowDef] = {}
        self._steps: dict[str, Callable[..., Any]] = {}

    def workflow(self, fn=None, *, name: str | None = None, version: int = 1):
        def register(f):
            wf = WorkflowDef(name or f.__name__, version, f)
            existing = self._workflows.get((wf.name, wf.version))
            if existing and existing.fn is not f:
                raise ValueError(f"workflow {wf.name} v{wf.version} is already registered")
            self._workflows[(wf.name, wf.version)] = wf
            f.__relay_workflow__ = wf
            return f

        return register(fn) if fn else register

    def step(self, fn=None, *, name: str | None = None):
        def register(f):
            step_name = name or f.__name__
            existing = self._steps.get(step_name)
            if existing and existing is not f:
                raise ValueError(f"step {step_name} is already registered")
            self._steps[step_name] = f
            f.__relay_step__ = step_name
            return f

        return register(fn) if fn else register

    def get_workflow(self, name: str, version: int | None = None) -> WorkflowDef:
        if version is None:
            versions = [v for (n, v) in self._workflows if n == name]
            if not versions:
                raise KeyError(f"no workflow named {name}")
            version = max(versions)
        return self._workflows[(name, version)]

    def get_step(self, name: str) -> Callable[..., Any]:
        return self._steps[name]


registry = Registry()
workflow = registry.workflow
step = registry.step
