"""A unit of scheduled work (Annex D): a name and `run`, which returns how many items it
processed and how many failed. The scheduler gives every run its correlation id and deadline."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class RunReport:
    job: str
    processed: int
    failed: int
    timed_out: bool = False


@dataclass(frozen=True)
class RunContext:
    correlation_id: str
    deadline: float
    """A monotonic instant after which the run takes no more items."""


class Job(Protocol):
    name: str

    def run(self, context: RunContext) -> RunReport: ...
