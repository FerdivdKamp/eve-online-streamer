"""Runtime configuration and validation for the processor."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessorConfig:
    """Validated settings shared by source and Flink environment."""

    bootstrap_servers: str = "redpanda:9092"
    topic: str = "market-observations"
    group_id: str = "eve-market-processor-v1"
    start_from: str = "committed"
    parallelism: int = 3
    checkpoint_interval_ms: int = 30000

    def __post_init__(self) -> None:
        """Reject settings that would produce an unusable stream job."""
        for name in ("bootstrap_servers", "topic", "group_id"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.start_from not in {"committed", "earliest", "latest"}:
            raise ValueError("start_from must be committed, earliest, or latest")
        if self.parallelism < 1:
            raise ValueError("parallelism must be positive")
        if self.checkpoint_interval_ms < 1000:
            raise ValueError("checkpoint_interval_ms must be at least 1000")

    @classmethod
    def from_env(cls) -> "ProcessorConfig":
        """Read environment variables used by the submitted job."""
        return cls(
            bootstrap_servers=os.getenv("PROCESSOR_BOOTSTRAP_SERVERS", "redpanda:9092"),
            topic=os.getenv("PROCESSOR_TOPIC", "market-observations"),
            group_id=os.getenv("PROCESSOR_GROUP_ID", "eve-market-processor-v1"),
            start_from=os.getenv("PROCESSOR_START_FROM", "committed"),
            parallelism=int(os.getenv("PROCESSOR_PARALLELISM", "3")),
            checkpoint_interval_ms=int(os.getenv("PROCESSOR_CHECKPOINT_MS", "30000")),
        )
