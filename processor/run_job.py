"""Entry point used by the Flink CLI inside the processor image."""

from eve_processor.job import main


if __name__ == "__main__":
    main()
