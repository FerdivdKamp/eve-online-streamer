# Task: Implement a Kafka debugging consumer

Create a small, independent Python consumer in `consumer/`, using Poetry and `confluent-kafka`.

The consumer is a debugging and learning utility, not a replacement for our planned Flink processor.

Requirements:
- Consume from Redpanda, defaulting to `localhost:19092` and topic `market-observations`.
- Support `--topic`, `--group`, `--from-beginning`, `--max-messages`, `--no-commit`, and `--validate`.
- Print Kafka partition, offset, timestamp, key, and JSON payload for each message.
- Support persistent consumer groups with explicit offset commits after successful processing.
- Disable automatic offset commits.
- Ensure `--no-commit` does not advance stored consumer-group offsets.
- Explain in the README that `--from-beginning` applies only when the group has no existing committed offsets.
- Handle Ctrl+C gracefully and use bounded polling so shutdown is responsive.
- Add deterministic unit tests using a mocked Kafka consumer.
- Use type hints and Google-style docstrings.
- Add a CLI entry point: `poetry run eve-consumer`.

Keep the implementation simple, synchronous, and independent from the producer. Do not implement market analysis.

Include a README with examples demonstrating replay, offset commits, and consumer-group partition assignment.