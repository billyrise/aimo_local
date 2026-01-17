# Non-goals (Explicitly Out of Scope)

The following are not to be implemented in v1.4 unless the user explicitly requests them.

1. Real-time streaming ingestion (Kafka/Flink/Kinesis)
2. Multi-tenant cloud compute where customer data is co-located
3. Automatic policy enforcement / blocking changes in customer systems
4. Full differential privacy or cryptographic anonymization beyond deterministic redaction and hashing
5. SIEM bi-directional integrations (forwarding and alerting can be added later)
6. UI dashboard frontend (JSON outputs are sufficient in v1.4)
7. Training a proprietary ML classifier as a replacement for rules + LLM (may be a later optional phase)
