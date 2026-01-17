# Sample Logs

This directory contains synthetic test logs for development and testing.

## Purpose

- **Unit Testing**: Validate parser behavior for each vendor
- **Regression Testing**: Ensure normalization stability
- **Development**: Quick iteration without real customer data

## File Naming Convention

```
<vendor>_<scenario>_<rows>.csv
```

Examples:
- `paloalto_normal_10.csv` - 10 normal Palo Alto logs
- `zscaler_edge_cases_20.csv` - 20 edge case Zscaler logs
- `netskope_mixed_50.csv` - 50 mixed Netskope logs

## Required Scenarios per Vendor

Each vendor should have sample logs covering:

1. **Normal**: Typical allow/block events
2. **Edge Cases**: Missing fields, unusual values
3. **Timestamps**: Various date/time formats
4. **Encoding**: UTF-8 with special characters
5. **Large Values**: Max bytes_sent values

## Data Generation

These are **completely synthetic** logs with:
- Fake IP addresses (10.x.x.x, 192.168.x.x)
- Fake user IDs (user001, user002, etc.)
- Real but public domain names (google.com, openai.com)
- Randomized but realistic bytes values

## Generation Script

```python
# scripts/generate_sample_logs.py
python scripts/generate_sample_logs.py --vendor paloalto --count 10 --output sample_logs/
```

## Important Notes

1. **No Real PII**: Never use real customer data
2. **Reproducible**: Use fixed random seeds for consistency
3. **Version Control**: Sample logs are committed to repo
4. **Update When Needed**: After schema changes, regenerate samples

## Sample Data Schema

Each sample file should include at minimum:
- Timestamp (in vendor's native format)
- User identifier
- Source IP
- Destination domain/URL
- Action (allow/block)
- Bytes sent/received
- HTTP method (if applicable)

## Validation

```bash
# Validate all sample logs parse correctly
python -m pytest tests/test_sample_logs.py -v
```
