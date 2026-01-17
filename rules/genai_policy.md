# GenAI Classification Policy

## Purpose
This document defines the policy for classifying services as `genai` (Generative AI) in the AIMO Analysis Engine.

## Classification Criteria

### usage_type = "genai"
A service is classified as `genai` when:
1. **Primary function is LLM/AI-based text generation** (ChatGPT, Claude, Gemini, etc.)
2. **Primary function is AI-based content generation** (Midjourney, DALL-E, Stable Diffusion)
3. **Service provides AI-powered writing/translation** (DeepL, Grammarly AI features)
4. **Service hosts/runs AI models** (Hugging Face, Replicate)

### Risk Levels
- **high**: Direct LLM/GenAI services where users input potentially sensitive data
- **medium**: AI-augmented services with limited data exposure (translation, grammar check)
- **low**: Reserved for enterprise-managed AI services on allowlist

## Detection Flow

```
URL/Domain → Rule Match?
    ├─ YES (genai rule) → Apply rule classification
    └─ NO → LLM Analysis
              ├─ confidence >= 0.7 → Apply LLM classification
              └─ confidence < 0.7 → usage_type = "unknown"
```

## Enterprise Allowlist

Organizations can configure `config/allowlist.yaml` to:
1. **Reclassify enterprise-managed AI** (e.g., Azure OpenAI on corporate subscription)
2. **Lower risk level** for approved GenAI tools
3. **Exempt internal AI services** (e.g., `ai.internal.company.com`)

### Example allowlist entry:
```yaml
allowlist:
  - domain_suffix: "openai.azure.com"
    override_risk: "low"
    notes: "Enterprise Azure OpenAI subscription"
  - domain_exact: "ai.internal.example.com"
    override_usage_type: "business"
    override_risk: "low"
    notes: "Internal AI service"
```

## Shadow AI Detection Priority

For Shadow AI monitoring, GenAI services are prioritized in the following order:

1. **Critical (priority 50)**: ChatGPT, Claude, Gemini, Copilot - direct LLM access
2. **High (priority 60)**: Translation/writing AI - document content exposure
3. **Medium (priority 70)**: AI-augmented SaaS - partial AI features
4. **Low (priority 100+)**: General business SaaS

## Reporting

In audit reports, GenAI findings are always reported with:
- Service name and category
- Risk level justification
- Access count and unique users
- bytes_sent aggregation (data input indicator)
- First/last seen timestamps
