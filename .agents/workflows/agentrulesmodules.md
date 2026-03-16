---
description: Autonomous prospecting agent rules. It scrapes domains, analyzes relevance and quality, pulls SEO metrics, caches results, and stores structured data.
---

# 741 Studio – Agent Rules Book (Prospecting Pipeline)

This defines constraints, execution protocols, and data governance for agents.
**Goals:** Prevent duplicate work, prevent unnecessary API usage, preserve data consistency, ensure deterministic workflows, maintain reproducibility.

## 1. Core Operating Principles
Agents must strictly prioritize:
1. Data reuse over re-scraping
2. Deterministic workflows
3. API cost efficiency
4. Data consistency
5. Clear phase execution
6. **Explicit Execution Boundaries**: Never execute a master script for an entire list or array without explicit user confirmation.
7. **Strict Test Scoping**: When performing isolated testing, strictly enforce targeting on the exact row number requested.

## 2. Single Source of Truth (SSOT) & Duplication Prevention
- Primary key: `domain` (e.g., `example.com`). All modules reference this.
- Before any action: Check if domain exists in the dataset. If yes, load existing record, skip API calls, run only missing phases.
- **Duplicate Prevention Rule (CRM Integrity)**: Module 3 must fetch all existing domains from Column A before writing. If domain already exists in sheet, **skip entry**. Only append new domains.

## 3. API Usage Rules (Token Protection)
- **Never re-query the same domain**: Before calling DataForSEO, Gemini, or scraping, check `data/cache/`. If processed, skip API call and load cache.
- **Cache everything**: Every external call creates a cached record (`cache/domain_analysis/example.com.json`) to resume from if a crash occurs.
- **One AI call per domain maximum**: Gemini analysis must only run once. Store `ai_analysis_complete = true`.

## 4. Phase Execution & Protocol
Strict phase order: `Phase 0 (Discovery) -> Phase 1 (Qualitative) -> Phase 2 (Traffic) -> Phase 3 (SEO Risk) -> Phase 4 (DB Entry)`.
- Skip phases already completed.
- **Column Stability**: Never rely on column order (e.g., `row[1]`). Always reference by name (`row["domain"]`).
- **Domain Processing Lock**: Set `domain_status = "processing"` before and `"complete"` after. Skip domains actively `"processing"`.
- **Red Flag Detection**: If "guest post", "paid post", or "advertising" phrases appear, mark `phase1_status = rejected` and skip Phases 2 and 3.

## 5. Traffic & SEO Governance
- **DataForSEO Exclusive**: Traffic must only be pulled once per domain. Must use DataForSEO (Never Ahrefs). Measure: `traffic_volume`, `domain_rank`, `referring_domains`, `total_backlinks`, `spam_score`.
- Calculate inbound ratio as `referring_domains / total_backlinks`.
- **Cost Tracking**: Parse "cost" natively returned by DataForSEO payload (`total_cost_usd`, `cost_breakdown`). Gemini logged as "Gemini: Free".

## 6. Execution, Safe Resume, & Scaling
- **Execution Limits**: Max 100 discovered, 30 vetted, 10 API calls/min. Sleep 1-2s for SEO limit, 4s for Gemini.
- **Web Scraping Timeout & Header**: Always use `User-Agent: Mozilla/5.0` to avoid blocks. Hard timeout = 10s. If timeout, `status = timeout` and skip. Sleep 2s between scrapes if scale > 100.
- **Graceful Degradation**: Wrap all scraping and API calls in `try/except`. If failure occurs, mark error and continue next domain. Never crash the pipeline.
- **Safe Restart Protocol**: Load cache, load processed domains, skip completed, resume at next unfinished domain.

## 7. Logging & Error Recovery
- **Failure/Time Tracking**: Log `timestamp`, `domain`, `module`, `error_type`, `action_taken`, and `duration_seconds` to `logs/pipeline.log`.
- **Anti-Loop**: Never repeat operation > 2 attempts. If 2nd attempt fails, log error, skip domain.
- **Client Isolation**: Run strictly inside client sub-folders (e.g., `clients/client_01/cache/`). Never mix data. **Self-Exclusion Rule**: Parse `client_profile_template.json` to prevent scraping or analyzing the client's own domain.

## 8. Directory & Reporting Rules
- **Module 4 (Directories)**: Confirm business listing page and category match. Avoid duplicate submissions.
- **Module 5 (Reporting)**: Log new domains, rejections, tier 1/tier 2 opportunities, errors, and API usage daily.
- **Append-Only Database Rule**: Agents are strictly forbidden from clearing, erasing, or wiping the main Google Sheet. All operations must be Append New Rows Only.

## 9. Deterministic Output Format
All modules must precisely return structured JSON objects. No free text.
```json
{
 "domain": "example.com",
 "phase1_topical_match": "TRUE",
 "phase2_traffic": "3200",
 "spam_score": "2",
 "email": "contact@example.com"
}
```

*Final Master Protocol: "Do I already have this data?" -> If Yes: Load, skip API, continue pipeline.*
