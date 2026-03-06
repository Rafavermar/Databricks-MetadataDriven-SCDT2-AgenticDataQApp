import json
import logging
import os
from typing import Any, Dict, Optional

from databricks import sql
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

LOGGER = logging.getLogger("dq_rescue_agent")

LATEST_ISSUE_QUERY = """
SELECT *
FROM workspace.agentic_poc.dq_issues_log
ORDER BY timestamp DESC
LIMIT 1
"""

ISSUES_QUERY_TEMPLATE = """
SELECT *
FROM workspace.agentic_poc.dq_issues_log
WHERE `timestamp` >= current_timestamp() - INTERVAL {window_hours} HOURS
ORDER BY `timestamp` DESC
LIMIT {max_issues}
"""

SYSTEM_PROMPT = (
    "You are an expert Staff Data Engineer for a Databricks Free Edition pipeline. "
    "Architecture context: MongoDB source -> workspace.agentic_poc.bronze_customers -> "
    "SCD2 workspace.agentic_poc.silver_customers -> DQ rules in workspace.agentic_poc.dq_rules -> "
    "issues logged to workspace.agentic_poc.dq_issues_log. "
    "The workflow stages are SetupMetadata, IngestBronze, Scd2Silver, and DqEngine. "
    "Given a failure or DQ issue, explain root cause, identify likely failing stage, "
    "and propose the smallest safe fix with minimal Python Spark code or SQL plus quick validation checks."
)

UI_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Data Quality Agent</title>
  <style>
    :root {
      --bg: #f2f6f3;
      --panel: #ffffff;
      --ink: #132218;
      --muted: #4f6356;
      --accent: #0f7a47;
      --accent-dark: #0a5d35;
      --error: #9f1d1d;
      --border: #d3ddd5;
      --shadow: 0 12px 26px rgba(19, 34, 24, 0.08);
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      background:
        radial-gradient(circle at 10% 0%, #dceadf 0%, transparent 32%),
        radial-gradient(circle at 100% 10%, #dce5f0 0%, transparent 30%),
        var(--bg);
      min-height: 100vh;
    }
    .wrapper {
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }
    .top {
      display: flex;
      gap: 16px;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }
    .title {
      font-size: 28px;
      font-weight: 700;
      margin: 0;
    }
    .subtitle {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .actions {
      display: flex;
      gap: 8px;
    }
    .ghost-link {
      text-decoration: none;
      color: var(--accent);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 13px;
      background: #fff;
    }
    .status {
      margin-bottom: 16px;
      background: #e9f5ed;
      border: 1px solid #c4decf;
      border-radius: 12px;
      padding: 12px 14px;
      font-size: 14px;
      color: #1c5635;
    }
    .status.error {
      background: #fdeeee;
      border-color: #f1c3c3;
      color: var(--error);
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 16px;
      min-height: 360px;
      display: flex;
      flex-direction: column;
    }
    .panel h2 {
      margin: 0 0 8px;
      font-size: 18px;
    }
    .panel .hint {
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .control-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      cursor: pointer;
      background: var(--accent);
      color: #fff;
      font-weight: 600;
      font-size: 13px;
    }
    button:hover {
      background: var(--accent-dark);
    }
    button.secondary {
      background: #edf3ef;
      color: #1d3323;
      border: 1px solid var(--border);
    }
    label {
      font-size: 13px;
      margin: 8px 0 6px;
      color: var(--muted);
    }
    textarea {
      width: 100%;
      min-height: 90px;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      font: inherit;
      resize: vertical;
      margin-bottom: 12px;
      background: #fbfdfb;
    }
    pre {
      margin: 0;
      padding: 12px;
      border: 1px solid #e4ece6;
      border-radius: 10px;
      background: #f8fbf9;
      overflow-x: auto;
      overflow-y: auto;
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
      flex: 0 0 auto;
    }
    #issuePayload {
      min-height: 120px;
      max-height: 190px;
    }
    #summaryPayload {
      min-height: 260px;
      max-height: 380px;
    }
    #analysisOutput {
      min-height: 340px;
      max-height: 680px;
      flex: 1;
    }
    .kv {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 12px;
    }
    .kv .item {
      background: #f6faf7;
      border: 1px solid #e0e8e2;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
    }
    .kv .label {
      color: var(--muted);
      display: block;
      margin-bottom: 4px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    .footer {
      margin-top: 14px;
      color: var(--muted);
      font-size: 12px;
    }
    @media (max-width: 980px) {
      .grid {
        grid-template-columns: 1fr;
      }
      .wrapper {
        padding: 16px;
      }
    }
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="top">
      <div>
        <h1 class="title">Data Quality Agent</h1>
        <p class="subtitle">Interactive view on top of /latest-issue and /analyze</p>
      </div>
      <div class="actions">
        <a class="ghost-link" href="/docs" target="_blank" rel="noreferrer">Open API docs</a>
        <a class="ghost-link" href="/health" target="_blank" rel="noreferrer">Health</a>
      </div>
    </div>

    <div id="status" class="status">Ready. Select mode, load data, and analyze.</div>

    <div class="grid">
      <section class="panel">
        <h2>Issue context</h2>
        <p class="hint">Use latest issue or aggregated summary, then run concise technical analysis.</p>
        <div class="control-row">
          <button id="loadIssueBtn">Load latest issue</button>
          <button id="loadSummaryBtn">Load summary</button>
          <button id="analyzeBtn">Analyze now</button>
          <button id="clearBtn" class="secondary">Clear</button>
        </div>

        <div class="kv">
          <div class="item">
            <span class="label">Analysis mode</span>
            <select id="modeSelect" style="width:100%;border:1px solid var(--border);border-radius:8px;padding:6px;">
              <option value="summary" selected>summary</option>
              <option value="latest">latest</option>
            </select>
          </div>
          <div class="item">
            <span class="label">Concise output</span>
            <label style="display:flex;align-items:center;gap:8px;margin:0;">
              <input id="conciseToggle" type="checkbox" />
              <span>enabled</span>
            </label>
          </div>
          <div class="item">
            <span class="label">Window hours</span>
            <input id="windowHoursInput" type="number" min="1" max="168" value="24" style="width:100%;border:1px solid var(--border);border-radius:8px;padding:6px;" />
          </div>
          <div class="item">
            <span class="label">Max issues</span>
            <input id="maxIssuesInput" type="number" min="1" max="1000" value="200" style="width:100%;border:1px solid var(--border);border-radius:8px;padding:6px;" />
          </div>
          <div class="item">
            <span class="label">Sample size</span>
            <input id="sampleSizeInput" type="number" min="1" max="10" value="3" style="width:100%;border:1px solid var(--border);border-radius:8px;padding:6px;" />
          </div>
        </div>

        <div class="kv">
          <div class="item"><span class="label">Rule</span><span id="ruleField">-</span></div>
          <div class="item"><span class="label">Severity</span><span id="severityField">-</span></div>
          <div class="item"><span class="label">Record key</span><span id="recordKeyField">-</span></div>
          <div class="item"><span class="label">Timestamp</span><span id="timestampField">-</span></div>
        </div>

        <label for="contextInput">Additional context (optional)</label>
        <textarea id="contextInput" placeholder="Example: Pipeline run after add_bad failed at DqEngine with 2 issues"></textarea>

        <label>Issues summary payload</label>
        <pre id="summaryPayload">No summary loaded.</pre>

        <label>Latest issue payload</label>
        <pre id="issuePayload">No issue loaded.</pre>
      </section>

      <section class="panel">
        <h2>Agent analysis</h2>
        <p class="hint">Direct, technical remediation based on selected analysis mode.</p>
        <label>Analysis output</label>
        <pre id="analysisOutput">No analysis yet.</pre>
        <div class="footer">Provider: <span id="providerField">-</span></div>
      </section>
    </div>
  </div>

  <script>
    const state = { issue: null, summary: null };

    function setStatus(message, isError = false) {
      const status = document.getElementById("status");
      status.textContent = message;
      status.classList.toggle("error", Boolean(isError));
    }

    function prettyJson(value) {
      return JSON.stringify(value, null, 2);
    }

    function safeParseJson(text) {
      if (typeof text !== "string") {
        return text;
      }
      try {
        return JSON.parse(text);
      } catch (_) {
        return text;
      }
    }

    function getAnalysisOptions() {
      const mode = document.getElementById("modeSelect").value;
      const windowHours = Number(document.getElementById("windowHoursInput").value || 24);
      const maxIssues = Number(document.getElementById("maxIssuesInput").value || 200);
      const sampleSize = Number(document.getElementById("sampleSizeInput").value || 3);
      const concise = Boolean(document.getElementById("conciseToggle").checked);
      return {
        analysis_mode: mode,
        window_hours: Math.min(Math.max(windowHours, 1), 168),
        max_issues: Math.min(Math.max(maxIssues, 1), 1000),
        sample_size: Math.min(Math.max(sampleSize, 1), 10),
        concise: concise
      };
    }

    function toggleSummaryOptions() {
      const mode = document.getElementById("modeSelect").value;
      const disabled = mode === "latest";
      document.getElementById("windowHoursInput").disabled = disabled;
      document.getElementById("maxIssuesInput").disabled = disabled;
      document.getElementById("sampleSizeInput").disabled = disabled;
    }

    function renderIssue(issue) {
      const ruleField = document.getElementById("ruleField");
      const severityField = document.getElementById("severityField");
      const recordKeyField = document.getElementById("recordKeyField");
      const timestampField = document.getElementById("timestampField");
      const issuePayload = document.getElementById("issuePayload");

      if (!issue || issue.message) {
        ruleField.textContent = "-";
        severityField.textContent = "-";
        recordKeyField.textContent = "-";
        timestampField.textContent = "-";
        issuePayload.textContent = issue && issue.message ? issue.message : "No issue loaded.";
        return;
      }

      const issueView = { ...issue, record_payload: safeParseJson(issue.record_payload) };
      ruleField.textContent = issue.rule_name || issue.rule_id || "-";
      severityField.textContent = issue.severity || "-";
      recordKeyField.textContent = issue.record_key || "-";
      timestampField.textContent = issue.timestamp || "-";
      issuePayload.textContent = prettyJson(issueView);
    }

    function renderSummary(summary) {
      const summaryPayload = document.getElementById("summaryPayload");
      if (!summary || summary.message) {
        summaryPayload.textContent = summary && summary.message ? summary.message : "No summary loaded.";
        return;
      }
      summaryPayload.textContent = prettyJson(summary);
    }

    async function loadLatestIssue() {
      setStatus("Loading latest issue...");
      try {
        const response = await fetch("/latest-issue", { method: "GET" });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.detail || "Failed to load latest issue.");
        }
        state.issue = data;
        renderIssue(data);
        state.summary = null;
        renderSummary({ message: "Summary not loaded (latest mode)." });
        setStatus(data.message ? data.message : "Latest issue loaded.");
      } catch (error) {
        setStatus(error.message || "Failed to load latest issue.", true);
      }
    }

    async function loadSummary() {
      const options = getAnalysisOptions();
      setStatus("Loading issues summary...");
      try {
        const params = new URLSearchParams({
          window_hours: String(options.window_hours),
          max_issues: String(options.max_issues),
          sample_size: String(options.sample_size)
        });
        const response = await fetch("/issues-summary?" + params.toString(), { method: "GET" });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.detail || "Failed to load summary.");
        }
        state.summary = data;
        renderSummary(data);
        if (data.latest_issue) {
          state.issue = data.latest_issue;
          renderIssue(data.latest_issue);
        }
        setStatus(data.message ? data.message : "Issues summary loaded.");
      } catch (error) {
        setStatus(error.message || "Failed to load summary.", true);
      }
    }

    async function runAnalysis() {
      const analysisOutput = document.getElementById("analysisOutput");
      const providerField = document.getElementById("providerField");
      const contextInput = document.getElementById("contextInput");
      const options = getAnalysisOptions();
      const payload = {
        additional_context: contextInput.value.trim() || null,
        analysis_mode: options.analysis_mode,
        window_hours: options.window_hours,
        max_issues: options.max_issues,
        sample_size: options.sample_size,
        concise: options.concise
      };

      setStatus("Running analysis...");
      analysisOutput.textContent = "Running...";

      try {
        const response = await fetch("/analyze", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.detail || "Analysis failed.");
        }

        state.issue = data.issue || state.issue;
        renderIssue(state.issue);
        state.summary = data.issues_summary || state.summary;
        renderSummary(data.issues_summary || state.summary);
        analysisOutput.textContent = data.analysis || "No analysis returned.";
        providerField.textContent = data.provider || "-";
        setStatus("Analysis completed (" + (data.analysis_mode || options.analysis_mode) + " mode).");
      } catch (error) {
        analysisOutput.textContent = "Analysis failed.";
        setStatus(error.message || "Analysis failed.", true);
      }
    }

    function clearUi() {
      state.issue = null;
      state.summary = null;
      document.getElementById("contextInput").value = "";
      document.getElementById("analysisOutput").textContent = "No analysis yet.";
      document.getElementById("providerField").textContent = "-";
      renderIssue(null);
      renderSummary(null);
      setStatus("Cleared.");
    }

    document.getElementById("modeSelect").addEventListener("change", toggleSummaryOptions);
    document.getElementById("loadIssueBtn").addEventListener("click", loadLatestIssue);
    document.getElementById("loadSummaryBtn").addEventListener("click", loadSummary);
    document.getElementById("analyzeBtn").addEventListener("click", runAnalysis);
    document.getElementById("clearBtn").addEventListener("click", clearUi);

    toggleSummaryOptions();
    loadLatestIssue();
  </script>
</body>
</html>
"""


class AnalyzeRequest(BaseModel):
    additional_context: Optional[str] = None
    analysis_mode: str = Field(default="summary")
    window_hours: int = Field(default=24, ge=1, le=168)
    max_issues: int = Field(default=200, ge=1, le=1000)
    sample_size: int = Field(default=3, ge=1, le=10)
    concise: bool = False


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def get_sql_connection():
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "").strip()
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
    access_token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if not server_hostname or not http_path or not access_token:
        raise RuntimeError(
            "Missing SQL connector environment variables. "
            "Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN."
        )

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )


def fetch_latest_dq_issue() -> Optional[Dict[str, Any]]:
    with get_sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(LATEST_ISSUE_QUERY)
            row = cursor.fetchone()
            if not row:
                return None
            column_names = [desc[0] for desc in cursor.description]
            return dict(zip(column_names, row))


def fetch_recent_dq_issues(window_hours: int, max_issues: int) -> list[Dict[str, Any]]:
    query = ISSUES_QUERY_TEMPLATE.format(
        window_hours=window_hours,
        max_issues=max_issues,
    )
    with get_sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return []
            column_names = [desc[0] for desc in cursor.description]
            return [dict(zip(column_names, row)) for row in rows]


def _parse_record_payload(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return str(payload)


def build_issues_summary(
    issues: list[Dict[str, Any]],
    window_hours: int,
    sample_size: int,
) -> Dict[str, Any]:
    grouped: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    distinct_record_keys: set[str] = set()

    for issue in issues:
        rule_id = str(issue.get("rule_id") or "")
        rule_name = str(issue.get("rule_name") or "")
        severity = str(issue.get("severity") or "")
        failed_condition = str(issue.get("failed_condition") or "")
        group_key = (rule_id, rule_name, severity, failed_condition)

        if group_key not in grouped:
            grouped[group_key] = {
                "rule_id": rule_id,
                "rule_name": rule_name,
                "severity": severity,
                "failed_condition": failed_condition,
                "failed_rows": 0,
                "record_keys": set(),
                "latest_timestamp": issue.get("timestamp"),
            }

        grouped[group_key]["failed_rows"] += 1
        record_key = issue.get("record_key")
        if record_key:
            record_key_str = str(record_key)
            grouped[group_key]["record_keys"].add(record_key_str)
            distinct_record_keys.add(record_key_str)

    rules_summary = []
    for group in grouped.values():
        rules_summary.append(
            {
                "rule_id": group["rule_id"],
                "rule_name": group["rule_name"],
                "severity": group["severity"],
                "failed_condition": group["failed_condition"],
                "failed_rows": group["failed_rows"],
                "distinct_records": len(group["record_keys"]),
                "latest_timestamp": group["latest_timestamp"],
            }
        )

    rules_summary.sort(key=lambda row: row["failed_rows"], reverse=True)

    samples = []
    for issue in issues[:sample_size]:
        samples.append(
            {
                "rule_id": issue.get("rule_id"),
                "rule_name": issue.get("rule_name"),
                "severity": issue.get("severity"),
                "record_key": issue.get("record_key"),
                "failed_condition": issue.get("failed_condition"),
                "timestamp": issue.get("timestamp"),
                "record_payload": _parse_record_payload(issue.get("record_payload")),
            }
        )

    return {
        "window_hours": window_hours,
        "total_issues": len(issues),
        "distinct_rules": len(rules_summary),
        "distinct_records": len(distinct_record_keys),
        "rules": rules_summary,
        "samples": samples,
    }


def _extract_anthropic_text(message: Any) -> str:
    parts = []
    for block in getattr(message, "content", []):
        if getattr(block, "type", "") == "text":
            parts.append(getattr(block, "text", ""))
    return "\n".join(parts).strip()


def _extract_gemini_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if text:
        return text.strip()
    return str(response)


def _get_gemini_response(prompt: str, model: str, api_key: str, concise: bool) -> str:
    combined_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"

    try:
        from google import genai
    except Exception as import_exc:
        LOGGER.warning("google-genai import unavailable, trying google-generativeai fallback: %s", import_exc)
    else:
        client = genai.Client(api_key=api_key)
        try:
            response = client.models.generate_content(
                model=model,
                contents=combined_prompt,
                config={"temperature": 0.2},
            )
        except TypeError:
            response = client.models.generate_content(
                model=model,
                contents=combined_prompt,
            )
        return _extract_gemini_text(response)

    try:
        import google.generativeai as legacy_genai
    except Exception as legacy_exc:
        raise RuntimeError(
            "Gemini SDK import failed. Install google-genai or google-generativeai in app dependencies."
        ) from legacy_exc

    legacy_genai.configure(api_key=api_key)
    legacy_model = legacy_genai.GenerativeModel(model_name=model)
    legacy_response = legacy_model.generate_content(
        combined_prompt,
        generation_config={"temperature": 0.2},
    )
    return _extract_gemini_text(legacy_response)


def get_llm_response(prompt: str, concise: bool = True) -> str:
    provider = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
    LOGGER.info("Using LLM provider: %s", provider)

    if provider == "gemini":
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY is not set")

        model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        return _get_gemini_response(
            prompt=prompt,
            model=model,
            api_key=api_key,
            concise=concise,
        )

    if provider == "openai":
        from openai import OpenAI

        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")

        model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        output_text = getattr(response, "output_text", None)
        return output_text.strip() if output_text else str(response)

    if provider == "anthropic":
        import anthropic

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")

        model = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return _extract_anthropic_text(message)

    raise ValueError(f"Unsupported LLM provider: {provider}")


def build_analysis_prompt(
    analysis_mode: str,
    latest_issue: Dict[str, Any],
    issues_summary: Dict[str, Any],
    additional_context: Optional[str],
    concise: bool,
) -> str:
    latest_issue_json = json.dumps(
        {**latest_issue, "record_payload": _parse_record_payload(latest_issue.get("record_payload"))},
        default=str,
        indent=2,
    )
    summary_json = json.dumps(issues_summary, default=str, indent=2)
    context_block = additional_context.strip() if additional_context else "No extra context provided."
    mode_block = (
        "Mode: latest (single issue). Focus on the latest failing record and immediate fix."
        if analysis_mode == "latest"
        else "Mode: summary (aggregated). Prioritize highest-frequency failing rules and cross-record pattern."
    )

    if concise:
        output_rules = (
            "Output constraints:\n"
            "- Direct, technical style.\n"
            "- Compact but complete.\n"
            "- Use exactly these sections:\n"
            "  1) Root cause\n"
            "  2) Affected records\n"
            "  3) Fix options (SQL/PySpark)\n"
            "  4) Validation\n"
            "- Focus on highest-impact findings and fixes first.\n"
            "- Up to 3 code snippets.\n"
            "- Prefer bullets, short technical paragraphs allowed.\n"
            "- Avoid motivational or generic wording."
        )
    else:
        output_rules = (
            "Output sections:\n"
            "1) Root cause\n"
            "2) Affected records\n"
            "3) Fix options (SQL/PySpark)\n"
            "4) Validation\n"
            "Style:\n"
            "- Pragmatic staff-engineer tone.\n"
            "- Direct and factual.\n"
            "- No fluff, no motivational language.\n"
            "- Include concrete SQL/PySpark examples and specific checks.\n"
            "- Expand details where needed for safe execution."
        )

    return (
        "Data quality incident context:\n"
        f"{mode_block}\n\n"
        f"Latest issue:\n{latest_issue_json}\n\n"
        f"Aggregated summary:\n{summary_json}\n\n"
        f"Additional context:\n{context_block}\n\n"
        "Task:\n"
        "- Summarize what failed, why it failed, and where (pipeline stage).\n"
        "- Propose minimal safe remediation options.\n"
        "- Keep recommendations aligned with Databricks Free Edition and this pipeline architecture.\n\n"
        f"{output_rules}"
    )


configure_logging()
app = FastAPI(title="Data Quality Agent", version="1.0.0")


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui")


@app.get("/ui", response_class=HTMLResponse)
def ui() -> str:
    return UI_HTML


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/latest-issue")
def latest_issue() -> Dict[str, Any]:
    try:
        issue = fetch_latest_dq_issue()
        if not issue:
            return {"message": "No data quality issues found in dq_issues_log"}
        return issue
    except Exception as exc:
        LOGGER.exception("Failed to fetch latest DQ issue")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/issues-summary")
def issues_summary(
    window_hours: int = Query(default=24, ge=1, le=168),
    max_issues: int = Query(default=200, ge=1, le=1000),
    sample_size: int = Query(default=3, ge=1, le=10),
) -> Dict[str, Any]:
    try:
        issues = fetch_recent_dq_issues(window_hours=window_hours, max_issues=max_issues)
        if not issues:
            return {
                "message": "No data quality issues found in dq_issues_log",
                "window_hours": window_hours,
                "max_issues": max_issues,
            }

        summary = build_issues_summary(
            issues=issues,
            window_hours=window_hours,
            sample_size=sample_size,
        )
        return {
            "window_hours": window_hours,
            "max_issues": max_issues,
            "sample_size": sample_size,
            "latest_issue": issues[0],
            "summary": summary,
        }
    except Exception as exc:
        LOGGER.exception("Failed to build DQ issues summary")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze")
def analyze(request: AnalyzeRequest) -> Dict[str, Any]:
    try:
        analysis_mode = (request.analysis_mode or "summary").strip().lower()
        if analysis_mode not in {"latest", "summary"}:
            raise HTTPException(status_code=400, detail="analysis_mode must be 'latest' or 'summary'")

        if analysis_mode == "latest":
            latest_only = fetch_latest_dq_issue()
            issues = [latest_only] if latest_only else []
        else:
            issues = fetch_recent_dq_issues(
                window_hours=request.window_hours,
                max_issues=request.max_issues,
            )

        if not issues:
            return {
                "message": "No data quality issues found in dq_issues_log",
                "analysis": "Nothing to analyze.",
            }

        latest_issue = issues[0]
        summary = build_issues_summary(
            issues=issues,
            window_hours=request.window_hours,
            sample_size=request.sample_size,
        )
        prompt = build_analysis_prompt(
            analysis_mode=analysis_mode,
            latest_issue=latest_issue,
            issues_summary=summary,
            additional_context=request.additional_context,
            concise=request.concise,
        )
        response = get_llm_response(prompt=prompt, concise=request.concise)
        return {
            "provider": os.environ.get("LLM_PROVIDER", "gemini").strip().lower(),
            "analysis_mode": analysis_mode,
            "issue": latest_issue,
            "issues_summary": summary,
            "analysis": response,
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Analysis failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("agent_app:app", host="0.0.0.0", port=8000, reload=False)
