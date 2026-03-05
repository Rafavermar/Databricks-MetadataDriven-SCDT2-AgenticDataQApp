import json
import logging
import os
from typing import Any, Dict, Optional

from databricks import sql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

LOGGER = logging.getLogger("dq_rescue_agent")

LATEST_ISSUE_QUERY = """
SELECT *
FROM workspace.agentic_poc.dq_issues_log
ORDER BY timestamp DESC
LIMIT 1
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


class AnalyzeRequest(BaseModel):
    additional_context: Optional[str] = None


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


def _extract_anthropic_text(message: Any) -> str:
    parts = []
    for block in getattr(message, "content", []):
        if getattr(block, "type", "") == "text":
            parts.append(getattr(block, "text", ""))
    return "\n".join(parts).strip()


def get_llm_response(prompt: str) -> str:
    provider = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
    LOGGER.info("Using LLM provider: %s", provider)

    if provider == "gemini":
        from google import genai

        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY is not set")

        model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model,
            contents=f"{SYSTEM_PROMPT}\n\n{prompt}",
        )
        text = getattr(response, "text", None)
        return text.strip() if text else str(response)

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
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return _extract_anthropic_text(message)

    raise ValueError(f"Unsupported LLM provider: {provider}")


def build_analysis_prompt(issue: Dict[str, Any], additional_context: Optional[str]) -> str:
    issue_json = json.dumps(issue, default=str, indent=2)
    context_block = additional_context.strip() if additional_context else "No extra context provided."
    return (
        "Latest data quality issue:\n"
        f"{issue_json}\n\n"
        f"Additional context:\n{context_block}\n\n"
        "Please provide:\n"
        "1) Root cause summary\n"
        "2) Likely upstream defect\n"
        "3) Minimal Scala Spark remediation snippet for Silver cleanup\n"
        "4) Immediate validation SQL checks"
    )


configure_logging()
app = FastAPI(title="Agentic Data Quality App", version="1.0.0")


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


@app.post("/analyze")
def analyze(request: AnalyzeRequest) -> Dict[str, Any]:
    try:
        issue = fetch_latest_dq_issue()
        if not issue:
            return {
                "message": "No data quality issues found in dq_issues_log",
                "analysis": "Nothing to analyze.",
            }

        prompt = build_analysis_prompt(issue, request.additional_context)
        response = get_llm_response(prompt)
        return {
            "provider": os.environ.get("LLM_PROVIDER", "gemini").strip().lower(),
            "issue": issue,
            "analysis": response,
        }
    except Exception as exc:
        LOGGER.exception("Analysis failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("agent_app:app", host="0.0.0.0", port=8000, reload=False)
