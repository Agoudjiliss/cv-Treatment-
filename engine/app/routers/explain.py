from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, Field
from fastapi import APIRouter
from langchain_core.prompts import ChatPromptTemplate

from app.llm.ollama_client import OllamaClient

router = APIRouter()


class ExplainRequest(BaseModel):
    jobDescription: str
    cvJson: dict[str, Any]
    vectorScore: float


class ExplainResponse(BaseModel):
    explanation: str | None
    matchedSkills: list[str] = Field(default_factory=list)
    missingSkills: list[str] = Field(default_factory=list)


def build_explain_prompt(job_description: str, cv_json: dict[str, Any], score: float) -> str:
    name = str(cv_json.get("contact", {}).get("name", ""))
    skills = cv_json.get("skills", {}).get("technical", [])
    years = len(cv_json.get("experience", []))
    return (
        "You are an expert recruiter. Given the job and CV below, write exactly 2–3 sentences\n"
        f"explaining the match quality. Vector similarity score: {score:.2f}/1.00.\n"
        "Be specific about key matching skills and any important gaps. Do not add preamble.\n\n"
        f"JOB: {job_description}\n\n"
        f"CV SUMMARY: name={name}, skills={skills}, experience_years={years}\n\n"
        "Response (2–3 sentences only):"
    )


def compute_skill_overlap(job_description: str, cv_json: dict[str, Any]) -> tuple[list[str], list[str]]:
    jd_tokens = {t.strip(" ,.;:()[]").lower() for t in job_description.split() if len(t.strip()) > 2}
    cv_skills = [str(s) for s in cv_json.get("skills", {}).get("technical", [])]
    matched = [s for s in cv_skills if s.lower() in jd_tokens]
    missing = sorted({t for t in jd_tokens if t not in {m.lower() for m in matched}})[:8]
    return matched[:8], missing


def _get_client() -> OllamaClient:
    import os
    return OllamaClient(
        model_name=os.getenv("OLLAMA_MODEL", "llama3.2:3b"),
        base_url=os.getenv("OLLAMA_BASE_URL", "http://ollama:11434"),
        timeout_seconds=int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180")),
    )


@router.post("/api/v1/cv/explain", response_model=ExplainResponse)
async def explain(req: ExplainRequest) -> ExplainResponse:
    matched, missing = compute_skill_overlap(req.jobDescription, req.cvJson)
    prompt = build_explain_prompt(req.jobDescription, req.cvJson, req.vectorScore)
    try:
        client = _get_client()
        explanation = await asyncio.to_thread(
            lambda: client.call(
                prompt_template=ChatPromptTemplate.from_template("{prompt}"),
                params={"prompt": prompt},
            )
        )
    except Exception:
        explanation = None
    return ExplainResponse(explanation=explanation, matchedSkills=matched, missingSkills=missing)
