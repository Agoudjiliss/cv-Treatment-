from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel, Field


class Contact(BaseModel):
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    linkedin: str | None = None
    location: str | None = None


class EducationItem(BaseModel):
    degree: str | None = None
    institution: str | None = None
    year: str | None = None


class ExperienceItem(BaseModel):
    title: str | None = None
    company: str | None = None
    duration: str | None = None
    description: str | None = None


class Skills(BaseModel):
    technical: List[str] = Field(default_factory=list)
    soft: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)


class CertificationItem(BaseModel):
    name: str | None = None
    institution: str | None = None
    expiration: str | None = None


class ProjectItem(BaseModel):
    name: str | None = None
    description: str | None = None
    technologies: List[str] = Field(default_factory=list)
    url: str | None = None


class CvExtractionResult(BaseModel):
    contact: Contact = Field(default_factory=Contact)
    education: List[EducationItem] = Field(default_factory=list)
    experience: List[ExperienceItem] = Field(default_factory=list)
    certifications: List[CertificationItem] = Field(default_factory=list)
    projects: List[ProjectItem] = Field(default_factory=list)
    skills: Skills = Field(default_factory=Skills)
    summary: str | None = None
    confidence: float = 0.0
    meta: dict[str, Any] = Field(default_factory=dict, alias="_meta")

    model_config = {"populate_by_name": True, "serialize_by_alias": True}
