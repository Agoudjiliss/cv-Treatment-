from __future__ import annotations

from typing import Any, List

from pydantic import AliasChoices, BaseModel, Field


class Contact(BaseModel):
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    linkedin: str | None = None
    location: str | None = None


class EducationItem(BaseModel):
    """HR DTO: diplôme / formation."""

    institution: str | None = None
    establishment: str | None = None
    typeEducation: str | None = None
    dateGraduation: int | str | None = None


class ExperienceItem(BaseModel):
    """HR DTO: expérience professionnelle."""

    role: str | None = None
    company: str | None = None
    location: str | None = None
    startDate: str | None = None
    endDate: str | None = None
    description: str | None = None


class LanguageProficiency(BaseModel):
    language: str | None = None
    proficiency: str | None = None


class Skills(BaseModel):
    """
    HR DTO: niveau global, référentiel, langues structurées.
    technical / soft restent pour compatibilité et scoring explain.
    """

    score: str | None = None
    catalog_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("catalogId", "catalog_id"),
        serialization_alias="catalogId",
    )
    languages: List[LanguageProficiency] = Field(default_factory=list)
    technical: List[str] = Field(default_factory=list)
    soft: List[str] = Field(default_factory=list)


class CertificationItem(BaseModel):
    title: str | None = None
    issuer: str | None = None
    issueDate: str | None = None
    expiryDate: str | None = None
    description: str | None = None


class AchievementItem(BaseModel):
    projectName: str | None = None
    description: str | None = None
    startDate: str | None = None
    endDate: str | None = None


class CvExtractionResult(BaseModel):
    contact: Contact = Field(default_factory=Contact)
    education: List[EducationItem] = Field(default_factory=list)
    experience: List[ExperienceItem] = Field(default_factory=list)
    certifications: List[CertificationItem] = Field(default_factory=list)
    achievement: List[AchievementItem] = Field(default_factory=list)
    skills: Skills = Field(default_factory=Skills)
    summary: str | None = None
    confidence: float = 0.0
    meta: dict[str, Any] = Field(default_factory=dict, alias="_meta")

    model_config = {"populate_by_name": True, "serialize_by_alias": True}
