from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum
import re


class CompanySegment(str, Enum):
    MAVIS = "МАВИС"
    VOTONYA = "ВОТОНЯ"
    BOTH = "ОБА"


class Department(BaseModel):
    id: str = Field(..., description="ID подразделения")
    title: str = Field(..., min_length=1, description="Название подразделения")


class Company(BaseModel):
    id: str = Field(..., description="ID компании")
    title: str = Field(..., min_length=1, description="Название компании")
    segment: CompanySegment = Field(default=CompanySegment.MAVIS, description="Сегмент компании")

    departments: List[Department] = Field(
        default_factory=list,
        description="Подразделения компании (many-to-many)"
    )


class CompanySegmentDetector:
    """Определяет сегмент компании по названию"""

    MAVIS_COMPANIES = {
        "соцстрой", "мавис-монтаж", "мавис-недвижимость", "мавис-монолит",
        "стройарсенал", "мавис-инновации", "мавис-град", "мавис-строй",
        "мавис-бетон", "графстрой", "стройтек", "петергофстрой",
        "новаград", "лигастрой", "мавис"
    }


    @classmethod
    def _clean_name(cls, name: str) -> str:
        """Убирает кавычки и приводит к нижнему регистру"""
        if not name:
            return ""
        cleaned = re.sub(r'["\'«»]', '', name)
        return cleaned.strip().lower()


    @classmethod
    def _is_in_mavis_list(cls, name: str) -> bool:
        """Проверяет, есть ли компания в списке MAVIS_COMPANIES"""
        cleaned = cls._clean_name(name)
        # Проверяем точное совпадение или содержит "мавис"
        return cleaned in cls.MAVIS_COMPANIES or "мавис" in cleaned

    @classmethod
    def detect_segment_for_companies(cls, company_names: List[str]) -> CompanySegment:
        """
        Логика:
        1. Если компания одна и содержит 'вотоня' -> VOTONYA
        2. Если нет 'вотоня', проверяем все компании:
           - Если ВСЕ компании есть в MAVIS_COMPANIES -> MAVIS
           - Если хотя бы одной нет в MAVIS_COMPANIES -> BOTH
        """
        if not company_names:
            return CompanySegment.MAVIS

        cleaned_names = [cls._clean_name(c) for c in company_names if c]

        # 1. Если ровно одна компания и она содержит 'вотоня'
        if len(cleaned_names) == 1 and 'вотоня' in cleaned_names[0]:
            return CompanySegment.VOTONYA

        # 2. Проверяем все компании на присутствие в MAVIS_COMPANIES
        all_in_mavis = all(cls._is_in_mavis_list(c) for c in cleaned_names)
        if all_in_mavis:
            return CompanySegment.MAVIS

        # 3. Если хотя бы одной нет в MAVIS_COMPANIES
        return CompanySegment.BOTH