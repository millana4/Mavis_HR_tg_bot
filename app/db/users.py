from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict
from enum import Enum
from datetime import date, datetime

from app.db.organization import Company, Department, CompanySegment, CompanySegmentDetector
from app.db.roles import UserRole
from app.services.utils import normalize_phones_string


class AccessRole(str, Enum):
    USER = "user"
    CONTENT_BROADCAST_ADMIN = "content_broadcast_admin"
    PULSE_ADMIN = "pulse_admin"
    FEEDBACK_ADMIN = "feedback_admin"


class Employment(BaseModel):
    company: Company
    department: Optional[Department] = None
    position: Optional[str] = None
    date_employment: Optional[date] = None
    is_main: bool = False

class User(BaseModel):
    # --- Идентификация ---
    id: str = Field(..., description="СНИЛС")
    fio: str = Field(..., min_length=1, description="ФИО")
    previous_surname: Optional[str] = Field(None, description="Предыдущая фамилия")

    # --- Исходные контакты (как пришли) ---
    phone_private: Optional[str] = Field(
        None,
        description="Телефон из 1C (может содержать несколько номеров)"
    )
    email_private: Optional[EmailStr] = Field(
        None,
        description="Email личный из 1С"
    )

    # --- Нормализованные контакты ---
    phones: List[str] = Field(
        default_factory=list,
        description="Нормализованные личные телефоны"
    )
    corp_emails: List[EmailStr] = Field(
        default_factory=list,
        description="Корпоративные email"
    )
    internal_phone: Optional[str] = Field(
        None,
        description="Внутренний телефон"
    )

    # --- Работа ---
    employments: List[Employment] = Field(
        default_factory=list,
        description="Места работы пользователя"
    )

    date_employment: Optional[date] = Field(
        None,
        description="Дата устройства на работу (самая ранняя из всех записей 1С, если их несколько)"
    )

    # поле для хранения объединённых строк трудоустройств
    employment_strings: set[str] = Field(
        default_factory=set,
        description="Набор строк для сравнения: FIO, Previous_surname, Phones, Company, Department, Position"
    )

    # --- Профиль ---
    location: Optional[str] = None
    photo: Optional[str] = Field(None, description="URL фото")

    # --- Системные поля ---
    role: Optional[UserRole] = Field(
        None,
        description="Роль пользователя (пустая при загрузке из 1С)"
    )
    access_roles: List[AccessRole] = Field(
        default_factory=list,
        description="Права доступа"
    )

    id_messenger: Optional[str] = Field(
        None,
        description="ID пользователя в мессенджере"
    )
    date_registered: Optional[datetime] = Field(
        None,
        description="Дата регистрации в сервисе"
    )


    def to_auth_table_format(self) -> List[Dict]:
        """Конвертирует пользователя в формат таблицы авторизации (по одной записи на каждый телефон)"""
        auth_records = []

        # Если есть телефоны - создаем записи для каждого
        if self.phones:
            for phone in self.phones:
                auth_records.append({
                    'SNILS': self.id,  # СНИЛС
                    'FIO': self.fio,
                    'Phone': phone,
                    'Role': None,
                    'ID_messenger': '',
                    'Date_registration': None,
                })
        return auth_records



