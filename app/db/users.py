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

    @classmethod
    def from_1c_data(cls, row_data: Dict) -> Optional['User']:
        """Создает пользователя из данных 1С"""
        try:
            snils = row_data.get('Name')
            fio = row_data.get('FIO')

            if not snils or not fio:
                return None

            # Нормализуем телефоны
            phone_private = row_data.get('Phone_private', '')
            phones = normalize_phones_string(phone_private) if phone_private else []

            employment_date_str = row_data.get('Date_employment')
            employment_date = None

            if employment_date_str:
                try:
                    employment_date = datetime.strptime(employment_date_str, '%Y-%m-%d').date()
                except:
                    pass

            # Создаем компанию и определяем сегмент - Мавис или Вотоня, или оба
            company_name = row_data.get('Company', '')

            company = Company(
                id=company_name.lower().replace(' ', '_'),
                title=company_name,
                segment=CompanySegment.BOTH # Временное значение, переопределяется при создании в сводной таблице
            )

            # Создаем отдел
            department_name = row_data.get('Department', '')
            department = None
            if department_name:
                department = Department(
                    id=department_name.lower().replace(' ', '_'),
                    title=department_name
                )

            # Создаем трудоустройство
            employment = Employment(
                company=company,
                department=department,
                position=row_data.get('Position'),
                date_employment=employment_date,
                is_main=row_data.get('Is_main') == 'Да'
            )

            return cls(
                id=snils,
                fio=fio,
                date_employment=employment_date,
                phone_private=phone_private,
                phones=phones,
                corp_emails=[],
                internal_phone=None,
                employments=[employment],
                location=None,
                photo=None,
                role=None,
                access_roles=[],
                id_messenger=None,
                date_registered=None
            )

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Ошибка создания User из данных 1С: {e}")
            return None

    def to_pivot_table_format(self) -> Dict:
        """Конвертирует пользователя в формат сводной таблицы"""
        companies = set()
        departments = set()
        positions = set()
        earliest_employment_date = self.date_employment

        for emp in self.employments:
            company_name = emp.company.title.strip() if emp.company.title else ''
            dept_name = emp.department.title.strip() if emp.department else ''
            pos_name = emp.position.strip() if emp.position else ''

            if company_name:
                companies.add(company_name)
            if dept_name:
                departments.add(dept_name)
            if pos_name:
                positions.add(pos_name)

            if emp.date_employment:
                if not earliest_employment_date or emp.date_employment < earliest_employment_date:
                    earliest_employment_date = emp.date_employment

        company_names_list = list(companies)
        company_segment = CompanySegmentDetector.detect_segment_for_companies(company_names_list)

        return {
            'Name': self.id,  # СНИЛС
            'FIO': self.fio,
            'Previous_surname': self.previous_surname,
            'Company_segment': company_segment.value,
            'Companies': ', '.join(sorted(companies)) if companies else None,
            'Departments': ', '.join(sorted(departments)) if departments else None,
            'Positions': ', '.join(sorted(positions)) if positions else None,
            'Internal_numbers': None,
            'Date_employment': earliest_employment_date.isoformat() if earliest_employment_date else None,
            'Email_mavis': None,
            'Email_other': None,
            'Email_votonia': None,
            'Phones': list(self.phones) if self.phones else None,
            'Location': None,
            'Photo': None,
        }


    def to_auth_table_format(self) -> List[Dict]:
        """Конвертирует пользователя в формат таблицы авторизации (по одной записи на каждый телефон)"""
        auth_records = []

        # Если есть телефоны - создаем записи для каждого
        if self.phones:
            for phone in self.phones:
                auth_records.append({
                    'Name': self.id,  # СНИЛС
                    'FIO': self.fio,
                    'Phone': phone,
                    'Role': None,
                    'ID_messenger': '',
                    'Date_registration': None,
                })
        return auth_records



