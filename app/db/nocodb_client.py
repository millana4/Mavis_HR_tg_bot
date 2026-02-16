import aiohttp
import logging
from typing import Any, Dict, List, Optional, Union
from config import Config

logger = logging.getLogger(__name__)


class NocoDBClient:
    def __init__(self):
        self.base_url = Config.NOCOBD_SERVER.rstrip('/')
        self.headers = {
            "xc-token": Config.NOCOBD_API_TOKEN,
            "Content-Type": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _make_request(self, method: str, url: str, **kwargs) -> Any:
        await self._ensure_session()
        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"NocoDB API error {response.status}: {error_text}")
                    raise Exception(f"NocoDB API error {response.status}: {error_text}")
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {e}")
            raise Exception(f"Request failed: {e}")

    async def get_all(self, table_id: str, fields: Optional[List[str]] = None, where: Optional[str] = None,
                      sort: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Получить все записи таблицы"""
        logger.info(f"Getting all records from table {table_id}")
        url = f"{self.base_url}/api/v2/tables/{table_id}/records"

        params = {"limit": limit, "offset": offset}
        if fields:
            params["fields"] = ",".join(fields)
        if where:
            params["where"] = where
        if sort:
            params["sort"] = sort

        response = await self._make_request("GET", url, params=params)
        records = response.get("list", [])
        page_info = response.get("pageInfo", {})

        if not page_info.get("isLastPage", True):
            total_rows = page_info.get("totalRows", 0)
            if offset + limit < total_rows:
                next_records = await self.get_all(
                    table_id=table_id,
                    fields=fields,
                    where=where,
                    sort=sort,
                    limit=limit,
                    offset=offset + limit
                )
                records.extend(next_records)

        logger.info(f"Retrieved {len(records)} records from table {table_id}")
        return records

    async def get_record(self, table_id: str, record_id: Union[int, str], fields: Optional[List[str]] = None) -> \
    Optional[Dict]:
        """Получить одну запись по ID"""
        logger.info(f"Getting record {record_id} from table {table_id}")
        url = f"{self.base_url}/api/v2/tables/{table_id}/records/{record_id}"
        params = {}
        if fields:
            params["fields"] = ",".join(fields)

        try:
            return await self._make_request("GET", url, params=params)
        except Exception as e:
            if "404" in str(e):
                logger.warning(f"Record {record_id} not found in table {table_id}")
                return None
            raise

    async def create_record(self, table_id: str, data: Dict[str, Any]) -> List[Dict]:
        """Создать новую запись в таблице"""
        logger.info(f"Creating record in table {table_id}")
        url = f"{self.base_url}/api/v2/tables/{table_id}/records"
        # NocoDB ожидает массив записей для создания
        payload = [data]
        response = await self._make_request("POST", url, json=payload)
        return response  # Возвращает массив созданных записей с их Id


    async def update_record(self, table_id: str, record_id: Union[int, str], data: Dict[str, Any]) -> Dict:
        """Изменить существующую запись"""
        logger.info(f"Updating record {record_id} in table {table_id}")
        url = f"{self.base_url}/api/v2/tables/{table_id}/records"
        payload = [{**data, "Id": record_id}]
        response = await self._make_request("PATCH", url, json=payload)

        if isinstance(response, list) and len(response) > 0:
            logger.info(f"Record {record_id} updated successfully")
            return response[0]
        logger.info(f"Record {record_id} updated")
        return response

    async def delete_record(self, table_id: str, record_id: Union[int, str]) -> bool:
        """Удалить запись по ID"""
        logger.info(f"Deleting record {record_id} from table {table_id}")
        url = f"{self.base_url}/api/v2/tables/{table_id}/records"
        payload = [{"Id": record_id}]
        response = await self._make_request("DELETE", url, json=payload)

        if isinstance(response, list) and len(response) > 0:
            deleted = response[0].get("Id") == record_id
            if deleted:
                logger.info(f"Record {record_id} deleted successfully")
            return deleted
        logger.info(f"Record {record_id} deletion processed")
        return False

    async def create_column(self, table_id: str, column_name: str, column_type: str = "SingleLineText",
                            options: Optional[Dict[str, Any]] = None) -> Dict:
        """Создать новую колонку в таблице"""
        logger.info(f"Creating column {column_name} in table {table_id}")
        column_data = {
            "title": column_name,
            "column_name": column_name.lower().replace(" ", "_"),
            "uidt": self._get_uidt_from_type(column_type),
            "dt": column_type
        }
        if options:
            column_data.update(options)

        url = f"{self.base_url}/api/v2/tables/{table_id}/columns"
        try:
            return await self._make_request("POST", url, json=column_data)
        except Exception as e:
            if "404" in str(e):
                logger.error(f"Column creation endpoint not found for table {table_id}")
                raise Exception("Column creation endpoint not found")
            raise

    def _get_uidt_from_type(self, column_type: str) -> str:
        """Преобразует тип колонки в UIDT"""
        type_mapping = {
            "SingleLineText": "SingleLineText", "LongText": "LongText",
            "Number": "Number", "Decimal": "Decimal", "Currency": "Currency",
            "Percent": "Percent", "Email": "Email", "PhoneNumber": "PhoneNumber",
            "URL": "URL", "Checkbox": "Checkbox", "Date": "Date",
            "DateTime": "DateTime", "Time": "Time", "Year": "Year",
            "Duration": "Duration", "Rating": "Rating", "SingleSelect": "SingleSelect",
            "MultiSelect": "MultiSelect", "Attachment": "Attachment",
            "JSON": "JSON", "Geometry": "Geometry", "Barcode": "Barcode",
            "QRCode": "QRCode",
        }
        return type_mapping.get(column_type, "SingleLineText")

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
