# -*- coding: utf-8 -*-
"""
Elba → Bitrix24 Контрагенты Sync (Companies & Contacts)

Скрипт получает всех контрагентов из Контур Эльбы и их контактных лиц,
создаёт компании и контакты в Bitrix24. Идентификация по пользовательскому
полю UF_CRM_ELBA_ID (как для компаний, так и для контактов).

Требуется .env рядом со скриптом или в окружении:
  ELBA_TOKEN=<токен Эльбы (X-Kontur-ApiKey)>
  BITRIX_WEBHOOK_URL=https://<your-domain>.bitrix24.ru/rest/1/WEBHOOK_CODE/

Запуск:
  python /workspace/sync_elba_counterparties.py

Зависимости (requirements.txt):
  requests
  python-dotenv
  tenacity
  tqdm
"""
import os
import sys
import logging
from typing import Dict, List, Any, Optional, Tuple

import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from tqdm import tqdm

# ------------------------- Конфигурация и логирование -------------------------
load_dotenv()

ELBA_TOKEN: Optional[str] = os.getenv("ELBA_TOKEN")
BITRIX_WEBHOOK: Optional[str] = os.getenv("BITRIX_WEBHOOK_URL")

if not ELBA_TOKEN:
    raise SystemExit("Ошибка: в окружении не задан ELBA_TOKEN (X-Kontur-ApiKey)")
if not BITRIX_WEBHOOK:
    raise SystemExit("Ошибка: в окружении не задан BITRIX_WEBHOOK_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

ELBA_API_BASE = "https://elba-api.kontur.ru/v1"

# ------------------------------- Вспомогательные ------------------------------

def chunked(iterable: List[Any], size: int) -> List[List[Any]]:
    return [iterable[i : i + size] for i in range(0, len(iterable), size)]


def extract_name_parts(full_name: str) -> Tuple[str, str, str]:
    if not full_name:
        return "", "", ""
    parts = str(full_name).split()
    last_name = parts[0] if len(parts) > 0 else ""
    first_name = parts[1] if len(parts) > 1 else ""
    second_name = parts[2] if len(parts) > 2 else ""
    return last_name, first_name, second_name


# ----------------------------- Bitrix24 обертки -------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
)
def bitrix_call(method: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{BITRIX_WEBHOOK}{method}"
    resp = requests.post(url, json=params or {}, timeout=(10, 60))
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(
            f"Bitrix24 API error {data.get('error')}: {data.get('error_description')}"
        )
    return data.get("result") if isinstance(data, dict) else data


def create_bitrix_userfield(entity_type: str, field_name: str, label: str) -> Any:
    method = f"crm.{entity_type}.userfield.add"
    params = {
        "fields": {
            "FIELD_NAME": field_name,
            "EDIT_FORM_LABEL": {"ru": label},
            "LIST_COLUMN_LABEL": {"ru": label},
            "USER_TYPE_ID": "string",
            "MANDATORY": "N",
        }
    }
    return bitrix_call(method, params)


def ensure_userfields() -> None:
    try:
        contact_fields = bitrix_call(
            "crm.contact.userfield.list", {"filter": {"FIELD_NAME": "UF_CRM_ELBA_ID"}}
        ) or []
        if not any(f.get("FIELD_NAME") == "UF_CRM_ELBA_ID" for f in contact_fields):
            uf_id = create_bitrix_userfield("contact", "UF_CRM_ELBA_ID", "ID Эльбы")
            logger.info(f"Создано поле для контактов UF_CRM_ELBA_ID: {uf_id}")

        company_fields = bitrix_call(
            "crm.company.userfield.list", {"filter": {"FIELD_NAME": "UF_CRM_ELBA_ID"}}
        ) or []
        if not any(f.get("FIELD_NAME") == "UF_CRM_ELBA_ID" for f in company_fields):
            uf_id = create_bitrix_userfield("company", "UF_CRM_ELBA_ID", "ID Эльбы")
            logger.info(f"Создано поле для компаний UF_CRM_ELBA_ID: {uf_id}")

        # Необязательное поле для ИНН (если планируем записывать)
        company_inn_fields = bitrix_call(
            "crm.company.userfield.list", {"filter": {"FIELD_NAME": "UF_CRM_INN"}}
        ) or []
        if not any(f.get("FIELD_NAME") == "UF_CRM_INN" for f in company_inn_fields):
            uf_id = create_bitrix_userfield("company", "UF_CRM_INN", "ИНН")
            logger.info(f"Создано поле для компаний UF_CRM_INN: {uf_id}")

    except Exception as e:
        logger.error(f"Ошибка проверки/создания пользовательских полей: {e}")
        raise


# ------------------------------- Elba API -------------------------------------

def elba_headers() -> Dict[str, str]:
    return {
        "X-Kontur-ApiKey": ELBA_TOKEN or "",
        "Accept": "application/json",
    }


def get_organization_id() -> str:
    try:
        resp = requests.get(
            f"{ELBA_API_BASE}/organizations",
            headers=elba_headers(),
            params={"limit": 1},
            timeout=(10, 60),
        )
        resp.raise_for_status()
        data = resp.json() or {}
        orgs = data.get("organizations") or data.get("items") or []
        if not orgs:
            raise RuntimeError("Не найдена собственная организация (GET /organizations)")
        return orgs[0].get("id") or orgs[0].get("organizationId")
    except Exception as e:
        logger.error(f"Ошибка получения organizationId: {e}")
        raise


def fetch_all_paginated(url: str, params: Dict[str, Any], item_keys: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    skip = 0
    limit = int(params.get("limit", 100))

    while True:
        local_params = dict(params)
        local_params.update({"skip": skip, "limit": limit})
        try:
            resp = requests.get(url, headers=elba_headers(), params=local_params, timeout=(10, 60))
            resp.raise_for_status()
            data = resp.json() or {}
            # Попробуем разные ключи ответов
            batch: List[Dict[str, Any]] = []
            for key in item_keys:
                if isinstance(data.get(key), list):
                    batch = data.get(key)  # type: ignore[assignment]
                    break
            if not batch and isinstance(data, list):
                batch = data  # иногда может прийти массив напрямую

            items.extend(batch)
            logger.debug(f"{url}: получено {len(batch)} (всего {len(items)})")
            if len(batch) < limit:
                break
            skip += limit
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", "?")
            logger.warning(f"{url}: HTTP {status}. Прерываю пагинацию: {e}")
            break
        except Exception as e:
            logger.warning(f"{url}: ошибка запроса: {e}")
            break

    return items


def get_elba_counterparties(organization_id: str) -> List[Dict[str, Any]]:
    """Пытаемся получить контрагентов по нескольким известным вариантам эндпоинтов."""
    candidate_endpoints = [
        f"{ELBA_API_BASE}/organizations/{organization_id}/counterparties",
        f"{ELBA_API_BASE}/organizations/{organization_id}/contractors",
        f"{ELBA_API_BASE}/counterparties",
        f"{ELBA_API_BASE}/contractors",
    ]

    for endpoint in candidate_endpoints:
        items = fetch_all_paginated(endpoint, {"limit": 100}, ["items", "counterparties", "contractors"])
        if items:
            logger.info(f"Контрагенты получены из {endpoint}: {len(items)}")
            return items
        logger.info(f"{endpoint} не вернул данные, пробуем следующий…")

    logger.error(
        "Не удалось получить список контрагентов из Эльбы. Проверьте права токена и документацию."
    )
    return []


def get_elba_contacts_for_counterparty(organization_id: str, counterparty_id: str) -> List[Dict[str, Any]]:
    """Пробуем несколько вариантов эндпоинтов для контактных лиц контрагента."""
    candidates = [
        f"{ELBA_API_BASE}/organizations/{organization_id}/counterparties/{counterparty_id}/contacts",
        f"{ELBA_API_BASE}/organizations/{organization_id}/contractors/{counterparty_id}/contacts",
        f"{ELBA_API_BASE}/counterparties/{counterparty_id}/contacts",
        f"{ELBA_API_BASE}/contractors/{counterparty_id}/contacts",
    ]

    for url in candidates:
        contacts = fetch_all_paginated(url, {"limit": 100}, ["items", "contacts"])
        if contacts:
            return contacts
    return []


# ----------------------------- Маппинг в Bitrix24 ------------------------------

def map_company_fields_from_cp(cp: Dict[str, Any]) -> Dict[str, Any]:
    title = cp.get("shortName") or cp.get("name") or cp.get("inn") or "Без названия"
    fields: Dict[str, Any] = {
        "TITLE": title,
        "UF_CRM_ELBA_ID": str(cp.get("id") or cp.get("counterpartyId") or cp.get("contractorId") or ""),
    }

    inn = cp.get("inn") or cp.get("INN")
    if inn:
        fields["UF_CRM_INN"] = inn

    # Общие контакты контрагента (если есть) — сохраняем как рабочие
    contact_info = cp.get("contactInfo") or {}
    phone = contact_info.get("phone") or contact_info.get("phoneNumber")
    email = contact_info.get("email") or contact_info.get("eMail")

    if phone:
        fields.setdefault("PHONE", []).append({"VALUE": str(phone), "VALUE_TYPE": "WORK"})
    if email:
        fields.setdefault("EMAIL", []).append({"VALUE": str(email), "VALUE_TYPE": "WORK"})

    return fields


def map_contact_fields_from_person(person: Dict[str, Any], cp: Dict[str, Any]) -> Dict[str, Any]:
    full_name = (
        person.get("fullName")
        or person.get("fio")
        or person.get("name")
        or f"Контакт {person.get('id') or ''}"
    )
    last_name, first_name, second_name = extract_name_parts(full_name)

    fields: Dict[str, Any] = {
        "LAST_NAME": last_name,
        "NAME": first_name,
        "SECOND_NAME": second_name,
        # Уникальность контакта — по связке ЭльбаID контрагента + ID контакта в Эльбе
        "UF_CRM_ELBA_ID": f"{str(cp.get('id') or cp.get('counterpartyId') or cp.get('contractorId') or '')}:{str(person.get('id') or '')}",
    }

    phone = person.get("phone") or person.get("phoneNumber")
    email = person.get("email") or person.get("eMail")

    if phone:
        fields.setdefault("PHONE", []).append({"VALUE": str(phone), "VALUE_TYPE": "WORK"})
    if email:
        fields.setdefault("EMAIL", []).append({"VALUE": str(email), "VALUE_TYPE": "WORK"})

    return fields


# ------------------------ Поиск существующих в Bitrix24 -----------------------

def find_existing_by_elba_ids(entity_type: str, elba_ids: List[str]) -> Dict[str, str]:
    """Возвращает мапу elba_id -> bitrix_id для уже существующих сущностей."""
    if not elba_ids:
        return {}
    method = f"crm.{entity_type}.list"
    result: Dict[str, str] = {}

    # Bitrix ограничивает размер страницы; пройдемся чанками
    for group in chunked(elba_ids, 50):
        res = bitrix_call(
            method,
            {
                "filter": {"UF_CRM_ELBA_ID": [str(x) for x in group]},
                "select": ["ID", "UF_CRM_ELBA_ID"],
            },
        ) or []
        for row in res:
            key = str(row.get("UF_CRM_ELBA_ID"))
            if key:
                result[key] = str(row.get("ID"))

    return result


# --------------------------------- Создание -----------------------------------

def create_company(fields: Dict[str, Any]) -> str:
    return str(bitrix_call("crm.company.add", {"fields": fields}))


def create_contact(fields: Dict[str, Any]) -> str:
    return str(bitrix_call("crm.contact.add", {"fields": fields}))


# -------------------------------- Основной ход --------------------------------

def main() -> None:
    try:
        logger.info("Проверяю пользовательские поля в Bitrix24…")
        ensure_userfields()

        logger.info("Получаю organizationId в Эльбе…")
        org_id = get_organization_id()
        logger.info(f"organizationId: {org_id}")

        logger.info("Запрашиваю контрагентов из Эльбы…")
        counterparties = get_elba_counterparties(org_id)
        logger.info(f"Получено контрагентов: {len(counterparties)}")
        if not counterparties:
            return

        # Идентификаторы для компаний
        company_elba_ids: List[str] = [
            str(cp.get("id") or cp.get("counterpartyId") or cp.get("contractorId") or "")
            for cp in counterparties
        ]
        company_elba_ids = [eid for eid in company_elba_ids if eid]
        existing_companies = find_existing_by_elba_ids("company", company_elba_ids)

        # Итерируем контрагентов, создаём компании и контактные лица
        for cp in tqdm(counterparties, desc="Синхронизация компаний и контактов"):
            cp_elba_id = str(
                cp.get("id") or cp.get("counterpartyId") or cp.get("contractorId") or ""
            )
            if not cp_elba_id:
                logger.warning("Пропуск контрагента без ID")
                continue

            # Компания
            company_id: Optional[str] = existing_companies.get(cp_elba_id)
            if company_id:
                logger.debug(f"Компания уже существует Elba={cp_elba_id} → Bitrix={company_id}")
            else:
                company_fields = map_company_fields_from_cp(cp)
                company_id = create_company(company_fields)
                existing_companies[cp_elba_id] = company_id
                logger.info(f"Создана компания Bitrix ID={company_id} для Elba={cp_elba_id}")

            # Контактные лица
            persons = []
            # 1) Явный список в карточке
            for key in ("contacts", "contactPersons", "persons"):
                if isinstance(cp.get(key), list) and cp.get(key):
                    persons = cp.get(key)  # type: ignore[assignment]
                    break
            # 2) Отдельный запрос, если внутри нет
            if not persons:
                persons = get_elba_contacts_for_counterparty(org_id, cp_elba_id)

            if not persons:
                continue

            # Соберём ID для поиска имеющихся контактов
            contact_elba_ids = [
                f"{cp_elba_id}:{str(p.get('id') or p.get('personId') or '')}"
                for p in persons
                if (p.get("id") or p.get("personId"))
            ]
            existing_contacts = find_existing_by_elba_ids("contact", contact_elba_ids)

            for person in persons:
                pid = str(person.get("id") or person.get("personId") or "")
                if not pid:
                    continue
                uniq = f"{cp_elba_id}:{pid}"
                if uniq in existing_contacts:
                    logger.debug(f"Контакт уже существует Elba={uniq} → Bitrix={existing_contacts[uniq]}")
                    continue

                contact_fields = map_contact_fields_from_person(person, cp)
                # Привяжем к компании
                if company_id:
                    contact_fields["COMPANY_ID"] = company_id
                cid = create_contact(contact_fields)
                logger.info(f"Создан контакт Bitrix ID={cid} для Elba={uniq}")

        logger.info("Синхронизация завершена")

    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}")
        raise


if __name__ == "__main__":
    main()