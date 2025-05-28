import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs
from typing import Optional
from avito import fetch_and_process_items
from auto import parse_auto
from avito_id import get_avito_links_with_seller_id
import asyncio
import time
import re
from logg import get_logger

logger = get_logger(__name__)


# Авторизация через словарь credentials
CREDENTIALS_DICT = {
    "type": "service_account",
    "project_id": "excelproject-455714",
    "private_key_id": "91bee9e13a81b39e2fed4a8d0247495d889005b4",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCSmZKio6dVZCD/\n3zMg0JipTmiCQY/VoJo4BAjLgTGMnJVCqB7w4sYBr+Qcb+XFQ77+RLVtFKeTnFG1\n5JDYNZvvIgWNg+vl6JiXeBrGjihpykMQqROXTttJ5Dk38VZ3PpMyEE6miivFDuZC\nB+8AcJUsIr15i3Vv0TaW13lF0R+2PVaRffFlKDS+NLkG5LKfsY8r+jbN8FiVbdLR\nmxzdT2OROewYxEJcvZn/4EywmMsLCrXb1tP7h35+nz5JPp0xVbZK6WM0G4G2WAgw\nl13WJ4cgcLek/gk7JiH50UdOEFRz6INsg+oMo3e6LKt/moOwjdhtmZ8gYW9pyKLm\nVhIeUs9tAgMBAAECggEAB19x4HwajlDa2AOBrsTO6LTozKQ/d578IpURXCrDMy8s\n0o1iAPtmue7+qe92vtUJZgBOn43KX8Ic2ekE39rSXNR5MwTPeGCaTAPeVN4RakCh\n3tSiB5oPrUetGQMqNhUPkhT/36BTUzXMnsWHX55J4k5c+R/qaBU9iZiMoNZDogag\nMW6G9qSoJeN4BvqNqR+ICOdTt9dCKHRxkByaUPTwksDsCWHo297U4BPcWsPjgP4Z\nJy4xbdTTOh4Ui7raB2KYd7ID2Js/WQ1fqq06VFU2mJo/2kCldL6f8d71OihPZL+4\nnqoCdg/1mju1kHKOxKye1DR336RGiMEasFnsNgUgAQKBgQDLhwkaqQ/5RO7xOuFE\nfbQ0acklAO7dhXeIomxV9UbdTn+Hct26YBLonSIM3/cYqu0AjJFpu1FrQmIuNBgS\nQcmETar4eiHdB/MhI/p6bB9CaMK2NAsXMnxb4hbTSHR99leLWuGoVE8PQRhhEIQ3\n23b5pyRTUQLFcCtirzOJv/JFQQKBgQC4ZUY0x56KMErpyBW17MVXnjPRPnu7tLsa\nm3ES5tkurD43HB5Hib1GmL6GwbM6sgLSuZMJoKhSsjH7b+px6Fp/ey/ZvwQ1vs3b\nPVHvuFvsp3VtNUKAhxp/OB/OCEkTnLP4Mb2CVfeKejhYZv/8+rqjD4XBjI/knQ9Q\nQfYP1FTjLQKBgQCEAP8spX5QxB7dory8eXNJk1r8fxBt6MTQf9gYIE9n9iPMq/mX\nifx5loChLRnMi//PnVwq4W07TgDzyqHaJYUYJG/BXSVdgGx2kClDAaF8pwmytyqC\nTyJNTeRUAOhdUksRfU5iqNvmHug6/EVlHRibb4al6yMK/2eER/H7Y900gQKBgQCL\nzu2uMvQ33mnOW5BqgX0W87JiIjf6mAuNHvJa3IEq7Bm3+y/SGdNS5Zj/33mfNT0C\nvQWJNTCqksVm2PIvL3b+VU5wkG4GuganBhVL5sJ76nQUO1+Sx90FPG6Q7qNJpXSm\n6D/BxKCNdCGolV/eVdSQscI+f+7R7Wug9II2ek1qeQKBgHg6ODKqZdrD+cxGJ9O7\ntmyrfrcrRJx+sIHJARNegvzilMmwqy85I0JSEfi3uYCsN/s4b5O5s1rSRAw50Di6\nDTa6zmCmBvgE5qVsB4KObzPNAkbkCRJ/BWbBRYs9Ta2x6JcZaM5reiGb+YFWQ7IL\nK/EGIF58hknnXCowedGKKGfR\n-----END PRIVATE KEY-----\n",
    "client_email": "drive-bot@excelproject-455714.iam.gserviceaccount.com",
    "client_id": "115902541194622230230",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-bot%40excelproject-455714.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

SPREADSHEET_ID = "1c3yRBMlcOmzK_g8FWCVD_rztGMXfjsuhF12YVVYNU94"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(CREDENTIALS_DICT, SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SPREADSHEET_ID)


def filter_rows_by_seller(data: List[List[str]], seller_id: str) -> List[List[str]]:
    """
    Удаляет из списка списков все строки, у которых seller_id находится в 9-м столбце (индекс 8).
    Первая строка (заголовки) сохраняется.

    :param data: Таблица в виде списка списков
    :param seller_id: ID продавца, который нужно удалить
    :return: Новый список списков без строк с этим seller_id
    """
    if not data:
        return []

    filtered = [row for row in data if len(row) > 8 and row[8] != seller_id]
    return filtered


def convert_dicts_to_rows(data: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Преобразует список словарей в список списков с заголовками.

    Порядок полей:
    Марка(mark), Год(year), Цвет(color), Модель(model),
    Цена без ндс(price), Цена с ндс(price_with_markup),
    Артикул(id), Ссылка(link), ID продавца(seller)
    """
    headers = [
        ("Марка", "mark"),
        ("Год", "year"),
        ("Цвет", "color"),
        ("Модель", "model"),
        ("Цена без НДС", "price"),
        ("Цена с НДС", "price_with_markup"),
        ("Артикул", "id"),
        ("Ссылка", "link"),
        ("ID продавца", "seller"),
    ]

    result = [[name for name, _ in headers]]  # заголовки
    for item in data:
        row = [item.get(key, "") for _, key in headers]
        result.append(row)

    return result


def group_by_mark(data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Группирует список словарей по ключу 'mark'.

    :param data: Список словарей, каждый с ключом 'mark'
    :return: Словарь {марка: [словарь1, словарь2, ...]}
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for item in data:
        mark = item.get('mark', 'UNKNOWN')
        grouped.setdefault(mark, []).append(item)

    return grouped


def sheet_exists(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> bool:
    """
    Проверяет, существует ли лист с таким названием в таблице.

    :param spreadsheet: Объект gspread.Spreadsheet
    :param sheet_name: Название листа
    :return: True, если лист существует, иначе False
    """
    return any(ws.title == sheet_name for ws in spreadsheet.worksheets())


def extract_seller_id(url: str) -> Optional[str]:
    """
    Определяет seller_id из ссылки Avito или Auto.ru.
    Если ссылка не распознана или параметр не найден, возвращает None.
    """
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # Avito: sellerId в query-параметрах
    if 'avito.ru' in domain:
        params = parse_qs(parsed.query)
        seller_list = params.get('sellerId') or params.get('sellerid')  # на всякий случай
        return seller_list[0] if seller_list else None

    # Auto.ru: seller_id — часть пути после 'reseller'
    if 'auto.ru' in domain:
        # Пример пути: /reseller/3tyBCKbkMS4Uwo1iy0GVpaR1qCiB5CZo/all/...
        parts = parsed.path.strip('/').split('/')
        if 'reseller' in parts:
            idx = parts.index('reseller')
            if idx + 1 < len(parts):
                return parts[idx + 1]
        return None

    # Не тот домен
    return None


def safe_get_worksheets(spreadsheet, retries: int = 3, delay: float = 2.0):
    for attempt in range(retries):
        try:
            return spreadsheet.worksheets()
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при получении списка листов (попытка {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Не удалось получить список листов после нескольких попыток")


def overwrite_sheets(spreadsheet: gspread.Spreadsheet, grouped_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Полностью пересоздаёт все листы в таблице, кроме 'Sellers'.
    Каждой марке создаётся свой лист с данными.
    """
    # 1. Удаляем все листы кроме 'Sellers'
    for sheet in safe_get_worksheets(spreadsheet):
        if sheet.title != "Sellers":
            try:
                spreadsheet.del_worksheet(sheet)
                logger.info(f"🗑️ Удалён лист: {sheet.title}")
                time.sleep(0.5)  # чтобы не упереться в квоты
            except Exception as e:
                logger.error(f"❌ Не удалось удалить лист '{sheet.title}': {e}", exc_info=True)

    # 2. Создаём и записываем новые листы по маркам
    for mark, items in grouped_data.items():
        logger.info(f"⏳ Создаём лист: {mark}")
        rows = convert_dicts_to_rows(items)

        try:
            rows_count = len(rows)
            cols_count = len(rows[0]) if rows else 9
            ws = spreadsheet.add_worksheet(title=mark, rows=str(rows_count), cols=str(cols_count))
            logger.info(f"➕ Создан лист '{mark}'")
        except Exception as e:
            logger.error(f"❌ Ошибка при создании листа '{mark}': {e}", exc_info=True)
            continue

        try:
            ws.update('A1', rows)
            logger.info(f"📤 Данные записаны в '{mark}' — {len(rows) - 1} строк")
        except Exception as e:
            logger.error(f"❌ Ошибка при записи данных в '{mark}': {e}", exc_info=True)


def get_seller_links(spreadsheet: gspread.Spreadsheet) -> List[str]:
    """
    Открывает лист 'Seller' и возвращает все валидные ссылки из первого столбца.

    Учитываются только строки, которые начинаются с http или https.

    :param spreadsheet: объект Google Spreadsheet
    :return: список валидных ссылок
    """
    try:
        ws = spreadsheet.worksheet("Sellers")
        column_values = ws.col_values(1)  # первый столбец (A)

        # Регулярка на http/https ссылки
        url_regex = re.compile(r'^https?://')

        # Возвращаем только строки, которые являются ссылками
        return [link.strip() for link in column_values if url_regex.match(link.strip())]

    except gspread.exceptions.WorksheetNotFound:
        logger.error("❌ Лист 'Seller' не найден.")
        return []


def run_pipeline(spreadsheet: gspread.Spreadsheet):
    """
    Главный пайплайн: достаёт ссылки из листа Sellers,
    парсит Avito и Auto, группирует и заливает в таблицу.
    """
    logger.info("🚀 Запуск парсера...")
    start = time.time()

    raw_links = get_seller_links(spreadsheet)
    logger.info(f"🔗 Найдено ссылок: {len(raw_links)}")

    auto_links = [link for link in raw_links if 'auto.ru' in link]
    raw_avito_links = [link for link in raw_links if 'avito.ru' in link]

    # Обновляем ссылки с sellerId для Avito
    logger.info("🧠 Обновляем Avito-ссылки через Playwright...")
    try:
        avito_links = asyncio.run(get_avito_links_with_seller_id(raw_avito_links))
        logger.info(f"🔄 Обновлено Avito-ссылок: {len(avito_links)}")
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении Avito-ссылок: {e}", exc_info=True)
        avito_links = []

    all_data: List[Dict[str, Any]] = []

    # Парсинг Avito
    for link in avito_links:
        logger.info(f"[Avito] 🔍 Парсим: {link}")
        seller_id = extract_seller_id(link)
        if not seller_id:
            logger.warning(f"[Avito] ❌ Не удалось извлечь seller_id из {link}")
            continue
        try:
            data = fetch_and_process_items(seller_id)
            all_data.extend(data)
            logger.info(f"[Avito] ✅ Найдено записей: {len(data)}")
        except Exception as e:
            logger.error(f"[Avito] ❌ Ошибка при парсинге seller_id={seller_id}: {e}", exc_info=True)

    # Парсинг Auto.ru
    for link in auto_links:
        logger.info(f"[Auto] 🔍 Парсим: {link}")
        shop_id = extract_seller_id(link)
        if not shop_id:
            logger.warning(f"[Auto] ❌ Не удалось извлечь shop_id из {link}")
            continue
        try:
            data = parse_auto(shop_id)
            all_data.extend(data)
            logger.info(f"[Auto] ✅ Найдено записей: {len(data)}")
        except Exception as e:
            logger.error(f"[Auto] ❌ Ошибка при парсинге shop_id={shop_id}: {e}", exc_info=True)

    if not all_data:
        logger.warning("⚠️ Нет данных для загрузки. Парсинг завершён без результатов.")
        return

    logger.info(f"📊 Всего записей собрано: {len(all_data)}")
    grouped_data = group_by_mark(all_data)
    overwrite_sheets(spreadsheet, grouped_data)

    duration = round(time.time() - start, 2)
    logger.info(f"✅ Готово. Обновление завершено за {duration} сек.")


if __name__ == '__main__':
    ss = gc.open_by_key(SPREADSHEET_ID)
    run_pipeline(ss)


