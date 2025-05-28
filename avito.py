import time
from logg import get_logger
from playwright.async_api import Page, async_playwright, TimeoutError as AsyncTimeoutError
import asyncio
from urllib.parse import urlparse, parse_qs, urlsplit, urlunsplit
import random
from typing import List, Dict, Optional, Any
from curl_cffi import requests
from geetest import PuzzleCaptchaSolver
from html import unescape
import re
import os
from uuid import uuid4
from model import extract_brand_model

# Инициализируем логгер для этого модуля
logger = get_logger(__name__)

MAX_TABS = 2
CAPTCHA_RETRIES = 3  # всего попыток решить капчу
solve_captcha_async_counter = 0


def strip_url_params(url: str) -> str:
    """
    Убирает из URL все GET-параметры и фрагмент (часть после #).
    Возвращает только схему, хост и путь.
    """
    try:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
    except Exception as e:
        logger.error(f"Ошибка удаления параметров из URL {url}: {e}", exc_info=True)
        return url


def extract_year(text: str) -> Optional[int]:
    """
    Ищет в тексте четырехзначное число в диапазоне 1900–2099 и возвращает его как int.
    Если год не найден — возвращает None.
    """
    try:
        match = re.search(r'\b(19|20)\d{2}\b', text)
        if match:
            return int(match.group(0))
    except Exception as e:
        logger.warning(f"Не удалось извлечь год из '{text}': {e}")
    return None


def extract_url(css_string: str) -> str:
    """Извлекает URL из CSS-свойства, например: url("https://example.com/image.jpg")"""
    try:
        decoded = unescape(css_string)
        pattern = r'url\("([^\"]+)"\)'
        match = re.search(pattern, decoded)
        return match.group(1) if match else ""
    except Exception as e:
        logger.error(f"Ошибка извлечения URL из CSS '{css_string}': {e}", exc_info=True)
        return ""


def download_file(url: str, save_path: str):
    try:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        logger.info(f"Файл загружен: {save_path}")
    except Exception as e:
        logger.error(f"Ошибка загрузки {url} в {save_path}: {e}", exc_info=True)


async def safe_get_title(page: Page) -> str:
    try:
        return await page.title()
    except Exception:
        try:
            return await page.evaluate("() => document.title")
        except Exception as e:
            logger.warning(f"Не удалось получить заголовок страницы: {e}")
            return ""


async def solve_captcha_async(page: Page) -> bool:
    global solve_captcha_async_counter
    pictures_dir = "pictures"
    os.makedirs(pictures_dir, exist_ok=True)
    files_to_remove = []

    try:
        title = await safe_get_title(page)
        if 'Авито' in title:
            solve_captcha_async_counter += 1
            logger.debug(f"{solve_captcha_async_counter}) Капча не обнаружена, продолжаем.")
            return True

        logger.info("Обнаружена капча, пытаемся решить...")
        try:
            await page.click('.form-action')
            await page.click('.button')
        except Exception:
            logger.debug("Начальные кнопки капчи не найдены, продолжаем.")
        await asyncio.sleep(2)

        bg_elem = await page.wait_for_selector('.geetest_bg', timeout=10000)
        style_bg = await bg_elem.get_attribute('style') or ''
        bg_url = extract_url(style_bg)
        bg_path = os.path.join(pictures_dir, f"{uuid4()}.png")
        files_to_remove.append(bg_path)
        download_file(bg_url, bg_path)

        slice_elem = await page.wait_for_selector('.geetest_slice_bg', timeout=10000)
        style_slice = await slice_elem.get_attribute('style') or ''
        slice_url = extract_url(style_slice)
        slice_path = os.path.join(pictures_dir, f"{uuid4()}.png")
        files_to_remove.append(slice_path)
        download_file(slice_url, slice_path)

        result_path = os.path.join(pictures_dir, f"{uuid4()}.png")
        files_to_remove.append(result_path)

        solver = PuzzleCaptchaSolver(
            gap_image_path=slice_path,
            bg_image_path=bg_path,
            output_image_path=result_path
        )
        offset = solver.discern() - 10

        slider = await page.wait_for_selector('.geetest_btn', timeout=10000)
        box = await slider.bounding_box()
        if not box:
            logger.error("Ползунок капчи не найден на странице.")
            return False

        start_x = box['x'] + box['width'] / 2
        start_y = box['y'] + box['height'] / 2
        await page.mouse.move(start_x, start_y)
        await page.mouse.down()
        await page.mouse.move(start_x + offset - random.randint(1, 2), start_y, steps=10)
        await page.mouse.up()

        await asyncio.sleep(3)
        try:
            await page.wait_for_load_state('domcontentloaded', timeout=10000)
        except Exception:
            pass
        await asyncio.sleep(1)

        final_title = await safe_get_title(page)
        if 'Авито' in final_title:
            solve_captcha_async_counter += 1
            logger.info(f"{solve_captcha_async_counter}) Капча успешно решена.")
            return True

        logger.warning(f"{solve_captcha_async_counter}) Не удалось решить капчу.")
        return False

    except AsyncTimeoutError:
        logger.error("Таймаут при ожидании элементов капчи.")
        return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при решении капчи: {e}", exc_info=True)
        return False
    finally:
        for fpath in files_to_remove:
            try:
                if os.path.exists(fpath):
                    os.remove(fpath)
            except Exception:
                logger.debug(f"Не удалось удалить временный файл {fpath}")


async def enrich_data(items: List[Dict]) -> List[Dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        sem = asyncio.Semaphore(MAX_TABS)

        async def worker(item: Dict) -> Dict:
            async with sem:
                page: Page = await context.new_page()
                try:
                    try:
                        await page.goto(item['link'], timeout=60000, wait_until='domcontentloaded')
                    except Exception as e:
                        logger.error(f"Ошибка перехода по ссылке {item['link']}: {e}", exc_info=True)
                        item['color'] = None
                        return item

                    for attempt in range(1, CAPTCHA_RETRIES + 1):
                        if await solve_captcha_async(page):
                            break
                        logger.warning(f"Попытка {attempt} решить капчу не удалась для {item['link']}")
                        await asyncio.sleep(1)
                    else:
                        logger.error(f"Все попытки решить капчу не удались для {item['link']}")
                        item['color'] = None
                        return item

                    try:
                        await page.wait_for_selector("li[class*='params-paramsList']", timeout=10000)
                        elems = await page.query_selector_all("li[class*='params-paramsList']")
                        color = 'Нет данных'
                        for el in elems:
                            text = (await el.text_content()).lower() or ''
                            if 'цвет' in text:
                                parts = text.split(':', 1)
                                if len(parts) == 2:
                                    color = parts[1].strip()
                                break
                        item['color'] = color
                    except Exception as e:
                        logger.error(f"Ошибка при извлечении цвета для {item['link']}: {e}", exc_info=True)
                        item['color'] = None
                    return item
                except Exception as e:
                    logger.error(f"Ошибка в обработчике для {item['link']}: {e}", exc_info=True)
                    item['color'] = None
                    return item
                finally:
                    await page.close()

        results = await asyncio.gather(*(worker(it) for it in items), return_exceptions=False)
        await browser.close()
        return results


def extract_data(item: dict) -> dict:
    try:
        article = item.get('id')
        title = item.get('title', '')
        year = extract_year(title)

        raw = item.get('priceDetailed', {}).get('value')
        price = None
        price_with_markup = None  # Для цены с наценкой

        if raw is not None:
            try:
                price = int(raw)
                price_with_markup = int(price * 1.2)  # Цена с наценкой 20%
            except (TypeError, ValueError) as e:
                logger.warning(f"Не удалось преобразовать цену '{raw}': {e}")

        try:
            data = extract_brand_model(title)
        except Exception as e:
            logger.error(f"Ошибка при получении бренда/модели из '{title}': {e}", exc_info=True)
            data = {'brand': None, 'model': None}

        raw_brand = data.get('brand')
        raw_model = data.get('model')

        # приводим бренд
        if raw_brand and 'mercedes' in raw_brand.lower():
            brand = "MERCEDES"
        else:
            brand = raw_brand.upper() if raw_brand else None

        # модель кладём прямо
        model = raw_model

        link = strip_url_params('https://www.avito.ru' + item.get('urlPath', ''))

        return {
            'id': article,
            'title': title,
            'year': year,
            'price': price,  # Оригинальная цена
            'price_with_markup': price_with_markup,  # Цена с наценкой
            'link': link,
            'mark': brand,
            'model': model,
        }
    except Exception as e:
        logger.error(f"Ошибка извлечения данных из элемента: {e}\nЭлемент: {item}", exc_info=True)
        return {
            'id': article,
            'title': item.get('title'),
            'year': None,
            'price': None,
            'price_with_markup': None,  # Если не удалось извлечь цену, оставляем None
            'link': None,
            'mark': None,
            'model': None,
        }


def fetch_avito_items(seller_id: str) -> List[dict]:
    items: list[dict] = []
    page = 1
    logger.info(f"Начинаем загрузку объявлений для sellerId={seller_id}")
    while True:
        logger.debug(f"Запрашиваю страницу {page}")
        params = {"p": page, "sellerId": seller_id}
        try:
            resp = requests.get(
                "https://www.avito.ru/web/1/profile/items",
                params=params,
                impersonate="chrome120"
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Ошибка при запросе страницы {page}: {e}", exc_info=True)
            break

        total = data.get("totalCount")
        page_items = data.get("catalog", {}).get("items", [])

        if not data.get('status') and len(page_items) == 0:
            logger.error("Товары отсутствуют, прерываю выполнение.")
            break

        logger.info(f"Страница {page}: получено {len(page_items)} из {total} объявлений")
        items.extend(page_items)

        if len(items) >= total:
            logger.info(f"Собрано все объявления ({len(items)} из {total})")
            break

        page += 1
        logger.debug("Жду 15 секунд перед следующим запросом...")
        time.sleep(15)

    return items


def fetch_and_process_items(seller_id: str) -> List[Dict[str, Any]]:
    """
    Получает и обрабатывает все офферы продавца по seller_id.
    """
    try:
        # Получаем сырые офферы по seller_id
        raw_items = fetch_avito_items(seller_id)
        extracted: List[Dict[str, Any]] = []

        # Извлекаем нужные поля и логируем прогресс
        for i, item in enumerate(raw_items, start=1):
            extracted.append(extract_data(item))
            logger.info(f'Обработана машина №{i}')

        # Обогащаем данные дополнительными полями
        enriched = asyncio.run(enrich_data(extracted))

        # Добавляем seller_id в каждую запись
        for rec in enriched:
            rec['seller'] = seller_id

        return enriched

    except Exception as e:
        logger.error(f"Ошибка в fetch_and_process_items для seller_id {seller_id}: {e}", exc_info=True)
        return []



if __name__ == '__main__':
