from playwright.async_api import Page, async_playwright, TimeoutError as AsyncTimeoutError
import asyncio
import random
from geetest import PuzzleCaptchaSolver
import os
from urllib.parse import urlparse
from uuid import uuid4
from html import unescape
import re
from curl_cffi import requests
from logg import get_logger

logger = get_logger(__name__)
solve_captcha_async_counter = 0


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


def trim_avito_shop_url(url: str) -> str:
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")

    if len(parts) >= 3 and parts[0] == "brands":
        # Возвращаем до /brands/<название_магазина>
        return f"{parsed.scheme}://{parsed.netloc}/brands/{parts[1]}"
    else:
        return url  # если структура не подходит — вернём как есть


async def get_id(links: list[str]) -> list[str]:
    from asyncio import Semaphore, gather
    result_links = []
    semaphore = Semaphore(2)  # максимум 2 вкладки одновременно

    async def process_link(context, link: str):
        async with semaphore:
            page = await context.new_page()
            try:
                await page.goto(link, timeout=60000)

                captcha_result = await solve_captcha_async(page)
                if not captcha_result:
                    logger.info(f"Капча не была обнаружена или не решена: {link}")

                for _ in range(30):  # максимум 60 секунд ожидания sellerId
                    current_url = page.url
                    if 'sellerId=' in current_url:
                        result_links.append(current_url)
                        logger.info(f"Найден sellerId: {current_url}")
                        return
                    await asyncio.sleep(1)

                logger.warning(f"Не найден sellerId в течение таймаута: {link}")
            except Exception as e:
                logger.error(f"Ошибка при обработке {link}: {e}", exc_info=True)
            finally:
                await page.close()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()  # один общий контекст (одно окно)
        await gather(*[process_link(context, link) for link in links])
        await context.close()
        await browser.close()

    return result_links


async def get_avito_links_with_seller_id(raw_links: list[str]) -> list[str]:
    """
    Принимает список оригинальных ссылок Avito (магазины),
    возвращает ссылки с параметром ?sellerId=...
    """
    trimmed = [trim_avito_shop_url(link) for link in raw_links]
    updated = await get_id(trimmed)
    return list(set(updated))


async def main():
    links = [
        'https://www.avito.ru/brands/i342952992/items/all?s=search_page_share&sellerId=3b2a3bcf1dc3ebcf05de2a2536e5b44a']
    await get_avito_links_with_seller_id(links)


if __name__ == '__main__':
    asyncio.run(main())