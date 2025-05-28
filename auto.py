import time
from curl_cffi import requests
from logg import get_logger
from color import get_color_json
import pprint
from urllib.parse import urlparse


# получаем логгер для этого модуля
logger = get_logger(__name__)

COOKIES = {
    'suid': 'e13849a258e7ae5e79caeb87d11a43f1.8de5d4bf6fd420fcfe3e535f9678e6d8',
    'autoruuid': 'g681b3f092dhtgk7fj093fm31jiva40a.a7174c6b95fbef111c068faaf940cc46',
    'gids': '971',
    'L': 'ACcJAEFSXlFnVE96Aw0EbEJgDXdQB191MycjMVZbGVQTMgEa.1746381440.16139.340315.907c6f530a5bb536649f93da8ff1f020',
    'crookie': 'qxWZswCP2xgF03xvOu1xjbXh3zuFmovNUJQNLMLBnOw352IisAlylxptCrzbCDrGZONCZE+f5CfrJ2FI4nKOuKc3Ec8=',
    'cmtchd': 'MTc0NjYxNjA3Mzg0NA==',
    '_ym_uid': '1746616076572343877',
    'fp': '48433b09ab5d409af891dcb22b7050df%7C1746616076688',
    'gradius': '100',
    '_csrf_token': 'b2191f78193448caaab5b8560ec0e6f9ff1bf56d08eaa5c1',
    'Session_id': '3:1746797775.5.0.1746381440940:sWuuXw:3544.1.2:1|1913289149.-1.2.3:1746381440.6:2096839587.7:1746381440|61:10031876.647899.KfTuvqpQRxnbSYGODNMH48usWdA',
    'sessar': '1.1201.CiA4uChZxd2WPFYU7cAiPQ45eLQ_oZiUAlObAeADb72kYQ.KCZDdk0z1lRlWCwia5qkMymrd566bHyIf7rokOvay9k',
    'yandex_login': 'alliakhmedov',
    'ys': 'udn.cDrQkNC70Lgg0JDRhdC80LXQtNC%2B0LI%3D#c_chck.12534923',
    'i': 'hQEVPNwxctZgFQo4Llyz4OKJBCoH0qQexDmSSgokaDdLrcgVFMuSywI7dnQgfBdc03xiDTAwAxTsbQQAFEZQ6Aa5RXo=',
    'yandexuid': '605221281718812686',
    'mda2_beacon': '1746797775592',
    'sso_status': 'sso.passport.yandex.ru:synchronized',
    '_ym_isad': '2',
    'yaPassportTryAutologin': '1',
    'from': 'direct',
    'autoru_sid': '104598072%7C1746801376232.3600.YmdXfqBh9C4Nzx95gNzlWw.ZTTWpbpfjG_200VvnC0Ln2Vs61fhjNP_wu2odkl8w6Y',
    'autoru-visits-count': '3',
    'autoru-visits-session-unexpired': '1',
    'gdpr': '0',
    '_ym_visorc': 'b',
    'spravka': 'dD0xNzQ2ODAxNzA3O2k9MTc2LjExMy45MS4xNDk7RD1DMjY4QzM4RDQ3MUUwRkU1MTAzMDkwQTA3RUNDRTZBRTJCQzE0MEFDQjRFRjFEQkVCRTBBNDU0MzEzODVBQjk0NDcwNzZFNTBFNjVERkVENUIwRjRFN0VEMjA0MzAxRkZBNDUxNDFFRDUxN0RFRkZDODI4MDBEOThFQTcwMUUxREEzQUJDOTAwRTAxRjU1MkEyRjAwNTMxQ0U2O3U9MTc0NjgwMTcwNzY0OTI0MjkxOTtoPWY4MDc5MGFjZGI2Y2U2NDllMTM3ZTRkMmMxZjVjNmRl',
    '_yasc': 'jCgmKdTUQNyLlYyieeTC6NSN/g9Rajo0z26kDp0heehZKWa2r3GAkerYhne9DlOD3Jq6MecqRQop',
    'layout-config': '{"screen_height":810,"screen_width":1440,"win_width":755,"win_height":675}',
    'autoru_crashreport': '{%22route_name%22:%22reseller-public-page%22%2C%22app_id%22:%22af-desktop-search%22%2C%22time_spent%22:%221%22%2C%22chat_indicator_unread%22:false%2C%22billing_opened%22:false}',
    'cycada': '+fTBvLO/rpsu8vqYIVfISkOrJsvSWtGyZ0GWMgNn//8=',
    '_ym_d': '1746802146',
    'count-visits': '9',
    'yaPlusUserPlusBalance': '{%22id%22:%22104598072%22%2C%22plusPoints%22:24}',
    'from_lifetime': '1746802148636',
}

HEADERS = {
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://auto.ru',
    'priority': 'u=1, i',
    'referer': 'https://auto.ru/reseller/3tyBCKbkMS4Uwo1iy0GVpaR1qCiB5CZo/all/?status=ACTIVE&sort=cr_date-desc&page=8',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'same-origin',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'x-client-app-version': '371.0.16498906',
    'x-client-date': '1746802150372',
    'x-csrf-token': 'b2191f78193448caaab5b8560ec0e6f9ff1bf56d08eaa5c1',
    'x-page-request-id': '53eee4f62327d84e53677bf0a3c7296f',
    'x-requested-with': 'XMLHttpRequest',
    'x-retpath-y': 'https://auto.ru/reseller/3tyBCKbkMS4Uwo1iy0GVpaR1qCiB5CZo/all/?status=ACTIVE&sort=cr_date-desc&page=8',
    'x-yafp': '{"a1":"FOmeQPiOeB+5gw==;0","a2":"po/+jOi0BpEV3UWDv+Jy+wqwPtgjaQ==;1","a3":"9R0QPBRlHQf3+jDFghH4SQ==;2","a4":"u8tgFniUWboa7NieXEkugFGj9wWvMKUuPpRdF1MdQ0NQ9g==;3","a5":"uHOanKEaztEJRQ==;4","a6":"Hr8=;5","a7":"L1PAReucNosmDg==;6","a8":"MbZ9WuHXkjE=;7","a9":"fQlO7d+irbGJjg==;8","b1":"s5ZE3z6hdqM=;9","b2":"c9N6INuBAy+0cw==;10","b3":"8qrulGTc8w/iwQ==;11","b4":"Uv675XdJgjY=;12","b5":"SiPPWVhxy77aHw==;13","b6":"zC318Mrej4TeWQ==;14","b7":"honIeT9AWD82iQ==;15","b8":"+mKaiOqarWKOPQ==;16","b9":"D5Gfdm4mf7bkYg==;17","c1":"UXYkAA==;18","c2":"SUFFEV+sh+Zs6S+Lgoj0TQ==;19","c3":"5PAE8X/u7YPW1yX96VTyjQ==;20","c4":"jlVw2rSaIl8=;21","c5":"sT6rrXX2rj4=;22","c6":"Y+gDPg==;23","c7":"/3t20cb3YjQ=;24","c8":"lns=;25","c9":"2uss5Di12wI=;26","d1":"nNZrccLKPOo=;27","d2":"d6kZnw==;28","d3":"WFK3kl7mKFbYWw==;29","d4":"aN7PIpuDp94=;30","d5":"aSWNXbbB21HTpw==;31","d7":"5UYKCxnJkTk=;32","d8":"gTE+LGND/WI6CZHAzAH3AKibYFofIbAoVF8=;33","d9":"+khM9qtSsAU=;34","e1":"msV+7vzxV2Qb2w==;35","e2":"p1cR1GbSXU4=;36","e3":"p3EZ3NgNGUc=;37","e4":"Y6KKti+Mba8=;38","e5":"qLtxOkI0i/xs3A==;39","e6":"kVlGzC9azZY=;40","e7":"6tWSD9zffSHszw==;41","e8":"InntVaE100U=;42","e9":"wHMaOIWrH5I=;43","f1":"Mepk+/bA9N4aLw==;44","f2":"rbLxQveu67U=;45","f3":"r+3yf9qh7Qu2QA==;46","f4":"7RLjU6h0/vI=;47","f5":"GoFIJGJSIuZ7PQ==;48","f6":"jGML/T1rVH74+Q==;49","f7":"Iu1BH2K0hPE8wQ==;50","f8":"5NGFboAL4imkCw==;51","f9":"tc2Sea0QUvI=;52","g1":"t+JLf3O1aq+juw==;53","g2":"nIf2UtyjUM15lg==;54","g3":"dXWIY9sPMMU=;55","g4":"/dKyxxpu38Y/sg==;56","g5":"pZngNi0YbG8=;57","g6":"DrCUpU4AGOE=;58","g7":"0hDvzoiDmaM=;59","g8":"QKYPMAKv72I=;60","g9":"le66pVrAJiE=;61","h1":"MqW+1pzn75kwPw==;62","h2":"nFXu9pmqk1IgAg==;63","h3":"4cQ1WNc2Uu2uHg==;64","h4":"r1HylMBJfHzQ/g==;65","h5":"4cmIHY7ShzE=;66","h6":"ftwNu9cbeYnRJw==;67","h7":"EE891i4Kyq9wggT4FGF7q1IAC9ZiA6icC04IqoKnKF7dqfWn;68","h8":"sFYkTPnobd/l1A==;69","h9":"jaAXXPcL/eO+RA==;70","i1":"F5GMZj2Exn4=;71","i2":"2w/SKFe1ZOaLJw==;72","i3":"qTTSn5PgduZqgQ==;73","i4":"Svhgb6z/dnDjQg==;74","i5":"JOxml/RpklIZMQ==;75","z1":"CLpdtspCG9rH8avF5s/OY+iTko7dQOyHZsiG/O/h2i11DcINiyxuqF2BqwGjWGIqtC9ExgsqKxzWXZkk98i8IA==;76","z2":"K9TreWKA+AU+f9hWEgh7nvBH2nOvWQv7oR8f7FNUlQYKQpd/HI7GZ3rLFcQlB3d1fvBCgfhfNZK6G+zFeo9QMQ==;77","y2":"zz5NZNncwq5nDQ==;78","y3":"AisQHyl8UckAKg==;79","y6":"tcoOhpZxH7n8vA==;80","y8":"IosqRyE2RpEFuA==;81","x4":"TLDhMjJdSrvgWA==;82","z4":"VcTInBf7p1u1Zw==;83","z5":"Mvts2SjCarQ=;84","z6":"BmEQDMGy/ifsehn7;85","z7":"o0Vpjn1FJkQ/i1hW;86","z8":"ENDiV5r3+zS0wMRB;87","z9":"Cn3eWJ+Tpk8cIvvK;88","y1":"U5K0qG/Qh28zifH8;89","y4":"s1ozR2+rLF4745Gj;90","y5":"mRdHgxPfQifAcRVQzP0=;91","y7":"Bbu04D25vt2hBrEg;92","y9":"tTKFLwy1Gc6oE0Ai6Ow=;93","y10":"WaqagJsenjEguZ45XsE=;94","x1":"SfNjtDry5/ORQWZf;95","x2":"rQNd64Sjcb09LBuHwzc=;96","x3":"29HQvUaul6SZyDD/;97","x5":"1WOEF4/ThOBjMfXO;98","z3":"OJnvHtFEH1rvkayrzs91aNgVmlwJE9ZNFtQOgF8bJCs=;99","v":"6.3.1","pgrdt":"Ojetz6WlpjQvB7BnwfgQEPRAyO4=;100","pgrd":"6hbB0c8UP7PB7F4mrpAUC0R6AiB2PU6l6dfbW7fEji5shm8cnsmy6YbCIuax1Ymr1lOHfLjYUohcwyipcQZz+f4HT+wqQOdKZxDGk3z/MocWMzJf29Paniexm6h2hfh34MGs1H0onrolovbPjd+AwV8vqjrNaSl98JdZ+dYkdUabMjD2HXyuERrKSVXk5y8V7t57A4NyzSVPEH7wk/kmYG8aNdQ="}',
    # 'cookie': 'suid=e13849a258e7ae5e79caeb87d11a43f1.8de5d4bf6fd420fcfe3e535f9678e6d8; autoruuid=g681b3f092dhtgk7fj093fm31jiva40a.a7174c6b95fbef111c068faaf940cc46; gids=971; L=ACcJAEFSXlFnVE96Aw0EbEJgDXdQB191MycjMVZbGVQTMgEa.1746381440.16139.340315.907c6f530a5bb536649f93da8ff1f020; crookie=qxWZswCP2xgF03xvOu1xjbXh3zuFmovNUJQNLMLBnOw352IisAlylxptCrzbCDrGZONCZE+f5CfrJ2FI4nKOuKc3Ec8=; cmtchd=MTc0NjYxNjA3Mzg0NA==; _ym_uid=1746616076572343877; fp=48433b09ab5d409af891dcb22b7050df%7C1746616076688; gradius=100; _csrf_token=b2191f78193448caaab5b8560ec0e6f9ff1bf56d08eaa5c1; Session_id=3:1746797775.5.0.1746381440940:sWuuXw:3544.1.2:1|1913289149.-1.2.3:1746381440.6:2096839587.7:1746381440|61:10031876.647899.KfTuvqpQRxnbSYGODNMH48usWdA; sessar=1.1201.CiA4uChZxd2WPFYU7cAiPQ45eLQ_oZiUAlObAeADb72kYQ.KCZDdk0z1lRlWCwia5qkMymrd566bHyIf7rokOvay9k; yandex_login=alliakhmedov; ys=udn.cDrQkNC70Lgg0JDRhdC80LXQtNC%2B0LI%3D#c_chck.12534923; i=hQEVPNwxctZgFQo4Llyz4OKJBCoH0qQexDmSSgokaDdLrcgVFMuSywI7dnQgfBdc03xiDTAwAxTsbQQAFEZQ6Aa5RXo=; yandexuid=605221281718812686; mda2_beacon=1746797775592; sso_status=sso.passport.yandex.ru:synchronized; _ym_isad=2; yaPassportTryAutologin=1; from=direct; autoru_sid=104598072%7C1746801376232.3600.YmdXfqBh9C4Nzx95gNzlWw.ZTTWpbpfjG_200VvnC0Ln2Vs61fhjNP_wu2odkl8w6Y; autoru-visits-count=3; autoru-visits-session-unexpired=1; gdpr=0; _ym_visorc=b; spravka=dD0xNzQ2ODAxNzA3O2k9MTc2LjExMy45MS4xNDk7RD1DMjY4QzM4RDQ3MUUwRkU1MTAzMDkwQTA3RUNDRTZBRTJCQzE0MEFDQjRFRjFEQkVCRTBBNDU0MzEzODVBQjk0NDcwNzZFNTBFNjVERkVENUIwRjRFN0VEMjA0MzAxRkZBNDUxNDFFRDUxN0RFRkZDODI4MDBEOThFQTcwMUUxREEzQUJDOTAwRTAxRjU1MkEyRjAwNTMxQ0U2O3U9MTc0NjgwMTcwNzY0OTI0MjkxOTtoPWY4MDc5MGFjZGI2Y2U2NDllMTM3ZTRkMmMxZjVjNmRl; _yasc=jCgmKdTUQNyLlYyieeTC6NSN/g9Rajo0z26kDp0heehZKWa2r3GAkerYhne9DlOD3Jq6MecqRQop; layout-config={"screen_height":810,"screen_width":1440,"win_width":755,"win_height":675}; autoru_crashreport={%22route_name%22:%22reseller-public-page%22%2C%22app_id%22:%22af-desktop-search%22%2C%22time_spent%22:%221%22%2C%22chat_indicator_unread%22:false%2C%22billing_opened%22:false}; cycada=+fTBvLO/rpsu8vqYIVfISkOrJsvSWtGyZ0GWMgNn//8=; _ym_d=1746802146; count-visits=9; yaPlusUserPlusBalance={%22id%22:%22104598072%22%2C%22plusPoints%22:24}; from_lifetime=1746802148636',
}


def extract_data(offer):
    """
    Извлекает данные автомобиля, с логированием ошибок
    """
    try:
        mark = offer.get('vehicle_info', {}).get('mark_info', {}).get('code').replace('_', ' ')
        model = offer.get('vehicle_info', {}).get('model_info', {}).get('name')
        title = offer.get('title')
        article = offer.get('id')
        year = offer.get('documents', {}).get('year')
        color = None
        try:
            color = get_color_json(offer.get('color_hex', '')).get('color')
        except Exception as e:
            logger.warning(f"Не удалось получить цвет для {offer.get('color_hex')}: {e}")
        link = offer.get('url')
        price = offer.get('price_info', {}).get('price')

        price_with_markup = None
        if price is not None:
            price_with_markup = price * 1.2  # Цена с наценкой 20%

        result = {
            'id': article,
            'year': year,
            'mark': mark,
            'model': model,
            'color': color,
            'link': link,
            'price': int(price) if price is not None else None,  # Оригинальная цена
            'price_with_markup': int(price_with_markup) if price_with_markup is not None else None,  # Цена с наценкой
            'title': title
        }
        logger.debug(f"Extracted data: {result}")
        return result
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных из оффера: {e}", exc_info=True)
        return {'id': article, 'mark': None, 'model': None, 'color': None, 'link': None, 'price': None, 'price_with_markup': None, 'title': None}


def fetch_all_offers(
    encrypted_user_id: str,
    cookies: dict,
    headers: dict,
    sort: str = "cr_date-desc",
    category: str = "all",
    status: list[str] = ("ACTIVE",),
    geo_radius: int = 100,
) -> list[dict]:
    """
    Скачивает все офферы для given encrypted_user_id.
    Листает страницы, пока в ответе data['offers'] не станет пустым.
    Возвращает список всех офферов.
    """
    all_offers = []
    page = 1

    logger.info(f"Начинаем загрузку офферов магазина {encrypted_user_id}")
    while True:
        logger.debug(f"Запрос страницы {page}")
        time.sleep(3)

        payload = {
            "status": list(status),
            "encrypted_user_id": encrypted_user_id,
            "category": category,
            "sort": sort,
            "page": page,
            "geo_radius": geo_radius,
        }

        try:
            resp = requests.post(
                "https://auto.ru/-/ajax/desktop-search/getOtherUserOffers/",
                cookies=cookies,
                headers=headers,
                json=payload,
                impersonate="chrome120"
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Ошибка при запросе страницы {page}: {e}", exc_info=True)
            break

        try:
            data = resp.json()
        except ValueError as e:
            logger.error(f"Некорректный JSON на странице {page}: {e}")
            break

        offers = data.get("offers", [])
        logger.info(f"Страница {page}: найдено {len(offers)} офферов")

        if not offers:
            logger.info("Офферы закончились — выходим.")
            break

        all_offers.extend(offers)
        page += 1

    logger.info(f"Завершено: всего офферов {len(all_offers)}")
    return all_offers


def parse_auto(shop_id) -> list[dict]:
    """
    Принимает ссылку вида https://auto.ru/reseller/<shop_id>/...
    Извлекает shop_id, скачивает и обрабатывает офферы
    """
    try:
        offers = fetch_all_offers(
            encrypted_user_id=shop_id,
            cookies=COOKIES,
            headers=HEADERS,
        )
        logger.info(f"Получено {len(offers)} офферов для обработки (shop_id={shop_id})")
        results = []
        for offer in offers:
            rec = extract_data(offer)
            rec['seller'] = shop_id
            results.append(rec)
        return results
    except Exception as e:
        logger.critical(f"Не удалось спарсить магазин {shop_id}: {e}", exc_info=True)
        return []
