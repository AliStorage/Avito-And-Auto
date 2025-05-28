import re
import json
from g4f.client import Client
from g4f.Provider import RetryProvider, ChatgptFree
from typing import Optional, Dict

# инициализируем клиента
PROVIDERS = [ChatgptFree]
client = Client(provider=RetryProvider(PROVIDERS, shuffle=False), timeout=15)


def get_color_json(hex_code: str, retries: int = 5) -> Optional[Dict[str, Optional[str]]]:
    """
    Возвращает словарь:
      - {"color": "<простое русское название цвета>"}
      - None, если после retries попыток не удалось получить цвет
    Всегда JSON-словарь без лишних ключей и текста.
    """
    # нормализация hex
    h = hex_code.strip().lstrip('#')
    if not re.fullmatch(r'[0-9A-Fa-f]{3}|[0-9A-Fa-f]{6}', h):
        # некорректный код сразу None
        return {"color": None}
    if len(h) == 3:
        h = ''.join(2*c for c in h)
    h = h.lower()

    system = {
        "role": "system",
        "content": (
            "Вы — API, отвечающее строго JSON на русском языке. "
            "Когда получаете hex-код, отвечайте только одним ключом 'color' со значением — простым названием цвета (одно слово), "
            "например: 'красный', 'синий', 'зелёный', 'розовый'. "
            "Не используйте прилагательных (тёмный, светлый и т.п.) и не добавляйте ничего лишнего."
        )
    }
    user = {
        "role": "user",
        "content": f"Hex-код: #{h}. Верни JSON {{\"color\":\"...\"}}."
    }

    for attempt in range(1, retries + 1):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[system, user],
                stream=False,
                web_search=False
            )
            raw = resp.choices[0].message.content.strip().strip("```").strip()
            result = json.loads(raw)
            # если ключ color присутствует и не пуст
            if isinstance(result, dict) and result.get("color"):
                return {"color": result.get("color")}
        except Exception:
            # пропускаем и повторяем
            pass
    # после всех попыток
    return {"color": None}


if __name__ == "__main__":
    for code in ["#040001", "#ff0000", "00f", "7b463b", "zzz"]:
        out = get_color_json(code)
        print(f"{code} → {out}")
