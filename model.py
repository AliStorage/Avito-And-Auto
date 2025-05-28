import json
from g4f.client import Client
from g4f.Provider import ChatgptFree

# Инициализация клиента ChatgptFree
client = Client(
    provider=ChatgptFree,
    timeout=20
)


def extract_brand_model(text: str) -> dict[str, str | None]:
    """
    Отправляет текст в модель и возвращает марку и модель авто.
    Если ответ некорректен, поля устанавливаются в None.
    """
    system = {
        "role": "system",
        "content": (
            "You are a JSON extractor. "
            "Extract only the car brand and model from the user text. "
            "Respond with a JSON object exactly in this format: "
            "{\"brand\": <string|null>, \"model\": <string|null>}. "
            "Do not output anything else."
        )
    }
    user = {"role": "user", "content": text}

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[system, user],
        stream=False,
        web_search=False
    )
    raw = resp.choices[0].message.content.strip()
    # Убираем возможные ```json```
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {"brand": None, "model": None}

    return {"brand": obj.get("brand").replace('_', ' '), "model": obj.get("model")}


if __name__ == "__main__":
    # 20 тестовых описаний автомобилей
    tests = [
        "Mercedes-Benz GLE-класс 2.0 AT, 2025, 265 км",
        "Toyota Land Cruiser 4.5 AT, 2018, 197 602 км",
        "Land Rover Range Rover 4.4 AT, 2025, 46 км",
        "BMW X5 M50d 3.0 AT, 2023, 15 000 км",
        "Audi Q7 3.0 TDI AT, 2022, 30 000 км",
        "Porsche Cayenne 3.0 AT, 2025, 19 км",
        "Lexus RX 350 3.5 AT, 2021, 45 000 км",
        "Nissan Patrol 5.6 AT, 2019, 80 000 км",
        "Infiniti QX80 5.6 AT, 2020, 55 000 км",
        "Cadillac Escalade 6.2 AT, 2024, 5 000 км",
        "Volvo XC90 T6 2.0 AT, 2023, 12 000 км",
        "Jeep Grand Cherokee 3.6 AT, 2021, 60 000 км",
        "Kia Sorento 2.2 AT, 2022, 25 000 км",
        "Hyundai Santa Fe 2.4 AT, 2020, 70 000 км",
        "Mitsubishi Pajero Sport 2.4 AT, 2019, 90 000 км",
        "Chevrolet Tahoe 5.3 AT, 2023, 10 000 км",
        "Ford Explorer 3.0 AT, 2022, 20 000 км",
        "GMC Yukon 5.3 AT, 2021, 50 000 км",
        "Subaru Forester 2.5 AT, 2020, 65 000 км",
        "Toyota RAV4 2.0 AT, 2023, 8 000 км"
    ]

    for t in tests:
        res = extract_brand_model(t)
        print(f"{t!r} → brand={res['brand'].replace('_', ' ')}, model={res['model']}")
