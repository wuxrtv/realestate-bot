"""
Дефолтный клиент (твоё агентство).
Заполни своими данными.
"""

from ._base import ClientConfig

config = ClientConfig(
    slug="default",
    name="Default Agency",
    admin_phones=[
        # Номера телефонов adminов БЕЗ + и БЕЗ пробелов
        # "971585369077",
    ],
    drive_root_id="",   # ID папки Google Drive
    umar_contact="@support",
    bot_character="",   # пустой = стандартный Tony
)
