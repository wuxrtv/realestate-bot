"""
Default client — твоё агентство.
Чтобы добавить нового клиента: скопируй этот файл,
переименуй (например client_alpha.py) и заполни данные.
"""

from ._base import ClientConfig

config = ClientConfig(
    slug="default",
    name="Default Agency",
    admin_phones=[
        # Добавь номера adminов БЕЗ + и БЕЗ пробелов
        # "971585369077",
    ],
    drive_root_id="",  # ID корневой папки Google Drive
    umar_contact="@support",
    admin_password="toni2024",
    bot_character="",  # пустой = использует стандартного Tony
)
