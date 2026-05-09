"""
ПРИМЕР: как добавить нового клиента.
Скопируй этот файл, убери _ из названия (например: alpha_realty.py)
и заполни данные. После перезапуска бот автоматически подхватит клиента.
"""

from ._base import ClientConfig

config = ClientConfig(
    slug="alpha-realty",
    name="Alpha Realty Dubai",
    admin_phones=[
        "971501234567",   # номер менеджера агентства
        "971509876543",   # второй менеджер (опционально)
    ],
    drive_root_id="ВОТ_ID_ИХ_ПАПКИ_В_GOOGLE_DRIVE",
    umar_contact="@alpha_manager",
    admin_password="alpha2024",
    bot_character="""
Ты Tony — AI ассистент агентства Alpha Realty.
Alpha специализируется на элитных виллах и пентхаусах в Дубае.
Когда представляешься — ты Tony from Alpha Realty.
Акцент на эксклюзивности и премиум сегменте.
""",
)
