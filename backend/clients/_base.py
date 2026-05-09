from dataclasses import dataclass, field


@dataclass
class ClientConfig:
    slug: str                        # уникальный ключ, например "alpha-realty"
    name: str                        # название агентства
    admin_phones: list               # номера телефонов adminов (без +), например ["971585369077"]
    drive_root_id: str = ""          # ID папки Google Drive для этого клиента
    umar_contact: str = "@support"   # контакт когда юнит не найден
    admin_password: str = "toni2024" # пароль для /admin/slug панели
    bot_character: str = ""          # характер Tony для этого клиента (пустой = стандартный)
    wa_groups: list = field(default_factory=list)  # список ID групп (опционально, для ручной привязки)
