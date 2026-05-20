from dataclasses import dataclass


@dataclass
class ClientConfig:
    slug: str                       # уникальный ключ, например "alpha-realty"
    name: str                       # название агентства
    admin_phones: list              # номера adminов БЕЗ +, например ["971585369077"]
    drive_root_id: str = ""         # ID папки Google Drive
    contact: str = "@support"       # контакт когда юнит не найден
    bot_character: str = ""         # характер Tony (пустой = стандартный)
    is_owner: bool = False          # платформенный владелец — доступ ко всем агентствам
