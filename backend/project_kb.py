"""
Project knowledge base — static facts Tony uses when answering questions.
Add new projects as entries in _KB. Keys are lowercase substrings of project names.
"""

_KB: dict[str, str] = {
    "saas hills": """
━━━ PROJECT KNOWLEDGE — SAAS HILLS ━━━

DEVELOPER:
SAAS Properties — premium UAE developer.
Portfolio: SAAS Tower Dubai, One Reem Island, Reem Five, Reem Nine,
Reem Eight, Reem Eleven, SAAS Business Tower (Abu Dhabi).
Known for timeless design, innovation, sustainability.

PROJECT:
Name: SAAS Hills
Location: Dubai Science Park
Two towers — Tower A (30 floors), Tower B (32 floors)
Total: 857 exclusive residences
Types: Studio, 1BR, 2BR, 3BR, Townhouses, Sky Villas
Completion: Q4 2027
Payment Plan: 40/60

UNIQUE FEATURES:
- VLED Technology — first in the world. Eliminates viruses, bacteria, mould from air. Installed in every apartment.
- 10-metre waterfall at building entrance
- Dubai's only residence with private indoor pool
- 530-metre running trail around both towers
- Wellness facilities: 1,500 sqm across both towers

AMENITIES:
Spa + massage centre | 2 jacuzzis | 4 yoga studios
Cardio + calisthenics gym + bungee studio
3 outdoor pools + indoor pool + plunge pool
Sandy beach rooftop pool
Padel + basketball + squash + petanque courts
Private cinema | Indoor community kitchen
Kids play areas + games lounge | Co-working spaces
Retail + cafes on ground floor | 24/7 concierge

LOCATION — DISTANCES:
Sheikh Zayed Road — 9 min | Al Khail Road — 5 min
Dubai Hills Mall — 3 min | Burj Al Arab — 12 min
Dubai International Airport — 20 min | Al Maktoum Airport — 25 min
Repton School — 3 min | King's College Hospital — 5 min
Mediclinic Parkview Hospital — nearby
Els Club Golf Course — nearby | Dubai Miracle Garden — nearby

NEARBY BUSINESS HUBS:
Dubai Hills Business Park | Dubai Media City | Dubai Internet City

UNIT DETAILS:
Studios — Fully Furnished
1BR, 2BR, 3BR — Fully Fitted (with maid's room)
Townhouses — 3 and 4 bedrooms
Sky Villas — 4 bedrooms, duplex, panoramic views

ARCHITECTURE:
Curved contemporary design | Enhances natural airflow
Floor-to-ceiling glass | Signature wave-shaped podium

KNOWLEDGE RULES:
- Answer any SAAS Hills question from the above data
- Never say "I don't know" if the answer is listed here
- If question not covered → redirect to admin contact
- Never invent data not listed here
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""",
}


def get_knowledge(project_names: list[str]) -> str:
    """Return knowledge blocks for all matching projects. Empty string if none match."""
    blocks = []
    for name in project_names:
        name_lower = name.lower()
        for key, text in _KB.items():
            if key in name_lower and text not in blocks:
                blocks.append(text.strip())
    return "\n\n".join(blocks)
