from __future__ import annotations

import re
import unicodedata


def clean_cv_text(raw: str) -> str:
    if not raw:
        return ""
    text = unicodedata.normalize("NFKC", raw)
    text = text.replace("\x00", " ")
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()
