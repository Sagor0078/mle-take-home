"""Turn a company's free-text fields into the text we embed."""

from typing import Literal

EmbeddingSource = Literal["nature", "fallback"]


def clean_description(text: str) -> str:
    """Collapse whitespace and a phrase that is repeated end to end.

    Some registry descriptions repeat one phrase dozens of times; embedding that
    would only add noise. We find the shortest token block that, repeated, rebuilds
    the whole text, and keep one copy.
    """
    tokens = text.split()
    n = len(tokens)
    for period in range(1, n // 2 + 1):
        if n % period == 0 and tokens == tokens[:period] * (n // period):
            tokens = tokens[:period]
            break
    return " ".join(tokens)


def embedding_text(
    nature: str | None, industry: str | None
) -> tuple[str, EmbeddingSource] | None:
    """Pick what to embed: the business description, else the industry, else nothing."""
    if nature:
        return clean_description(nature), "nature"
    if industry:
        return f"{industry} business", "fallback"
    return None
