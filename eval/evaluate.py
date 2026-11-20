"""Compare embedding models on the companies in the database. Run with `just eval`.

Reads active companies from DATABASE_URL (read only), embeds each distinct text once per
model, and prints a markdown table of the metrics in eval/metrics.py, the rank of every
Malay<->English pair, and the top 4 for the README's mock company.
"""

import csv
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import psycopg

from app.config import Settings
from app.embedding import make_embedder
from app.text import embedding_text
from eval.metrics import (
    industry_precision_at_k,
    industry_precision_chance,
    mean_reciprocal_rank,
    pair_recall_at_k,
    target_ranks,
)

MODELS = [
    "fake-hashed-bow",
    "BAAI/bge-small-en-v1.5",
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
]
PAIRS_PATH = Path(__file__).parent / "pairs.csv"
EXAMPLE_REG_NO = "SYN-001550"
K = 4


@dataclass(frozen=True)
class Company:
    reg_no: str
    industry: str | None
    state: str | None
    text: str  # exactly what the service embeds


def load_companies(database_url: str) -> list[Company]:
    """Active companies that have something to embed, in reg_no order."""
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(
            "SELECT reg_no, nature, industry, state FROM companies WHERE active ORDER BY reg_no"
        ).fetchall()
    companies = []
    for reg_no, nature, industry, state in rows:
        chosen = embedding_text(nature, industry)
        if chosen is not None:
            companies.append(Company(reg_no, industry, state, chosen[0]))
    return companies


def load_pairs() -> list[tuple[str, str]]:
    """Hand-labelled (malay, english) descriptions of the same business."""
    with PAIRS_PATH.open(newline="") as f:
        return [(row["malay"], row["english"]) for row in csv.DictReader(f)]


def embed_timed(model_name: str, texts: list[str]) -> tuple[np.ndarray, float]:
    """Vectors for `texts`, and the best of three timings in ms per 100 texts.

    Model loading (and any download) happens before the clock starts.
    """
    embedder = make_embedder(model_name)
    timings = []
    for _ in range(3):
        start = time.perf_counter()
        vectors = np.stack(embedder.embed(texts))
        timings.append(time.perf_counter() - start)
    return vectors, min(timings) / len(texts) * 100 * 1000


def top_k_for(reg_no: str, companies: list[Company], vectors: np.ndarray, k: int):
    """The service's ranking for one company: cosine, then same industry, same state, reg_no."""
    i = next(n for n, c in enumerate(companies) if c.reg_no == reg_no)
    query = companies[i]
    scores = vectors @ vectors[i]
    candidates = [
        (-float(scores[j]), c.industry != query.industry, c.state != query.state, c.reg_no, j)
        for j, c in enumerate(companies)
        if j != i
    ]
    return [(companies[j], -neg) for neg, *_, j in sorted(candidates)[:k]]


def evaluate(model_name: str, companies: list[Company], pairs: list[tuple[str, str]]) -> dict:
    """Every metric for one model."""
    corpus = sorted({c.text for c in companies})
    position = {text: n for n, text in enumerate(corpus)}
    corpus_vecs, ms_per_100 = embed_timed(model_name, corpus)

    texts = [c.text for c in companies]
    industries = [c.industry for c in companies]
    vectors = corpus_vecs[[position[t] for t in texts]]

    malay = [position[m] for m, _ in pairs]
    english = [position[e] for _, e in pairs]
    ms_vecs, en_vecs = corpus_vecs[malay], corpus_vecs[english]

    return {
        "p_all": industry_precision_at_k(vectors, industries, K),
        "p_distinct": industry_precision_at_k(vectors, industries, K, texts=texts),
        "ms_en_r1": pair_recall_at_k(ms_vecs, corpus_vecs, english, 1, exclude_idx=malay),
        "ms_en_r5": pair_recall_at_k(ms_vecs, corpus_vecs, english, 5, exclude_idx=malay),
        "ms_en_mrr": mean_reciprocal_rank(ms_vecs, corpus_vecs, english, exclude_idx=malay),
        "en_ms_r1": pair_recall_at_k(en_vecs, corpus_vecs, malay, 1, exclude_idx=english),
        "en_ms_r5": pair_recall_at_k(en_vecs, corpus_vecs, malay, 5, exclude_idx=english),
        "en_ms_mrr": mean_reciprocal_rank(en_vecs, corpus_vecs, malay, exclude_idx=english),
        "ms_en_ranks": target_ranks(ms_vecs, corpus_vecs, english, exclude_idx=malay),
        "en_ms_ranks": target_ranks(en_vecs, corpus_vecs, malay, exclude_idx=english),
        "ms_per_100": ms_per_100,
        "example": top_k_for(EXAMPLE_REG_NO, companies, vectors, K),
    }


def main() -> None:
    companies = load_companies(Settings.from_env().database_url)
    pairs = load_pairs()
    corpus = {c.text for c in companies}
    missing = [t for pair in pairs for t in pair if t not in corpus]
    assert not missing, f"pairs.csv texts not found in the data: {missing}"

    results = {model: evaluate(model, companies, pairs) for model in MODELS}
    texts = [c.text for c in companies]
    industries = [c.industry for c in companies]

    print(
        f"{len(companies)} active companies, {len(corpus)} distinct texts, "
        f"{len(pairs)} Malay<->English pairs, k={K}\n"
    )
    print(
        "| Model | Industry P@4 (all) | Industry P@4 (other texts) "
        "| ms→en R@1 | ms→en R@5 | ms→en MRR | en→ms R@1 | en→ms R@5 | en→ms MRR | ms / 100 texts |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    print(
        f"| random neighbours | {industry_precision_chance(industries):.3f} "
        f"| {industry_precision_chance(industries, texts=texts):.3f} | | | | | | | |"
    )
    for model, r in results.items():
        print(
            f"| {model} | {r['p_all']:.3f} | {r['p_distinct']:.3f} "
            f"| {r['ms_en_r1']:.2f} | {r['ms_en_r5']:.2f} | {r['ms_en_mrr']:.3f} "
            f"| {r['en_ms_r1']:.2f} | {r['en_ms_r5']:.2f} | {r['en_ms_mrr']:.3f} "
            f"| {r['ms_per_100']:.1f} |"
        )

    print("\nRank of the counterpart, ms→en / en→ms (1 = found first):\n")
    print("| Malay | " + " | ".join(model.split("/")[-1] for model in MODELS) + " |")
    print("|---|" + "---|" * len(MODELS))
    for n, (malay, _) in enumerate(pairs):
        cells = [f"{r['ms_en_ranks'][n]} / {r['en_ms_ranks'][n]}" for r in results.values()]
        print(f"| {malay} | " + " | ".join(cells) + " |")

    query = next(c for c in companies if c.reg_no == EXAMPLE_REG_NO)
    print(f"\nTop {K} for {EXAMPLE_REG_NO} ({query.industry}, {query.state}): {query.text}\n")
    for model, r in results.items():
        print(f"{model}:")
        for company, score in r["example"]:
            print(f"  {score:.3f}  {company.reg_no}  {company.industry} / {company.state}  {company.text}")


if __name__ == "__main__":
    main()
