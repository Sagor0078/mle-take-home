"""Pure retrieval metrics for the model evaluation. Vectors are rows, L2-normalised,
so a dot product is cosine similarity."""

import numpy as np


def _excluded_neighbours(n: int, texts: list[str] | None) -> np.ndarray:
    """Boolean (n, n): True where column j may not be a neighbour of row i.

    A row is never its own neighbour. With `texts`, rows with the same text are not
    neighbours either.
    """
    if texts is None:
        return np.eye(n, dtype=bool)
    labels = np.array(texts, dtype=object)
    return labels[:, None] == labels[None, :]


def industry_precision_at_k(
    vectors: np.ndarray, industries: list[str | None], k: int, texts: list[str] | None = None
) -> float:
    """Mean, over rows that have an industry, of the share of their k nearest neighbours
    (excluding self) with the same industry. A neighbour without an industry is a miss.

    With `texts`, neighbours whose text is identical to the row's are skipped too, so a
    model is not credited for matching a string to itself. Ties are broken by row order.
    """
    sims = vectors @ vectors.T
    sims[_excluded_neighbours(len(vectors), texts)] = -np.inf
    scores = []
    for i, industry in enumerate(industries):
        if industry is None:
            continue
        top = np.argsort(-sims[i], kind="stable")[:k]
        scores.append(sum(industries[j] == industry for j in top) / k)
    return float(np.mean(scores))


def industry_precision_chance(industries: list[str | None], texts: list[str] | None = None) -> float:
    """What industry_precision_at_k would be with random neighbours: for each labelled row,
    the share of its allowed neighbours that have the same industry."""
    allowed = ~_excluded_neighbours(len(industries), texts)
    labels = np.array(industries, dtype=object)
    scores = []
    for i, industry in enumerate(industries):
        if industry is None:
            continue
        others = labels[allowed[i]]
        scores.append(np.mean(others == industry))
    return float(np.mean(scores))


def target_ranks(
    query_vecs: np.ndarray,
    corpus_vecs: np.ndarray,
    target_idx: list[int],
    exclude_idx: list[int] | None = None,
) -> np.ndarray:
    """1-based rank of each query's target among the corpus, by cosine similarity.

    `exclude_idx[i]` is a corpus item ignored for query i (the query's own text, when the
    query is itself in the corpus). An item that ties with the target counts as ranked
    above it, so ties never flatter a model.
    """
    sims = query_vecs @ corpus_vecs.T
    ranks = []
    for i, target in enumerate(target_idx):
        row = sims[i].copy()
        if exclude_idx is not None:
            row[exclude_idx[i]] = -np.inf
        target_sim = row[target]
        row[target] = -np.inf
        ranks.append(1 + int(np.sum(row >= target_sim)))
    return np.array(ranks)


def pair_recall_at_k(
    query_vecs: np.ndarray,
    corpus_vecs: np.ndarray,
    target_idx: list[int],
    k: int,
    exclude_idx: list[int] | None = None,
) -> float:
    """Share of queries whose target is in the top k of the corpus."""
    return float(np.mean(target_ranks(query_vecs, corpus_vecs, target_idx, exclude_idx) <= k))


def mean_reciprocal_rank(
    query_vecs: np.ndarray,
    corpus_vecs: np.ndarray,
    target_idx: list[int],
    exclude_idx: list[int] | None = None,
) -> float:
    """Mean of 1 / rank of each query's target."""
    return float(np.mean(1 / target_ranks(query_vecs, corpus_vecs, target_idx, exclude_idx)))
