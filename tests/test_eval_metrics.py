import numpy as np
import pytest

from eval.metrics import (
    industry_precision_at_k,
    industry_precision_chance,
    mean_reciprocal_rank,
    pair_recall_at_k,
    target_ranks,
)


def unit(*rows: list[float]) -> np.ndarray:
    """Stack rows and L2-normalise each one."""
    m = np.array(rows, dtype=np.float32)
    return m / np.linalg.norm(m, axis=1, keepdims=True)


# Two tight clusters on orthogonal axes.
CLUSTERS = unit([1, 0.0], [1, 0.1], [1, 0.2], [0.0, 1], [0.1, 1], [0.2, 1])


def test_precision_is_one_for_perfect_clusters():
    industries = ["Food", "Food", "Food", "Tech", "Tech", "Tech"]
    assert industry_precision_at_k(CLUSTERS, industries, k=2) == pytest.approx(1.0)


def test_precision_counts_cross_cluster_neighbours():
    # k=3 forces each row's third neighbour into the other cluster: 2 of 3 match.
    industries = ["Food", "Food", "Food", "Tech", "Tech", "Tech"]
    assert industry_precision_at_k(CLUSTERS, industries, k=3) == pytest.approx(2 / 3)


def test_precision_skips_unlabelled_queries_but_counts_them_as_misses():
    # Row 2 has no industry: it is not scored, and as a neighbour it never matches.
    industries = ["Food", "Food", None, "Tech", "Tech", "Tech"]
    # Rows 0 and 1: neighbours {1 or 0, 2} -> 1/2 each. Rows 3-5: 2/2 each.
    assert industry_precision_at_k(CLUSTERS, industries, k=2) == pytest.approx((0.5 * 2 + 3) / 5)


def test_precision_can_skip_identical_text_neighbours():
    # Rows 0 and 1 share a text. Skipping it, row 0's nearest neighbours are rows 2 and 3.
    vectors = unit([1, 0.0], [1, 0.0], [1, 0.3], [0.0, 1])
    industries = ["Food", "Food", "Food", "Tech"]
    texts = ["cafe", "cafe", "bakery", "software"]
    # Row 0: {2, 3} -> 1/2. Row 1: {2, 3} -> 1/2. Row 2: {0, 1} -> 1. Row 3: {2, 0 or 1} -> 0.
    expected = (0.5 + 0.5 + 1 + 0) / 4
    assert industry_precision_at_k(vectors, industries, k=2, texts=texts) == pytest.approx(expected)
    # Without skipping, row 0 and row 1 find each other first.
    assert industry_precision_at_k(vectors, industries, k=2) == pytest.approx((1 + 1 + 1 + 0) / 4)


def test_chance_precision_is_share_of_same_industry_among_others():
    industries = ["Food", "Food", "Food", "Tech", None]
    # Food rows: 2 of 4 others are Food. Tech row: 0 of 4.
    assert industry_precision_chance(industries) == pytest.approx((0.5 * 3 + 0) / 4)


def test_chance_precision_skips_identical_texts():
    industries = ["Food", "Food", "Food", "Tech"]
    texts = ["cafe", "cafe", "bakery", "software"]
    # Row 0: others {2, 3} -> 1/2. Row 1: same. Row 2: {0, 1, 3} -> 2/3. Row 3: {0, 1, 2} -> 0.
    expected = (0.5 + 0.5 + 2 / 3 + 0) / 4
    assert industry_precision_chance(industries, texts=texts) == pytest.approx(expected)


# Corpus of four items; two queries whose targets land at known ranks.
CORPUS = unit([1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 1, 0])
QUERIES = unit(
    [1, 0.1, 0],  # target 0 is the closest corpus item: rank 1
    [0.9, 0.5, 0.1],  # order is 3, 0, then target 1: rank 3
)
TARGETS = [0, 1]


def test_target_ranks_known_values():
    assert target_ranks(QUERIES, CORPUS, TARGETS).tolist() == [1, 3]


def test_recall_at_k_known_ranks():
    assert pair_recall_at_k(QUERIES, CORPUS, TARGETS, k=1) == pytest.approx(0.5)
    assert pair_recall_at_k(QUERIES, CORPUS, TARGETS, k=3) == pytest.approx(1.0)


def test_mrr_known_ranks():
    # Ranks 1 and 3 -> (1 + 1/3) / 2.
    assert mean_reciprocal_rank(QUERIES, CORPUS, TARGETS) == pytest.approx(0.6667, abs=1e-4)


def test_ranks_ignore_the_query_itself():
    # The query is corpus item 0; excluding it, target 1 is the best remaining item.
    corpus = unit([1, 0], [1, 0.5], [0, 1])
    query = corpus[:1]
    assert target_ranks(query, corpus, [1]).tolist() == [2]
    assert target_ranks(query, corpus, [1], exclude_idx=[0]).tolist() == [1]


def test_ties_with_the_target_count_against_it():
    # Items 0 and 1 are identical: a tie is not resolved in the target's favour.
    corpus = unit([1, 0], [1, 0], [0, 1])
    assert target_ranks(unit([1, 0]), corpus, [1]).tolist() == [2]
