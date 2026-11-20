from pathlib import Path

import numpy as np

from app.embedding import FakeEmbedder, make_embedder, model_cache_dir, text_hash


def test_fake_embedder_deterministic_and_normalised():
    a, b = FakeEmbedder().embed(["coffee shop", "coffee shop"])
    assert a.shape == (384,) and a.dtype == np.float32
    assert np.allclose(a, b) and np.isclose(np.linalg.norm(a), 1.0)


def test_fake_embedder_similar_texts_closer():
    q, near, far = FakeEmbedder().embed(["coffee shop", "coffee cafe", "steel fabrication"])
    assert q @ near > q @ far


def test_fake_embedder_empty_text_is_finite():
    (v,) = FakeEmbedder().embed([""])
    assert np.all(np.isfinite(v)) and np.isclose(np.linalg.norm(v), 1.0)


def test_text_hash_is_sha256_hex():
    assert text_hash("a") == "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"


def test_make_embedder_fake():
    assert make_embedder("fake-hashed-bow").model_name == "fake-hashed-bow"


def test_model_cache_dir_is_persistent_by_default(monkeypatch):
    monkeypatch.delenv("FASTEMBED_CACHE_PATH", raising=False)
    assert model_cache_dir() == str(Path.home() / ".cache" / "fastembed")


def test_model_cache_dir_overridable(monkeypatch):
    monkeypatch.setenv("FASTEMBED_CACHE_PATH", "/somewhere/else")
    assert model_cache_dir() == "/somewhere/else"
