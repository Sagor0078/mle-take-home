"""Embedders: a real fastembed model for production, a hashed bag of words for tests."""

import hashlib
from typing import Protocol

import numpy as np

DIMENSION = 384


class Embedder(Protocol):
    model_name: str

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        """One float32, L2-normalised vector of shape (384,) per text."""
        ...


def text_hash(text: str) -> str:
    """sha256 hex of the text, used to skip re-embedding unchanged text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class FakeEmbedder:
    """Deterministic offline embedder: counts lowercased words in 384 hashed buckets."""

    model_name = "fake-hashed-bow"

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        return [self._embed_one(text) for text in texts]

    @staticmethod
    def _embed_one(text: str) -> np.ndarray:
        vec = np.zeros(DIMENSION, dtype=np.float32)
        for word in text.lower().split():
            # hashlib, not hash(): Python's hash() is salted per process.
            vec[int(hashlib.sha256(word.encode("utf-8")).hexdigest(), 16) % DIMENSION] += 1.0
        if not vec.any():
            vec[0] = 1.0
        return vec / np.linalg.norm(vec)


class FastEmbedder:
    """Wraps a fastembed model. The model loads (and may download) on construction."""

    def __init__(self, model_name: str):
        from fastembed import TextEmbedding

        self.model_name = model_name
        self._model = TextEmbedding(model_name=model_name)
        dim = len(next(iter(self._model.embed(["dimension probe"]))))
        assert dim == DIMENSION, f"{model_name} has dimension {dim}, expected {DIMENSION}"

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        out = []
        for vec in self._model.embed(texts):
            vec = np.asarray(vec, dtype=np.float32)
            out.append(vec / np.linalg.norm(vec))
        return out


def make_embedder(model_name: str) -> Embedder:
    """The fake embedder for its reserved name, otherwise a fastembed model."""
    if model_name == FakeEmbedder.model_name:
        return FakeEmbedder()
    return FastEmbedder(model_name)
