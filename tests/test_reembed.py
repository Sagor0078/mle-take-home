import numpy as np
import pytest

from app.embedding import FakeEmbedder
from app.ingest import handle_message
from app.reembed import reembed
from tests.test_store import msg, scalar

pytestmark = pytest.mark.usefixtures("clean_db")


class OtherModel(FakeEmbedder):
    """A stand-in for a second model: same buckets, opposite sign."""

    model_name = "other-model"

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        return [-vec for vec in super().embed(texts)]


async def seed(pool, embedder):
    await handle_message(pool, embedder, msg("SYN-000001", "2026-01-01T10:00:00+08:00",
                                             natureOfBusiness="Coffee shop"), "m1")
    await handle_message(pool, embedder, msg("SYN-000002", "2026-01-01T10:00:00+08:00",
                                             industry="Retail"), "m2")
    await handle_message(pool, embedder, msg("SYN-000003", "2026-01-01T10:00:00+08:00",
                                             companyName="No text Sdn Bhd"), "m3")


async def test_reembed_switches_every_embedding_to_the_new_model(pool, embedder):
    await seed(pool, embedder)
    other = OtherModel()

    assert await reembed(pool, other) == 3

    assert await scalar(pool, "SELECT count(*) FROM companies WHERE embedding_model = 'other-model'") == 2
    assert await scalar(pool, "SELECT count(*) FROM companies WHERE embedding_model = %s",
                        embedder.model_name) == 0
    stored = await scalar(pool, "SELECT embedding FROM companies WHERE reg_no = 'SYN-000001'")
    assert np.allclose(stored.to_numpy(), other.embed(["Coffee shop"])[0])
    # Without text there is still nothing to embed.
    assert await scalar(pool, "SELECT embedding FROM companies WHERE reg_no = 'SYN-000003'") is None


async def test_reembed_with_the_same_model_changes_nothing(pool, embedder):
    await seed(pool, embedder)
    before = await scalar(pool, "SELECT count(*) FROM embedding_cache")

    await reembed(pool, embedder)

    assert await scalar(pool, "SELECT count(*) FROM embedding_cache") == before
    assert await scalar(pool, "SELECT count(*) FROM companies WHERE embedding_model = %s",
                        embedder.model_name) == 2
