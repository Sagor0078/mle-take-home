from app.text import clean_description, embedding_text


def test_collapses_repeated_phrase():
    assert clean_description("Perunding pengurusan untuk PKS " * 40) == "Perunding pengurusan untuk PKS"


def test_leaves_normal_text():
    assert clean_description("Operating cafes and coffee shops") == "Operating cafes and coffee shops"


def test_does_not_collapse_partial_repeat():
    assert clean_description("a b a") == "a b a"


def test_collapses_whitespace():
    assert clean_description("  a \n b  ") == "a b"


def test_embedding_text_prefers_nature():
    assert embedding_text("Cafes", "Food & Beverage") == ("Cafes", "nature")


def test_embedding_text_fallback():
    assert embedding_text(None, "Logistics") == ("Logistics business", "fallback")


def test_embedding_text_none():
    assert embedding_text(None, None) is None
