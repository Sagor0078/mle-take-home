-- pgvector stores vectors with EXTERNAL storage, so once a row passes ~2 kB the 1.5 kB embedding
-- is moved to the TOAST table. The similarity scan then fetches every candidate's vector (and the
-- query vector, again for every candidate) through the TOAST index: 24k buffer reads and ~35 ms
-- for 2,000 rows. MAIN keeps the vector in the row unless the row would exceed a page.
-- The row is then over ~2 kB, so the short text columns (*_message_id, embedding_text_hash) go to
-- TOAST instead: harmless for the similarity query (it does not read them), a small cost on the merge.
ALTER TABLE companies ALTER COLUMN embedding SET STORAGE MAIN;

-- SET STORAGE only affects new values; rewrite the existing ones (an exact round trip).
UPDATE companies SET embedding = embedding::real[]::vector WHERE embedding IS NOT NULL;
