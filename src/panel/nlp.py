"""
NLP utilities for OLX listing description analysis.

Uses scikit-learn TF-IDF + cosine similarity — no LLM calls, no API costs.

Duplicate scoring (composite):
    0.70 × text cosine similarity
    0.20 × area within ±10%
    0.10 × rooms match
Pair is a candidate when composite ≥ threshold (default 0.75).
"""

from __future__ import annotations

import re
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------------------------------------
# Portuguese stop-words (curated for real-estate listings)
# ---------------------------------------------------------------------------

PT_STOPWORDS: frozenset[str] = frozenset({
    # articles / determiners
    "a", "o", "as", "os", "um", "uma", "uns", "umas",
    # prepositions
    "de", "da", "do", "das", "dos", "em", "na", "no", "nas", "nos",
    "para", "por", "pelo", "pela", "pelos", "pelas",
    "com", "sem", "sob", "sobre", "entre", "até", "após", "ante",
    # conjunctions / particles
    "que", "e", "ou", "mas", "se", "não", "nem", "já", "mais",
    "muito", "também", "bem", "aqui", "ali", "ainda", "onde",
    "como", "quando", "qual", "quais", "isso", "este", "esta",
    "esse", "essa", "aquele", "aquela", "ao", "aos", "à", "às",
    # common verbs (inflected forms that add no signal)
    "é", "são", "foi", "será", "foram", "está", "estão", "estará",
    "tem", "têm", "ter", "ser", "estar", "fazer", "pode", "podem",
    "possui", "possuem", "fica", "ficam", "trata", "conta", "dispõe",
    "oferece", "oferece", "inclui", "incluem", "permite", "permite",
    # generic real-estate filler
    "imóvel", "imovel", "casa", "apartamento", "apto", "residência",
    "localizado", "localizada", "situado", "situada",
    "composto", "composta", "contendo", "conta",
    "excelente", "ótimo", "ótima", "lindo", "linda", "lindo",
    "novo", "nova", "ótima", "maravilhoso", "maravilhosa",
    "venha", "agende", "visita", "oportunidade", "imperdível",
    "anuncio", "anúncio", "acesse", "saiba", "contato",
    "metros", "metro", "m²", "m2",
})


# ---------------------------------------------------------------------------
# Text pre-processing
# ---------------------------------------------------------------------------

def _clean(text: str) -> str:
    """Lowercase, remove punctuation, collapse whitespace. Keep accented chars."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    # Replace bare numbers (area/price carry signal via structured fields,
    # not raw number tokens in text)
    text = re.sub(r"\b\d+\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Corpus model
# ---------------------------------------------------------------------------

def build_corpus_model(descriptions: list[str]) -> TfidfVectorizer:
    """
    Fit a TF-IDF vectorizer on the full corpus.

    - Portuguese stopwords
    - 1–2 ngrams (bigrams capture "piso porcelanato", "área gourmet", etc.)
    - min_df=2: term must appear in ≥2 docs to filter hapax legomena
    - max_df=0.85: ignore terms present in >85% of docs (near-universal filler)
    - sublinear_tf: log(1+tf) reduces weight of high-frequency terms
    """
    vec = TfidfVectorizer(
        preprocessor=_clean,
        stop_words=list(PT_STOPWORDS),
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.85,
        sublinear_tf=True,
        dtype=np.float32,
    )
    vec.fit(descriptions)
    return vec


# ---------------------------------------------------------------------------
# Per-document keyword extraction
# ---------------------------------------------------------------------------

def extract_keywords(description: str, vectorizer: TfidfVectorizer, n: int = 8) -> list[str]:
    """
    Return the top-n TF-IDF terms for a single description.
    Returns [] if description is empty or no terms survive the vocabulary.
    """
    if not description or not description.strip():
        return []
    tfidf_row = vectorizer.transform([description]).toarray()[0]
    if tfidf_row.sum() == 0:
        return []
    vocab_inv = {i: t for t, i in vectorizer.vocabulary_.items()}
    top_indices = tfidf_row.argsort()[::-1]
    result = []
    for idx in top_indices:
        if len(result) >= n:
            break
        if tfidf_row[idx] == 0:
            break
        result.append(vocab_inv[idx])
    return result


def extract_keywords_batch(
    descriptions: list[str],
    vectorizer: TfidfVectorizer,
    n: int = 8,
) -> list[list[str]]:
    """Vectorised batch version — much faster than calling extract_keywords in a loop."""
    if not descriptions:
        return []
    matrix = vectorizer.transform(descriptions).toarray()
    vocab_inv = {i: t for t, i in vectorizer.vocabulary_.items()}
    result = []
    for row in matrix:
        if row.sum() == 0:
            result.append([])
            continue
        top_indices = row.argsort()[::-1]
        kws = []
        for idx in top_indices:
            if len(kws) >= n:
                break
            if row[idx] == 0:
                break
            kws.append(vocab_inv[idx])
        result.append(kws)
    return result


# ---------------------------------------------------------------------------
# Pairwise duplicate candidate detection
# ---------------------------------------------------------------------------

def find_duplicate_candidates(
    df: pd.DataFrame,
    threshold: float = 0.75,
) -> pd.DataFrame:
    """
    Compute pairwise description similarity and return candidate duplicate pairs.

    Parameters
    ----------
    df        : DataFrame with columns url, description (+ area_m2, rooms, title optional)
    threshold : minimum composite score to include a pair

    Returns
    -------
    DataFrame with columns:
        url_a, url_b, title_a, title_b,
        text_sim, area_bonus, rooms_bonus, composite,
        shared_keywords
    Sorted by composite descending.
    """
    # Work only on rows with non-trivial descriptions
    has_desc = df["description"].fillna("").str.strip().str.len() > 30
    work = df[has_desc].reset_index(drop=True)

    if len(work) < 2:
        return pd.DataFrame(
            columns=["url_a", "url_b", "title_a", "title_b",
                     "text_sim", "area_bonus", "rooms_bonus", "composite",
                     "shared_keywords"]
        )

    descriptions = work["description"].fillna("").tolist()
    vec = build_corpus_model(descriptions)
    matrix = vec.transform(descriptions)
    sim_matrix = cosine_similarity(matrix)  # (n, n) float32

    # Pre-compute keywords for all documents (batch is much faster)
    all_keywords: list[list[str]] = extract_keywords_batch(descriptions, vec, n=10)

    rows = []
    n = len(work)
    for i in range(n):
        for j in range(i + 1, n):
            text_sim = float(sim_matrix[i, j])
            if text_sim < 0.25:
                continue  # cheap pre-filter before heavier calcs

            # Structured-field bonuses
            a_i = work.at[i, "area_m2"] if "area_m2" in work.columns else None
            a_j = work.at[j, "area_m2"] if "area_m2" in work.columns else None
            area_bonus = 0.0
            if pd.notna(a_i) and pd.notna(a_j) and float(a_i) > 0:
                if abs(float(a_i) - float(a_j)) / float(a_i) <= 0.10:
                    area_bonus = 0.20

            r_i = work.at[i, "rooms"] if "rooms" in work.columns else None
            r_j = work.at[j, "rooms"] if "rooms" in work.columns else None
            rooms_bonus = 0.10 if (pd.notna(r_i) and pd.notna(r_j) and r_i == r_j) else 0.0

            composite = round(0.70 * text_sim + area_bonus + rooms_bonus, 3)
            if composite < threshold:
                continue

            shared = sorted(set(all_keywords[i]) & set(all_keywords[j]))

            rows.append({
                "url_a":           work.at[i, "url"],
                "url_b":           work.at[j, "url"],
                "title_a":         work.at[i, "title"] if "title" in work.columns else "",
                "title_b":         work.at[j, "title"] if "title" in work.columns else "",
                "text_sim":        round(text_sim, 3),
                "area_bonus":      area_bonus,
                "rooms_bonus":     rooms_bonus,
                "composite":       composite,
                "shared_keywords": shared,
            })

    if not rows:
        return pd.DataFrame(
            columns=["url_a", "url_b", "title_a", "title_b",
                     "text_sim", "area_bonus", "rooms_bonus", "composite",
                     "shared_keywords"]
        )

    return pd.DataFrame(rows).sort_values("composite", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Union-Find clustering
# ---------------------------------------------------------------------------

def _find(uf: list[int], x: int) -> int:
    """Path-compressed find."""
    root = x
    while uf[root] != root:
        root = uf[root]
    # Path compression
    while uf[x] != root:
        uf[x], x = root, uf[x]
    return root


def _union(uf: list[int], a: int, b: int) -> None:
    uf[_find(uf, a)] = _find(uf, b)


def suggest_groups(candidates_df: pd.DataFrame) -> list[list[str]]:
    """
    Cluster candidate pairs via union-find.
    Returns list of URL groups (each group ≥ 2 URLs), sorted by group size desc.
    """
    if candidates_df.empty:
        return []

    urls = list({u for col in ("url_a", "url_b") for u in candidates_df[col].tolist()})
    idx = {u: i for i, u in enumerate(urls)}
    uf = list(range(len(urls)))

    for _, row in candidates_df.iterrows():
        _union(uf, idx[row["url_a"]], idx[row["url_b"]])

    groups: dict[int, list[str]] = defaultdict(list)
    for url in urls:
        groups[_find(uf, idx[url])].append(url)

    return sorted(
        [g for g in groups.values() if len(g) >= 2],
        key=len,
        reverse=True,
    )
