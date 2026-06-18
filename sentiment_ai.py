"""AI sentiment analyzer (rubert-tiny2) with in-memory cache and lexicon fallback."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any, Optional

from sentiment_lexicon import SENTIMENT_NEGATIVE, SENTIMENT_POSITIVE

logger = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────
MODEL_NAME = os.getenv("SENTIMENT_MODEL", "cointegrated/rubert-tiny2-cedr-emotion-detection")
SCORE_THRESHOLD = 0.6
MAX_SEQ_LENGTH = 512

_LABEL_MAP = {
    "positive": "positive",
    "joy": "positive",
    "negative": "negative",
    "anger": "negative",
    "fear": "negative",
    "sadness": "negative",
    "surprise": "neutral",
    "neutral": "neutral",
}

# ─── Runtime state ──────────────────────────────────────────────────────────
_tokenizer: Any = None
_model: Any = None
_device: Any = None
_cache: dict[str, dict[str, Any]] = {}
_hits = 0
_misses = 0


def _text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_model_sync() -> None:
    """Lazy model loading (synchronous, ~10 s on CPU)."""
    global _tokenizer, _model, _device
    if _model is not None:
        return

    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    logger.info("[sentiment_ai] Loading model %s ...", MODEL_NAME)
    _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    _model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    _model.to(_device)
    _model.eval()
    logger.info("[sentiment_ai] Model loaded on %s", _device)


def _lexicon_sentiment(text: Optional[str]) -> dict[str, Any]:
    """Fallback rule-based sentiment using the shared lexicon."""
    txt = (text or "").lower()
    pos = sum(1 for w in SENTIMENT_POSITIVE if w in txt)
    neg = sum(1 for w in SENTIMENT_NEGATIVE if w in txt)
    if pos > neg:
        label = "positive"
    elif neg > pos:
        label = "negative"
    else:
        label = "neutral"
    return {"label": label, "score": 1.0 if label != "neutral" else 0.0, "source": "lexicon"}


def _ai_sentiment_sync(text: str) -> dict[str, Any]:
    """Run model inference for a single text (synchronous)."""
    import torch

    _load_model_sync()
    assert _tokenizer is not None and _model is not None

    inputs = _tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=MAX_SEQ_LENGTH,
        padding=True,
    )
    inputs = {k: v.to(_device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = _model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)[0]
        label_id = int(torch.argmax(probs))
        raw_label = _model.config.id2label.get(label_id, "neutral")
        score = float(probs[label_id])

    mapped = _LABEL_MAP.get(raw_label, "neutral")
    if score < SCORE_THRESHOLD:
        mapped = "neutral"

    return {"label": mapped, "score": score, "source": "ai", "raw_label": raw_label}


def _ai_sentiment_batch_sync(texts: list[str]) -> list[dict[str, Any]]:
    """Run model inference for a batch of texts (synchronous)."""
    import torch

    _load_model_sync()
    assert _tokenizer is not None and _model is not None

    inputs = _tokenizer(
        texts,
        return_tensors="pt",
        truncation=True,
        max_length=MAX_SEQ_LENGTH,
        padding=True,
    )
    inputs = {k: v.to(_device) for k, v in inputs.items()}

    results: list[dict[str, Any]] = []
    with torch.no_grad():
        outputs = _model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)
        labels = torch.argmax(probs, dim=-1)
        for i, text in enumerate(texts):
            label_id = int(labels[i])
            raw_label = _model.config.id2label.get(label_id, "neutral")
            score = float(probs[i][label_id])
            mapped = _LABEL_MAP.get(raw_label, "neutral")
            if score < SCORE_THRESHOLD:
                mapped = "neutral"
            results.append({"label": mapped, "score": score, "source": "ai", "raw_label": raw_label})
    return results


def _analyze_sync(text: Optional[str]) -> dict[str, Any]:
    """Cache → AI → lexicon fallback for a single text."""
    text = text or ""
    if not text.strip():
        return {"label": "neutral", "score": 0.0, "source": "lexicon"}

    h = _text_hash(text)
    if h in _cache:
        return _cache[h]

    try:
        result = _ai_sentiment_sync(text)
    except Exception as exc:
        logger.warning("[sentiment_ai] AI failed, using lexicon fallback: %s", exc)
        result = _lexicon_sentiment(text)

    _cache[h] = result
    return result


def _analyze_batch_sync(texts: list[Optional[str]]) -> list[dict[str, Any]]:
    """Cache-aware batch inference with lexicon fallback."""
    results: list[Optional[dict[str, Any]]] = [None] * len(texts)
    missing_indices: list[int] = []
    missing_texts: list[str] = []

    for i, text in enumerate(texts):
        text = text or ""
        if not text.strip():
            results[i] = {"label": "neutral", "score": 0.0, "source": "lexicon"}
            continue
        h = _text_hash(text)
        if h in _cache:
            results[i] = _cache[h]
        else:
            missing_indices.append(i)
            missing_texts.append(text)

    if missing_texts:
        try:
            ai_results = _ai_sentiment_batch_sync(missing_texts)
            for idx_in_missing, orig_idx in enumerate(missing_indices):
                result = ai_results[idx_in_missing]
                results[orig_idx] = result
                _cache[_text_hash(missing_texts[idx_in_missing])] = result
        except Exception as exc:
            logger.warning("[sentiment_ai] Batch AI failed, using lexicon fallback: %s", exc)
            for idx_in_missing, orig_idx in enumerate(missing_indices):
                result = _lexicon_sentiment(missing_texts[idx_in_missing])
                results[orig_idx] = result
                _cache[_text_hash(missing_texts[idx_in_missing])] = result

    return [r for r in results if r is not None]


# ─── Public async API ───────────────────────────────────────────────────────
async def analyze(text: Optional[str]) -> dict[str, Any]:
    """Analyze a single text asynchronously (model runs in a thread)."""
    global _hits, _misses
    text = text or ""
    if not text.strip():
        return {"label": "neutral", "score": 0.0, "source": "lexicon"}

    h = _text_hash(text)
    if h in _cache:
        _hits += 1
        return _cache[h]

    _misses += 1
    result = await asyncio.to_thread(_analyze_sync, text)
    return result


async def analyze_batch(texts: list[Optional[str]]) -> list[dict[str, Any]]:
    """Analyze a batch of texts asynchronously (model runs in a thread)."""
    global _hits, _misses

    missing_count = 0
    for text in texts:
        text = text or ""
        if not text.strip():
            continue
        h = _text_hash(text)
        if h not in _cache:
            missing_count += 1

    if missing_count == 0:
        _hits += len(texts)
    else:
        _hits += len(texts) - missing_count
        _misses += missing_count

    return await asyncio.to_thread(_analyze_batch_sync, texts)


def get_stats() -> dict[str, Any]:
    """Return model and cache statistics."""
    return {
        "model_loaded": _model is not None,
        "model_name": MODEL_NAME,
        "cache_size": len(_cache),
        "hits": _hits,
        "misses": _misses,
    }
