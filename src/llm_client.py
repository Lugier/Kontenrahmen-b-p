"""
LLM Client — OpenAI API wrapper with retry, caching, JSON schema enforcement.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import httpx
from openai import OpenAI
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache DB setup
# ---------------------------------------------------------------------------

_CREATE_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key TEXT PRIMARY KEY,
    model TEXT,
    prompt_hash TEXT,
    system_hash TEXT,
    response_text TEXT,
    tokens_prompt INTEGER,
    tokens_completion INTEGER,
    latency_ms INTEGER,
    created_at REAL
)
"""


class LLMClient:
    """Encapsulated OpenAI client with caching, retry, and JSON parsing."""

    def __init__(
        self,
        model: str = "gpt-5-mini-2025-08-07",
        api_key: Optional[str] = None,
        cache_db_path: str | Path = "llm_cache.db",
        max_retries: int = 3,
    ):
        load_dotenv()
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "OPENAI_API_KEY not set. Set it in .env or pass via --api_key / env var."
            )
        
        # Explicitly create httpx client to avoid proxy issues and set a long timeout for reasoning models
        http_client = httpx.Client(trust_env=False, timeout=600.0)
        self.client = OpenAI(api_key=self.api_key, http_client=http_client)
        self.max_retries = max_retries

        # SQLite cache — thread-local connections (safe for ThreadPoolExecutor)
        self.cache_db_path = Path(cache_db_path)
        self._local = threading.local()   # each thread gets its own .conn
        self._ensure_schema()             # create table in main thread conn

        self._call_count = 0
        self._total_tokens = 0
        self._stats_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def call(
        self,
        prompt: str,
        system_prompt: str = "You are a helpful assistant.",
        json_schema: Optional[Dict[str, Any]] = None,
        temperature: float = 0.1,
        schema_version: str = "v1",
        use_cache: bool = True,
        reasoning_effort: Optional[str] = None,
    ) -> Dict[str, Any] | str:
        """Call the LLM. Returns parsed JSON dict if json_schema is given, else raw string."""
        cache_key = self._make_cache_key(system_prompt, prompt, schema_version)

        # Check cache
        if use_cache:
            cached = self._get_cache(cache_key)
            if cached is not None:
                logger.info("Cache hit for key %s", cache_key[:16])
                if json_schema:
                    return self._parse_json(cached)
                return cached

        # Build messages
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        # Call with retry
        logger.info("Requesting LLM model %s (reasoning: %s, effort: %s)...", self.model, "gpt-5" in self.model or "o" in self.model, reasoning_effort)
        response_text = self._call_with_retry(messages, temperature, json_schema, reasoning_effort=reasoning_effort)

        # Cache
        self._set_cache(cache_key, response_text)

        if json_schema:
            return self._parse_json(response_text)
        return response_text

    def call_batch(
        self,
        items: list[Dict[str, Any]],
        system_prompt: str,
        prompt_template: str,
        json_schema: Optional[Dict[str, Any]] = None,
        batch_size: int = 100,
        temperature: float = 0.1,
        schema_version: str = "v1",
    ) -> list[Dict[str, Any]]:
        """Call LLM on batches of items. Returns list of parsed results."""
        all_results = []
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_json = json.dumps(batch, ensure_ascii=False, indent=None)
            prompt = prompt_template.replace("{{BATCH}}", batch_json)
            result = self.call(
                prompt=prompt,
                system_prompt=system_prompt,
                json_schema=json_schema,
                temperature=temperature,
                schema_version=schema_version,
            )
            if isinstance(result, dict):
                # Expected: {"results": [...]}
                if "results" in result:
                    all_results.extend(result["results"])
                else:
                    all_results.append(result)
            elif isinstance(result, list):
                all_results.extend(result)
            else:
                all_results.append(result)
        return all_results

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "call_count": self._call_count,
            "total_tokens": self._total_tokens,
            "model": self.model,
            "cache_db": str(self.cache_db_path),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Return a per-thread SQLite connection (creates one if needed)."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.cache_db_path))
        return self._local.conn

    def _ensure_schema(self) -> None:
        conn = self._get_conn()
        conn.execute(_CREATE_CACHE_TABLE)
        conn.commit()

    def _call_with_retry(
        self,
        messages: list,
        temperature: float,
        json_schema: Optional[Dict[str, Any]] = None,
        reasoning_effort: Optional[str] = None,
    ) -> str:
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                t0 = time.time()

                kwargs: Dict[str, Any] = {
                    "model": self.model,
                    "messages": messages,
                }
                
                if "gpt-5" in self.model or "o1" in self.model or "o3" in self.model:
                    # Reasoning models: reasoning_effort is supported, temperature often restricted
                    kwargs["reasoning_effort"] = reasoning_effort or "low"
                    kwargs["max_completion_tokens"] = 64000
                else:
                    kwargs["temperature"] = temperature
                    kwargs["max_tokens"] = 16000
                
                if json_schema:
                    kwargs["response_format"] = {"type": "json_object"}
                    # Append schema hint to system message
                    schema_hint = (
                        "\n\nYou MUST respond with valid JSON matching this schema:\n"
                        + json.dumps(json_schema, indent=2)
                    )
                    kwargs["messages"] = [
                        {**messages[0], "content": messages[0]["content"] + schema_hint},
                        *messages[1:],
                    ]

                response = self.client.chat.completions.create(**kwargs)
                text = response.choices[0].message.content or ""

                latency_ms = int((time.time() - t0) * 1000)
                prompt_tokens = getattr(response.usage, "prompt_tokens", 0) or 0
                completion_tokens = getattr(response.usage, "completion_tokens", 0) or 0

                with self._stats_lock:
                    self._call_count += 1
                    self._total_tokens += prompt_tokens + completion_tokens

                logger.info(
                    "LLM call #%d: %d prompt + %d completion tokens, %dms",
                    self._call_count, prompt_tokens, completion_tokens, latency_ms,
                )

                return text

            except Exception as e:
                last_error = e
                wait = 2 ** attempt
                logger.warning(
                    "LLM call attempt %d/%d failed: %s. Retrying in %ds...",
                    attempt, self.max_retries, str(e), wait,
                )
                time.sleep(wait)

        raise RuntimeError(
            f"LLM call failed after {self.max_retries} attempts: {last_error}"
        )

    def _parse_json(self, text: str) -> Dict[str, Any]:
        """Parse JSON from LLM response, with repair attempts."""
        text = text.strip()

        # Strip markdown code fences
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last lines if they are fences
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Repair attempt: find JSON object/array in text
        match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Last resort: try to fix common issues
        text_fixed = text.replace("'", '"')
        text_fixed = re.sub(r",\s*([}\]])", r"\1", text_fixed)  # trailing commas
        try:
            return json.loads(text_fixed)
        except json.JSONDecodeError:
            logger.error("Failed to parse JSON from LLM response:\n%s", text[:500])
            return {"_raw": text, "_parse_error": True}

    def _make_cache_key(self, system_prompt: str, prompt: str, schema_version: str) -> str:
        payload = f"{self.model}||{schema_version}||{system_prompt}||{prompt}"
        return hashlib.sha256(payload.encode()).hexdigest()

    def _get_cache(self, key: str) -> Optional[str]:
        row = self._get_conn().execute(
            "SELECT response_text FROM llm_cache WHERE cache_key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def _set_cache(self, key: str, response_text: str) -> None:
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO llm_cache (cache_key, model, response_text, created_at) "
            "VALUES (?, ?, ?, ?)",
            (key, self.model, response_text, time.time()),
        )
        conn.commit()

    def close(self):
        # Close the main thread connection
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
