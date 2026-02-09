"""HELM client for Kaggle-hosted models via ngrok."""

import json
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from helm.common.cache import CacheConfig
from helm.common.hierarchical_logger import hexception
from helm.common.request import (
    wrap_request_time,
    Request,
    RequestResult,
    GeneratedOutput,
    EMBEDDING_UNAVAILABLE_REQUEST_RESULT,
)
from helm.clients.client import CachingClient

logger = logging.getLogger(__name__)


class KaggleModelClient(CachingClient):
    """Client for models hosted on Kaggle notebooks via ngrok.

    Includes retry logic for unstable ngrok connections.
    The api_url is read from KAGGLE_API_URL environment variable.
    The api_key is read from KAGGLE_API_KEY environment variable (optional but recommended).
    Stats are logged locally to KAGGLE_STATS_FILE (default: kaggle_stats.jsonl).
    """

    def __init__(
        self,
        cache_config: CacheConfig,
        timeout: int = 180,
    ):
        super().__init__(cache_config=cache_config)
        self.api_url = os.environ.get("KAGGLE_API_URL")
        if not self.api_url:
            raise ValueError(
                "KAGGLE_API_URL environment variable required. "
                "Get this from your Kaggle notebook's ngrok output."
            )
        self.api_key: Optional[str] = os.environ.get("KAGGLE_API_KEY")
        if not self.api_key:
            logger.warning(
                "KAGGLE_API_KEY not set. Requests will be unauthenticated. "
                "Set this to match your Kaggle notebook's API_KEY secret."
            )
        self.timeout = timeout

        # Local stats logging
        stats_file = os.environ.get("KAGGLE_STATS_FILE", "kaggle_stats.jsonl")
        self.stats_path = Path(stats_file)
        self.stats_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Kaggle stats will be logged to: {self.stats_path.absolute()}")

        # Fetch model config for cache key differentiation
        self.model_config = self._fetch_model_config()
        logger.info(f"Kaggle model config: {self.model_config}")

    def _fetch_model_config(self) -> Dict[str, str]:
        """Fetch model config from Kaggle server for cache key differentiation.

        This ensures cache entries are invalidated when model configuration changes
        (e.g., switching between 4-bit and 8-bit quantization).
        """
        try:
            response = requests.get(f"{self.api_url}/health", timeout=10)
            response.raise_for_status()
            data = response.json()
            return {
                "kaggle_model_label": data.get("model", "unknown"),
                "kaggle_quantization": data.get("quantization", "unknown"),
            }
        except Exception as e:
            logger.warning(f"Failed to fetch model config from Kaggle server: {e}")
            return {"kaggle_model_label": "unknown", "kaggle_quantization": "unknown"}

    def _log_stats(self, response: Dict, cached: bool, error: Optional[str] = None) -> None:
        """Append request stats to local JSONL file."""
        stats_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cached": cached,
            "success": response.get("success", False) if response else False,
            "error": error,
            "input_tokens": response.get("input_tokens") if response else None,
            "output_tokens": response.get("output_tokens") if response else None,
            "total_tokens": response.get("total_tokens") if response else None,
            "inference_time": response.get("inference_time") if response else None,
            "tokens_per_second": response.get("tokens_per_second") if response else None,
            "finish_reason": response.get("finish_reason") if response else None,
        }
        try:
            with open(self.stats_path, "a") as f:
                f.write(json.dumps(stats_entry) + "\n")
        except Exception as e:
            logger.warning(f"Failed to log stats: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.Timeout)
        ),
    )
    def _send_request(self, url: str, json_data: Dict, timeout: int) -> Dict:
        """Send request with automatic retry on network failures."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        response = requests.post(url, json=json_data, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def make_request(self, request: Request) -> RequestResult:
        if request.embedding:
            return EMBEDDING_UNAVAILABLE_REQUEST_RESULT

        raw_request = {
            "prompt": request.prompt,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
            "stop_sequences": request.stop_sequences or [],
            **self.model_config,  # Include model config in cache key
        }

        cache_key = CachingClient.make_cache_key(raw_request, request)

        def do_it() -> Dict[str, Any]:
            return self._send_request(
                f"{self.api_url}/generate",
                raw_request,
                self.timeout,
            )

        try:
            response, cached = self.cache.get(cache_key, wrap_request_time(do_it))

            if not response.get("success", False):
                self._log_stats(response, cached, response.get("error"))
                return RequestResult(
                    success=False,
                    cached=cached,
                    error=response.get("error", "Unknown Kaggle API error"),
                    completions=[],
                    embedding=[],
                )

            # Map Kaggle finish_reason to HELM's expected format
            # Valid values: "length", "stop", "endoftext", "unknown"
            kaggle_finish_reason = response.get("finish_reason", "unknown")

            completions = [
                GeneratedOutput(
                    text=response["text"],
                    logprob=0.0,
                    tokens=[],
                    finish_reason={"reason": kaggle_finish_reason},
                )
            ]

            # Log to console
            logger.info(
                f"Kaggle request: input={response.get('input_tokens')}, "
                f"output={response.get('output_tokens')}, "
                f"time={response.get('inference_time', 0):.2f}s, "
                f"tok/s={response.get('tokens_per_second', 0):.1f}"
            )

            # Log to local file
            self._log_stats(response, cached)

            return RequestResult(
                success=True,
                cached=cached,
                request_time=response.get("inference_time"),
                completions=completions,
                embedding=[],
            )
        except Exception as e:
            hexception(e)
            self._log_stats(None, False, str(e))
            return RequestResult(
                success=False,
                cached=False,
                error=f"Kaggle API error after retries: {str(e)}",
                completions=[],
                embedding=[],
            )
