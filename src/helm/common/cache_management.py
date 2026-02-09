"""Cache management utilities for HELM."""

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def clear_cache(
    cache_path: str,
    pattern: Optional[str] = None,
    older_than_hours: Optional[int] = None,
) -> int:
    """Clear cache entries.

    Args:
        cache_path: Path to a cache file or directory containing cache files.
        pattern: Only clear entries where the key contains this pattern.
        older_than_hours: Only clear entries older than N hours.

    Returns:
        Number of entries removed.
    """
    if not os.path.exists(cache_path):
        logger.warning(f"Cache path does not exist: {cache_path}")
        return 0

    removed = 0

    if os.path.isfile(cache_path) and cache_path.endswith(".sqlite"):
        removed = _clear_sqlite_cache(cache_path, pattern, older_than_hours)
    elif os.path.isdir(cache_path):
        for file in Path(cache_path).rglob("*.sqlite"):
            removed += _clear_sqlite_cache(str(file), pattern, older_than_hours)

    return removed


def _clear_sqlite_cache(
    path: str,
    pattern: Optional[str],
    older_than_hours: Optional[int],
) -> int:
    """Clear entries from a single SQLite cache file."""
    try:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='unnamed'")
        if not cursor.fetchone():
            conn.close()
            return 0

        if pattern is None and older_than_hours is None:
            # Clear all entries
            cursor.execute("DELETE FROM unnamed")
            removed = cursor.rowcount
        else:
            # Selective clear - need to inspect each entry
            cursor.execute("SELECT key, value FROM unnamed")
            removed = 0
            keys_to_remove: List[str] = []

            for key, value in cursor.fetchall():
                should_remove = False

                if pattern and pattern in key:
                    should_remove = True

                if older_than_hours:
                    try:
                        # Value is pickled, so we need to unpickle it
                        try:
                            from cPickle import loads
                        except ImportError:
                            from pickle import loads
                        data = loads(value)
                        created_at = data.get("_cache_created_at", 0)
                        age_hours = (datetime.now().timestamp() - created_at) / 3600
                        if age_hours > older_than_hours:
                            should_remove = True
                    except Exception:
                        pass

                if should_remove:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                cursor.execute("DELETE FROM unnamed WHERE key = ?", (key,))
                removed += 1

        conn.commit()
        conn.close()
        if removed > 0:
            logger.info(f"Removed {removed} entries from {path}")
        return removed
    except Exception as e:
        logger.error(f"Error clearing cache {path}: {e}")
        return 0


def get_cache_stats(cache_path: str) -> dict:
    """Get statistics for a cache file or directory.

    Args:
        cache_path: Path to a cache file or directory containing cache files.

    Returns:
        Dictionary with cache statistics.
    """
    stats = {
        "total_entries": 0,
        "total_size_mb": 0.0,
        "files": [],
    }

    if not os.path.exists(cache_path):
        logger.warning(f"Cache path does not exist: {cache_path}")
        return stats

    if os.path.isfile(cache_path):
        files = [cache_path]
    else:
        files = [str(f) for f in Path(cache_path).rglob("*.sqlite")]

    for file_path in files:
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)

            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()

            # Check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='unnamed'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM unnamed")
                count = cursor.fetchone()[0]
            else:
                count = 0

            conn.close()

            stats["files"].append(
                {
                    "path": file_path,
                    "entries": count,
                    "size_mb": round(size_mb, 2),
                }
            )
            stats["total_entries"] += count
            stats["total_size_mb"] += size_mb
        except Exception as e:
            logger.error(f"Error reading cache stats from {file_path}: {e}")

    stats["total_size_mb"] = round(stats["total_size_mb"], 2)
    return stats
