"""CLI for HELM cache management."""

import argparse
import json
import sys

from helm.common.cache_management import clear_cache, get_cache_stats


def main():
    parser = argparse.ArgumentParser(
        description="HELM Cache Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View cache statistics
  helm-cache stats prod_env/cache/

  # Clear all cache entries (with dry-run first)
  helm-cache clear prod_env/cache/ --dry-run
  helm-cache clear prod_env/cache/

  # Clear old entries
  helm-cache clear prod_env/cache/ --older-than-hours 24

  # Clear entries matching a pattern
  helm-cache clear prod_env/cache/ --pattern "kaggle"
""",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cache entries")
    clear_parser.add_argument("path", help="Cache path (file or directory)")
    clear_parser.add_argument(
        "--pattern",
        help="Only clear entries where the key contains this pattern",
    )
    clear_parser.add_argument(
        "--older-than-hours",
        type=int,
        help="Only clear entries older than N hours",
    )
    clear_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be cleared without actually clearing",
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show cache statistics")
    stats_parser.add_argument("path", help="Cache path (file or directory)")
    stats_parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output as JSON",
    )

    args = parser.parse_args()

    if args.command == "clear":
        if args.dry_run:
            stats = get_cache_stats(args.path)
            print(f"Would clear up to {stats['total_entries']} entries ({stats['total_size_mb']} MB)")
            print(f"Files that would be affected: {len(stats['files'])}")
            for f in stats["files"]:
                print(f"  {f['path']}: {f['entries']} entries ({f['size_mb']} MB)")
        else:
            removed = clear_cache(args.path, args.pattern, args.older_than_hours)
            print(f"Cleared {removed} cache entries")

    elif args.command == "stats":
        stats = get_cache_stats(args.path)
        if args.json_output:
            print(json.dumps(stats, indent=2))
        else:
            print(f"Total entries: {stats['total_entries']}")
            print(f"Total size: {stats['total_size_mb']} MB")
            print(f"Files: {len(stats['files'])}")
            for f in stats["files"]:
                print(f"  {f['path']}: {f['entries']} entries ({f['size_mb']} MB)")


if __name__ == "__main__":
    main()
