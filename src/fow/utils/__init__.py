from pathlib import Path


def pathto(*paths) -> Path:
    """Get full path relative to root directory (future-of-work-datsa)."""
    return Path(__file__).parents[3].joinpath(*paths)
