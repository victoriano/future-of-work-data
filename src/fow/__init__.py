from pathlib import Path

ROOT_DIR = Path(__file__).parents[2]


def pathto(*paths) -> Path:
    """Get full path relative to root directory (future-of-work-datsa)."""
    return ROOT_DIR.joinpath(*paths)
