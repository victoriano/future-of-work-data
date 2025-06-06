from os.path import dirname, join

def pathto(*paths) -> str:
    """Get full path to local resource."""
    return join(dirname(__file__), *paths)