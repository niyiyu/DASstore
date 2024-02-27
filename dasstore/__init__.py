import logging

from ._version import __version__  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(thread)d %(levelname)s: %(message)s")
