import logging
import os
from typing import Optional

def setup_logging(level: Optional[str] = None) -> None:
    lvl = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
