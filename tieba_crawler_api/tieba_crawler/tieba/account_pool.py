from __future__ import annotations

import itertools
import logging
import random
import threading
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Account:
    """A single Tieba login credential pair."""
    bduss: str = ""
    stoken: str = ""
    label: str = ""  # optional human-readable label, e.g. "account-1"

    @property
    def is_valid(self) -> bool:
        """An account is considered usable if it has a non-empty BDUSS."""
        return bool(self.bduss and self.bduss.strip())

    def __repr__(self) -> str:
        tag = self.label or self.bduss[:8] + "..."
        return f"Account({tag})"


class AccountPool:
    """Thread-safe pool of Tieba accounts with rotation strategies.

    Usage:
        pool = AccountPool.from_json_or_single(accounts_json, bduss, stoken)

        # Round-robin (good for crawling — spreads load evenly)
        account = pool.next()

        # Random pick (good for relay — less predictable)
        account = pool.random()

        # Get all accounts (for batch operations)
        accounts = pool.all()
    """

    def __init__(self, accounts: List[Account]) -> None:
        # Filter out invalid accounts
        self._accounts: List[Account] = [a for a in accounts if a.is_valid]
        if not self._accounts:
            # Keep a single empty account so callers don't crash
            # (crawling can work without BDUSS)
            self._accounts = [Account()]
            log.warning("AccountPool: no valid accounts provided. Using empty (anonymous) account.")
        else:
            log.info("AccountPool: loaded %d valid account(s).", len(self._accounts))

        self._cycle = itertools.cycle(range(len(self._accounts)))
        self._lock = threading.Lock()

    @staticmethod
    def from_json_or_single(
        accounts_json: Optional[list] = None,
        bduss: str = "",
        stoken: str = "",
    ) -> "AccountPool":
        """Build pool from ACCOUNTS_JSON list or fall back to single BDUSS/STOKEN.

        accounts_json format:
        [
            {"bduss": "...", "stoken": "...", "label": "account-1"},
            {"bduss": "...", "stoken": "...", "label": "account-2"}
        ]
        """
        accounts: List[Account] = []

        if accounts_json and isinstance(accounts_json, list):
            for i, item in enumerate(accounts_json):
                if isinstance(item, dict):
                    accounts.append(Account(
                        bduss=str(item.get("bduss", "") or "").strip(),
                        stoken=str(item.get("stoken", "") or "").strip(),
                        label=str(item.get("label", "") or f"account-{i + 1}").strip(),
                    ))
                elif isinstance(item, str):
                    # Shorthand: just a BDUSS string
                    accounts.append(Account(
                        bduss=item.strip(),
                        stoken="",
                        label=f"account-{i + 1}",
                    ))

        # If no accounts from JSON, fall back to single env vars
        if not accounts:
            accounts.append(Account(bduss=bduss, stoken=stoken, label="default"))

        return AccountPool(accounts)

    def next(self) -> Account:
        """Round-robin: return the next account in rotation. Thread-safe."""
        with self._lock:
            idx = next(self._cycle)
        return self._accounts[idx]

    def random(self) -> Account:
        """Random pick: return a random account."""
        return random.choice(self._accounts)

    def all(self) -> List[Account]:
        """Return all valid accounts."""
        return list(self._accounts)

    @property
    def size(self) -> int:
        return len(self._accounts)

    @property
    def has_authenticated(self) -> bool:
        """True if at least one account has a non-empty BDUSS."""
        return any(a.is_valid for a in self._accounts)

    def __len__(self) -> int:
        return len(self._accounts)

    def __repr__(self) -> str:
        return f"AccountPool(size={self.size}, authenticated={self.has_authenticated})"
