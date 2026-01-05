"""
Identity management for DAS System v3.
Deterministic Account & Nonce Management.
"""
import typing as t
from collections import defaultdict
from eth_account import Account

Account.enable_unaudited_hdwallet_features()

# Nonce Manager
class NonceManager:
    """
    Singleton manager for nonces (local counting only).
    Supports per‑shard nonces via optional scope.
    """
    _instance: t.Optional["NonceManager"] = None
    _nonces: t.Dict[t.Tuple[str, str], int]

    def __new__(cls) -> "NonceManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._nonces = defaultdict(int)
        return cls._instance

    def _key(self, address: str, scope: t.Optional[t.Union[int, str]] = None) -> t.Tuple[str, str]:
        """
        Convert scope to string representation for consistent keying.
        If scope is None, returns "global".
        """
        if scope is None:
            return (address, "global")
        return (address, str(scope))

    def get_and_increment(self, address: str, scope: t.Optional[t.Union[int, str]] = "global") -> int:
        """
        Return the current nonce for address on the given scope, then increment it.
        If scope is None, treats nonce globally (legacy behavior).
        """
        key = self._key(address, scope)
        nonce = self._nonces[key]
        self._nonces[key] = nonce + 1
        return nonce

    def peek(self, address: str, scope: t.Optional[t.Union[int, str]] = "global") -> int:
        """
        Return current nonce without incrementing.
        """
        key = self._key(address, scope)
        return self._nonces[key]

    def reset(self, address: t.Optional[str] = None, scope: t.Optional[t.Union[int, str]] = "global") -> None:
        """
        Reset nonce(s) for testing.
        """
        if address is None:
            self._nonces.clear()
        else:
            key = self._key(address, scope)
            self._nonces.pop(key, None)


# User Manager
class UserManager:
    """
    Derives deterministic accounts from a mnemonic.
    """
    def __init__(self, mnemonic: str) -> None:
        self.mnemonic = mnemonic
        self.nonce_manager = NonceManager()

    def get_user(self, index: int) -> Account:
        """
        Derive the private key for the user at m/44'/60'/0'/0/{index}.
        Ensures the user is registered in NonceManager (start at 0).
        """
        # Derivation path following BIP‑44
        path = f"m/44'/60'/0'/0/{index}"
        account = Account.from_mnemonic(self.mnemonic, account_path=path)
        address = account.address

        # Ensure the address exists in NonceManager (defaults to 0)
        _ = self.nonce_manager.peek(address)  # this will create entry if missing
        return account