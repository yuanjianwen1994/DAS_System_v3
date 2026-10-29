// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @title MigrationHarness
/// @notice Minimal ledger for testing cross-domain migration and business-use invariants.
/// @dev SAME-EVM, TRUSTED-ATTESTOR ABSTRACTION ONLY. Domains are labels in one contract,
///      and one trusted ECDSA signer authorizes destination settlement. This does not
///      implement production client isolation, validator/light-client verification,
///      finality proofs, or a production bridge. Its purpose is to test the receipt,
///      replay, pending-balance, and post-credit business-state rules explicitly.
contract MigrationHarness {
    bytes32 private constant RECEIPT_TYPEHASH = keccak256(
        "MigrationReceipt(uint256 chainId,address harness,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sender,address recipient,bytes32 payloadHash,uint64 nonce)"
    );

    // secp256k1n / 2; enforcing low-s avoids ECDSA malleability.
    uint256 private constant SECP256K1N_HALF =
        0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0;

    address public immutable admin;
    address public immutable attestor;

    struct PendingMigration {
        bytes32 sourceDomain;
        bytes32 destinationDomain;
        bytes32 asset;
        uint256 amount;
        address sender;
        address recipient;
        uint64 nonce;
        bytes payload;
        bool exists;
    }

    /// @dev Balances are isolated by the domain label, asset, and account.
    mapping(bytes32 domain => mapping(bytes32 asset => mapping(address account => uint256)))
        public balanceOf;
    mapping(bytes32 receiptId => PendingMigration) private pending;
    mapping(bytes32 receiptId => bool) public settled;
    mapping(bytes32 sourceDomain => mapping(address sender => uint64)) public nextNonce;

    // Asset-wide accounting counters make the conservation invariant auditable.
    mapping(bytes32 asset => uint256) public fundedByAsset;
    mapping(bytes32 asset => uint256) public totalBalanceByAsset;
    mapping(bytes32 asset => uint256) public pendingByAsset;
    mapping(bytes32 asset => uint256) public spentByAsset;

    event Funded(
        bytes32 indexed domain,
        bytes32 indexed asset,
        address indexed account,
        uint256 amount
    );
    event MigrationStarted(
        bytes32 indexed receiptId,
        bytes32 indexed sourceDomain,
        bytes32 indexed destinationDomain,
        bytes32 asset,
        uint256 amount,
        address sender,
        address recipient,
        uint64 nonce,
        bytes32 payloadHash
    );
    event MigrationSettled(
        bytes32 indexed receiptId,
        bytes32 indexed destinationDomain,
        bytes32 indexed asset,
        uint256 amount,
        address recipient,
        bytes32 payloadHash
    );
    event BusinessSpend(
        bytes32 indexed domain,
        bytes32 indexed asset,
        address indexed account,
        uint256 amount,
        bytes32 actionPayloadHash
    );

    error Unauthorized();
    error InvalidDomain();
    error InvalidAmount();
    error InvalidAddress();
    error InsufficientBalance();
    error UnknownReceipt();
    error AlreadySettled();
    error InvalidAttestation();
    error AccountingInvariantFailed();

    constructor(address trustedAttestor) {
        if (trustedAttestor == address(0)) revert InvalidAddress();
        admin = msg.sender;
        attestor = trustedAttestor;
    }

    /// @notice Seed test funds. This is a harness faucet and is admin-only.
    function fund(
        bytes32 domain,
        bytes32 asset,
        address account,
        uint256 amount
    ) external {
        if (msg.sender != admin) revert Unauthorized();
        if (domain == bytes32(0)) revert InvalidDomain();
        if (account == address(0)) revert InvalidAddress();
        if (amount == 0) revert InvalidAmount();

        balanceOf[domain][asset][account] += amount;
        fundedByAsset[asset] += amount;
        totalBalanceByAsset[asset] += amount;
        _assertConservation(asset);
        emit Funded(domain, asset, account, amount);
    }

    /// @notice Debit source spendable balance into a pending migration receipt.
    /// @return receiptId Commitment to the complete migration envelope and nonce.
    function beginMigration(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external returns (bytes32 receiptId) {
        if (sourceDomain == bytes32(0) || destinationDomain == bytes32(0)) {
            revert InvalidDomain();
        }
        if (sourceDomain == destinationDomain) revert InvalidDomain();
        if (recipient == address(0)) revert InvalidAddress();
        if (amount == 0) revert InvalidAmount();

        uint256 available = balanceOf[sourceDomain][asset][msg.sender];
        if (available < amount) revert InsufficientBalance();

        uint64 nonce = nextNonce[sourceDomain][msg.sender];
        if (nonce == type(uint64).max) revert InvalidAmount();
        nextNonce[sourceDomain][msg.sender] = nonce + 1;

        receiptId = _receiptId(
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            msg.sender,
            recipient,
            keccak256(payload),
            nonce
        );
        // Nonce and all envelope fields are bound, so receiptId collisions indicate
        // a programming/configuration error rather than a valid duplicate transfer.
        if (pending[receiptId].exists || settled[receiptId]) revert AlreadySettled();

        balanceOf[sourceDomain][asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        pendingByAsset[asset] += amount;
        pending[receiptId] = PendingMigration({
            sourceDomain: sourceDomain,
            destinationDomain: destinationDomain,
            asset: asset,
            amount: amount,
            sender: msg.sender,
            recipient: recipient,
            nonce: nonce,
            payload: payload,
            exists: true
        });
        _assertConservation(asset);

        emit MigrationStarted(
            receiptId,
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            msg.sender,
            recipient,
            nonce,
            keccak256(payload)
        );
    }

    /// @notice Credit destination balance only after the trusted signer attests the receipt.
    /// @dev Anyone may relay a valid attestation; only the bound recipient gets credited.
    function settleMigration(bytes32 receiptId, bytes calldata signature) external {
        if (settled[receiptId]) revert AlreadySettled();
        PendingMigration memory migration = pending[receiptId];
        if (!migration.exists) revert UnknownReceipt();

        bytes32 expectedId = _receiptId(
            migration.sourceDomain,
            migration.destinationDomain,
            migration.asset,
            migration.amount,
            migration.sender,
            migration.recipient,
            keccak256(migration.payload),
            migration.nonce
        );
        if (expectedId != receiptId || !_validAttestation(receiptId, signature)) {
            revert InvalidAttestation();
        }

        // Mark replay state before effects; a revert rolls all state back atomically.
        settled[receiptId] = true;
        delete pending[receiptId];
        pendingByAsset[migration.asset] -= migration.amount;
        balanceOf[migration.destinationDomain][migration.asset][migration.recipient] +=
            migration.amount;
        totalBalanceByAsset[migration.asset] += migration.amount;
        _assertConservation(migration.asset);

        emit MigrationSettled(
            receiptId,
            migration.destinationDomain,
            migration.asset,
            migration.amount,
            migration.recipient,
            keccak256(migration.payload)
        );
    }

    /// @notice Execute a simple value-consuming business action from spendable balance.
    /// @dev Pending funds are excluded from `balanceOf`, so they cannot fund this action.
    function executeBusiness(
        bytes32 domain,
        bytes32 asset,
        uint256 amount,
        bytes calldata actionPayload
    ) external {
        if (domain == bytes32(0)) revert InvalidDomain();
        if (amount == 0) revert InvalidAmount();

        uint256 available = balanceOf[domain][asset][msg.sender];
        if (available < amount) revert InsufficientBalance();
        balanceOf[domain][asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        spentByAsset[asset] += amount;
        _assertConservation(asset);

        emit BusinessSpend(domain, asset, msg.sender, amount, keccak256(actionPayload));
    }

    /// @notice Check per-asset conservation: funded = balances + pending + business spend.
    function checkInvariant(bytes32 asset) external view returns (bool) {
        return
            fundedByAsset[asset] ==
            totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset];
    }

    function pendingReceipt(
        bytes32 receiptId
    ) external view returns (PendingMigration memory) {
        return pending[receiptId];
    }

    /// @dev Full envelope hash, namespaced by chain and harness instance. In production,
    ///      domain identifiers must be defined by the bridge protocol, not user strings.
    function _receiptId(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        uint256 amount,
        address sender,
        address recipient,
        bytes32 payloadHash,
        uint64 nonce
    ) private view returns (bytes32) {
        return
            keccak256(
                abi.encode(
                    RECEIPT_TYPEHASH,
                    block.chainid,
                    address(this),
                    sourceDomain,
                    destinationDomain,
                    asset,
                    amount,
                    sender,
                    recipient,
                    payloadHash,
                    nonce
                )
            );
    }

    /// @dev Attestor signs the 32-byte receiptId using the EIP-191 personal-message prefix.
    function _validAttestation(
        bytes32 receiptId,
        bytes calldata signature
    ) private view returns (bool) {
        if (signature.length != 65) return false;
        bytes32 r;
        bytes32 s;
        uint8 v;
        assembly ("memory-safe") {
            r := calldataload(signature.offset)
            s := calldataload(add(signature.offset, 32))
            v := byte(0, calldataload(add(signature.offset, 64)))
        }
        if (uint256(s) > SECP256K1N_HALF || (v != 27 && v != 28)) return false;

        bytes32 digest = keccak256(
            abi.encodePacked("\x19Ethereum Signed Message:\n32", receiptId)
        );
        return ecrecover(digest, v, r, s) == attestor;
    }

    function _assertConservation(bytes32 asset) private view {
        if (
            fundedByAsset[asset] !=
            totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset]
        ) revert AccountingInvariantFailed();
    }
}
