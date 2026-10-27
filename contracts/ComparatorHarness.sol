// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @title ComparatorHarness
/// @notice Balance-backed abstract comparison paths for cross-domain transfers.
/// @dev SAME-EVM ABSTRACT MODELS ONLY. Domains are labels in one contract; this file
///      does not reproduce production Polkadot XCM execution, Avalanche consensus,
///      validator sets, light clients, proofs, or network isolation. It preserves the
///      named source-authorization and one-time-consumption rules so experiments do
///      not gain speed by removing the basic asset-safety properties being compared.
contract ComparatorHarness {
    bytes32 private constant TELEPORT_TYPEHASH = keccak256(
        "TeleportReceipt(uint256 chainId,address harness,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sender,address recipient,bytes32 payloadHash,uint64 nonce)"
    );
    bytes32 private constant ATOMIC_UTXO_TYPEHASH = keccak256(
        "AtomicExportUTXO(uint256 chainId,address harness,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sourceOwner,address recipient,bytes32 payloadHash,uint64 nonce)"
    );

    address public immutable admin;

    struct TeleportReceipt {
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

    struct AtomicUTXO {
        bytes32 sourceDomain;
        bytes32 destinationDomain;
        bytes32 asset;
        uint256 amount;
        address sourceOwner;
        address recipient;
        uint64 nonce;
        bytes payload;
        bool locked;
        bool consumed;
    }

    /// @dev Spendable funds are isolated by domain label, asset label, and account.
    mapping(bytes32 domain => mapping(bytes32 asset => mapping(address account => uint256)))
        public balanceOf;
    mapping(bytes32 destinationDomain => mapping(bytes32 sourceDomain => bool))
        public trustedTeleportSource;
    mapping(bytes32 receiptId => TeleportReceipt) private teleportReceipts;
    mapping(bytes32 receiptId => bool) public teleportSettled;
    mapping(bytes32 sourceDomain => mapping(address sender => uint64))
        public nextTeleportNonce;
    mapping(bytes32 utxoId => AtomicUTXO) private atomicUtxos;
    mapping(bytes32 sourceDomain => mapping(address owner => uint64))
        public nextAtomicNonce;

    // Shared accounting across both comparison paths.
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
    event TrustedTeleportSourceSet(
        bytes32 indexed destinationDomain,
        bytes32 indexed sourceDomain,
        bool trusted
    );
    event TeleportStarted(
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
    event TeleportSettled(
        bytes32 indexed receiptId,
        bytes32 indexed destinationDomain,
        bytes32 indexed asset,
        uint256 amount,
        address recipient,
        bytes32 payloadHash
    );
    event AtomicExportLocked(
        bytes32 indexed utxoId,
        bytes32 indexed sourceDomain,
        bytes32 indexed destinationDomain,
        bytes32 asset,
        uint256 amount,
        address sourceOwner,
        address recipient,
        uint64 nonce,
        bytes32 payloadHash
    );
    event AtomicImportConsumed(
        bytes32 indexed utxoId,
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
    error InvalidAddress();
    error InvalidAmount();
    error InsufficientBalance();
    error UnknownReceipt();
    error AlreadyConsumed();
    error UntrustedSource();
    error EnvelopeMismatch();
    error AccountingInvariantFailed();

    constructor() {
        admin = msg.sender;
    }

    /// @notice Admin-only faucet for initializing the local test ledger.
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

    /// @notice Configure a destination's explicit trusted-source channel for the teleport abstraction.
    function setTrustedTeleportSource(
        bytes32 destinationDomain,
        bytes32 sourceDomain,
        bool trusted
    ) external {
        if (msg.sender != admin) revert Unauthorized();
        if (
            destinationDomain == bytes32(0) ||
            sourceDomain == bytes32(0) ||
            destinationDomain == sourceDomain
        ) revert InvalidDomain();
        trustedTeleportSource[destinationDomain][sourceDomain] = trusted;
        emit TrustedTeleportSourceSet(destinationDomain, sourceDomain, trusted);
    }

    /// @notice Debit source spendable balance into a teleport-inspired pending receipt.
    function beginTeleport(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external returns (bytes32 receiptId) {
        _validateTransfer(sourceDomain, destinationDomain, recipient, amount);
        uint256 available = balanceOf[sourceDomain][asset][msg.sender];
        if (available < amount) revert InsufficientBalance();

        uint64 nonce = nextTeleportNonce[sourceDomain][msg.sender];
        if (nonce == type(uint64).max) revert InvalidAmount();
        nextTeleportNonce[sourceDomain][msg.sender] = nonce + 1;
        receiptId = _teleportReceiptId(
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            msg.sender,
            recipient,
            keccak256(payload),
            nonce
        );
        if (teleportReceipts[receiptId].exists || teleportSettled[receiptId]) {
            revert AlreadyConsumed();
        }

        balanceOf[sourceDomain][asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        pendingByAsset[asset] += amount;
        teleportReceipts[receiptId] = TeleportReceipt({
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

        emit TeleportStarted(
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

    /// @notice Settle a teleport-inspired receipt after source trust and envelope checks.
    /// @dev This same-harness receipt lookup stands in for an abstract trusted channel;
    ///      it is not a cross-chain proof or a production XCM executor.
    function settleTeleport(
        bytes32 receiptId,
        bytes32 destinationDomain,
        bytes32 asset,
        address sender,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external {
        if (teleportSettled[receiptId]) revert AlreadyConsumed();
        TeleportReceipt memory receipt = teleportReceipts[receiptId];
        if (!receipt.exists) revert UnknownReceipt();
        if (!trustedTeleportSource[destinationDomain][receipt.sourceDomain]) {
            revert UntrustedSource();
        }
        if (
            receipt.destinationDomain != destinationDomain ||
            receipt.asset != asset ||
            receipt.sender != sender ||
            receipt.recipient != recipient ||
            receipt.amount != amount ||
            keccak256(receipt.payload) != keccak256(payload) ||
            _teleportReceiptId(
                receipt.sourceDomain,
                destinationDomain,
                asset,
                amount,
                sender,
                recipient,
                keccak256(payload),
                receipt.nonce
            ) != receiptId
        ) revert EnvelopeMismatch();

        // Record one-time consumption before credit. Reverts roll back both atomically.
        teleportSettled[receiptId] = true;
        delete teleportReceipts[receiptId];
        pendingByAsset[asset] -= amount;
        balanceOf[destinationDomain][asset][recipient] += amount;
        totalBalanceByAsset[asset] += amount;
        _assertConservation(asset);
        emit TeleportSettled(
            receiptId,
            destinationDomain,
            asset,
            amount,
            recipient,
            keccak256(payload)
        );
    }

    /// @notice Lock source balance in an export UTXO for the atomic export/import abstraction.
    function exportAtomic(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external returns (bytes32 utxoId) {
        _validateTransfer(sourceDomain, destinationDomain, recipient, amount);
        uint256 available = balanceOf[sourceDomain][asset][msg.sender];
        if (available < amount) revert InsufficientBalance();

        uint64 nonce = nextAtomicNonce[sourceDomain][msg.sender];
        if (nonce == type(uint64).max) revert InvalidAmount();
        nextAtomicNonce[sourceDomain][msg.sender] = nonce + 1;
        utxoId = _atomicUtxoId(
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            msg.sender,
            recipient,
            keccak256(payload),
            nonce
        );
        if (atomicUtxos[utxoId].locked || atomicUtxos[utxoId].consumed) {
            revert AlreadyConsumed();
        }

        balanceOf[sourceDomain][asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        pendingByAsset[asset] += amount;
        atomicUtxos[utxoId] = AtomicUTXO({
            sourceDomain: sourceDomain,
            destinationDomain: destinationDomain,
            asset: asset,
            amount: amount,
            sourceOwner: msg.sender,
            recipient: recipient,
            nonce: nonce,
            payload: payload,
            locked: true,
            consumed: false
        });
        _assertConservation(asset);

        emit AtomicExportLocked(
            utxoId,
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

    /// @notice Consume the locked export UTXO once, then credit only its bound destination owner.
    /// @dev UTXO consumption and balance credit occur in one EVM transaction. This models
    ///      atomic consume-once accounting, not Avalanche consensus or cross-chain proofs.
    function importAtomic(
        bytes32 utxoId,
        bytes32 destinationDomain,
        bytes32 asset,
        address sourceOwner,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external {
        AtomicUTXO storage utxo = atomicUtxos[utxoId];
        if (!utxo.locked) {
            if (utxo.consumed) revert AlreadyConsumed();
            revert UnknownReceipt();
        }
        if (
            utxo.destinationDomain != destinationDomain ||
            utxo.asset != asset ||
            utxo.sourceOwner != sourceOwner ||
            utxo.recipient != recipient ||
            utxo.amount != amount ||
            keccak256(utxo.payload) != keccak256(payload) ||
            _atomicUtxoId(
                utxo.sourceDomain,
                destinationDomain,
                asset,
                amount,
                sourceOwner,
                recipient,
                keccak256(payload),
                utxo.nonce
            ) != utxoId
        ) revert EnvelopeMismatch();

        // A revert on any subsequent effect rolls back this consumption flag as well.
        utxo.locked = false;
        utxo.consumed = true;
        pendingByAsset[asset] -= amount;
        balanceOf[destinationDomain][asset][recipient] += amount;
        totalBalanceByAsset[asset] += amount;
        _assertConservation(asset);
        emit AtomicImportConsumed(
            utxoId,
            destinationDomain,
            asset,
            amount,
            recipient,
            keccak256(payload)
        );
    }

    /// @notice Consume only the caller's spendable, already credited balance.
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

    /// @notice Shared accounting invariant: funded = spendable + pending + business spend.
    function checkInvariant(bytes32 asset) external view returns (bool) {
        return
            fundedByAsset[asset] ==
            totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset];
    }

    function teleportReceipt(bytes32 receiptId) external view returns (TeleportReceipt memory) {
        return teleportReceipts[receiptId];
    }

    function atomicUtxo(bytes32 utxoId) external view returns (AtomicUTXO memory) {
        return atomicUtxos[utxoId];
    }

    function _validateTransfer(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        address recipient,
        uint256 amount
    ) private pure {
        if (
            sourceDomain == bytes32(0) ||
            destinationDomain == bytes32(0) ||
            sourceDomain == destinationDomain
        ) revert InvalidDomain();
        if (recipient == address(0)) revert InvalidAddress();
        if (amount == 0) revert InvalidAmount();
    }

    function _teleportReceiptId(
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
                    TELEPORT_TYPEHASH,
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

    function _atomicUtxoId(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        uint256 amount,
        address sourceOwner,
        address recipient,
        bytes32 payloadHash,
        uint64 nonce
    ) private view returns (bytes32) {
        return
            keccak256(
                abi.encode(
                    ATOMIC_UTXO_TYPEHASH,
                    block.chainid,
                    address(this),
                    sourceDomain,
                    destinationDomain,
                    asset,
                    amount,
                    sourceOwner,
                    recipient,
                    payloadHash,
                    nonce
                )
            );
    }

    function _assertConservation(bytes32 asset) private view {
        if (
            fundedByAsset[asset] !=
            totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset]
        ) revert AccountingInvariantFailed();
    }
}
