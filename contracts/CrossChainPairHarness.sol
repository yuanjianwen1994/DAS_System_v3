// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @notice Two-chain accounting harness used with two independent Anvil processes.
/// @dev Source-side exports remain pending locally; destination import is audited
///      separately by matching receipt IDs, with no synthetic source acknowledgement.
contract CrossChainSourceHarness {
    bytes32 private constant RECEIPT_TYPEHASH = keccak256(
        "CrossChainReceipt(uint256 sourceChainId,address sourceEndpoint,uint256 destinationChainId,address destinationEndpoint,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sender,address recipient,bytes32 payloadHash,uint64 nonce)"
    );
    address public immutable admin;

    struct ExportRecord {
        bytes32 sourceDomain;
        bytes32 destinationDomain;
        bytes32 asset;
        uint256 amount;
        uint256 destinationChainId;
        address destinationEndpoint;
        address sender;
        address recipient;
        bytes32 payloadHash;
        uint64 nonce;
        bool active;
    }

    mapping(bytes32 => mapping(address => uint256)) public balanceOf;
    mapping(bytes32 => ExportRecord) public exports;
    mapping(bytes32 => mapping(address => uint64)) public nextNonce;
    mapping(bytes32 => uint256) public fundedByAsset;
    mapping(bytes32 => uint256) public totalBalanceByAsset;
    mapping(bytes32 => uint256) public pendingByAsset;
    mapping(bytes32 => uint256) public spentByAsset;

    event ExportStarted(
        bytes32 indexed receiptId,
        uint256 sourceChainId,
        address sourceEndpoint,
        uint256 destinationChainId,
        address destinationEndpoint,
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        uint256 amount,
        address sender,
        address recipient,
        bytes32 payloadHash,
        uint64 nonce
    );
    event BusinessSpend(bytes32 indexed domain, bytes32 indexed asset, address indexed account, uint256 amount);

    error Unauthorized();
    error InvalidEnvelope();
    error InsufficientBalance();
    error AccountingInvariantFailed();

    constructor() {
        admin = msg.sender;
    }

    function fund(bytes32 asset, address account, uint256 amount) external {
        if (msg.sender != admin) revert Unauthorized();
        if (asset == bytes32(0) || account == address(0) || amount == 0) revert InvalidEnvelope();
        balanceOf[asset][account] += amount;
        fundedByAsset[asset] += amount;
        totalBalanceByAsset[asset] += amount;
        _assertConservation(asset);
    }

    function beginExport(
        uint256 destinationChainId,
        address destinationEndpoint,
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount,
        bytes32 payloadHash
    ) external returns (bytes32 receiptId) {
        if (
            destinationChainId == 0 || destinationChainId == block.chainid ||
            destinationEndpoint == address(0) || sourceDomain == bytes32(0) ||
            destinationDomain == bytes32(0) || sourceDomain == destinationDomain ||
            asset == bytes32(0) || recipient == address(0) || amount == 0
        ) revert InvalidEnvelope();
        uint256 available = balanceOf[asset][msg.sender];
        if (available < amount) revert InsufficientBalance();
        uint64 nonce = nextNonce[asset][msg.sender];
        nextNonce[asset][msg.sender] = nonce + 1;
        receiptId = _receiptId(
            block.chainid, address(this), destinationChainId, destinationEndpoint,
            sourceDomain, destinationDomain, asset, amount, msg.sender, recipient,
            payloadHash, nonce
        );
        if (exports[receiptId].active) revert InvalidEnvelope();

        balanceOf[asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        pendingByAsset[asset] += amount;
        exports[receiptId] = ExportRecord(
            sourceDomain, destinationDomain, asset, amount, destinationChainId,
            destinationEndpoint, msg.sender, recipient, payloadHash, nonce, true
        );
        _assertConservation(asset);
        emit ExportStarted(
            receiptId, block.chainid, address(this), destinationChainId, destinationEndpoint,
            sourceDomain, destinationDomain, asset, amount, msg.sender, recipient,
            payloadHash, nonce
        );
    }

    function executeBusiness(bytes32 domain, bytes32 asset, uint256 amount) external {
        uint256 available = balanceOf[asset][msg.sender];
        if (domain == bytes32(0) || amount == 0 || available < amount) revert InsufficientBalance();
        balanceOf[asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        spentByAsset[asset] += amount;
        _assertConservation(asset);
        emit BusinessSpend(domain, asset, msg.sender, amount);
    }

    function checkInvariant(bytes32 asset) external view returns (bool) {
        return fundedByAsset[asset] == totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset];
    }

    function _assertConservation(bytes32 asset) private view {
        if (fundedByAsset[asset] != totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset]) revert AccountingInvariantFailed();
    }

    function _receiptId(
        uint256 sourceChainId, address sourceEndpoint, uint256 destinationChainId,
        address destinationEndpoint, bytes32 sourceDomain, bytes32 destinationDomain,
        bytes32 asset, uint256 amount, address sender, address recipient,
        bytes32 payloadHash, uint64 nonce
    ) private pure returns (bytes32) {
        return keccak256(abi.encode(
            RECEIPT_TYPEHASH, sourceChainId, sourceEndpoint, destinationChainId,
            destinationEndpoint, sourceDomain, destinationDomain, asset, amount,
            sender, recipient, payloadHash, nonce
        ));
    }

}

/// @dev ModelKind: 0 DAS receipt attestation; 1 teleport trusted-source abstraction;
///      2 atomic-export UTXO abstraction. All execute on independent local chains.
contract CrossChainDestinationHarness {
    bytes32 private constant RECEIPT_TYPEHASH = keccak256(
        "CrossChainReceipt(uint256 sourceChainId,address sourceEndpoint,uint256 destinationChainId,address destinationEndpoint,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sender,address recipient,bytes32 payloadHash,uint64 nonce)"
    );
    uint256 private constant HALF_N =
        0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0;

    address public immutable admin;
    address public immutable attestor;
    uint8 public immutable modelKind;
    bytes32 public immutable localDomain;
    mapping(bytes32 => mapping(address => uint256)) public balanceOf;
    mapping(bytes32 => bool) public importedReceipt;
    mapping(bytes32 => uint256) public importedByAsset;
    mapping(bytes32 => uint256) public totalBalanceByAsset;
    mapping(bytes32 => uint256) public spentByAsset;
    mapping(uint256 => mapping(address => mapping(bytes32 => bool))) public trustedSource;

    event ImportSettled(bytes32 indexed receiptId, bytes32 indexed asset, address indexed recipient, uint256 amount, bytes32 payloadHash);
    event BusinessSpend(bytes32 indexed domain, bytes32 indexed asset, address indexed account, uint256 amount);

    error Unauthorized();
    error InvalidEnvelope();
    error UntrustedSource();
    error InvalidAttestation();
    error AlreadyImported();
    error InsufficientBalance();
    error AccountingInvariantFailed();

    constructor(address trustedAttestor, uint8 kind, bytes32 domain) {
        if (trustedAttestor == address(0) || kind > 2 || domain == bytes32(0)) revert InvalidEnvelope();
        admin = msg.sender;
        attestor = trustedAttestor;
        modelKind = kind;
        localDomain = domain;
    }

    function setTrustedSource(uint256 sourceChainId, address sourceEndpoint, bytes32 sourceDomain, bool trust) external {
        if (msg.sender != admin) revert Unauthorized();
        if (sourceChainId == block.chainid || sourceChainId == 0 || sourceEndpoint == address(0) || sourceDomain == bytes32(0)) revert InvalidEnvelope();
        trustedSource[sourceChainId][sourceEndpoint][sourceDomain] = trust;
    }

    function importReceipt(
        bytes32 receiptId, uint256 sourceChainId, address sourceEndpoint,
        bytes32 sourceDomain, bytes32 destinationDomain, bytes32 asset,
        uint256 amount, address sender, address recipient, bytes32 payloadHash,
        uint64 nonce, bytes calldata signature
    ) external {
        if (importedReceipt[receiptId]) revert AlreadyImported();
        if (
            sourceChainId == 0 || sourceChainId == block.chainid || sourceEndpoint == address(0) ||
            sourceDomain == bytes32(0) || destinationDomain != localDomain || asset == bytes32(0) ||
            amount == 0 || sender == address(0) || recipient == address(0)
        ) revert InvalidEnvelope();
        // Teleport abstraction models a preconfigured trusted source channel in addition
        // to envelope authentication. Other models retain their own receipt/UTXO guard.
        if (modelKind == 1 && !trustedSource[sourceChainId][sourceEndpoint][sourceDomain]) revert UntrustedSource();
        bytes32 expected = keccak256(abi.encode(
            RECEIPT_TYPEHASH, sourceChainId, sourceEndpoint, block.chainid, address(this),
            sourceDomain, destinationDomain, asset, amount, sender, recipient, payloadHash, nonce
        ));
        if (expected != receiptId || _recover(receiptId, signature) != attestor) revert InvalidAttestation();

        // Set-before-credit provides exact-once consumption. For modelKind 2 this is
        // the harness's atomic-export UTXO consume-on-import abstraction.
        importedReceipt[receiptId] = true;
        importedByAsset[asset] += amount;
        balanceOf[asset][recipient] += amount;
        totalBalanceByAsset[asset] += amount;
        _assertConservation(asset);
        emit ImportSettled(receiptId, asset, recipient, amount, payloadHash);
    }

    function executeBusiness(bytes32 domain, bytes32 asset, uint256 amount) external {
        uint256 available = balanceOf[asset][msg.sender];
        if (domain != localDomain || amount == 0 || available < amount) revert InsufficientBalance();
        balanceOf[asset][msg.sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        spentByAsset[asset] += amount;
        _assertConservation(asset);
        emit BusinessSpend(domain, asset, msg.sender, amount);
    }

    function checkInvariant(bytes32 asset) external view returns (bool) {
        return importedByAsset[asset] == totalBalanceByAsset[asset] + spentByAsset[asset];
    }

    function _assertConservation(bytes32 asset) private view {
        if (importedByAsset[asset] != totalBalanceByAsset[asset] + spentByAsset[asset]) revert AccountingInvariantFailed();
    }

    function _recover(bytes32 digest, bytes calldata signature) private pure returns (address signer) {
        if (signature.length != 65) return address(0);
        bytes32 r; bytes32 s; uint8 v;
        assembly { r := calldataload(signature.offset) s := calldataload(add(signature.offset, 32)) v := byte(0, calldataload(add(signature.offset, 64))) }
        if (v < 27) v += 27;
        if (uint256(s) > HALF_N || (v != 27 && v != 28)) return address(0);
        return ecrecover(keccak256(abi.encodePacked("\x19Ethereum Signed Message:\n32", digest)), v, r, s);
    }
}
