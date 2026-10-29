// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @title AdmissionAblationHarness
/// @notice UNSAFE TEST-ONLY admission-rule ablations; NEVER use as a SOTA comparator.
/// @dev Deploy one instance per experiment with exactly one rule removed. This is a
///      same-EVM ledger harness, not a bridge or production isolation mechanism. A
///      deliberate exploit is recorded instead of reverted so experiments can inspect
///      the resulting balances, pending amounts, and invariant diagnostics.
contract AdmissionAblationHarness {
    bytes32 private constant RECEIPT_TYPEHASH = keccak256(
        "AblationReceipt(uint256 chainId,address harness,bytes32 sourceDomain,bytes32 destinationDomain,bytes32 asset,uint256 amount,address sender,address recipient,bytes32 payloadHash,uint64 nonce)"
    );

    enum RemovedRule {
        None,
        SourceAuthorization,
        DestinationPayloadBinding,
        ReplayConsumption,
        SourceDebit
    }

    struct PendingReceipt {
        bytes32 sourceDomain;
        bytes32 destinationDomain;
        bytes32 asset;
        uint256 amount;
        address sender;
        address recipient;
        uint64 nonce;
        bytes payload;
        bool exists;
        bool pendingReleased;
        bool consumed;
    }

    address public immutable admin;
    RemovedRule public immutable removedRule;

    mapping(bytes32 domain => mapping(bytes32 asset => mapping(address account => uint256)))
        public balanceOf;
    mapping(bytes32 receiptId => PendingReceipt) private receipts;
    mapping(bytes32 sourceDomain => mapping(address sender => uint64)) public nextNonce;

    mapping(bytes32 asset => uint256) public fundedByAsset;
    mapping(bytes32 asset => uint256) public totalBalanceByAsset;
    mapping(bytes32 asset => uint256) public pendingByAsset;
    mapping(bytes32 asset => uint256) public spentByAsset;

    /// @dev Safety findings are tracked separately from arithmetic conservation because
    ///      an unauthorized transfer may conserve tokens while violating admission.
    mapping(bytes32 asset => uint256) public admissionViolationsByAsset;

    event Funded(bytes32 indexed domain, bytes32 indexed asset, address indexed account, uint256 amount);
    event MigrationStarted(
        bytes32 indexed receiptId,
        bytes32 indexed sourceDomain,
        bytes32 indexed destinationDomain,
        bytes32 asset,
        uint256 amount,
        address caller,
        address statedSender,
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
        bytes32 submittedPayloadHash,
        bool replay
    );
    event AblationExploitObserved(
        RemovedRule indexed removedRule,
        bytes32 indexed receiptId,
        bytes32 indexed asset,
        address caller,
        bytes32 detail
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
    error EnvelopeMismatch();

    constructor(RemovedRule ruleToRemove) {
        admin = msg.sender;
        removedRule = ruleToRemove;
    }

    /// @notice Admin-only faucet to create known test balances.
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
        emit Funded(domain, asset, account, amount);
    }

    /// @notice Debit source balance to pending and create a receipt for the configured ablation.
    /// @dev `sender` is explicit only so SourceAuthorization can be ablated reproducibly.
    function beginMigration(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address sender,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external returns (bytes32 receiptId) {
        if (
            sourceDomain == bytes32(0) ||
            destinationDomain == bytes32(0) ||
            sourceDomain == destinationDomain
        ) revert InvalidDomain();
        if (sender == address(0) || recipient == address(0)) revert InvalidAddress();
        if (amount == 0) revert InvalidAmount();

        if (removedRule != RemovedRule.SourceAuthorization && msg.sender != sender) {
            revert Unauthorized();
        }
        if (msg.sender != sender) {
            admissionViolationsByAsset[asset] += 1;
        }

        uint256 available = balanceOf[sourceDomain][asset][sender];
        if (available < amount) revert InsufficientBalance();
        uint64 nonce = nextNonce[sourceDomain][sender];
        if (nonce == type(uint64).max) revert InvalidAmount();
        nextNonce[sourceDomain][sender] = nonce + 1;

        bytes32 payloadHash = keccak256(payload);
        receiptId = _receiptId(
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            sender,
            recipient,
            payloadHash,
            nonce
        );
        if (receipts[receiptId].exists) revert AlreadyConsumed();

        balanceOf[sourceDomain][asset][sender] = available - amount;
        totalBalanceByAsset[asset] -= amount;
        pendingByAsset[asset] += amount;
        receipts[receiptId] = PendingReceipt({
            sourceDomain: sourceDomain,
            destinationDomain: destinationDomain,
            asset: asset,
            amount: amount,
            sender: sender,
            recipient: recipient,
            nonce: nonce,
            payload: payload,
            exists: true,
            pendingReleased: false,
            consumed: false
        });

        if (msg.sender != sender) {
            emit AblationExploitObserved(
                removedRule,
                receiptId,
                asset,
                msg.sender,
                keccak256("unauthorized source debit")
            );
        }
        emit MigrationStarted(
            receiptId,
            sourceDomain,
            destinationDomain,
            asset,
            amount,
            msg.sender,
            sender,
            recipient,
            nonce,
            payloadHash
        );
    }

    /// @notice Attempt destination settlement; disabled rules record successful violations.
    /// @dev In the binding ablation, the caller can substitute source/destination/recipient/payload.
    ///      In the replay ablation, a receipt can credit repeatedly; pending is released only once
    ///      so the accounting diagnostic exposes the extra credit rather than masking it.
    function settleMigration(
        bytes32 receiptId,
        bytes32 submittedSourceDomain,
        bytes32 submittedDestinationDomain,
        address submittedRecipient,
        bytes calldata submittedPayload
    ) external {
        PendingReceipt storage receipt = receipts[receiptId];
        if (!receipt.exists) revert UnknownReceipt();

        bool replay = receipt.pendingReleased;
        if (replay && removedRule != RemovedRule.ReplayConsumption) {
            revert AlreadyConsumed();
        }

        bool envelopeMatches =
            submittedSourceDomain == receipt.sourceDomain &&
            submittedDestinationDomain == receipt.destinationDomain &&
            submittedRecipient == receipt.recipient &&
            keccak256(submittedPayload) == keccak256(receipt.payload);
        if (!envelopeMatches && removedRule != RemovedRule.DestinationPayloadBinding) {
            revert EnvelopeMismatch();
        }

        bytes32 creditedDomain =
            removedRule == RemovedRule.DestinationPayloadBinding
                ? submittedDestinationDomain
                : receipt.destinationDomain;
        address creditedRecipient =
            removedRule == RemovedRule.DestinationPayloadBinding
                ? submittedRecipient
                : receipt.recipient;

        if (!envelopeMatches) {
            admissionViolationsByAsset[receipt.asset] += 1;
            emit AblationExploitObserved(
                removedRule,
                receiptId,
                receipt.asset,
                msg.sender,
                keccak256("destination or payload binding bypass")
            );
        }
        if (replay) {
            admissionViolationsByAsset[receipt.asset] += 1;
            emit AblationExploitObserved(
                removedRule,
                receiptId,
                receipt.asset,
                msg.sender,
                keccak256("receipt replay credited twice")
            );
        }

        // The first use transfers accounting from pending to balance. An ablated replay
        // can credit again but cannot release the same pending amount a second time.
        if (!receipt.pendingReleased) {
            receipt.pendingReleased = true;
            pendingByAsset[receipt.asset] -= receipt.amount;
        }
        if (removedRule != RemovedRule.ReplayConsumption) {
            receipt.consumed = true;
        }
        balanceOf[creditedDomain][receipt.asset][creditedRecipient] += receipt.amount;
        totalBalanceByAsset[receipt.asset] += receipt.amount;

        emit MigrationSettled(
            receiptId,
            creditedDomain,
            receipt.asset,
            receipt.amount,
            creditedRecipient,
            keccak256(submittedPayload),
            replay
        );
    }

    /// @notice Explicit negative control for mint without any source debit or receipt.
    /// @dev Available only in the SourceDebit ablation; deliberately breaks conservation.
    function mintWithoutSourceDebit(
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount
    ) external {
        if (removedRule != RemovedRule.SourceDebit) revert Unauthorized();
        if (destinationDomain == bytes32(0)) revert InvalidDomain();
        if (recipient == address(0)) revert InvalidAddress();
        if (amount == 0) revert InvalidAmount();

        admissionViolationsByAsset[asset] += 1;
        balanceOf[destinationDomain][asset][recipient] += amount;
        totalBalanceByAsset[asset] += amount;
        emit AblationExploitObserved(
            removedRule,
            bytes32(0),
            asset,
            msg.sender,
            keccak256("mint without source debit or receipt")
        );
        emit MigrationSettled(
            bytes32(0),
            destinationDomain,
            asset,
            amount,
            recipient,
            bytes32(0),
            false
        );
    }

    /// @notice Spend only the caller's previously credited balance.
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
        emit BusinessSpend(domain, asset, msg.sender, amount, keccak256(actionPayload));
    }

    /// @notice Pure arithmetic diagnostic; returns false instead of reverting on over-credit.
    function checkAccountingConservation(bytes32 asset) public view returns (bool) {
        return
            fundedByAsset[asset] ==
            totalBalanceByAsset[asset] + pendingByAsset[asset] + spentByAsset[asset];
    }

    /// @notice Combined diagnostic; false for either accounting loss/creation or recorded ablation exploit.
    function checkInvariant(bytes32 asset) external view returns (bool) {
        return
            checkAccountingConservation(asset) && admissionViolationsByAsset[asset] == 0;
    }

    function pendingReceipt(bytes32 receiptId) external view returns (PendingReceipt memory) {
        return receipts[receiptId];
    }

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
}
