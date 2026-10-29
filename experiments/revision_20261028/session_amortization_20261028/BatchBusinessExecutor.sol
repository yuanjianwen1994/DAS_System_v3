// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

import "../../../contracts/MigrationHarness.sol";

/// @notice Same-EVM batching control for the session amortization experiment.
/// @dev Logical domains and receipt settlement still use MigrationHarness on one
///      EVM. This helper only batches theta business spends into one transaction.
contract BatchBusinessExecutor {
    MigrationHarness public immutable ledger;

    constructor(address ledgerAddress) {
        require(ledgerAddress != address(0), "zero ledger");
        ledger = MigrationHarness(ledgerAddress);
    }

    function executeBusinessBatch(
        bytes32 domain,
        bytes32 asset,
        uint256 operationCount,
        bytes32 sessionId
    ) external {
        require(operationCount > 0, "empty batch");
        for (uint256 i = 0; i < operationCount; ++i) {
            ledger.executeBusiness(domain, asset, 1, abi.encode(sessionId, i));
        }
    }

    function beginExit(
        bytes32 sourceDomain,
        bytes32 destinationDomain,
        bytes32 asset,
        address recipient,
        uint256 amount,
        bytes calldata payload
    ) external returns (bytes32) {
        return ledger.beginMigration(
            sourceDomain, destinationDomain, asset, recipient, amount, payload
        );
    }
}
