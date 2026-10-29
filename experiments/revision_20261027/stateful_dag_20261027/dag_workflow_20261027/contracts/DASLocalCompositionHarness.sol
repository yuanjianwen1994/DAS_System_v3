// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @notice Local Type-II composition harness for parameterized stateful DAGs.
/// @dev Entry/exit are separate boundary-storage transactions. This is not a
///      bridge proof or a production TSC/network migration implementation.
contract DASLocalCompositionHarness {
    uint64 private constant ROUND_MULTIPLIER = 0xd6e8feb86659fd93;
    uint64 private constant WORKFLOW_MULTIPLIER = 0x9e3779b97f4a7c15;
    uint64 private constant NODE_MULTIPLIER = 0xa0761d6478bd642f;

    struct Session {
        bool active;
        uint32 expectedWorkflows;
        uint32 executedWorkflows;
        uint64 aggregate;
        bytes32 entryCommitment;
        bytes32 exitCommitment;
    }

    mapping(uint64 => Session) private sessions;
    mapping(uint64 => mapping(uint64 => uint64)) public workflowChecksums;
    mapping(uint64 => mapping(uint64 => mapping(uint32 => uint64))) public nodeChecksums;
    mapping(uint64 => mapping(uint64 => uint32)) public workflowOperationCounts;
    mapping(uint64 => mapping(uint64 => bool)) private workflowSeen;

    event SessionEntered(uint64 indexed sessionId, uint32 expectedWorkflows, bytes32 entryCommitment);
    event WorkflowCompleted(uint64 indexed sessionId, uint64 indexed workflowId, uint64 checksum, uint32 nodeCount);
    event SessionExited(uint64 indexed sessionId, uint32 executedWorkflows, uint64 aggregate, bytes32 exitCommitment);

    /// @dev Measured separately as migration-in boundary storage cost.
    function enterSession(uint64 sessionId, uint32 expectedWorkflows, bytes32 entryCommitment) external {
        Session storage s = sessions[sessionId];
        require(!s.active && s.expectedWorkflows == 0, "session id already used");
        require(expectedWorkflows > 0 && entryCommitment != bytes32(0), "invalid session manifest");
        s.active = true;
        s.expectedWorkflows = expectedWorkflows;
        s.entryCommitment = entryCommitment;
        emit SessionEntered(sessionId, expectedWorkflows, entryCommitment);
    }

    /// @notice Executes theta independent workflows in one local Type-II-state call.
    function executeWorkflows(
        uint64 sessionId,
        uint64[] calldata workflowIds,
        uint64[] calldata seeds,
        uint32[][] calldata parents,
        uint32 operationsPerNode
    ) external returns (uint64[] memory checksums) {
        Session storage s = sessions[sessionId];
        require(s.active, "session not active");
        require(workflowIds.length > 0 && workflowIds.length == seeds.length, "workflow input mismatch");
        require(s.executedWorkflows + workflowIds.length <= s.expectedWorkflows, "session workflow count exceeded");
        require(parents.length > 0 && operationsPerNode > 0, "empty DAG or operation count");
        checksums = new uint64[](workflowIds.length);

        for (uint256 i = 0; i < workflowIds.length; ++i) {
            checksums[i] = _executeWorkflow(sessionId, workflowIds[i], seeds[i], parents, operationsPerNode, type(uint32).max);
            s.executedWorkflows += 1;
            s.aggregate ^= _rotateLeft(checksums[i], s.executedWorkflows % 64);
            emit WorkflowCompleted(sessionId, workflowIds[i], checksums[i], uint32(parents.length));
        }
    }

    /// @dev Fault-injection probe. A reverting transaction rolls back all its writes.
    function probeFailure(
        uint64 sessionId,
        uint64 workflowId,
        uint64 seed,
        uint32[][] calldata parents,
        uint32 operationsPerNode,
        uint32 failAtNode
    ) external {
        require(sessions[sessionId].active, "session not active");
        require(parents.length > 0 && failAtNode < parents.length, "invalid failure probe");
        _executeWorkflow(sessionId, workflowId, seed, parents, operationsPerNode, failAtNode);
        revert("failure node was not reached");
    }

    /// @dev Measured separately as migration-out boundary storage cost.
    function exitSession(uint64 sessionId, bytes32 exitCommitment) external returns (uint64 aggregate) {
        Session storage s = sessions[sessionId];
        require(s.active && s.executedWorkflows == s.expectedWorkflows, "session incomplete");
        require(exitCommitment != bytes32(0), "invalid exit commitment");
        s.active = false;
        s.exitCommitment = exitCommitment;
        aggregate = s.aggregate;
        emit SessionExited(sessionId, s.executedWorkflows, aggregate, exitCommitment);
    }

    /// @dev Clears an intentionally incomplete fault-probe session after recording the failure.
    function abortSession(uint64 sessionId) external {
        Session storage s = sessions[sessionId];
        require(s.active && s.executedWorkflows < s.expectedWorkflows, "session is not incomplete");
        s.active = false;
    }

    function sessionState(uint64 sessionId) external view returns (Session memory) {
        return sessions[sessionId];
    }

    function _executeWorkflow(
        uint64 sessionId,
        uint64 workflowId,
        uint64 seed,
        uint32[][] calldata parents,
        uint32 operationsPerNode,
        uint32 failAtNode
    ) private returns (uint64 checksum) {
        require(!workflowSeen[sessionId][workflowId], "workflow id already used");
        uint64[] memory outputs = new uint64[](parents.length);
        uint64 totalOperations;
        for (uint32 nodeId = 0; nodeId < parents.length; ++nodeId) {
            outputs[nodeId] = _calculateNode(workflowId, seed, nodeId, parents[nodeId], outputs, operationsPerNode);
            nodeChecksums[sessionId][workflowId][nodeId] = outputs[nodeId];
            require(nodeId != failAtNode, "injected stage failure");
            totalOperations += operationsPerNode;
        }
        for (uint32 nodeId = 0; nodeId < parents.length; ++nodeId) {
            bool hasChild;
            for (uint256 child = 0; child < parents.length && !hasChild; ++child) {
                for (uint256 p = 0; p < parents[child].length; ++p) {
                    if (parents[child][p] == nodeId) {
                        hasChild = true;
                        break;
                    }
                }
            }
            if (!hasChild) checksum ^= _rotateLeft(outputs[nodeId], (nodeId * 11) % 64);
        }
        workflowChecksums[sessionId][workflowId] = checksum;
        workflowOperationCounts[sessionId][workflowId] = uint32(totalOperations);
        workflowSeen[sessionId][workflowId] = true;
    }

    function _calculateNode(
        uint64 workflowId,
        uint64 seed,
        uint32 nodeId,
        uint32[] calldata parentIds,
        uint64[] memory priorOutputs,
        uint32 operationsPerNode
    ) private pure returns (uint64 value) {
        unchecked {
            value = seed ^ (workflowId * WORKFLOW_MULTIPLIER) ^ (uint64(nodeId) * NODE_MULTIPLIER);
            for (uint256 p = 0; p < parentIds.length; ++p) {
                uint32 parentId = parentIds[p];
                require(parentId < nodeId, "DAG must be topologically ordered");
                value = _rotateLeft(value, (parentId * 13 + 7) % 64) ^ priorOutputs[parentId];
                value += uint64(parentId) * WORKFLOW_MULTIPLIER;
            }
            for (uint32 round = 0; round < operationsPerNode; ++round) {
                value = _rotateLeft(value, 7) * ROUND_MULTIPLIER + (uint64(round) ^ uint64(nodeId) ^ workflowId);
            }
        }
    }

    function _rotateLeft(uint64 value, uint32 amount) private pure returns (uint64) {
        if (amount == 0) return value;
        return (value << amount) | (value >> (64 - amount));
    }
}
