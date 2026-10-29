// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @notice One independently deployed, independently stateful DAG node.
/// @dev All mutations are reachable only through its owning router's external CALL.
contract DAGStage {
    uint64 private constant ROUND_MULTIPLIER = 0xd6e8feb86659fd93;
    uint64 private constant WORKFLOW_MULTIPLIER = 0x9e3779b97f4a7c15;
    uint64 private constant NODE_MULTIPLIER = 0xa0761d6478bd642f;

    address public immutable router;
    uint32 public immutable nodeId;
    uint32 public immutable failAtNode;
    uint64 public immutable failWorkflowId;
    uint32[] private expectedParents;

    struct NodeResult {
        bool exists;
        uint64 sessionId;
        uint64 checksum;
        uint32 inputCount;
        uint32 operationCount;
    }

    mapping(uint64 => mapping(uint64 => NodeResult)) private results;
    mapping(uint64 => mapping(uint64 => mapping(uint32 => uint64))) public inputValues;
    mapping(uint64 => mapping(uint64 => mapping(uint32 => bool))) public hasInput;

    event ParentInputConsumed(
        uint64 indexed sessionId,
        uint64 indexed workflowId,
        uint32 indexed nodeId,
        uint32 parentNodeId,
        uint64 value
    );
    event NodeExecuted(
        uint64 indexed sessionId,
        uint64 indexed workflowId,
        uint32 indexed nodeId,
        uint64 checksum,
        uint32 inputCount,
        uint32 operationCount
    );

    constructor(address router_, uint32 nodeId_, uint32[] memory parents_, uint64 failWorkflowId_, uint32 failAtNode_) {
        require(router_ != address(0), "router is zero");
        router = router_;
        nodeId = nodeId_;
        failAtNode = failAtNode_;
        failWorkflowId = failWorkflowId_;
        uint32 previous;
        for (uint256 i = 0; i < parents_.length; ++i) {
            require(parents_[i] < nodeId_, "parents must be topological");
            if (i > 0) require(parents_[i] > previous, "parents must be unique and ordered");
            expectedParents.push(parents_[i]);
            previous = parents_[i];
        }
    }

    function parentIds() external view returns (uint32[] memory) {
        return expectedParents;
    }

    function nodeResult(uint64 sessionId, uint64 workflowId) external view returns (NodeResult memory) {
        return results[sessionId][workflowId];
    }

    function execute(
        uint64 sessionId,
        uint64 workflowId,
        uint64 seed,
        uint32[] calldata parentIds_,
        uint64[] calldata parentValues_,
        uint32 operations
    ) external returns (uint64 checksum) {
        require(msg.sender == router, "only router");
        require(!results[sessionId][workflowId].exists, "node already executed");
        require(parentIds_.length == expectedParents.length && parentValues_.length == expectedParents.length, "parent input count mismatch");
        require(operations > 0, "zero operations");

        for (uint256 i = 0; i < expectedParents.length; ++i) {
            require(parentIds_[i] == expectedParents[i], "unexpected predecessor");
            require(!hasInput[sessionId][workflowId][parentIds_[i]], "duplicate predecessor input");
            hasInput[sessionId][workflowId][parentIds_[i]] = true;
            inputValues[sessionId][workflowId][parentIds_[i]] = parentValues_[i];
            emit ParentInputConsumed(sessionId, workflowId, nodeId, parentIds_[i], parentValues_[i]);
        }

        // The injected revert happens after input writes, to verify whole-transaction rollback.
        require(workflowId != failWorkflowId || nodeId != failAtNode, "injected stage failure");
        unchecked {
            checksum = seed ^ (workflowId * WORKFLOW_MULTIPLIER) ^ (uint64(nodeId) * NODE_MULTIPLIER);
            for (uint256 i = 0; i < expectedParents.length; ++i) {
                uint32 parentId = expectedParents[i];
                checksum = _rotateLeft(checksum, (parentId * 13 + 7) % 64) ^ parentValues_[i];
                checksum += uint64(parentId) * WORKFLOW_MULTIPLIER;
            }
            for (uint32 round = 0; round < operations; ++round) {
                checksum = _rotateLeft(checksum, 7) * ROUND_MULTIPLIER + (uint64(round) ^ uint64(nodeId) ^ workflowId);
            }
        }
        results[sessionId][workflowId] = NodeResult(true, sessionId, checksum, uint32(parentIds_.length), operations);
        emit NodeExecuted(sessionId, workflowId, nodeId, checksum, uint32(parentIds_.length), operations);
    }

    function _rotateLeft(uint64 value, uint32 amount) private pure returns (uint64) {
        if (amount == 0) return value;
        return (value << amount) | (value >> (64 - amount));
    }
}

/// @notice Router that calls one independently deployed DAGStage per node.
/// @dev Entry and exit are measured separately; they are local storage proxies only.
contract DASDistributedCompositionHarness {
    uint32 public immutable nodeCount;
    address public immutable owner;
    uint32[][] private graphParents;
    address[] private stages;
    bool public stagesConfigured;

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

    event StagesConfigured(address[] stageAddresses);
    event SessionEntered(uint64 indexed sessionId, uint32 expectedWorkflows, bytes32 entryCommitment);
    event WorkflowCompleted(uint64 indexed sessionId, uint64 indexed workflowId, uint64 checksum, uint32 nodeCount);
    event SessionExited(uint64 indexed sessionId, uint32 executedWorkflows, uint64 aggregate, bytes32 exitCommitment);

    constructor(uint32[][] memory parents_) {
        require(parents_.length > 0, "empty graph");
        for (uint32 node = 0; node < parents_.length; ++node) {
            uint32 previous;
            for (uint256 i = 0; i < parents_[node].length; ++i) {
                require(parents_[node][i] < node, "graph must be topological");
                if (i > 0) require(parents_[node][i] > previous, "parents must be unique and ordered");
                previous = parents_[node][i];
            }
            graphParents.push(parents_[node]);
        }
        nodeCount = uint32(parents_.length);
        owner = msg.sender;
    }

    function setStages(address[] calldata stageAddresses) external {
        require(msg.sender == owner, "only owner");
        require(!stagesConfigured && stageAddresses.length == nodeCount, "stage manifest mismatch");
        for (uint32 node = 0; node < nodeCount; ++node) {
            require(stageAddresses[node] != address(0), "stage is zero");
            require(DAGStage(stageAddresses[node]).router() == address(this), "stage router mismatch");
            require(DAGStage(stageAddresses[node]).nodeId() == node, "stage node mismatch");
            uint32[] memory expected = DAGStage(stageAddresses[node]).parentIds();
            require(keccak256(abi.encode(expected)) == keccak256(abi.encode(graphParents[node])), "stage parent manifest mismatch");
            stages.push(stageAddresses[node]);
        }
        stagesConfigured = true;
        emit StagesConfigured(stageAddresses);
    }

    function stageAddresses() external view returns (address[] memory) {
        return stages;
    }

    function sessionState(uint64 sessionId) external view returns (Session memory) {
        return sessions[sessionId];
    }

    function enterSession(uint64 sessionId, uint32 expectedWorkflows, bytes32 entryCommitment) external {
        require(stagesConfigured, "stages not configured");
        Session storage s = sessions[sessionId];
        require(!s.active && s.expectedWorkflows == 0, "session id already used");
        require(expectedWorkflows > 0 && entryCommitment != bytes32(0), "invalid session manifest");
        s.active = true;
        s.expectedWorkflows = expectedWorkflows;
        s.entryCommitment = entryCommitment;
        emit SessionEntered(sessionId, expectedWorkflows, entryCommitment);
    }

    /// @notice Executes theta workflows in one router transaction using real external CALLs.
    function executeWorkflows(
        uint64 sessionId,
        uint64[] calldata workflowIds,
        uint64[] calldata seeds,
        uint32 operationsPerNode
    ) external returns (uint64[] memory checksums) {
        Session storage s = sessions[sessionId];
        require(s.active, "session not active");
        require(workflowIds.length > 0 && workflowIds.length == seeds.length, "workflow input mismatch");
        require(s.executedWorkflows + workflowIds.length <= s.expectedWorkflows, "session workflow count exceeded");
        checksums = new uint64[](workflowIds.length);
        for (uint256 i = 0; i < workflowIds.length; ++i) {
            checksums[i] = _executeWorkflow(sessionId, workflowIds[i], seeds[i], operationsPerNode);
            s.executedWorkflows += 1;
            s.aggregate ^= _rotateLeft(checksums[i], s.executedWorkflows % 64);
            emit WorkflowCompleted(sessionId, workflowIds[i], checksums[i], nodeCount);
        }
    }

    function exitSession(uint64 sessionId, bytes32 exitCommitment) external returns (uint64 aggregate) {
        Session storage s = sessions[sessionId];
        require(s.active && s.executedWorkflows == s.expectedWorkflows, "session incomplete");
        require(exitCommitment != bytes32(0), "invalid exit commitment");
        s.active = false;
        s.exitCommitment = exitCommitment;
        aggregate = s.aggregate;
        emit SessionExited(sessionId, s.executedWorkflows, aggregate, exitCommitment);
    }

    function abortSession(uint64 sessionId) external {
        Session storage s = sessions[sessionId];
        require(s.active && s.executedWorkflows < s.expectedWorkflows, "session is not incomplete");
        s.active = false;
    }

    function nodeResult(uint64 sessionId, uint64 workflowId, uint32 nodeId) external view returns (DAGStage.NodeResult memory) {
        return DAGStage(stages[nodeId]).nodeResult(sessionId, workflowId);
    }

    function nodeInput(uint64 sessionId, uint64 workflowId, uint32 nodeId, uint32 parentId) external view returns (bool present, uint64 value) {
        DAGStage stage = DAGStage(stages[nodeId]);
        return (stage.hasInput(sessionId, workflowId, parentId), stage.inputValues(sessionId, workflowId, parentId));
    }

    function _executeWorkflow(uint64 sessionId, uint64 workflowId, uint64 seed, uint32 operations) private returns (uint64 checksum) {
        require(!workflowSeen[sessionId][workflowId], "workflow id already used");
        uint64[] memory outputs = new uint64[](nodeCount);
        uint64 totalOperations;
        for (uint32 node = 0; node < nodeCount; ++node) {
            uint32[] memory parentIds = graphParents[node];
            uint64[] memory parentValues = new uint64[](parentIds.length);
            for (uint256 i = 0; i < parentIds.length; ++i) {
                parentValues[i] = outputs[parentIds[i]];
            }
            // This external contract call executes and stores this node in its own account storage.
            outputs[node] = DAGStage(stages[node]).execute(sessionId, workflowId, seed, parentIds, parentValues, operations);
            nodeChecksums[sessionId][workflowId][node] = outputs[node];
            totalOperations += operations;
        }
        for (uint32 node = 0; node < nodeCount; ++node) {
            bool hasChild;
            for (uint32 child = 0; child < nodeCount && !hasChild; ++child) {
                for (uint256 p = 0; p < graphParents[child].length; ++p) {
                    if (graphParents[child][p] == node) { hasChild = true; break; }
                }
            }
            if (!hasChild) checksum ^= _rotateLeft(outputs[node], (node * 11) % 64);
        }
        workflowChecksums[sessionId][workflowId] = checksum;
        workflowOperationCounts[sessionId][workflowId] = uint32(totalOperations);
        workflowSeen[sessionId][workflowId] = true;
    }

    function _rotateLeft(uint64 value, uint32 amount) private pure returns (uint64) {
        if (amount == 0) return value;
        return (value << amount) | (value >> (64 - amount));
    }
}
