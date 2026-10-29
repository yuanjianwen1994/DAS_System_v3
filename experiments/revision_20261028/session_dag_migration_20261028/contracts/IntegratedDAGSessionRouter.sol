// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

interface IMigrationLedger {
    function balanceOf(bytes32 domain, bytes32 asset, address account) external view returns (uint256);
    function settled(bytes32 receiptId) external view returns (bool);
    function beginMigration(bytes32 sourceDomain, bytes32 destinationDomain, bytes32 asset,
        address recipient, uint256 amount, bytes calldata payload) external returns (bytes32);
    function executeBusiness(bytes32 domain, bytes32 asset, uint256 amount, bytes calldata payload) external;
}

interface IDAGStage {
    function router() external view returns (address);
    function nodeId() external view returns (uint32);
    function parentIds() external view returns (uint32[] memory);
    function execute(uint64 sessionId, uint64 workflowId, uint64 seed,
        uint32[] calldata parentIds, uint64[] calldata parentValues, uint32 operations) external returns (uint64);
    function nodeResult(uint64 sessionId, uint64 workflowId) external view returns
        (bool exists, uint64 resultSessionId, uint64 checksum, uint32 inputCount, uint32 operationCount);
}

/// @notice Same-EVM composition of the existing receipt ledger and the actual external-CALL DAG stages.
/// @dev Controlled mechanism experiment only: domains are labels in one EVM; there is no cross-VM bridge.
contract IntegratedDAGSessionRouter {
    IMigrationLedger public immutable ledger;
    address public immutable owner;
    uint32 public immutable nodeCount;
    bytes32 public immutable sourceDomain;
    bytes32 public immutable destinationDomain;
    bytes32 public immutable asset;
    uint32[][] private graphParents;
    address[] private stages;
    bool public stagesConfigured;
    mapping(uint64 => bool) public migratedSession;
    mapping(uint64 => bytes32) public entryReceiptIds;
    mapping(uint64 => bytes32) public exitReceiptIds;
    uint256 public exitAmount;

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
    mapping(uint64 => mapping(uint64 => bool)) private workflowSeen;

    event SessionFunded(bool migrated, bytes32 indexed entryReceiptId);
    event SessionEntered(uint64 indexed sessionId, uint32 expectedWorkflows, bytes32 entryCommitment);
    event WorkflowCompleted(uint64 indexed sessionId, uint64 indexed workflowId, uint64 checksum, uint32 nodeCount);
    event SessionExited(uint64 indexed sessionId, uint32 executedWorkflows, uint64 aggregate, bytes32 exitCommitment);
    event ExitMigrationStarted(bytes32 indexed receiptId, uint256 amount);

    constructor(address ledger_, bytes32 sourceDomain_, bytes32 destinationDomain_, bytes32 asset_, uint32[][] memory parents_) {
        require(ledger_ != address(0) && sourceDomain_ != bytes32(0) && destinationDomain_ != bytes32(0), "invalid config");
        require(sourceDomain_ != destinationDomain_ && parents_.length > 0, "invalid domains or graph");
        ledger = IMigrationLedger(ledger_);
        owner = msg.sender;
        sourceDomain = sourceDomain_;
        destinationDomain = destinationDomain_;
        asset = asset_;
        for (uint32 node = 0; node < parents_.length; ++node) {
            uint32 previous;
            for (uint256 i = 0; i < parents_[node].length; ++i) {
                require(parents_[node][i] < node, "graph not topological");
                if (i > 0) require(parents_[node][i] > previous, "parents unordered");
                previous = parents_[node][i];
            }
            graphParents.push(parents_[node]);
        }
        nodeCount = uint32(parents_.length);
    }

    modifier onlyOwner() { require(msg.sender == owner, "only owner"); _; }

    function setStages(address[] calldata stageAddresses) external onlyOwner {
        require(!stagesConfigured && stageAddresses.length == nodeCount, "stage manifest mismatch");
        for (uint32 node = 0; node < nodeCount; ++node) {
            require(stageAddresses[node] != address(0), "stage is zero");
            IDAGStage stage = IDAGStage(stageAddresses[node]);
            require(stage.router() == address(this) && stage.nodeId() == node, "stage identity mismatch");
            require(keccak256(abi.encode(stage.parentIds())) == keccak256(abi.encode(graphParents[node])), "parent manifest mismatch");
            stages.push(stageAddresses[node]);
        }
        stagesConfigured = true;
    }

    function beginEntry(uint64 sessionId, uint256 amount, bytes calldata payload) external onlyOwner returns (bytes32 receiptId) {
        require(stagesConfigured && sessions[sessionId].expectedWorkflows == 0 && entryReceiptIds[sessionId] == bytes32(0), "entry unavailable");
        receiptId = ledger.beginMigration(sourceDomain, destinationDomain, asset, address(this), amount, payload);
        entryReceiptIds[sessionId] = receiptId;
        migratedSession[sessionId] = true;
        emit SessionFunded(true, receiptId);
    }

    function enterLocal(uint64 sessionId, uint256 amount, bytes32 commitment) external onlyOwner {
        require(stagesConfigured && sessions[sessionId].expectedWorkflows == 0 && entryReceiptIds[sessionId] == bytes32(0), "local entry unavailable");
        require(commitment != bytes32(0) && ledger.balanceOf(sourceDomain, asset, address(this)) >= amount, "local funds missing");
        migratedSession[sessionId] = false;
        emit SessionFunded(false, bytes32(0));
    }

    function enterSession(uint64 sessionId, uint32 expectedWorkflows, bytes32 entryCommitment,
        uint256 perNodeCost, uint256 finalExitAmount) external onlyOwner {
        require(stagesConfigured && expectedWorkflows > 0 && entryCommitment != bytes32(0), "invalid session");
        Session storage s = sessions[sessionId];
        require(!s.active && s.expectedWorkflows == 0, "session already used");
        bytes32 executionDomain = migratedSession[sessionId] ? destinationDomain : sourceDomain;
        if (migratedSession[sessionId]) require(ledger.settled(entryReceiptIds[sessionId]), "entry receipt unsettled");
        require(ledger.balanceOf(executionDomain, asset, address(this)) >= uint256(expectedWorkflows) * nodeCount * perNodeCost + finalExitAmount, "session funds insufficient");
        s.active = true;
        s.expectedWorkflows = expectedWorkflows;
        s.entryCommitment = entryCommitment;
        exitAmount = finalExitAmount;
        emit SessionEntered(sessionId, expectedWorkflows, entryCommitment);
    }

    /// @notice Workflow IDs are independent and can be submitted together; within each workflow
    ///         every stage consumes the actual stored checksum of its predecessor nodes.
    function executeWorkflows(uint64 sessionId, uint64[] calldata workflowIds, uint64[] calldata seeds,
        uint32 operationsPerNode, uint256 perNodeCost) external onlyOwner returns (uint64[] memory checksums) {
        Session storage s = sessions[sessionId];
        require(s.active && workflowIds.length > 0 && workflowIds.length == seeds.length, "invalid workflow batch");
        require(s.executedWorkflows + workflowIds.length <= s.expectedWorkflows, "too many workflows");
        checksums = new uint64[](workflowIds.length);
        for (uint256 i = 0; i < workflowIds.length; ++i) {
            checksums[i] = _executeWorkflow(sessionId, workflowIds[i], seeds[i], operationsPerNode, perNodeCost);
            s.executedWorkflows += 1;
            s.aggregate ^= _rotateLeft(checksums[i], s.executedWorkflows % 64);
            emit WorkflowCompleted(sessionId, workflowIds[i], checksums[i], nodeCount);
        }
    }

    function closeSession(uint64 sessionId, bytes32 exitCommitment) external onlyOwner returns (uint64 aggregate) {
        Session storage s = sessions[sessionId];
        require(s.active && s.executedWorkflows == s.expectedWorkflows && exitCommitment != bytes32(0), "session incomplete");
        for (uint64 w = 0; w < s.expectedWorkflows; ++w) {
            for (uint32 node = 0; node < nodeCount; ++node) {
                bool hasChild;
                for (uint32 child = 0; child < nodeCount && !hasChild; ++child) {
                    for (uint256 p = 0; p < graphParents[child].length; ++p) if (graphParents[child][p] == node) { hasChild = true; break; }
                }
                if (!hasChild) {
                    (bool exists,,,,) = IDAGStage(stages[node]).nodeResult(sessionId, w);
                    require(exists, "sink missing");
                }
            }
        }
        s.active = false;
        s.exitCommitment = exitCommitment;
        aggregate = s.aggregate;
        emit SessionExited(sessionId, s.executedWorkflows, aggregate, exitCommitment);
    }

    function beginExit(uint64 sessionId, bytes calldata payload) external onlyOwner returns (bytes32 receiptId) {
        Session storage s = sessions[sessionId];
        require(migratedSession[sessionId] && !s.active && s.executedWorkflows == s.expectedWorkflows && s.expectedWorkflows > 0 && exitReceiptIds[sessionId] == bytes32(0), "exit before complete");
        require(ledger.balanceOf(destinationDomain, asset, address(this)) == exitAmount, "exit balance mismatch");
        receiptId = ledger.beginMigration(destinationDomain, sourceDomain, asset, owner, exitAmount, payload);
        exitReceiptIds[sessionId] = receiptId;
        emit ExitMigrationStarted(receiptId, exitAmount);
    }

    function sessionState(uint64 sessionId) external view returns (Session memory) { return sessions[sessionId]; }
    function executionDomainBalance(uint64 sessionId) external view returns (uint256) {
        return ledger.balanceOf(migratedSession[sessionId] ? destinationDomain : sourceDomain, asset, address(this));
    }

    function _executeWorkflow(uint64 sessionId, uint64 workflowId, uint64 seed, uint32 operations, uint256 cost) private returns (uint64 checksum) {
        require(!workflowSeen[sessionId][workflowId] && operations > 0, "workflow invalid or repeated");
        uint64[] memory outputs = new uint64[](nodeCount);
        bytes32 executionDomain = migratedSession[sessionId] ? destinationDomain : sourceDomain;
        for (uint32 node = 0; node < nodeCount; ++node) {
            bytes memory action = abi.encode(sessionId, workflowId, node, operations);
            ledger.executeBusiness(executionDomain, asset, cost, action);
            uint32[] memory parentIds = graphParents[node];
            uint64[] memory parentValues = new uint64[](parentIds.length);
            for (uint256 i = 0; i < parentIds.length; ++i) parentValues[i] = outputs[parentIds[i]];
            outputs[node] = IDAGStage(stages[node]).execute(sessionId, workflowId, seed, parentIds, parentValues, operations);
            nodeChecksums[sessionId][workflowId][node] = outputs[node];
        }
        for (uint32 node = 0; node < nodeCount; ++node) {
            bool hasChild;
            for (uint32 child = 0; child < nodeCount && !hasChild; ++child)
                for (uint256 p = 0; p < graphParents[child].length; ++p) if (graphParents[child][p] == node) { hasChild = true; break; }
            if (!hasChild) checksum ^= _rotateLeft(outputs[node], (node * 11) % 64);
        }
        workflowChecksums[sessionId][workflowId] = checksum;
        workflowSeen[sessionId][workflowId] = true;
    }

    function _rotateLeft(uint64 value, uint32 amount) private pure returns (uint64) {
        if (amount == 0) return value;
        return (value << amount) | (value >> (64 - amount));
    }
}
