// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

// Controlled EVM execution-path screen, NOT a NEAR/Avalanche implementation.
// This common adapter gives every path the same four migration operations.
// Source/exec balances are test ledger units; authentication and consensus
// are outside this screen. All contracts run on one local EVM.
contract BoundaryAdapter {
    mapping(bytes32 => uint256) public settlement;
    mapping(bytes32 => uint256) public execution;
    mapping(bytes32 => uint256) public pending;
    mapping(bytes32 => uint256) public spent;
    function fund(bytes32 id) external { require(settlement[id] == 0); settlement[id] = 100; }
    function burnIn(bytes32 id) external { require(settlement[id] == 100); settlement[id] = 0; pending[id] = 100; }
    function mintIn(bytes32 id) external { require(pending[id] == 100); pending[id] = 0; execution[id] = 100; }
    function business(bytes32 id) external { require(execution[id] > 0); execution[id] -= 1; spent[id] += 1; }
    function burnOut(bytes32 id) external { require(pending[id] == 0); pending[id] = execution[id]; execution[id] = 0; }
    function mintOut(bytes32 id) external { require(pending[id] > 0); settlement[id] += pending[id]; pending[id] = 0; }
}

contract BusinessNode {
    mapping(bytes32 => uint256) public calls;
    function step(bytes32 session, bytes32 input) external returns (bytes32) {
        uint256 n = ++calls[session];
        return keccak256(abi.encode(input, n));
    }
}

contract Workflow {
    BoundaryAdapter public immutable boundary;
    BusinessNode[] public nodes;
    mapping(bytes32 => bytes32) public state;
    mapping(bytes32 => uint256) public cursor;
    mapping(bytes32 => uint256) public completed;
    constructor(address adapter, uint256 depth) {
        boundary = BoundaryAdapter(adapter);
        for (uint256 i; i < depth; ++i) nodes.push(new BusinessNode());
    }
    function local(bytes32 session) external {
        bytes32 out = state[session];
        for (uint256 i; i < nodes.length; ++i) out = nodes[i].step(session, out);
        state[session] = out;
        ++completed[session];
        boundary.business(session);
    }
    // One confirmed stage per transaction: an explicit asynchronous execution
    // model, not a claim that native NEAR calls consume EVM transactions/gas.
    function asyncStage(bytes32 session) external {
        uint256 at = cursor[session];
        state[session] = nodes[at].step(session, state[session]);
        if (at + 1 == nodes.length) {
            cursor[session] = 0;
            ++completed[session];
            boundary.business(session);
        } else cursor[session] = at + 1;
    }
}
