// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract TwoPhaseCommit {
    enum Phase { None, Locked, Committed, Aborted }

    struct Transaction {
        Phase phase;
        address participant;
    }

    mapping(bytes32 => Transaction) public transactions;

    event Locked(bytes32 indexed id, address participant);
    event Committed(bytes32 indexed id);
    event Aborted(bytes32 indexed id);

    /**
     * @dev Lock a transaction (prepare phase).
     */
    function lock(bytes32 id) external {
        require(transactions[id].phase == Phase.None, "Already locked or finalized");
        transactions[id] = Transaction({
            phase: Phase.Locked,
            participant: msg.sender
        });
        emit Locked(id, msg.sender);
    }

    /**
     * @dev Commit a previously locked transaction.
     */
    function commit(bytes32 id) external {
        Transaction storage txn = transactions[id];
        require(txn.phase == Phase.Locked, "Must be locked first");
        require(txn.participant == msg.sender, "Only participant can commit");
        txn.phase = Phase.Committed;
        emit Committed(id);
    }

    /**
     * @dev Abort a locked transaction.
     */
    function abort(bytes32 id) external {
        Transaction storage txn = transactions[id];
        require(txn.phase == Phase.Locked, "Must be locked first");
        require(txn.participant == msg.sender, "Only participant can abort");
        txn.phase = Phase.Aborted;
        emit Aborted(id);
    }
}