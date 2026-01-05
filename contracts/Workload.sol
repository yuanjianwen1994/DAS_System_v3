// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract Workload {
    event WorkDone(uint256 iterations, uint256 result);

    /**
     * @dev Simulate CPU‑bound work by performing a loop.
     * @param iterations Number of loop iterations.
     * @return result A dummy result (sum of i).
     */
    function doWork(uint256 iterations) external returns (uint256) {
        uint256 result = 0;
        for (uint256 i = 0; i < iterations; i++) {
            result += i;
        }
        emit WorkDone(iterations, result);
        return result;
    }
}