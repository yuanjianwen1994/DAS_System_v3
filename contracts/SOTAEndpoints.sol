// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @notice Teleport 式（Polkadot XCM teleport 的核心算法）：
/// 出账 burn、入账 mint，重放保护只用一个**单调递增的通道/账户序号**，
/// 不做目的账户绑定、不记录 per-receipt 消耗集。
/// 与 DASEndpoint 的差别：目的绑定缺失、消耗集粒度不同（一个 uint64 vs 位图）。
contract TeleportEndpoint {
    event Burn(address indexed to, uint256 amount, uint64 indexed nonce);
    event Mint(address indexed to, uint256 amount, uint64 indexed nonce);

    /// @dev 每个接收方一条序号流，模拟 XCM 通道的消息序号。
    mapping(address => uint64) public lastNonce;

    function burn(address to, uint256 amount, uint64 nonce) external {
        emit Burn(to, amount, nonce);
    }

    function mint(address to, uint256 amount, uint64 nonce) external {
        require(nonce > lastNonce[to], "stale message");
        lastNonce[to] = nonce;
        emit Mint(to, amount, nonce);
    }
}

/// @notice Avalanche 式（export/import，共享验证人集）：
/// 源侧导出、目的侧直接入账，**不验证任何来源证明**——
/// 因为 X-Chain 与 C-Chain 共用一套验证人集，原子性由共识层保证。
/// 与 DASEndpoint 的差别：省略了绑定与重放校验，因而也省略了相应开销。
contract AtomicEndpoint {
    event Export(address indexed to, uint256 amount);
    event Import(address indexed to, uint256 amount);

    function exportTo(address to, uint256 amount) external {
        emit Export(to, amount);
    }

    function importFrom(address to, uint256 amount) external {
        emit Import(to, amount);
    }
}
