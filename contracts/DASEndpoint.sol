// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @notice DAS 迁移端点：出账 burn + 入账 mint。
/// 入账侧实现论文 §III-E 的三条准入规则中链上可执行的两条：
///   1) Binding —— 目的分片必须是本合约所在分片（dstBinding 校验）；
///   2) Uniqueness —— 同一 receipt（以序列号标识）只能被消费一次（位图消耗集）。
/// 第三条 Finality 依赖链下最终性证明，本原型抽象化（见论文 §VI-A）。
contract DASEndpoint {
    event Burn(address indexed to, uint256 amount, uint64 indexed seq);
    event Mint(address indexed to, uint256 amount, uint64 indexed seq);

    /// @notice 部署时注入的域分隔符，标识本端点所属的分片。
    /// 不能依赖 address(this)：在模拟环境中各分片用同一部署者、同一 nonce，
    /// CREATE 出来的地址相同，chain-id 也相同，地址无法区分目的地。
    bytes32 public immutable DOMAIN;

    constructor(bytes32 domain) {
        DOMAIN = domain;
    }

    /// @dev 消耗集用位图：256 个 receipt 共用一个存储槽，
    /// 相比每 receipt 一个 mapping 槽位，把冷写（20k gas）摊销到约 5k。
    mapping(uint256 => uint256) private consumedBitmap;

    /// @notice 出账：源侧烧毁并发出可被 relayer 转发的 burn 事件。
    function burn(address to, uint256 amount, uint64 seq) external {
        emit Burn(to, amount, seq);
    }

    /// @notice 入账：校验目的地绑定与重放后铸造。
    /// @param dstBinding 必须是本端点的 DOMAIN
    function mint(address to, uint256 amount, uint64 seq, bytes32 dstBinding) external {
        require(dstBinding == DOMAIN, "wrong destination");

        uint256 word = uint256(seq) >> 8;
        uint256 mask = 1 << (uint256(seq) & 0xff);
        require(consumedBitmap[word] & mask == 0, "replay");

        consumedBitmap[word] |= mask;
        emit Mint(to, amount, seq);
    }
}
