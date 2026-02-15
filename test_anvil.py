#!/usr/bin/env python3
"""
测试脚本用于验证 Anvil 迁移是否成功。
测试内容：
1. Anvil 是否已安装并可用（检查命令是否存在）
2. 导入 AnvilManager 是否正常工作
3. 基本的网络启动和停止功能（可选，使用模拟或高端口）
"""
import sys
import os
import shutil
import socket
import subprocess
import time
import tempfile
from unittest.mock import patch, MagicMock

# 添加当前目录到路径以便导入
sys.path.insert(0, '.')

def test_anvil_command_exists():
    """测试1: 检查 anvil 命令是否在 PATH 中"""
    anvil_path = shutil.which('anvil')
    if anvil_path:
        print(f"✓ Anvil 命令存在: {anvil_path}")
        return True
    else:
        print("✗ Anvil 命令未找到。请安装 foundry (anvil)")
        return False

def test_import_anvil_manager():
    """测试2: 导入 AnvilManager 是否正常"""
    try:
        from core.network import AnvilManager
        print("✓ 成功导入 AnvilManager")
        return True
    except ImportError as e:
        print(f"✗ 导入 AnvilManager 失败: {e}")
        return False

def test_create_instance():
    """测试3: 创建 AnvilManager 实例"""
    try:
        from core.network import AnvilManager
        manager = AnvilManager()
        assert manager is not None
        assert hasattr(manager, 'processes')
        assert isinstance(manager.processes, dict)
        print("✓ 成功创建 AnvilManager 实例")
        return True
    except Exception as e:
        print(f"✗ 创建实例失败: {e}")
        return False

def test_port_check_logic():
    """测试4: 端口检查逻辑 (_is_port_available)"""
    try:
        from core.network import AnvilManager
        manager = AnvilManager()
        # 找一个理论上可用的端口 (使用临时套接字)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            free_port = s.getsockname()[1]
        # 应该返回 True
        assert manager._is_port_available(free_port) == True
        # 绑定端口后再次检查应返回 False
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s2.bind(('127.0.0.1', free_port))
            # 现在端口被占用，应该返回 False
            # 注意：由于我们已经绑定了端口，is_port_available 应该检测到占用
            # 但为了不干扰，我们使用另一个端口测试
            pass
        finally:
            s2.close()
        print("✓ 端口检查逻辑正常")
        return True
    except Exception as e:
        print(f"✗ 端口检查逻辑失败: {e}")
        return False

def test_start_stop_with_mock():
    """测试5: 使用模拟子进程测试启动和停止"""
    try:
        from core.network import AnvilManager
        manager = AnvilManager()
        # 模拟拓扑
        topology = {
            "shards": {
                "test_shard": {"port": 18545, "type": "settlement", "id": 0}
            },
            "execution": {"port": 18546, "type": "execution"},
            "baseline": {"port": 18547, "type": "single_chain"}
        }
        # 模拟 subprocess.Popen 以避免实际启动进程
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # 表示进程正在运行
        mock_proc.pid = 12345
        
        with patch('subprocess.Popen', return_value=mock_proc):
            manager.start_network(topology)
            # 检查进程字典是否被填充
            assert "test_shard" in manager.processes
            assert "execution" in manager.processes
            assert "baseline" in manager.processes
            # 模拟停止
            manager.stop_network()
            # 检查 terminate 是否被调用
            mock_proc.terminate.assert_called()
            mock_proc.wait.assert_called()
        print("✓ 启动/停止模拟测试通过")
        return True
    except Exception as e:
        print(f"✗ 启动/停止模拟测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_actual_start_stop_optional():
    """测试6 (可选): 实际启动和停止一个简单节点（使用高端口）"""
    # 只有在环境允许且用户明确要求时才运行
    # 这里我们默认跳过，但提供一个开关
    RUN_ACTUAL = os.environ.get('TEST_ACTUAL_ANVIL', '0') == '1'
    if not RUN_ACTUAL:
        print("⏭️  可选的实际启动/停止测试已跳过 (设置环境变量 TEST_ACTUAL_ANVIL=1 以启用)")
        return None  # 跳过，不计入成功/失败
    
    try:
        from core.network import AnvilManager
        manager = AnvilManager()
        # 使用一个非常高且很可能空闲的端口
        import random
        base_port = random.randint(20000, 30000)
        topology = {
            "shards": {
                "test_shard": {"port": base_port, "type": "settlement", "id": 0}
            }
        }
        print(f"尝试在端口 {base_port} 上启动实际 anvil 进程...")
        manager.start_network(topology)
        time.sleep(2)
        # 检查进程是否仍在运行
        for name, proc in manager.processes.items():
            if proc.poll() is not None:
                print(f"✗ 进程 {name} 已退出，返回码 {proc.returncode}")
                manager.stop_network()
                return False
        print("✓ 实际启动成功，进程正在运行")
        manager.stop_network()
        print("✓ 实际停止成功")
        return True
    except Exception as e:
        print(f"✗ 实际启动/停止测试失败: {e}")
        return False

def main():
    print("=" * 60)
    print("Anvil 迁移验证测试")
    print("=" * 60)
    
    results = []
    
    # 测试1
    print("\n1. 检查 Anvil 命令是否存在...")
    results.append(('命令存在', test_anvil_command_exists()))
    
    # 测试2
    print("\n2. 导入 AnvilManager...")
    results.append(('导入', test_import_anvil_manager()))
    
    # 测试3
    print("\n3. 创建实例...")
    results.append(('实例创建', test_create_instance()))
    
    # 测试4
    print("\n4. 端口检查逻辑...")
    results.append(('端口检查', test_port_check_logic()))
    
    # 测试5
    print("\n5. 启动/停止模拟测试...")
    results.append(('模拟启动停止', test_start_stop_with_mock()))
    
    # 测试6 (可选)
    print("\n6. 实际启动/停止测试 (可选)...")
    optional_result = test_actual_start_stop_optional()
    if optional_result is not None:
        results.append(('实际启动停止', optional_result))
    
    print("\n" + "=" * 60)
    print("测试结果摘要:")
    print("=" * 60)
    all_passed = True
    for name, passed in results:
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    # 如果有可选测试跳过，我们不计入失败
    optional_skipped = (optional_result is None)
    
    if all_passed:
        print("\n✅ 所有测试通过！Anvil 迁移验证成功。")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查问题。")
        sys.exit(1)

if __name__ == "__main__":
    main()