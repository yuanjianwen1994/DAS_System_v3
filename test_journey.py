#!/usr/bin/env python3
"""
快速测试 journey_id 跟踪功能。
"""
import sys
import inspect

sys.path.insert(0, '.')

from core.macro_traffic import MacroTrafficGenerator

# 检查 _send_and_wait 签名
sig = inspect.signature(MacroTrafficGenerator._send_and_wait)
print("_send_and_wait 签名:", sig)
params = list(sig.parameters.keys())
print("参数:", params)
if 'journey_id' in params:
    print("[OK] journey_id 参数存在")
else:
    print("[ERROR] journey_id 参数缺失")
    sys.exit(1)

# 检查 raw_logs 结构示例
print("\n检查日志字段...")
# 创建一个虚拟的 raw_log 条目（不实例化类，因为需要大量依赖）
# 我们只是检查代码中的字段名
import ast
with open('core/macro_traffic.py', 'r', encoding='utf-8') as f:
    content = f.read()
# 寻找 raw_logs.append 调用
import re
matches = re.findall(r'self\.raw_logs\.append\(\s*\{([^}]+)\}', content, re.DOTALL)
if matches:
    print(f"找到 {len(matches)} 个日志追加调用")
    for i, m in enumerate(matches[:2]):
        lines = m.strip().split('\n')
        print(f"  示例 {i+1}:")
        for line in lines[:5]:
            if 'journey_id' in line:
                print(f"    {line.strip()} <- 包含 journey_id")
                break
        else:
            print("    未找到 journey_id 字段，可能有问题")
else:
    print("未找到 raw_logs.append 调用")

# 检查工作循环是否传递 journey_id
print("\n检查工作循环调用...")
# 搜索 _send_and_wait 调用中是否包含 journey_id 参数
call_pattern = r'_send_and_wait\("([^"]+)",\s*([^,]+),\s*journey_id='
calls = re.findall(call_pattern, content)
if calls:
    print(f"找到 {len(calls)} 个包含 journey_id 的调用")
    for func, worker in calls[:3]:
        print(f"  {func} -> {worker}")
else:
    print("警告：未找到显式的 journey_id 参数传递，可能使用默认值")

print("\n测试完成。")