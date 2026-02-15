import subprocess
import sys
import time
import os

# 1. 模拟 config 中的配置
MNEMONIC = "myth like bonus scare over problem client lizard pioneer submit female collect"
GANACHE_CMD = "ganache.cmd" if sys.platform == "win32" else "ganache"
PORT = 8580

print(f"🔍 System Platform: {sys.platform}")
print(f"🔍 Testing Command: {GANACHE_CMD}")

# 2. 构造启动命令 (模拟 core/network.py 的行为)
cmd = [
    GANACHE_CMD,
    "--server.port", str(PORT),
    "--miner.blockTime", "12",
    "--wallet.mnemonic", MNEMONIC,
    "--wallet.totalAccounts", "200",
    "--wallet.defaultBalance", "10000",
    "--miner.blockGasLimit", "30000000"
]

print("🚀 Launching Ganache in debug mode...")
print(f"Command: {' '.join(cmd)}")

try:
    # 直接将输出导向控制台，方便看到报错
    proc = subprocess.Popen(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr
    )

    print("⏳ Waiting 5 seconds to see if it stays alive...")
    time.sleep(5)

    if proc.poll() is None:
        print("\n✅ Ganache is running successfully!")
        print(f"   PID: {proc.pid}")
        print("   Killing it now to clean up...")
        proc.terminate()
    else:
        print("\n❌ Ganache crashed immediately!")
        print(f"   Exit Code: {proc.returncode}")
        print("👉 Please analyze the error message above.")

except FileNotFoundError:
    print(f"\n❌ CRITICAL: executable '{GANACHE_CMD}' not found in PATH.")
    print("   Make sure you have installed it via 'npm install -g ganache'")
except Exception as e:
    print(f"\n❌ Python Error: {e}")