"""
Network management for DAS System v3.
Ganache Lifecycle & Web3 Connection Management.
"""
import os
import socket
import subprocess
import time
import typing as t
from web3 import Web3
from web3.providers import HTTPProvider

from config_global import MNEMONIC, BLOCK_TIME, GAS_LIMIT, get_topology


class GanacheManager:
    """
    Manages Ganache processes for each node in the topology.
    """
    def __init__(self) -> None:
        self.processes: t.Dict[str, subprocess.Popen] = {}

    def _is_port_available(self, port: int) -> bool:
        """
        Check if a TCP port is available on localhost.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return True
            except OSError:
                return False

    def start_network(self, topology: t.Dict[str, t.Any]) -> None:
        """
        Launches ganache subprocesses for each node in the topology.
        """
        # Create logs directory
        os.makedirs("logs", exist_ok=True)

        # Build flat node list
        flat_nodes = []
        if "shards" in topology:
            for name, cfg in topology["shards"].items():
                flat_nodes.append((name, cfg))
        if "execution" in topology:
            flat_nodes.append(("execution", topology["execution"]))
        if "baseline" in topology:
            flat_nodes.append(("baseline", topology["baseline"]))

        for name, cfg in flat_nodes:
            port = cfg["port"]
            if not self._is_port_available(port):
                raise RuntimeError(f"Port {port} for {name} is already in use.")

            # Log file path
            log_path = os.path.join("logs", f"ganache_{name}_{port}.log")
            log_file = open(log_path, "w", encoding="utf-8")

            # Verification print
            print(f"Starting {name} on port {port} with BlockTime={BLOCK_TIME}...")

            # Build command with equals‑sign syntax
            cmd = [
                "ganache.cmd",
                f"--server.port={port}",
                f"--server.host=127.0.0.1",  # <--- FORCE IPv4 BINDING
                f"--miner.blockTime={BLOCK_TIME}",
                f"--wallet.mnemonic={MNEMONIC}",
                "--wallet.totalAccounts=100",
                f"--miner.blockGasLimit={GAS_LIMIT}",
                "--chain.allowUnlimitedContractSize",
                "--chain.hardfork=shanghai",
                "--verbose",
            ]

            print(f"Starting {name}: {' '.join(cmd)}")
            if name == "baseline":
                import sys
                sys.stderr.write(f"[DEBUG] Baseline command: {' '.join(cmd)}\n")

            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.processes[name] = proc

            # Wait for process to start and check if it crashed
            time.sleep(2)
            if proc.poll() is not None:
                # Process died, read log file
                log_file.close()
                with open(log_path, "r") as f:
                    error_log = f.read()
                raise RuntimeError(f"{name} failed to start! Logs:\n{error_log}")

            print(f"Started {name} on port {port} (PID: {proc.pid})")

    def stop_network(self) -> None:
        """
        Terminates all ganache subprocesses gracefully.
        """
        for name, proc in self.processes.items():
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
                print(f"Stopped {name}")
        self.processes.clear()


class ConnectionManager:
    """
    Provides Web3 connections to each node.
    """
    def __init__(self, topology: t.Dict[str, t.Any]) -> None:
        self.topology = topology
        self._connections: t.Dict[str, Web3] = {}

    def get_web3(self, node_name: str, timeout: int = 30) -> Web3:
        """
        Returns a connected Web3 instance for the given node.
        Waits up to `timeout` seconds for the node to become available.
        """
        if node_name in self._connections:
            return self._connections[node_name]

        # Find port
        port = None
        if node_name in self.topology.get("shards", {}):
            port = self.topology["shards"][node_name]["port"]
        elif node_name == "execution":
            port = self.topology["execution"]["port"]
        elif node_name == "baseline":
            port = self.topology["baseline"]["port"]
        else:
            raise ValueError(f"Unknown node: {node_name}")

        provider = HTTPProvider(f"http://127.0.0.1:{port}")
        w3 = Web3(provider)

        print(f"Waiting for {node_name} to come online...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            if w3.is_connected():
                self._connections[node_name] = w3
                return w3
            time.sleep(1)

        raise ConnectionError(f"Cannot connect to {node_name} on port {port} after {timeout}s")