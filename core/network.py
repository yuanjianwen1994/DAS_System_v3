"""
Network management for DAS System v3.
Ganache Lifecycle & Web3 Connection Management.
"""
import os
import shutil
import socket
import subprocess
import time
import typing as t
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from web3 import Web3
from web3.providers import HTTPProvider

from config_global import MNEMONIC, BLOCK_TIME, GAS_LIMIT, NUM_USERS, ACCOUNT_BALANCE_ETH, get_topology, GANACHE_DATA_DIR


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

            # Database path on E: drive
            node_db_path = os.path.join(GANACHE_DATA_DIR, name)
            # Clean up previous db to prevent accumulation and ensure fresh state
            if os.path.exists(node_db_path):
                try:
                    shutil.rmtree(node_db_path)
                except Exception as e:
                    print(f"[System] Warning: Could not clean DB for {name}: {e}")
            os.makedirs(node_db_path, exist_ok=True)

            # Verification print
            print(f"Starting {name} on port {port} with BlockTime={BLOCK_TIME}...")

            # Build command with equals‑sign syntax
            cmd = [
                "ganache.cmd",
                f"--server.port={port}",
                f"--server.host=127.0.0.1",  # <--- FORCE IPv4 BINDING
                f"--miner.blockTime={BLOCK_TIME}",
                f"--wallet.mnemonic={MNEMONIC}",
                f"--wallet.totalAccounts={NUM_USERS}",
                f"--wallet.defaultBalance={ACCOUNT_BALANCE_ETH}",
                f"--miner.blockGasLimit={GAS_LIMIT}",
                "--chain.allowUnlimitedContractSize",
                "--chain.hardfork=shanghai",
                "--verbose",
                f"--database.dbPath={node_db_path}",
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
    Manages Web3 connections with a robust, thread‑safe HTTP Session.
    """
    def __init__(self, topology: t.Dict[str, t.Any]) -> None:
        self.topology = topology
        self._connections: t.Dict[str, Web3] = {}
        # Create a single global session for this process
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Creates a robust HTTP session with high connection pooling and aggressive retries.
        Critical for N=1600 high‑concurrency experiments.
        """
        session = requests.Session()
        # High pool size to prevent "NewConnectionError" under load
        adapter = HTTPAdapter(
            pool_connections=500,
            pool_maxsize=500,
            max_retries=Retry(
                total=10,
                backoff_factor=1.0, # Slow down retries to let Ganache breathe
                status_forcelist=[500, 502, 503, 504]
            )
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_web3(self, node_name: str, timeout: int = 30) -> Web3:
        """
        Returns a Web3 instance with the shared session.
        No blocking checks here to avoid log spam in hot paths.
        """
        # Return cached instance if available
        if node_name in self._connections:
            return self._connections[node_name]

        # Resolve port
        port = None
        if node_name in self.topology.get("shards", {}):
            port = self.topology["shards"][node_name]["port"]
        elif node_name == "execution":
            port = self.topology["execution"]["port"]
        elif node_name == "baseline":
            port = self.topology["baseline"]["port"]
        else:
            raise ValueError(f"Unknown node: {node_name}")

        url = f"http://127.0.0.1:{port}"
        
        # Ensure session exists
        if not hasattr(self, '_session') or self._session is None:
            self._session = self._create_session()

        # Create Web3 instance with the robust session
        provider = HTTPProvider(
            url,
            session=self._session,
            request_kwargs={"timeout": 120}
        )
        w3 = Web3(provider)
        
        # Cache and return (Do NOT check is_connected() here, it causes DoS)
        self._connections[node_name] = w3
        return w3