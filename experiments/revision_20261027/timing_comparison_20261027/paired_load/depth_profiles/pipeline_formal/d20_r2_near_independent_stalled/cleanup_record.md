# d20 r2 NEAR cleanup record

- Run wrapper PID: 1801024; Node PID: 1801026; Sandbox PID: 1801129.
- Both scheduled phases were fully recorded and state/event audited: 0.5/s 45/45, 1/s 90/90; 0 workflow failures and 0 unfinished.
- Results and raw files were written at 2026-10-27 19:31:51 UTC. The Node process then remained blocked in teardown until the exact Node and Sandbox PIDs were sent SIGTERM at 19:57:48 UTC. Matrix exit 143 is cleanup-only; it is not a workflow failure.
- Raw SHA-256: `25c7bef1ef523c0c5b2d49a885e0a87b916841d7053d2e626b85591a93e0cc84`.
- The supplemental run uses a new output directory and bounded cleanup; this snapshot remains unchanged.
