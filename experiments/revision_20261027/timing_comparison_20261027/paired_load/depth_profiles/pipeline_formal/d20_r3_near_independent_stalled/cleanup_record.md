# d20 r3 NEAR cleanup record

- Run wrapper PID: 1907537; Node PID: 1907539; Sandbox PID: 1907642.
- Both scheduled phases were fully recorded and state/event audited: 0.5/s 45/45, 1/s 90/90; 0 workflow failures and 0 unfinished.
- Results and raw files were written at 2026-10-27 20:07:27 UTC. The Node process then remained blocked in teardown until the exact Node and Sandbox PIDs were sent SIGTERM at 20:13:23 UTC. Matrix exit 143 is cleanup-only; it is not a workflow failure.
- Raw SHA-256: `c1d69f6c6b3ba7a4de6b66a583ebc611f7c412eff0db6704958f33e8c98f058`.
- The raw snapshot is retained unchanged; runner cleanup has since been bounded for future runs.
