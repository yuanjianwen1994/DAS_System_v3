# Seed 20261032 audit-state-machine short smoke audit

- Purpose: verify the sink-first path on the original seed32 trajectory prefix before replaying the full seed32 input.
- Input: first 8 exact offsets at each of lambda 0.5 and 1.0; seed 20261032, 8 stages, same NEAR 2.13.4/protocol 86 binary; no new seed.
- Runtime: NEAR {rpc["version"]["version"]}, protocol {rpc["protocol_version"]}, latest {rpc["latest_protocol_version"]}; runner exit 0; cleanup complete.
- Startup assertions: passed; pipeline mode 1; preheat 0 s and 20 s observation for this short smoke; no schedule/rate/warmup/measurement overrides.
- Remote/local SHA-256 matched for raw, result summary, audit checkpoint, and effective startup config.

- Phase 0 (lambda 0.5): 8 offered; trace checkpoint passed (8/8); 8 fully audited; 0 pending; 0 failure labels; block audit pending 0 .
- Phase 1 (lambda 1): 8 offered; trace checkpoint passed (8/8); 8 fully audited; 0 pending; 0 failure labels; block audit pending 0 .

- Sink became queryable before receipt-tree completion in 1 workflow(s), exercising the formerly premature-exit branch.
- Overall: 16/16 fully audited; audit pending=0; failure labels=0; every row has 8/8 stage states, matching sink state, matching receipt events, and a transaction hash.
- RPC ledger: 17 send_tx HTTP attempts, 1 HTTP/RPC error records, 1 same-payload retry record(s), 16/16 workflow rows linked to send_tx response transaction hash.
- One observed HTTP 408 `send_tx` timeout was followed by a retry of the same payload fingerprint and JSON-RPC id; the retry returned HTTP 200 and a transaction hash. This was retained in the ledger; the workflow audited successfully.
- Schedule SHA-256: `43952f0ae155175c136169cb2fa9867fb7e09308ffa6235010a6039588c0b659`; repaired runner SHA-256: `93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`.
- Result: **pass for proceeding to the full same-seed replay**. This is a tool-path smoke, not a performance result.
