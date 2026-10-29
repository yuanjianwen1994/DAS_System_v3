# Short smoke setup attempt 1

- Status: failed before the NEAR runner/sandbox started.
- Error: remote shell could not find `input/artifacts/stage_contract.wasm`; the first upload placed the artifact one directory above the expected location.
- Impact: no workflow was offered and no experiment process was launched. The failure is retained; the exact same smoke input is used after putting the artifact in the expected `input/artifacts/` directory.
