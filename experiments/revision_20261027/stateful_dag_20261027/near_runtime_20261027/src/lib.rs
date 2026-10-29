use near_sdk::borsh::{BorshDeserialize, BorshSerialize};
use near_sdk::collections::LookupMap;
use near_sdk::json_types::{U64};
use near_sdk::serde::{Deserialize, Serialize};
use near_sdk::{env, near, AccountId, BorshStorageKey, PanicOnDefault, Promise, PromiseOrValue};

const ROUND_MULTIPLIER: u64 = 0xd6e8_feb8_6659_fd93;
const WORKFLOW_MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;
const NODE_MULTIPLIER: u64 = 0xa076_1d64_78bd_642f;

fn calculate_node(workflow_id: u64, seed: u64, node_id: u32, inputs: &[(u32, u64)], operations: u32) -> u64 {
    let mut value = seed
        ^ workflow_id.wrapping_mul(WORKFLOW_MULTIPLIER)
        ^ (node_id as u64).wrapping_mul(NODE_MULTIPLIER);
    for (parent_id, parent_value) in inputs {
        let rotation = (((*parent_id as u32).wrapping_mul(13).wrapping_add(7)) % 64) as u32;
        value = value.rotate_left(rotation) ^ *parent_value;
        value = value.wrapping_add((*parent_id as u64).wrapping_mul(WORKFLOW_MULTIPLIER));
    }
    for round in 0..operations {
        value = value
            .rotate_left(7)
            .wrapping_mul(ROUND_MULTIPLIER)
            .wrapping_add((round as u64) ^ (node_id as u64) ^ workflow_id);
    }
    value
}

fn aggregate_sinks(parents: &[Vec<u32>], outputs: &[u64]) -> u64 {
    let mut aggregate = 0u64;
    for node_id in 0..parents.len() {
        let has_child = parents.iter().any(|incoming| incoming.contains(&(node_id as u32)));
        if !has_child {
            aggregate ^= outputs[node_id].rotate_left(((node_id * 11) % 64) as u32);
        }
    }
    aggregate
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(crate = "near_sdk::serde")]
pub struct GraphSpec {
    pub parents: Vec<Vec<u32>>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(crate = "near_sdk::serde")]
pub struct WorkflowInput {
    pub session_id: U64,
    pub workflow_id: U64,
    pub seed: U64,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(crate = "near_sdk::serde")]
pub struct WorkflowResult {
    pub session_id: U64,
    pub workflow_id: U64,
    pub seed: U64,
    pub checksum: U64,
    pub executed_nodes: u32,
    pub operation_count: u64,
    pub node_checksums: Vec<U64>,
    pub block_height: U64,
}

#[cfg(feature = "stage-contract")]
#[derive(BorshStorageKey, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
enum StageStorageKey {
    WorkflowResults,
    EdgeInputs,
    InputCounts,
    WorkflowSeeds,
    WorkflowSessions,
}

#[cfg(feature = "stage-contract")]
#[derive(BorshDeserialize, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
struct EdgeInput {
    value: u64,
    block_height: u64,
}

#[derive(BorshDeserialize, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
struct StoredStageResult {
    session_id: u64,
    checksum: u64,
    input_count: u32,
    operation_count: u32,
    block_height: u64,
}

#[cfg(feature = "stage-contract")]
#[derive(Serialize, Deserialize)]
#[serde(crate = "near_sdk::serde")]
pub struct StageEvidence {
    pub session_id: U64,
    pub node_id: u32,
    pub checksum: U64,
    pub input_count: u32,
    pub operation_count: u32,
    pub block_height: U64,
}

#[cfg(feature = "stage-contract")]
#[near(contract_state)]
#[derive(PanicOnDefault)]
pub struct Stage {
    node_id: u32,
    fail_workflow_id: Option<u64>,
    parents: Vec<u32>,
    successors: Vec<(AccountId, u32, u64)>,
    operations: u32,
    results: LookupMap<u64, StoredStageResult>,
    inputs: LookupMap<(u64, u32), EdgeInput>,
    input_counts: LookupMap<u64, u32>,
    seeds: LookupMap<u64, u64>,
    sessions: LookupMap<u64, u64>,
}

#[cfg(feature = "stage-contract")]
#[near_sdk::ext_contract(ext_stage)]
trait StageContract {
    fn receive_input(&mut self, session_id: U64, workflow_id: U64, seed: U64, parent_node_id: u32, value: U64) -> PromiseOrValue<()>;
    fn on_fanout_complete(&self, session_id: U64, workflow_id: U64, node_id: u32) -> bool;
}

#[cfg(feature = "stage-contract")]
#[near]
impl Stage {
    #[init]
    pub fn new(node_id: u32, fail_workflow_id: Option<U64>, parents: Vec<u32>, successors: Vec<(AccountId, u32, u64)>, operations: u32) -> Self {
        Self {
            node_id,
            fail_workflow_id: fail_workflow_id.map(|x| x.0),
            parents,
            successors,
            operations,
            results: LookupMap::new(StageStorageKey::WorkflowResults),
            inputs: LookupMap::new(StageStorageKey::EdgeInputs),
            input_counts: LookupMap::new(StageStorageKey::InputCounts),
            seeds: LookupMap::new(StageStorageKey::WorkflowSeeds),
            sessions: LookupMap::new(StageStorageKey::WorkflowSessions),
        }
    }

    pub fn start(&mut self, session_id: U64, workflow_id: U64, seed: U64) -> PromiseOrValue<()> {
        assert!(self.parents.is_empty(), "only source nodes accept start");
        assert!(self.results.get(&workflow_id.0).is_none(), "workflow node already completed");
        self.seeds.insert(&workflow_id.0, &seed.0);
        self.sessions.insert(&workflow_id.0, &session_id.0);
        self.process_node(session_id.0, workflow_id.0, seed.0, Vec::new())
    }

    pub fn receive_input(
        &mut self,
        session_id: U64,
        workflow_id: U64,
        seed: U64,
        parent_node_id: u32,
        value: U64,
    ) -> PromiseOrValue<()> {
        assert!(self.parents.contains(&parent_node_id), "unexpected predecessor edge");
        assert!(self.results.get(&workflow_id.0).is_none(), "workflow node already completed");
        assert!(self.inputs.get(&(workflow_id.0, parent_node_id)).is_none(), "duplicate DAG edge receipt");
        if let Some(existing_seed) = self.seeds.get(&workflow_id.0) {
            assert_eq!(existing_seed, seed.0, "predecessor seed mismatch");
        } else {
            self.seeds.insert(&workflow_id.0, &seed.0);
        }
        if let Some(existing_session) = self.sessions.get(&workflow_id.0) {
            assert_eq!(existing_session, session_id.0, "predecessor session mismatch");
        } else {
            self.sessions.insert(&workflow_id.0, &session_id.0);
        }
        self.inputs.insert(&
            (workflow_id.0, parent_node_id),
            &EdgeInput { value: value.0, block_height: env::block_height() },
        );
        env::log_str(&format!("DAG_EDGE|{}|{}|{}|{}|{}", session_id.0, workflow_id.0, self.node_id, parent_node_id, value.0));
        let count = self.input_counts.get(&workflow_id.0).unwrap_or(0) + 1;
        self.input_counts.insert(&workflow_id.0, &count);
        if count < self.parents.len() as u32 {
            return PromiseOrValue::Value(());
        }
        assert_eq!(count, self.parents.len() as u32, "too many predecessor edges");
        let values = self.parents.iter().map(|parent| {
            let edge = self.inputs.get(&(workflow_id.0, *parent)).expect("all predecessor edge values must exist");
            (*parent, edge.value)
        }).collect();
        self.process_node(session_id.0, workflow_id.0, seed.0, values)
    }

    #[cfg(feature = "stage-contract")]
    pub fn stage_result(&self, workflow_id: U64) -> Option<StageEvidence> {
        self.results.get(&workflow_id.0).map(|stored| StageEvidence {
            session_id: U64(stored.session_id),
            node_id: self.node_id,
            checksum: U64(stored.checksum),
            input_count: stored.input_count,
            operation_count: stored.operation_count,
            block_height: U64(stored.block_height),
        })
    }

    #[private]
    pub fn on_fanout_complete(&self, session_id: U64, workflow_id: U64, node_id: u32) -> bool {
        assert_eq!(node_id, self.node_id, "fanout callback target mismatch");
        match env::promise_result(0) {
            near_sdk::PromiseResult::Successful(_) => {
                env::log_str(&format!("DAG_JOIN|{}|{}|{}", session_id.0, workflow_id.0, node_id));
                true
            }
            near_sdk::PromiseResult::Failed => env::panic_str("DAG child failed before join completion"),
        }
    }

    fn process_node(&mut self, session_id: u64, workflow_id: u64, seed: u64, mut values: Vec<(u32, u64)>) -> PromiseOrValue<()> {
        assert_ne!(self.fail_workflow_id, Some(workflow_id), "injected stage failure");
        values.sort_by_key(|(parent, _)| *parent);
        let checksum = calculate_node(workflow_id, seed, self.node_id, &values, self.operations);
        self.results.insert(&workflow_id, &StoredStageResult {
            session_id,
            checksum,
            input_count: values.len() as u32,
            operation_count: self.operations,
            block_height: env::block_height(),
        });
        env::log_str(&format!("DAG_NODE|{}|{}|{}|{}|{}", session_id, workflow_id, self.node_id, checksum, self.operations));
        if self.successors.is_empty() {
            return PromiseOrValue::Value(());
        }
        let mut iter = self.successors.iter();
        let (first_account, _first_child, first_gas) = match iter.next() {
            Some(x) => x,
            None => return PromiseOrValue::Value(()),
        };
        let first = ext_stage::ext(first_account.clone())
            .with_static_gas(near_sdk::Gas::from_tgas(*first_gas))
            .receive_input(U64(session_id), U64(workflow_id), U64(seed), self.node_id, U64(checksum));
        if self.successors.len() == 1 {
            return PromiseOrValue::Promise(first);
        }
        let mut joint = first;
        for (account, _child_id, gas) in iter {
            joint = joint.and(
                ext_stage::ext(account.clone())
                    .with_static_gas(near_sdk::Gas::from_tgas(*gas))
                    .receive_input(U64(session_id), U64(workflow_id), U64(seed), self.node_id, U64(checksum)),
            );
        }
        let join = ext_stage::ext(env::current_account_id())
            .with_static_gas(near_sdk::Gas::from_tgas(2))
            .on_fanout_complete(U64(session_id), U64(workflow_id), self.node_id);
        PromiseOrValue::Promise(joint.then(join))
    }
}

#[cfg(feature = "composite-contract")]
#[derive(BorshStorageKey, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
enum CompositeStorageKey {
    WorkflowNodeChecksums,
    WorkflowResults,
}

#[cfg(feature = "composite-contract")]
#[derive(BorshDeserialize, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
struct StoredWorkflowResult {
    session_id: u64,
    checksum: u64,
    executed_nodes: u32,
    operation_count: u64,
    block_height: u64,
    seed: u64,
}

#[cfg(feature = "composite-contract")]
#[near(contract_state)]
#[derive(PanicOnDefault)]
pub struct SameReceiverDag {
    operations_per_node: u32,
    node_checksums: LookupMap<(u64, u32), u64>,
    results: LookupMap<u64, StoredWorkflowResult>,
}

#[cfg(feature = "composite-contract")]
#[near]
impl SameReceiverDag {
    #[init]
    pub fn new(operations_per_node: u32) -> Self {
        Self {
            operations_per_node,
            node_checksums: LookupMap::new(CompositeStorageKey::WorkflowNodeChecksums),
            results: LookupMap::new(CompositeStorageKey::WorkflowResults),
        }
    }

    /// Best same-receiver control: execute a whole theta-session in one legal function call/receipt.
    pub fn run_session(
        &mut self,
        session_id: U64,
        workflows: Vec<WorkflowInput>,
        graph: GraphSpec,
    ) -> Vec<WorkflowResult> {
        assert!(!workflows.is_empty(), "a session must include at least one workflow");
        validate_graph(&graph.parents);
        let mut seen = std::collections::BTreeSet::new();
        for flow in workflows.iter() {
            assert_eq!(flow.session_id.0, session_id.0, "all workflows must belong to this session");
            assert!(seen.insert(flow.workflow_id.0), "duplicate workflow id in session");
            assert!(self.results.get(&flow.workflow_id.0).is_none(), "workflow id already executed");
        }
        let mut output = Vec::with_capacity(workflows.len());
        for flow in workflows {
            let result = self.calculate_workflow(session_id.0, flow.workflow_id.0, flow.seed.0, &graph.parents);
            output.push(result);
        }
        output
    }

    pub fn workflow_result(&self, workflow_id: U64) -> Option<WorkflowResult> {
        self.results.get(&workflow_id.0).map(|stored| WorkflowResult {
            session_id: U64(stored.session_id),
            workflow_id,
            seed: U64(stored.seed),
            checksum: U64(stored.checksum),
            executed_nodes: stored.executed_nodes,
            operation_count: stored.operation_count,
            node_checksums: Vec::new(),
            block_height: U64(stored.block_height),
        })
    }

    /// Fault-injection probe. Prior node writes in this receipt must roll back atomically.
    pub fn probe_failure(&mut self, workflow_id: U64, seed: U64, graph: GraphSpec, fail_at_node: u32) {
        validate_graph(&graph.parents);
        assert!((fail_at_node as usize) < graph.parents.len(), "failure node outside graph");
        let mut outputs = Vec::with_capacity(graph.parents.len());
        for (node_id, inputs) in graph.parents.iter().enumerate() {
            let node_inputs: Vec<(u32, u64)> = inputs.iter().map(|parent| (*parent, outputs[*parent as usize])).collect();
            let checksum = calculate_node(workflow_id.0, seed.0, node_id as u32, &node_inputs, self.operations_per_node);
            self.node_checksums.insert(&(workflow_id.0, node_id as u32), &checksum);
            outputs.push(checksum);
            assert_ne!(node_id as u32, fail_at_node, "injected stage failure");
        }
    }

    fn calculate_workflow(&mut self, session_id: u64, workflow_id: u64, seed: u64, parents: &[Vec<u32>]) -> WorkflowResult {
        let mut outputs = Vec::with_capacity(parents.len());
        for (node_id, inputs) in parents.iter().enumerate() {
            for parent in inputs {
                assert!((*parent as usize) < node_id, "graph must be topologically ordered");
            }
            let node_inputs: Vec<(u32, u64)> = inputs.iter().map(|parent| (*parent, outputs[*parent as usize])).collect();
            let checksum = calculate_node(workflow_id, seed, node_id as u32, &node_inputs, self.operations_per_node);
            outputs.push(checksum);
            self.node_checksums.insert(&(workflow_id, node_id as u32), &checksum);
        }
        let checksum = aggregate_sinks(parents, &outputs);
        let operation_count = (parents.len() as u64) * (self.operations_per_node as u64);
        let block_height = env::block_height();
        self.results.insert(&workflow_id, &StoredWorkflowResult {
            session_id,
            checksum,
            executed_nodes: parents.len() as u32,
            operation_count,
            block_height,
            seed,
        });
        for (node_id, node_checksum) in outputs.iter().enumerate() {
            env::log_str(&format!("DAG_NODE|{}|{}|{}|{}|{}", session_id, workflow_id, node_id, node_checksum, self.operations_per_node));
        }
        WorkflowResult {
            session_id: U64(session_id),
            workflow_id: U64(workflow_id),
            seed: U64(seed),
            checksum: U64(checksum),
            executed_nodes: parents.len() as u32,
            operation_count,
            node_checksums: outputs.into_iter().map(U64).collect(),
            block_height: U64(block_height),
        }
    }
}

fn validate_graph(parents: &[Vec<u32>]) {
    assert!(!parents.is_empty(), "DAG must contain at least one node");
    for (node_id, inputs) in parents.iter().enumerate() {
        let mut seen = std::collections::BTreeSet::new();
        for parent in inputs {
            assert!((*parent as usize) < node_id, "graph must be topologically ordered");
            assert!(seen.insert(*parent), "duplicate predecessor edge");
        }
    }
}
