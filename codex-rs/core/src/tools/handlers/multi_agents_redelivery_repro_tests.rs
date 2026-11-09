//! dec-1109d (escalation 4 of dec-1109b): reproduction only, no behaviour change.
//! A spawned agent whose first turn is interrupted before it records its first
//! message, and which is then unloaded, is reloaded by a queue-only
//! `send_message`; mailbox recovery hands it the interrupted first message
//! again, which starts a turn. Control: a first turn that ends on its own is not
//! replayed.
use super::*;
use pretty_assertions::assert_eq;

fn describe_rows(rows: &[codex_state::AgentMailboxMessage]) -> Vec<String> {
    rows.iter()
        .map(|row| format!("{:?}/trigger={}", row.phase, row.communication.trigger_turn))
        .collect()
}

fn agent_ops(manager: &ThreadManager, agent_id: ThreadId) -> Vec<Op> {
    manager
        .captured_ops()
        .into_iter()
        .filter(|(thread_id, _)| *thread_id == agent_id)
        .map(|(_, op)| op)
        .collect()
}

fn describe_ops(ops: &[Op]) -> Vec<String> {
    ops.iter()
        .map(|op| match op {
            Op::InterAgentCommunication { communication } => format!(
                "IAC(trigger={}, id={:?})",
                communication.trigger_turn, communication.message_id
            ),
            other => format!("{other:?}").chars().take(40).collect(),
        })
        .collect()
}

fn deliveries_of(ops: &[Op], message_id: &str) -> usize {
    ops.iter()
        .filter(|op| {
            matches!(op, Op::InterAgentCommunication { communication }
                if communication.message_id.as_deref() == Some(message_id))
        })
        .count()
}

struct Outcome {
    first_phase_before_reload: codex_state::AgentMailboxPhase,
    redelivered: usize,
    status_after_reload: AgentStatus,
}

async fn spawn_unload_and_reload(interrupt: bool) -> Outcome {
    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config.multi_agent_v2.max_concurrent_threads_per_session = 4;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite");
    let state_db = init_state_db(&config)
        .await
        .expect("sqlite state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db.clone()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    set_turn_config(&mut turn, config);
    let session = Arc::new(session);
    let turn = Arc::new(turn);
    SpawnAgentHandlerV2::default()
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "spawn_agent",
            function_payload(json!({"message": "boot worker", "task_name": "worker"})),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.thread_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
    let first_id = agent_ops(&manager, agent_id)
        .iter()
        .find_map(|op| match op {
            Op::InterAgentCommunication { communication } if communication.trigger_turn => {
                communication.message_id.clone()
            }
            _ => None,
        })
        .expect("the spawn hands the first message over");
    if interrupt {
        // The parent interrupts the just-spawned agent (D435 pending path).
        let output = InterruptAgentHandler
            .handle(invocation(
                session.clone(),
                turn.clone(),
                "interrupt_agent",
                function_payload(json!({"target": "worker"})),
            ))
            .await
            .expect("interrupt_agent should succeed");
        let (content, _) = expect_text_output(output);
        let result: InterruptAgentResult =
            serde_json::from_str(&content).expect("result should be json");
        eprintln!(
            "DEC1109D interrupt previous_status={:?}",
            result.previous_status
        );
    }
    // Wait until the first turn has ended (interrupted, or on its own).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let settled = loop {
        let status = manager.agent_control().get_status(agent_id).await;
        if !matches!(status, AgentStatus::PendingInit | AgentStatus::Running) {
            break status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent did not settle"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first_row = |rows: &[codex_state::AgentMailboxMessage]| {
        rows.iter()
            .find(|row| row.communication.message_id.as_deref() == Some(first_id.as_str()))
            .map(|row| row.phase)
    };
    let rows = state_db
        .list_recoverable_agent_messages(agent_id)
        .await
        .expect("mailbox rows");
    // `list_recoverable_agent_messages` leaves out finished rows.
    let first_phase_before_reload =
        first_row(&rows).unwrap_or(codex_state::AgentMailboxPhase::Completed);
    let ops_before_reload = agent_ops(&manager, agent_id);
    eprintln!(
        "DEC1109D interrupt={interrupt} settled={settled:?} rows_before_reload={:?} ops_before_reload={:?}",
        describe_rows(&rows),
        describe_ops(&ops_before_reload)
    );

    let unloaded = manager
        .remove_thread(&agent_id)
        .await
        .expect("worker thread should be loaded before removal");
    unloaded
        .submit(Op::Shutdown {})
        .await
        .expect("removed worker should accept shutdown");
    unloaded.wait_until_terminated().await;
    // Queue-only: on its own this never starts a turn.
    SendMessageHandlerV2
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "send_message",
            function_payload(json!({"target": "worker", "message": "for later"})),
        ))
        .await
        .expect("send_message should reload the worker");
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let ops_after_reload: Vec<Op> = agent_ops(&manager, agent_id)
        .into_iter()
        .skip(ops_before_reload.len())
        .collect();
    let redelivered = deliveries_of(&ops_after_reload, &first_id);
    let status_after_reload = manager.agent_control().get_status(agent_id).await;
    eprintln!(
        "DEC1109D interrupt={interrupt} first_phase_before_reload={first_phase_before_reload:?} ops_after_reload={:?} redelivered={redelivered} status_after_reload={:?}",
        describe_ops(&ops_after_reload),
        format!("{status_after_reload:?}")
            .chars()
            .take(30)
            .collect::<String>()
    );
    Outcome {
        first_phase_before_reload,
        redelivered,
        status_after_reload,
    }
}

#[tokio::test]
async fn dec1109d_reload_redelivers_an_interrupted_first_message() {
    let outcome = spawn_unload_and_reload(/*interrupt*/ true).await;
    match outcome.first_phase_before_reload {
        // Handed to the old session but never recorded there (the interrupt
        // aborted the turn first): recovery treats it as never delivered and
        // hands it to the reloaded session again, which starts a turn.
        codex_state::AgentMailboxPhase::Submitted => {
            assert_eq!(outcome.redelivered, 1);
            assert!(
                !matches!(outcome.status_after_reload, AgentStatus::PendingInit),
                "a turn ran after the reload"
            );
        }
        // Recorded by the aborted turn: quarantined, never replayed.
        codex_state::AgentMailboxPhase::UnknownOutcome => assert_eq!(outcome.redelivered, 0),
        other => panic!("unexpected phase after interrupt: {other:?}"),
    }
}

#[tokio::test]
async fn dec1109d_control_a_first_turn_that_ended_on_its_own_is_not_replayed() {
    let outcome = spawn_unload_and_reload(/*interrupt*/ false).await;
    assert_eq!(outcome.redelivered, 0);
    assert_eq!(outcome.status_after_reload, AgentStatus::PendingInit);
}
