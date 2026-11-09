//! dec-1109d (escalation 4 of dec-1109b): reproduction only, no behaviour change.
//! A spawned agent whose first turn is interrupted and which is then unloaded is
//! reloaded by `send_message`; mailbox recovery may hand it the interrupted first
//! message again, which starts a turn.
use super::*;
use pretty_assertions::assert_eq;

const FIRST: &str = "dec1109d boot worker";

fn first_message_deliveries(manager: &ThreadManager, agent_id: ThreadId) -> usize {
    manager
        .captured_ops()
        .into_iter()
        .filter(|(thread_id, op)| {
            *thread_id == agent_id
                && matches!(op, Op::InterAgentCommunication { communication }
                    if communication.content.contains(FIRST))
        })
        .count()
}

#[tokio::test]
async fn dec1109d_reload_redelivers_an_interrupted_first_message() {
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
            function_payload(json!({"message": FIRST, "task_name": "worker"})),
        ))
        .await
        .expect("spawn worker");
    let agent_id = session
        .services
        .agent_control
        .resolve_agent_reference(session.thread_id, &turn.session_source, "worker")
        .await
        .expect("worker should resolve");
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
    let interrupted: InterruptAgentResult =
        serde_json::from_str(&content).expect("result should be json");
    let settled = settle_agent(&manager, agent_id).await;
    // Give the aborted turn time to finalize its mailbox rows.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase_of_first = |rows: Vec<codex_state::AgentMailboxMessage>| {
        rows.into_iter()
            .find(|row| row.communication.content.contains(FIRST))
            .map(|row| (row.phase, row.attempt_id))
    };
    let after_interrupt = phase_of_first(
        state_db
            .list_recoverable_agent_messages(agent_id)
            .await
            .expect("mailbox rows"),
    );
    let deliveries_before_reload = first_message_deliveries(&manager, agent_id);

    let unloaded = manager
        .remove_thread(&agent_id)
        .await
        .expect("worker thread should be loaded before removal");
    unloaded
        .submit(Op::Shutdown {})
        .await
        .expect("removed worker should accept shutdown");
    unloaded.wait_until_terminated().await;
    SendMessageHandlerV2
        .handle(invocation(
            session.clone(),
            turn.clone(),
            "send_message",
            function_payload(json!({"target": "worker", "message": "for later"})),
        ))
        .await
        .expect("send_message should reload the worker");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while first_message_deliveries(&manager, agent_id) == deliveries_before_reload
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let redelivered = first_message_deliveries(&manager, agent_id) - deliveries_before_reload;
    let status_after_reload = manager.agent_control().get_status(agent_id).await;
    eprintln!(
        "DEC1109D interrupt_previous={:?} settled={settled:?} first_row_after_interrupt={after_interrupt:?} redelivered={redelivered} status_after_reload={status_after_reload:?}",
        interrupted.previous_status
    );
    match after_interrupt.as_ref().map(|(phase, _)| *phase) {
        // Handed to the old session but never recorded there: recovery treats it
        // as never delivered and hands it to the reloaded session again.
        Some(codex_state::AgentMailboxPhase::Submitted) => assert_eq!(redelivered, 1),
        // Recorded by the aborted turn: quarantined, never replayed.
        Some(codex_state::AgentMailboxPhase::UnknownOutcome) | None => {
            assert_eq!(redelivered, 0)
        }
        Some(other) => panic!("unexpected phase after interrupt: {other:?}"),
    }
}
