//! Extraction is idempotent per record, Ed threads are held to the unit-level rules, a metadata-only
//! re-upsert queues no extraction, and a failed model call leaves the old facts alone.

mod common;

use common::{test_settings, TestApp};
use eunomia_backend::entities::{extract, service};
use eunomia_backend::jobs::kind;
use eunomia_backend::rid::RecordIdExt;
use eunomia_backend::sources::base::owner_key_str;
use eunomia_backend::sources::ed::EdSource;
use eunomia_backend::sources::mock::{route, serve, Mock, Route};
use eunomia_backend::sources::registry;
use eunomia_backend::state::OrgState;
use serde_json::{json, Value};
use surrealdb::types::RecordId;

async fn app() -> (TestApp, OrgState) {
    let mut settings = test_settings();
    settings.openai_api_key = Some("sk-test".into());
    settings.openai_base_url = "http://127.0.0.1:9/v1".into();
    let app = TestApp::with_settings(settings).await;
    let org = app.state.org(&app.user.org).await.unwrap();
    (app, org)
}

async fn point_openai_at(org: &OrgState, owner: &RecordId, base: &str) {
    org.db
        .test_raw()
        .query("UPSERT $id SET owner = $owner, openai_base_url = $base")
        .bind(("id", RecordId::new("app_settings", owner_key_str(owner))))
        .bind(("owner", owner.clone()))
        .bind(("base", base.to_string()))
        .await
        .unwrap()
        .check()
        .unwrap();
}

fn chat(content: Value) -> Route {
    route("POST", "/chat/completions", json!({"choices": [{"message": {"content": content.to_string()}}]}))
}

async fn mock_answering(content: Value) -> Mock {
    serve(vec![chat(content)]).await
}

async fn strings(org: &OrgState, sql: &str) -> Vec<String> {
    let mut res = org.db.test_raw().query(sql).await.unwrap();
    res.take::<Vec<String>>(0).unwrap()
}

async fn world_texts(org: &OrgState) -> Vec<String> {
    strings(org, "SELECT VALUE text FROM memory WHERE type = 'world' ORDER BY text").await
}

async fn relation_count(org: &OrgState) -> usize {
    strings(org, "SELECT VALUE <string> id FROM relates_to").await.len()
}

fn issue_record() -> extract::ExtractRecord {
    extract::ExtractRecord {
        id: "github:github.issue:acme/widget#7".into(),
        title: "Importer drops the last row".into(),
        body_text: "Ada Lovelace reported that the quarterly importer drops the last CSV row.".into(),
        occurred_at: None,
        vault: None,
        source: "github".into(),
        payload: Value::Null,
        owner_name: None,
    }
}

fn ed_record(replies: usize) -> extract::ExtractRecord {
    let mut body = String::from("Is the final open book?");
    for i in 0..replies {
        body.push_str(&format!("\n\nStudent{i}: I think the exam rules say something about item {i}."));
    }
    extract::ExtractRecord {
        id: "ed:ed.thread:501".into(),
        title: "Final exam".into(),
        body_text: body,
        occurred_at: Some("2026-05-01T10:00:00+00:00".into()),
        vault: None,
        source: "ed".into(),
        payload: json!({"course": "Intro to Testing", "course_code": "FIT9999", "category": "Exams", "author": "Student0"}),
        owner_name: None,
    }
}

/// (a) Extracting a record again leaves exactly what extracting it once did; hand-written memories stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extracting_twice_equals_once_and_keeps_hand_written_memories() {
    let (app, org) = app().await;
    let owner = app.user.id.clone();
    let answer = json!({
        "people": [{"name": "Ada Lovelace", "facts": ["Ada reported the importer bug", "Ada prefers CSV"]}],
        "organisations": [{"name": "Acme", "facts": ["Acme ships the importer"]}],
        "relations": [{"from": "Ada Lovelace", "from_kind": "person", "to": "Acme", "to_kind": "organisation", "label": "works_at"}]
    });
    let mock = mock_answering(answer).await;
    point_openai_at(&org, &owner, &mock.base).await;

    let ada = common::sys(service::upsert_entity(&org.db, &owner, "person", "Ada Lovelace", None, None)).await.unwrap();
    let ada_id: RecordId = eunomia_backend::rid::parse(&ada.id).unwrap();
    common::sys(service::add_memory(&org.db, &owner, &ada_id, "Ada wrote this one by hand", None, "world")).await.unwrap();

    let rec = issue_record();
    common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await.unwrap();
    let once = world_texts(&org).await;
    assert_eq!(once.len(), 4, "{once:?}");
    assert_eq!(relation_count(&org).await, 1);

    for _ in 0..2 {
        let touched = common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await.unwrap();
        assert!(touched.contains(&ada_id.to_string()), "the cleared subject is queued for consolidation: {touched:?}");
        assert_eq!(world_texts(&org).await, once);
        assert_eq!(relation_count(&org).await, 1);
    }
    assert!(once.contains(&"Ada wrote this one by hand".to_string()));
}

/// (b) An Ed thread with 30 replies and a model that returns far too much: at most 5 facts, all on the
/// unit organisation, no person, no relation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ed_thread_keeps_at_most_five_facts_on_the_unit_and_nothing_else() {
    let (app, org) = app().await;
    let owner = app.user.id.clone();
    let people: Vec<Value> = (0..15).map(|i| json!({"name": format!("Student{i}"), "facts": [format!("Student{i} guessed answer {i}")]})).collect();
    let orgs: Vec<Value> = (0..15).map(|i| json!({"name": format!("Dept {i}"), "facts": [format!("Rule {i} applies to the final exam"), "  "]})).collect();
    let answer = json!({
        "people": people, "organisations": orgs,
        "relations": [{"from": "Student0", "from_kind": "person", "to": "Dept 0", "to_kind": "organisation", "label": "member_of"}]
    });
    let mock = mock_answering(answer).await;
    point_openai_at(&org, &owner, &mock.base).await;

    let rec = ed_record(30);
    common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await.unwrap();

    let sent = mock.requests().into_iter().find(|r| r.path == "/chat/completions").unwrap().body;
    assert!(sent.contains("FIT9999") && sent.contains("lasting facts"), "the Ed prompt was used: {}", &sent[..sent.len().min(600)]);

    let texts = world_texts(&org).await;
    assert_eq!(texts.len(), 5, "{texts:?}");
    let on_unit = strings(&org, "SELECT VALUE text FROM memory WHERE subject.name = 'FIT9999' AND subject IS NOT NONE ORDER BY text").await;
    assert_eq!(on_unit, texts, "every fact sits on the unit");
    assert_eq!(strings(&org, "SELECT VALUE <string> id FROM organisation").await.len(), 1, "only the unit entity");
    assert!(strings(&org, "SELECT VALUE <string> id FROM person").await.is_empty());
    assert_eq!(relation_count(&org).await, 0);
}

/// (c) A re-upsert that changes only metadata queues no extraction; a body change does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_text_change_queues_extraction() {
    let app = TestApp::new().await;
    let org = app.state.org(&app.user.org).await.unwrap();
    let owner = app.user.id.clone();
    let thread = |updated: &str, answered: bool, document: &str| {
        json!({"id": 501, "course_id": 3, "title": "Final exam", "document": document, "category": "Exams", "type": "question",
               "is_answered": answered, "updated_at": updated, "deleted_at": null, "_course_code": "FIT9999", "_course_name": "Intro to Testing",
               "answers": [], "comments": []})
    };
    let jobs = || async {
        let mut res = app.state.control.test_raw().query("SELECT VALUE id FROM job WHERE kind = $k AND owner = $o").bind(("k", kind::EXTRACT)).bind(("o", owner.clone())).await.unwrap();
        res.take::<Vec<RecordId>>(0).unwrap().len()
    };
    let ingest = |raw: Value| {
        let org = org.clone();
        let owner = owner.clone();
        async move { registry::ingest(&org, &owner, &[raw], &EdSource).await.unwrap() }
    };

    let first = ingest(thread("2026-05-01T10:00:00Z", false, "Is the final open book? Please say.")).await;
    assert_eq!(first.written, 1);
    assert_eq!(jobs().await, 1);

    let meta = ingest(thread("2026-05-02T10:00:00Z", true, "Is the final open book? Please say.")).await;
    assert_eq!(meta.written, 1, "the record did change");
    assert_eq!(jobs().await, 1, "but its text did not, so nothing new to extract");

    ingest(thread("2026-05-03T10:00:00Z", true, "Is the final open book? Staff: yes, open book.")).await;
    assert_eq!(jobs().await, 2);
}

/// (d) A failed model call keeps the record's old facts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_model_call_keeps_the_old_facts() {
    let (app, org) = app().await;
    let owner = app.user.id.clone();
    let good = mock_answering(json!({"organisations": [{"name": "Acme", "facts": ["Acme ships the importer"]}]})).await;
    point_openai_at(&org, &owner, &good.base).await;
    let rec = issue_record();
    common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await.unwrap();
    let ed = ed_record(1);
    let ed_good = mock_answering(json!({"organisations": [{"name": "X", "facts": ["The final is open book"]}]})).await;
    point_openai_at(&org, &owner, &ed_good.base).await;
    common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &ed)).await.unwrap();
    let before = world_texts(&org).await;
    assert_eq!(before.len(), 2, "{before:?}");

    let down = serve(vec![route("POST", "/chat/completions", json!({"error": "down"})).status(500)]).await;
    point_openai_at(&org, &owner, &down.base).await;
    assert!(common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await.unwrap().is_empty());
    assert!(common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &ed)).await.is_err(), "Ed failures retry");
    assert_eq!(world_texts(&org).await, before);

    // an answer that is not valid JSON fails the same way
    let junk = serve(vec![route("POST", "/chat/completions", json!({"choices": [{"message": {"content": "not json"}}]}))]).await;
    point_openai_at(&org, &owner, &junk.base).await;
    let _ = common::sys(extract::extract_entities(&org.db, &org.settings, &owner, &rec)).await;
    assert_eq!(world_texts(&org).await, before);
}
