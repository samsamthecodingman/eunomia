//! Consolidating an entity with thousands of facts reads, embeds and prompts a bounded number of them
//! (an unbounded read took the database down).

mod common;

use common::{test_settings, TestApp};
use eunomia_backend::entities::consolidate::{self, MAX_CONSOLIDATE_FACTS};
use eunomia_backend::rid;
use eunomia_backend::sources::base::owner_key_str;
use eunomia_backend::sources::mock::{route, serve};
use serde_json::json;
use surrealdb::types::RecordId;



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consolidating_an_entity_with_thousands_of_facts_is_bounded() {
    let mut settings = test_settings();
    settings.openai_api_key = Some("sk-test".into());
    settings.openai_base_url = "http://127.0.0.1:9/v1".into();
    let app = TestApp::with_settings(settings.clone()).await;
    let owner = app.user.id.clone();
    let db = app.db().await;

    let mock = serve(vec![
        route("POST", "/embeddings", json!({"data": [{"index": 0, "embedding": vec![0.01_f32; eunomia_backend::embeddings::service::dim()]}]})),
        route("POST", "/chat/completions", json!({"choices": [{"message": {"content": "{\"belief\": \"Ada has many facts.\"}"}}]})),
    ])
    .await;
    db.test_raw()
        .query("UPSERT $id SET owner = $owner, openai_base_url = $base")
        .bind(("id", RecordId::new("app_settings", owner_key_str(&owner))))
        .bind(("owner", owner.clone()))
        .bind(("base", mock.base.clone()))
        .await
        .unwrap()
        .check()
        .unwrap();

    let out = app.tool("memory_write", json!({"subject_name": "Ada Lovelace", "subject_kind": "person", "text": "seed fact"})).await;
    let subject = rid::parse(out["entity"]["id"].as_str().unwrap()).unwrap();
    // fact-0 is the oldest, fact-2999 the newest; one raw query, not 3000 round trips
    db.test_raw()
        .query(
            "LET $vault = (SELECT VALUE vault FROM ONLY $subject);
             FOR $i IN 0..3000 {
                 CREATE memory SET owner = $owner, vault = $vault, subject = $subject, type = 'world',
                     text = 'fact-' + <string>$i, created_at = time::now() - duration::from_secs(3000 - $i);
             };",
        )
        .bind(("subject", subject.clone()))
        .bind(("owner", owner.clone()))
        .await
        .unwrap()
        .check()
        .unwrap();

    let obs = common::sys(consolidate::consolidate_subject(&db, &settings, &owner, &subject, None)).await.unwrap();
    assert!(obs.is_some(), "an observation is written");

    let chats: Vec<String> = mock.requests().into_iter().filter(|r| r.path == "/chat/completions").map(|r| r.body).collect();
    let listed = |body: &str| body.matches("fact-").count();
    let consolidation: Vec<_> = chats.iter().filter(|b| b.contains("New raw facts:")).collect();
    assert_eq!(consolidation.len(), 1, "one consolidation prompt");
    let n = listed(consolidation[0]);
    assert!(n > 0 && n <= MAX_CONSOLIDATE_FACTS as usize, "consolidation prompt lists {n} facts");
    assert!(consolidation[0].contains("fact-2999"), "the newest fact is kept");
    assert!(!consolidation[0].contains("fact-0\\n") && !consolidation[0].contains("fact-1\\n"), "the oldest facts are left out");
    for b in chats.iter().filter(|b| b.contains("Below are facts recorded")) {
        assert!(listed(b) <= 40, "supersede prompt lists {} facts", listed(b));
    }
    let embedded: usize = mock.requests().iter().filter(|r| r.path == "/embeddings").count();
    assert!(embedded <= 4, "{embedded} embedding calls: the supersede check is bounded");

    let mut res = db.test_raw().query("SELECT VALUE array::len(source_memories) FROM memory WHERE subject = $s AND type = 'observation'").bind(("s", subject.clone())).await.unwrap();
    let lineage: Vec<i64> = res.take(0).unwrap();
    assert_eq!(lineage.len(), 1);
    assert!(lineage[0] > 0 && lineage[0] <= MAX_CONSOLIDATE_FACTS, "lineage names {} facts", lineage[0]);

    // facts that fell outside the window do not make the observation look changed on the next run
    let before = mock.requests().len();
    let again = common::sys(consolidate::consolidate_subject(&db, &settings, &owner, &subject, None)).await.unwrap();
    assert!(again.is_none(), "nothing changed");
    assert_eq!(mock.requests().len(), before, "no model call when nothing changed");
}
