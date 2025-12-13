use picante::PicanteResult;
use std::sync::atomic::{AtomicUsize, Ordering};

static QUERY_CALLS: AtomicUsize = AtomicUsize::new(0);

#[picante::input]
pub struct Item {
    #[key]
    pub id: u32,
    pub value: String,
}

#[picante::interned]
pub struct Label {
    pub text: String,
}

#[picante::tracked]
pub async fn item_length<DB: DatabaseTrait>(db: &DB, item: Item) -> PicanteResult<u64> {
    QUERY_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(item.value(db)?.len() as u64)
}

#[picante::db(inputs(Item), interned(Label), tracked(item_length))]
pub struct Database {}

/// Helper to check that QUERY_CALLS increased by expected amount
fn assert_calls_increased_by(before: usize, expected_delta: usize) {
    let after = QUERY_CALLS.load(Ordering::SeqCst);
    let actual_delta = after.saturating_sub(before);
    assert_eq!(
        actual_delta, expected_delta,
        "expected {} new calls, got {} (before={}, after={})",
        expected_delta, actual_delta, before, after
    );
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_sees_data_at_snapshot_time() -> PicanteResult<()> {
    let db = Database::new();

    // Create an item
    let item = Item::new(&db, 1, "hello".into())?;
    assert_eq!(item.value(&db)?, "hello".to_string());

    // Query it (will compute and cache)
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len = item_length(&db, item).await?;
    assert_eq!(len, 5);
    assert_calls_increased_by(before, 1);

    // Create a snapshot
    let snapshot = DatabaseSnapshot::from_database(&db).await;

    // Snapshot sees the same item data
    assert_eq!(item.value(&snapshot)?, "hello".to_string());

    // Query on snapshot uses cached result (no recompute)
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len_snapshot = item_length(&snapshot, item).await?;
    assert_eq!(len_snapshot, 5);
    assert_calls_increased_by(before, 0); // No new calls - used cache

    // Now modify the database
    let _item2 = Item::new(&db, 1, "hello world".into())?;

    // Database sees the new value
    assert_eq!(item.value(&db)?, "hello world".to_string());

    // Snapshot still sees the old value
    assert_eq!(item.value(&snapshot)?, "hello".to_string());

    // Query on database recomputes (value changed)
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len_new = item_length(&db, item).await?;
    assert_eq!(len_new, 11);
    assert_calls_increased_by(before, 1);

    // Query on snapshot still returns old cached result
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len_snapshot2 = item_length(&snapshot, item).await?;
    assert_eq!(len_snapshot2, 5);
    assert_calls_increased_by(before, 0); // No new calls - used cache

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_shares_interned_values() -> PicanteResult<()> {
    let db = Database::new();

    // Intern a label
    let label = Label::new(&db, "tag".into())?;
    assert_eq!(label.text(&db)?, "tag".to_string());

    // Create snapshot
    let snapshot = DatabaseSnapshot::from_database(&db).await;

    // Snapshot can look up the same interned value
    assert_eq!(label.text(&snapshot)?, "tag".to_string());

    // New interned values after snapshot are still visible (append-only)
    let label2 = Label::new(&db, "new-tag".into())?;
    assert_eq!(label2.text(&snapshot)?, "new-tag".to_string());

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_can_compute_new_queries() -> PicanteResult<()> {
    let db = Database::new();

    // Create items
    let item1 = Item::new(&db, 1, "foo".into())?;
    let item2 = Item::new(&db, 2, "bar".into())?;

    // Only compute item1 on database
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len1 = item_length(&db, item1).await?;
    assert_eq!(len1, 3);
    assert_calls_increased_by(before, 1);

    // Create snapshot
    let snapshot = DatabaseSnapshot::from_database(&db).await;

    // item1 is cached in snapshot (no recompute)
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len1_snap = item_length(&snapshot, item1).await?;
    assert_eq!(len1_snap, 3);
    assert_calls_increased_by(before, 0); // No new calls - used cache

    // item2 was never computed - snapshot will compute it
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len2_snap = item_length(&snapshot, item2).await?;
    assert_eq!(len2_snap, 3);
    assert_calls_increased_by(before, 1);

    // Computing on snapshot doesn't affect database's cache
    // (they have separate DerivedIngredient instances)
    let before = QUERY_CALLS.load(Ordering::SeqCst);
    let len2_db = item_length(&db, item2).await?;
    assert_eq!(len2_db, 3);
    assert_calls_increased_by(before, 1); // Database computes it too

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_snapshots_are_independent() -> PicanteResult<()> {
    let db = Database::new();

    // Initial state
    let item = Item::new(&db, 1, "v1".into())?;
    let snap1 = DatabaseSnapshot::from_database(&db).await;

    // Modify and create another snapshot
    let _ = Item::new(&db, 1, "v2".into())?;
    let snap2 = DatabaseSnapshot::from_database(&db).await;

    // Modify again
    let _ = Item::new(&db, 1, "v3".into())?;

    // Each sees their respective version
    assert_eq!(item.value(&snap1)?, "v1".to_string());
    assert_eq!(item.value(&snap2)?, "v2".to_string());
    assert_eq!(item.value(&db)?, "v3".to_string());

    Ok(())
}
