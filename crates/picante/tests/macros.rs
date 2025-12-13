use picante::PicanteResult;
use std::sync::atomic::{AtomicUsize, Ordering};

static LEN_CALLS: AtomicUsize = AtomicUsize::new(0);
static SUM_CALLS: AtomicUsize = AtomicUsize::new(0);
static UNIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[picante::input]
pub struct Text {
    #[key]
    pub key: String,
    pub value: String,
}

#[picante::tracked]
pub async fn len<DB: HasTextIngredient>(db: &DB, text: Text) -> PicanteResult<u64> {
    LEN_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(text.value(db)?.len() as u64)
}

#[picante::tracked]
pub async fn sum<DB>(db: &DB, x: u32, y: u32) -> u64 {
    let _ = db;
    SUM_CALLS.fetch_add(1, Ordering::Relaxed);
    (x as u64) + (y as u64)
}

#[picante::tracked]
pub async fn unit_key<DB>(db: &DB) -> u64 {
    let _ = db;
    UNIT_CALLS.fetch_add(1, Ordering::Relaxed);
    42
}

#[picante::interned]
pub struct Word {
    pub text: String,
}

#[picante::db(inputs(Text), interned(Word), tracked(len, sum, unit_key))]
struct Db {}

#[tokio::test(flavor = "current_thread")]
async fn macros_basic_flow() -> PicanteResult<()> {
    LEN_CALLS.store(0, Ordering::Relaxed);
    let db = Db::new();
    let text = Text::new(&db, "a".into(), "hello".into())?;

    assert_eq!(len(&db, text).await?, 5);
    assert_eq!(len(&db, text).await?, 5);
    assert_eq!(LEN_CALLS.load(Ordering::Relaxed), 1);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn macros_tuple_keys_and_unit_key() -> PicanteResult<()> {
    SUM_CALLS.store(0, Ordering::Relaxed);
    UNIT_CALLS.store(0, Ordering::Relaxed);

    let db = Db::new();

    assert_eq!(sum(&db, 1, 2).await?, 3);
    assert_eq!(sum(&db, 1, 2).await?, 3);
    assert_eq!(SUM_CALLS.load(Ordering::Relaxed), 1);

    assert_eq!(sum(&db, 2, 3).await?, 5);
    assert_eq!(sum(&db, 2, 3).await?, 5);
    assert_eq!(SUM_CALLS.load(Ordering::Relaxed), 2);

    assert_eq!(unit_key(&db).await?, 42);
    assert_eq!(unit_key(&db).await?, 42);
    assert_eq!(UNIT_CALLS.load(Ordering::Relaxed), 1);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn macros_interned_works() -> PicanteResult<()> {
    let db = Db::new();

    let w1 = Word::new(&db, "hello".to_string())?;
    let w2 = Word::new(&db, "hello".to_string())?;
    assert_eq!(w1, w2);
    assert_eq!(w1.text(&db)?, "hello".to_string());

    Ok(())
}
