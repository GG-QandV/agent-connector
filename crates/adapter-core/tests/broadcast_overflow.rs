//! §3 из docs/design/concurrency_race_tests.rs: broadcast::channel(256)
//! overflow при медленном подписчике.
//!
//! Capacity 256 совпадает с `ActiveTask.tx` в adapter-core (`broadcast::channel(256)`).
//! Инвариант: переполнение всегда даёт явный `Lagged` с ненулевым счётчиком
//! пропущенных событий — тихая потеря событий недопустима, т.к. consumer
//! (executor) должен маппить Lagged в явную ошибку подписки.

use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 256; // = ActiveTask.tx в adapter-core

#[tokio::test(flavor = "multi_thread")]
async fn slow_subscriber_gets_explicit_lagged_error_not_silent_drop() {
    let (tx, mut rx) = broadcast::channel::<u64>(CHANNEL_CAPACITY);

    // Producer шлёт события быстрее, чем rx их читает — намеренно
    // превышаем capacity в 2 раза, не читая параллельно.
    let producer = {
        let tx = tx.clone();
        tokio::spawn(async move {
            for seq in 0..(CHANNEL_CAPACITY as u64 * 2) {
                let _ = tx.send(seq);
            }
        })
    };
    producer.await.unwrap();

    let mut got_lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                got_lagged = true;
                assert!(skipped > 0, "Lagged error must report a nonzero skip count");
                // После Lagged можно продолжать читать свежие события.
            }
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }

    assert!(
        got_lagged,
        "producer sent 2x capacity without reader keeping up — \
         must surface Lagged, not silently truncate"
    );
}
