use std::time::Duration;

use futures::Future;

pub async fn measure_time<F: Future>(f: F) ->(F::Output, Duration) {
    let now = std::time::Instant::now();
    let out = f.await;
    (out, now.elapsed())
}