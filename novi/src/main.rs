use novi_core::{log, sub_main, Novi};
use std::str::FromStr;

async fn the_main() {
    log::register();

    if std::env::args().nth(1).as_deref() == Some("--plugin-host") {
        sub_main().await;
        return;
    }

    let config = serde_yaml::from_reader(
        std::fs::File::open("config.yml").expect("failed to open config.yml"),
    )
    .expect("failed to parse config");
    let _novi = Novi::new(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL not found"),
        config,
    )
    .await
    .expect("initialization failed");

    std::future::pending::<()>().await;
}

fn main() {
    let _ = dotenvy::dotenv();

    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if let Some(threads) = std::env::var("WORKER_THREADS")
        .ok()
        .and_then(|s| usize::from_str(&s).ok())
    {
        rt.worker_threads(threads);
    }
    let rt = rt.build().expect("failed to build runtime");

    rt.block_on(the_main());
}
