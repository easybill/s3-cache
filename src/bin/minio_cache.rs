use std::collections::HashMap;

use minio_cache::{Config, start_app};

fn main() {
    // Safety: `set_var()` is only safe to call in single threaded programs when using
    // non-windows targets. Calling `set_var()` in the main function, before spawning any
    // other thread makes sure that this invariant is upheld.
    unsafe { std::env::set_var("AWS_EC2_METADATA_DISABLED", "true") };

    let vars: HashMap<String, String> = std::env::vars().collect();
    let config = Config::from_env(&vars);

    // Drop ENV hashmap to free memory before we start the server.
    drop(vars);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker_threads)
        .thread_stack_size(2 << 20) // 2 MiB
        .build()
        .expect("can't start tokio runtime");

    if let Err(error) = runtime.block_on(start_app(config)) {
        eprintln!("Application error: {error:?}");
        std::process::exit(1);
    }
}
