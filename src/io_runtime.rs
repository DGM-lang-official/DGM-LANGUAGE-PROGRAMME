use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

use crate::error::DgmError;

static IO_RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
const IO_RUNTIME_THREAD_STACK_SIZE: usize = 512 * 1024;
const IO_RUNTIME_THREAD_CAP: usize = 8;

pub fn runtime() -> Result<&'static Runtime, DgmError> {
    let result = IO_RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .worker_threads(
                std::thread::available_parallelism()
                    .map(|n| n.get().max(2))
                    .unwrap_or(2)
                    .min(IO_RUNTIME_THREAD_CAP),
            )
            .thread_name("dgm-io")
            .thread_stack_size(IO_RUNTIME_THREAD_STACK_SIZE)
            .build()
            .map_err(|err| format!("failed to initialize io runtime: {}", err))
    });
    result
        .as_ref()
        .map_err(|err| DgmError::runtime(err.clone()))
}

pub fn block_on<F, T>(future: F) -> Result<T, DgmError>
where
    F: Future<Output = Result<T, DgmError>>,
{
    runtime()?.block_on(future)
}
