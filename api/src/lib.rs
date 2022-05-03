#[cfg(not(target_os = "wasi"))]
pub use ipdis_api_native::*;

#[cfg(target_os = "wasi")]
pub use ipdis_api_wasi::*;
