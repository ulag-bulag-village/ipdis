use ipis::env::infer;

pub struct IpsisClientConfig {
    pub enable_get_next_hop: bool,
}

impl Default for IpsisClientConfig {
    fn default() -> Self {
        Self {
            enable_get_next_hop: infer("ipsis_enable_get_next_hop").unwrap_or(true),
        }
    }
}
