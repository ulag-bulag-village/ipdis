use ipis::{env::Infer, tokio};
use ipsis_api::server::IpsisServer;

#[tokio::main]
async fn main() {
    IpsisServer::infer().run().await
}
