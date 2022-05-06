use ipdis_api::server::IpdisServer;
use ipis::env::Infer;

#[tokio::main]
async fn main() {
    IpdisServer::infer().run().await
}
