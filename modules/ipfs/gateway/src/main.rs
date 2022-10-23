use std::net::SocketAddr;

use actix_web::{
    get,
    http::header::{self, CacheDirective, ETag, EntityTag},
    web, App, HttpResponse, HttpServer, Responder,
};
use actix_web_lab::body;
use ipis::{
    core::value::{chrono::DateTime, hash::Hash},
    env::{infer, Infer},
    logger,
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_api::{client::IpsisClient, common::Ipsis};

#[get("/ipfs/{path}/{size}")]
async fn get_ipfs(
    client: web::Data<IpsisClient>,
    path: web::Path<(String, u64)>,
) -> impl Responder {
    // parse route
    let (hash_raw, len) = path.into_inner();

    // parse path as IPFS-CID/IPI-HASH
    let hash: Hash = match hash_raw.parse() {
        Ok(hash) => hash,
        Err(e) => return HttpResponse::BadRequest().body(format!("{e}: {:?}", hash_raw.as_str())),
    };
    let path = Path { value: hash, len };

    // start downloading the data
    let mut data = match client.get_raw(&path).await {
        Ok(data) => data,
        Err(e) => {
            return HttpResponse::NotFound()
                .body(format!("{e}: {:?} as sized {len}", hash_raw.as_str()))
        }
    };

    // drop the size header
    match data.read_u64().await {
        Ok(_) => {}
        Err(_) => {
            return HttpResponse::InternalServerError()
                .body("Failed to connect to the IPSIS internal storage")
        }
    }

    // convert the datainto stream
    let (mut tx, rx) = body::writer();
    tokio::spawn(async move { tokio::io::copy(&mut data, &mut tx).await });

    HttpResponse::Ok()
        .insert_header((header::ACCEPT_RANGES, "bytes"))
        .append_header((
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            header::CONTENT_TYPE.as_str(),
        ))
        .append_header((header::ACCESS_CONTROL_ALLOW_HEADERS, header::RANGE.as_str()))
        .append_header((
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            header::USER_AGENT.as_str(),
        ))
        .append_header((header::ACCESS_CONTROL_ALLOW_HEADERS, "X-Requested-With"))
        .append_header((header::ACCESS_CONTROL_ALLOW_METHODS, "GET"))
        .append_header((header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"))
        .append_header((
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            header::CONTENT_LENGTH.as_str(),
        ))
        .append_header((
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            header::CONTENT_RANGE.as_str(),
        ))
        .append_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, "X-Chunked-Output"))
        .append_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, "X-Ipfs-Path"))
        .append_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, "X-Ipfs-Roots"))
        .append_header((header::ACCESS_CONTROL_EXPOSE_HEADERS, "X-Stream-Output"))
        .insert_header(header::CacheControl(vec![
            CacheDirective::Public,
            CacheDirective::MaxAge(29_030_400),
            CacheDirective::Extension("immutable".to_owned(), None),
        ]))
        .insert_header(ETag(EntityTag::new_strong(hash_raw.to_owned())))
        .insert_header(("X-Ipfs-Path", format!("/ipfs/{}", hash_raw.as_str())))
        .insert_header(("X-Ipfs-Roots", hash_raw.as_str()))
        .insert_header((header::DATE, DateTime::now().to_rfc2822()))
        .body(rx)
}

#[actix_web::main]
async fn main() {
    async fn try_main() -> ::ipis::core::anyhow::Result<()> {
        // Initialize config
        let addr =
            infer::<_, SocketAddr>("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:80".parse().unwrap());

        // Initialize client
        let client = web::Data::new(IpsisClient::try_infer().await?);

        // Start web server
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::clone(&client))
                .service(get_ipfs)
        })
        .bind(addr)
        .unwrap_or_else(|e| panic!("failed to bind to {addr}: {e}"))
        .run()
        .await
        .map_err(Into::into)
    }

    logger::init_once();
    try_main().await.expect("running a server")
}
