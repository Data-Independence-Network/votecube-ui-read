#[macro_use]
extern crate cdrs;
extern crate lru;

use std::sync::Arc;
use std::sync::Mutex;

use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, middleware, Result, web};
use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::ByName;
use cdrs::types::prelude::*;
use lru::LruCache;
use radix_fmt::*;

type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>;

type GetOpinionPreparedQuery = PreparedQuery;
type GetThreadPreparedQuery = PreparedQuery;

struct Queries {
    poll_opinion_by_ids: Arc<GetOpinionPreparedQuery>,
    poll_thread_by_ids: Arc<GetThreadPreparedQuery>,
}

async fn get_opinion(
    _: HttpRequest,
    path: web::Path<(u64, String, u64, )>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache<String, Vec<u8>>>>>,
) -> Result<HttpResponse, Error> {
    let cache_key = format!("O{}{}{}", format!("{}", radix_36(path.0)), path.1, format!("{}", radix_36(path.2)));

    if !cache.lock().unwrap().get(&cache_key).is_none() {
        let bytes = (*cache.lock().unwrap().get(&cache_key).unwrap()).clone();

        return Ok(HttpResponse::Ok().body(bytes));
    }

    let values_with_names = query_values! {
    "poll_id" => path.0,
    "date" => path.1.clone(),
    "opinion_id" => path.2
    };

    let rows = session.exec_with_values(
        &queries.poll_opinion_by_ids,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    if rows.len() == 1 {
        let opinion_row: &Row = &rows[0];
        let blob: Blob = opinion_row.by_name("data").expect("data value").unwrap();
        let bytes = blob.into_vec();
        cache.lock().unwrap().put(cache_key, bytes.clone());

        Ok(HttpResponse::Ok()
//            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok().finish())
    }
}

async fn get_thread(
    _: HttpRequest,
    path: web::Path<(u64, String, u64, )>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache<String, Vec<u8>>>>>,
) -> Result<HttpResponse, Error> {
    let cache_key = format!("T{}{}{}", format!("{}", radix_36(path.0)), path.1, format!("{}", radix_36(path.2)));

    if !cache.lock().unwrap().get(&cache_key).is_none() {
        let bytes = (*cache.lock().unwrap().get(&cache_key).unwrap()).clone();

        return Ok(HttpResponse::Ok().body(bytes));
    }

    let values_with_names = query_values! {
    "poll_id" => path.0,
    "date" => path.1.clone(),
    "thread_id" => path.2
    };

    let rows = session.exec_with_values(
        &queries.poll_thread_by_ids,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    if rows.len() == 1 {
        let thread_row: &Row = &rows[0];
        let blob: Blob = thread_row.by_name("data").expect("data value").unwrap();
        let bytes = blob.into_vec();
        cache.lock().unwrap().put(cache_key, bytes.clone());

        Ok(HttpResponse::Ok()
//            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok().finish())
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let user = "cassandra";
    let password = "cassandra";
    let auth = StaticPasswordAuthenticator::new(&user, &password);

    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth).build();
    let cluster_config = ClusterTcpConfig(vec![node]);

    let session = Arc::new(
        new_session(&cluster_config, RoundRobinSync::new()).expect("session should be created")
    );

    session.query("USE votecube;").expect("USE Keyspace error");

    let poll_opinion_by_ids: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from opinions WHERE poll_id = ? AND date = ? AND opinion_id = ?", true, true).unwrap()
    );

    let poll_thread_by_ids: Arc<GetThreadPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from threads WHERE poll_id = ? AND date = ? AND thread_id = ?", true, true).unwrap()
    );

    let cache: Arc<Mutex<LruCache<String, Vec<u8>>>> = Arc::new(Mutex::new(LruCache::new(2)));

    std::env::set_var("RUST_LOG", "actix_web=warn");
    env_logger::init();

    let queries = Arc::new(Queries {
        poll_opinion_by_ids: poll_opinion_by_ids.clone(),
        poll_thread_by_ids: poll_thread_by_ids.clone(),
    });


    HttpServer::new(move || {
        App::new()
            .data(session.clone())
            .data(queries.clone())
            .data(cache.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(|| async { "votecube-ui-read" }))
            .service(web::resource("/get/getOpinion/{pollId}/{date}/{opinionId}").route(web::get().to(get_opinion)))
            .service(web::resource("/get/getThread/{pollId}/{date}/{threadId}").route(web::get().to(get_thread)))
    })
        .bind("127.0.0.1:8443")?
        .run()
        .await
}
