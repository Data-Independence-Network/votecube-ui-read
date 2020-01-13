#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
extern crate chrono;
extern crate lru_rs_mem;
extern crate sysinfo;

use std::mem::transmute;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

//use actix_web::middleware;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, Result, web};
use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::IntoBytes;
use cdrs::frame::TryFromRow;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::ByName;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use chrono::{Datelike, DateTime, NaiveDateTime, Utc};
use lru_rs_mem::LruCache;
use serde_derive::Deserialize;
use sysinfo::{System, SystemExt};

type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>;

type GetOpinionPreparedQuery = PreparedQuery;
type GetPollPreparedQuery = PreparedQuery;
type GetThreadPreparedQuery = PreparedQuery;

#[derive(Deserialize)]
struct GetThreadParams {
    poll_id: u64,
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct OpinionIdsStruct {
    opinion_id: i64,
    create_es: i64,
}

struct Queries {
    poll_opinion_by_ids: Arc<GetOpinionPreparedQuery>,
    poll_opinion_ids_for_poll_period: Arc<GetOpinionPreparedQuery>,
    poll_opinion_ids_for_poll_period_since_create_dt: Arc<GetOpinionPreparedQuery>,
    poll_by_id: Arc<GetThreadPreparedQuery>,
    poll_thread_by_id: Arc<GetThreadPreparedQuery>,
}

static THREAD_ID_MASK: u64 = 1 << 63;
static POLL_ID_MASK: u64 = 1 << 62;
static OPINION_ID_MASK: u64 = 0;

static EARLIEST_GET_OPINION_IDS_PERIOD: u64 = 1;

static mut CURRENT_DATE: String = String::new();
static mut PREVIOUS_DATE: String = String::new();

static mut NUM_GET_OPINION_IDS_REQUESTS: u32 = 0;
static mut NUM_GET_OPINION_REQUESTS: u32 = 0;
static mut NUM_GET_POLL_REQUESTS: u32 = 0;
static mut NUM_GET_THREAD_REQUESTS: u32 = 0;

static mut YESTERDAY_DAY: u32 = 0;

// FIXME: update this value once number of opinions reaches 4B
static OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 9;

async fn get_opinion_ids(
    _: HttpRequest,
    path: web::Path<(u64, u64, u64, )>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
) -> Result<HttpResponse, Error> {
    // path.0 - poll_id
    // path.1 - period (0 || 1) 0 is the current day, 1 is the day before
    // path.2 - since epoch second (if more recent records are found)
//    println!("poll_id: {}, period: {}, create_es: {}", path.0, path.1, path.2);

    unsafe {
        NUM_GET_OPINION_IDS_REQUESTS += 1;
    }

    if path.1 > EARLIEST_GET_OPINION_IDS_PERIOD {
        return Ok(HttpResponse::BadRequest()
            .header("Cache-Control", "public, max-age=86400")
            .finish());
    }

    let query_date;
    unsafe {
        if path.1 == 0 {
            query_date = CURRENT_DATE.clone();
        } else {
            query_date = PREVIOUS_DATE.clone();
        }
    }

    let values_with_names: QueryValues;
    let query: &Arc<GetOpinionPreparedQuery>;
    if path.2 == 0 {
        values_with_names = query_values! {
    "poll_id" => path.0,
    "date" => query_date
    };
        query = &queries.poll_opinion_ids_for_poll_period;
    } else {
        values_with_names = query_values! {
    "poll_id" => path.0,
    "date" => query_date,
    "create_es" => path.2
    };
        query = &queries.poll_opinion_ids_for_poll_period_since_create_dt;
    }

    let rows = session.exec_with_values(
        query,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    let mut response = Vec::with_capacity(rows.len() * OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);

    for row in rows {
        let opinion_ids_row: OpinionIdsStruct = OpinionIdsStruct::try_from_row(row)
            .expect("into OpinionIdsStruct");
        response = encode_opinion_ids(&opinion_ids_row, response);
//        println!("struct got: {:?}", opinion_ids_row);
    }

    return Ok(HttpResponse::Ok()
        .header("Cache-Control", "public, max-age=60")
        .body(response));
}

fn encode_opinion_ids(
    opinion_ids_row: &OpinionIdsStruct,
    mut response: Vec<u8>,
) -> Vec<u8> {
    let mut byte_mask: u8;

    let opinion_id_significant_bytes;
    let opinion_id = opinion_ids_row.opinion_id as u64;
    let opinion_id_bytes: [u8; 8] = unsafe {
        // Poll Id in the period of a given time zone
        transmute(opinion_id)
    };
    if opinion_id < 256 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..1];
        byte_mask = 0;
    } else if opinion_id < 65536 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..2];
        byte_mask = 1;
    } else if opinion_id < 16777216 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..3];
        byte_mask = 2;
    } else if opinion_id < 4294967296 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..4];
        byte_mask = 3;
    } else if opinion_id < 1099511627776 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..5];
        byte_mask = 4;
    } else if opinion_id < 281474976710656 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..6];
        byte_mask = 5;
    } else if opinion_id < 72057594037927936 {
        opinion_id_significant_bytes = &opinion_id_bytes[0..7];
        byte_mask = 6;
    } else {
        opinion_id_significant_bytes = &opinion_id_bytes;
        byte_mask = 7;
    }

    let create_es_significant_bytes;
    let create_es = opinion_ids_row.create_es as u64;
    let create_es_bytes: [u8; 8] = unsafe {
        // Poll Id in the period of a given time zone
        transmute(create_es)
    };
    if create_es < 4294967296 {
        create_es_significant_bytes = &create_es_bytes[0..4];
    } else {
        create_es_significant_bytes = &create_es_bytes[0..5];
        byte_mask += 8;
    }

//    println!("opinion_id: {}", opinion_id_significant_bytes[0]);
//    println!("create_es: {}", u32::from_ne_bytes(clone_into_array(create_es_significant_bytes)));

    response.extend_from_slice(&[byte_mask]);
    response.extend_from_slice(opinion_id_significant_bytes);
    response.extend_from_slice(create_es_significant_bytes);

    return response;
}

//use std::convert::AsMut;
//
//fn clone_into_array<A, T>(slice: &[T]) -> A
//    where
//        A: Default + AsMut<[T]>,
//        T: Clone,
//{
//    let mut a = A::default();
//    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
//    a
//}

async fn get_opinion(
    _: HttpRequest,
    path: web::Path<(u64, String, u64, u64)>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_OPINION_REQUESTS += 1;
    }

    let cache_key = OPINION_ID_MASK + path.2;

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
            println!("Found Opinion in Cache");
            return Ok(HttpResponse::Ok()
                .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//                .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
                .header("Content-Encoding", "gzip")
                .body(cache_entry.clone()));
        }
        None => {
            // Nothing to do
        }
    }

    let values_with_names = query_values! {
    "poll_id" => path.0,
    "date" => path.1.clone(),
    "create_es" => path.2,
    "opinion_id" => path.3
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
            .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//            .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok()
            .header("Cache-Control", "public, max-age=86400")
            .finish())
    }
}

async fn get_poll(
    _: HttpRequest,
    path: web::Path<GetThreadParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_POLL_REQUESTS += 1;
    }

    let cache_key = POLL_ID_MASK + path.poll_id;

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
            println!("Found Poll in Cache");
            return Ok(HttpResponse::Ok()
                .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//                .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
                .header("Content-Encoding", "gzip")
                .body(cache_entry.clone()));
        }
        None => {
            // Nothing to do
        }
    }

    let values_with_names = query_values! {
    "poll_id" => path.poll_id
    };

    let rows = session.exec_with_values(
        &queries.poll_by_id,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    if rows.len() == 1 {
        let poll_row: &Row = &rows[0];
        let blob: Blob = poll_row.by_name("data").expect("data value").unwrap();
        let bytes = blob.into_vec();
        cache.lock().unwrap().put(cache_key, bytes.clone());

        Ok(HttpResponse::Ok()
            // .header("Cache-Control", "public")
            .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//            .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok()
            .header("Cache-Control", "public, max-age=86400")
            .finish())
    }
}

async fn get_thread(
    _: HttpRequest,
    path: web::Path<GetThreadParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_THREAD_REQUESTS += 1;
    }

    let cache_key = THREAD_ID_MASK + path.poll_id;

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
            println!("Found Thread in Cache");
            return Ok(HttpResponse::Ok()
                .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//                .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
                .header("Content-Encoding", "gzip")
                .body(cache_entry.clone()));
        }
        None => {
            // Nothing to do
        }
    }

    let values_with_names = query_values! {
    "poll_id" => path.poll_id
    };

    let rows = session.exec_with_values(
        &queries.poll_thread_by_id,
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
            // .header("Cache-Control", "public")
            .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
//            .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok()
            .header("Cache-Control", "public, max-age=86400")
            .finish())
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

    let poll_opinion_ids_for_poll_period: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT opinion_id, create_es from opinions WHERE poll_id = ? AND date = ?", false, false).unwrap()
    );

    let poll_opinion_ids_for_poll_period_since_create_dt: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT opinion_id, create_es from opinions WHERE poll_id = ? AND date = ? AND create_es >= ?", false, false).unwrap()
    );

    let poll_opinion_by_ids: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from opinions WHERE poll_id = ? AND date = ? AND create_es = ? AND opinion_id = ?", false, false).unwrap()
    );

    let poll_by_id: Arc<GetPollPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from polls WHERE poll_id = ?", false, false).unwrap()
    );

    let poll_thread_by_id: Arc<GetThreadPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from threads WHERE poll_id = ?", false, false).unwrap()
    );

    let sys = System::new();
    let free_ram_kb = sys.get_free_memory();
    let free_ram = (free_ram_kb * 1024) as usize;

//    println!("Free RAM KB B4 Cache: {}", free_ram_kb);

    let cache: Arc<Mutex<LruCache>> = Arc::new(Mutex::new(LruCache::new(free_ram, 100000000, 1000)));

    std::env::set_var("RUST_LOG", "actix_web=warn");
    env_logger::init();

    let queries = Arc::new(Queries {
        poll_opinion_by_ids: poll_opinion_by_ids.clone(),
        poll_opinion_ids_for_poll_period: poll_opinion_ids_for_poll_period.clone(),
        poll_opinion_ids_for_poll_period_since_create_dt: poll_opinion_ids_for_poll_period_since_create_dt.clone(),
        poll_by_id: poll_by_id.clone(),
        poll_thread_by_id: poll_thread_by_id.clone(),
    });

    let check_size_cache_ref = cache.clone();

    let seconds_since_epoch = SystemTime::now().duration_since(UNIX_EPOCH)
        .expect("Time went backwards").as_secs();
//    println!("seconds_since_epoch: {}", seconds_since_epoch);

    let naive_yesterday_same_time = NaiveDateTime::from_timestamp(
        seconds_since_epoch as i64 - 24 * 60 * 60, 0);
    let utc_yesterday: DateTime<Utc> = DateTime::from_utc(naive_yesterday_same_time, Utc);
    unsafe {
        YESTERDAY_DAY = utc_yesterday.day();
        CURRENT_DATE = format!(
            "{}-{:02}-{:02}",
            utc_yesterday.year(),
            utc_yesterday.month(),
            YESTERDAY_DAY,
        );
//        println!("Initialize CURRENT_DATE: {}", CURRENT_DATE);
    }

    thread::spawn(move || {
        let mut sys = System::new();
        loop {
            unsafe {
                let utc_now = Utc::now();
                let now_day = utc_now.day();
                if now_day != YESTERDAY_DAY {
                    PREVIOUS_DATE = CURRENT_DATE.clone();
                    CURRENT_DATE = format!(
                        "{}-{:02}-{:02}",
                        utc_now.year(),
                        utc_now.month(),
                        now_day,
                    );
//                    println!("Update PREVIOUS_DATE: {}", PREVIOUS_DATE);
//                    println!("Update CURRENT_DATE:  {}", CURRENT_DATE);
                    YESTERDAY_DAY = now_day;
                }

//                println!("# Requests");
                println!("get/opinionIds {}", NUM_GET_OPINION_IDS_REQUESTS);
                println!("get/opinion    {}", NUM_GET_OPINION_REQUESTS);
                println!("get/poll       {}", NUM_GET_POLL_REQUESTS);
                println!("get/thread     {}", NUM_GET_THREAD_REQUESTS);

                NUM_GET_OPINION_IDS_REQUESTS = 0;
                NUM_GET_OPINION_REQUESTS = 0;
                NUM_GET_POLL_REQUESTS = 0;
                NUM_GET_THREAD_REQUESTS = 0;
            }

            thread::sleep(Duration::from_secs(10));
            sys.refresh_memory();

            // TODO: change if needed
            // This read process is assumed to be the only process on the vm
            let free_ram_kb = sys.get_free_memory();

//            println!("Free RAM {}KB", free_ram_kb);

            if free_ram_kb < 10240 {
                println!("Free RAM below 10240KB - {}KB, shortening cache by 100", free_ram_kb);
                check_size_cache_ref.lock().unwrap().shorten_by(100);
            }
        }
    });

    HttpServer::new(move || {
        App::new()
            .data(session.clone())
            .data(queries.clone())
            .data(cache.clone())
            // enable logger
//            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(|| async { "votecube-ui-read" }))
            .service(web::resource("/get/opinionIds/{poll_id}/{date}/{create_es}").route(web::get().to(get_opinion_ids)))
            .service(web::resource("/get/opinion/{poll_id}/{date}/{create_es}/{opinion_id}").route(web::get().to(get_opinion)))
            .service(web::resource("/get/thread/{poll_id}").route(web::get().to(get_thread)))
            .service(web::resource("/get/poll/{poll_id}").route(web::get().to(get_poll)))
    })
        .bind("127.0.0.1:8444")?
        .run()
        .await
}
