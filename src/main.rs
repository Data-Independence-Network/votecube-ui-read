#[macro_use]
extern crate cdrs;
extern crate cdrs_helpers_derive;
extern crate chrono;
extern crate job_scheduler;
extern crate lru_rs_mem;
extern crate sysinfo;

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
//use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

//use actix_web::middleware;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, Result, web};
use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::TryFromRow;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::ByName;
use cdrs::types::prelude::*;
use chrono::{Datelike, Timelike, Utc};
use job_scheduler::{Job, JobScheduler};
use lru_rs_mem::LruCache;
use serde_derive::Deserialize;
use sysinfo::{System, SystemExt};
use votecube_rust_lib::{encode_opinion_ids, encode_poll_ids, OpinionIdsStruct, PollIdStruct};

type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>;

type GetOpinionPreparedQuery = PreparedQuery;
type GetPollPreparedQuery = PreparedQuery;
type GetThreadPreparedQuery = PreparedQuery;

#[derive(Deserialize)]
struct GetThreadParams {
    poll_id: u64,
}

struct Queries {
    poll_opinion_by_ids: Arc<GetOpinionPreparedQuery>,
    poll_opinion_ids_for_poll_period: Arc<GetOpinionPreparedQuery>,
    poll_opinion_ids_for_poll_period_since_create_dt: Arc<GetOpinionPreparedQuery>,
    poll_by_id: Arc<GetPollPreparedQuery>,
    poll_thread_by_id: Arc<GetThreadPreparedQuery>,
    recent_poll_ids_globally: Arc<GetPollPreparedQuery>,
}

static THREAD_ID_MASK: u64 = 1 << 63;
static POLL_ID_MASK: u64 = 1 << 62;
static OPINION_ID_MASK: u64 = 0;

static EARLIEST_LIST_OPINIONS_PERIOD: u64 = 1;

static mut DATE_HOURS: [String; 7] = [
    String::new(),
    String::new(),
    String::new(),
    String::new(),
    String::new(),
    String::new(),
    String::new(),
];

static mut NUM_LIST_OPINIONS_REQUESTS: u32 = 0;
static mut NUM_LIST_RECENT_POLLS_REQUESTS: u32 = 0;
static mut NUM_GET_OPINION_REQUESTS: u32 = 0;
static mut NUM_GET_POLL_REQUESTS: u32 = 0;
static mut NUM_GET_THREAD_REQUESTS: u32 = 0;

static OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 12;
static POLL_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 11;

async fn list_opinions(
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
        NUM_LIST_OPINIONS_REQUESTS += 1;
    }

    if path.1 > EARLIEST_LIST_OPINIONS_PERIOD {
        return Ok(HttpResponse::BadRequest()
            .header("Cache-Control", "public, max-age=86400")
            .finish());
    }

    let query_date;
    unsafe {
        query_date = DATE_HOURS[path.1 as usize].clone();
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
//        .header("Transfer-Encoding", "identity")
        .body(response));
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
    path: web::Path<(u64, String, u64, u64, u16)>,
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
    // Note version is not used in the query, its only there for caching purposes
    // if a version of an opinion updates then it will be a different URL
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
//            println!("Found Poll in Cache");
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

async fn list_recent_polls(
    _: HttpRequest,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
) -> Result<HttpResponse, Error> {
    // path.0 - poll_id
    // path.1 - period (0 || 1) 0 is the current day, 1 is the day before
    // path.2 - since epoch second (if more recent records are found)
//    println!("poll_id: {}, period: {}, create_es: {}", path.0, path.1, path.2);

    unsafe {
        NUM_LIST_RECENT_POLLS_REQUESTS += 1;
    }

    let query_date_hour_0;
    let query_date_hour_1;
    unsafe {
        query_date_hour_0 = DATE_HOURS[0].clone();
        query_date_hour_1 = DATE_HOURS[1].clone();
    }

    let query = &queries.recent_poll_ids_globally;
    let mut values_with_names = query_values! {
    "date" => query_date_hour_0
    };

    let mut rows = session.exec_with_values(
        query,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    if rows.len() < 200 {
        println!("Querying for yesterdays most recent poll ids");
        values_with_names = query_values! {
        "date" => query_date_hour_1
        };
        let mut rows1 = session.exec_with_values(
            query,
            values_with_names)
            .expect("query_with_values")
            .get_body()
            .expect("get body")
            .into_rows()
            .expect("into rows");
        rows.append(&mut rows1);
    }

    if rows.len() < 100 {
        println!("Querying CRDB for last 200 poll ids");
        // Query CockroachDB for last 200 poll ids
    }

    let mut poll_ids: Vec<u64> = Vec::with_capacity(rows.len());
    let mut response = Vec::with_capacity(rows.len() * POLL_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);

    for row in rows {
        let poll_id_row: PollIdStruct = PollIdStruct::try_from_row(row)
            .expect("into PollIdStruct");
        poll_ids.push(poll_id_row.poll_id as u64);
    }
    response = encode_poll_ids(&poll_ids, response);

    return Ok(HttpResponse::Ok()
        .header("Cache-Control", "public, max-age=60")
        .body(response));
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
        let response;
        let blob_option: Option<Blob> = thread_row.by_name("data").expect("data value");
        match blob_option {
            Some(data) => {
                let bytes = data.into_vec();
                cache.lock().unwrap().put(cache_key, bytes.clone());
                response = HttpResponse::Ok()
                    .header("Cache-Control", "public, max-age=86400") // By default cache for 1 day
                    .header("Content-Encoding", "gzip")
                    .body(bytes);
            }
            None => {
                response = HttpResponse::Ok()
                    .header("Cache-Control", "public, max-age=86400")
                    .finish();
            }
        }
        Ok(response)
    } else {
        Ok(HttpResponse::Ok()
            .header("Cache-Control", "public, max-age=86400")
            .finish())
    }
}

fn reset_date_hours() {
    unsafe {
        let mut date = Utc::now();
        for i in 0..7 {
            DATE_HOURS[i] = format!(
                "{}{:02}{:02}{:02}",
                date.year(),
                date.month(),
                date.day(),
                date.hour(),
            );
//            println!("DATE[{}]: {}", i, DATES[i]);
            date = date - chrono::Duration::hours(1);
        }
    }
}

fn check_mem_print_stats(
    lru_cache: &Arc<Mutex<LruCache>>,
    system: &mut System,
) {
    unsafe {
        if NUM_GET_OPINION_REQUESTS > 0 {
            println!("get/opinion       {}", NUM_GET_OPINION_REQUESTS);
        }
        if NUM_GET_POLL_REQUESTS > 0 {
            println!("get/poll          {}", NUM_GET_POLL_REQUESTS);
        }
        if NUM_GET_THREAD_REQUESTS > 0 {
            println!("get/thread        {}", NUM_GET_THREAD_REQUESTS);
        }
        if NUM_LIST_OPINIONS_REQUESTS > 0 {
            println!("list/opinions     {}", NUM_LIST_OPINIONS_REQUESTS);
        }
        if NUM_LIST_RECENT_POLLS_REQUESTS > 0 {
            println!("list/polls/recent {}", NUM_LIST_RECENT_POLLS_REQUESTS);
        }

        NUM_GET_OPINION_REQUESTS = 0;
        NUM_GET_POLL_REQUESTS = 0;
        NUM_GET_THREAD_REQUESTS = 0;
        NUM_LIST_OPINIONS_REQUESTS = 0;
        NUM_LIST_RECENT_POLLS_REQUESTS = 0;

        system.refresh_memory();

// TODO: This process is assumed to be the only process on the vm
        let free_ram_kb = system.get_free_memory();

//            println!("Free RAM {}KB", free_ram_kb);

        if free_ram_kb < 10240 {
            println!("Free RAM below 10240KB - {}KB, shortening cache by 100", free_ram_kb);
            lru_cache.lock().unwrap().shorten_by(100);
        }
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
        session.prepare_tw("SELECT opinion_id, create_es, version from opinion_ids WHERE poll_id = ? AND create_hour = ? BYPASS CACHE", false, false).unwrap()
    );

    let poll_opinion_ids_for_poll_period_since_create_dt: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT opinion_id, create_es, version from opinion_ids WHERE poll_id = ? AND create_hour = ? AND create_es >= ? BYPASS CACHE", false, false).unwrap()
    );

    let poll_opinion_by_ids: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from opinions WHERE poll_id = ? AND create_hour = ? AND create_es = ? AND opinion_id = ?", false, false).unwrap()
    );

    let recent_poll_ids_globally: Arc<GetPollPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT poll_id from poll_chronology WHERE create_hour = ? ORDER BY create_es DESC LIMIT 1000 BYPASS CACHE", false, false).unwrap()
    );

    let poll_by_id: Arc<GetPollPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from polls WHERE poll_id = ?", false, false).unwrap()
    );

    let poll_thread_by_id: Arc<GetThreadPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from threads WHERE poll_id = ?", false, false).unwrap()
    );

    let free_ram_kb = System::new().get_free_memory();
    let free_ram = (free_ram_kb * 1024) as usize;

//    println!("Free RAM KB B4 Cache: {}", free_ram_kb);

    let lru_cache: Arc<Mutex<LruCache>> = Arc::new(Mutex::new(LruCache::new(free_ram, 100000000, 1000)));
    std::env::set_var("RUST_LOG", "actix_web=warn");
    env_logger::init();

    let lru_cache_ref = lru_cache.clone();

    let queries = Arc::new(Queries {
        poll_opinion_by_ids: poll_opinion_by_ids.clone(),
        poll_opinion_ids_for_poll_period: poll_opinion_ids_for_poll_period.clone(),
        poll_opinion_ids_for_poll_period_since_create_dt: poll_opinion_ids_for_poll_period_since_create_dt.clone(),
        poll_by_id: poll_by_id.clone(),
        poll_thread_by_id: poll_thread_by_id.clone(),
        recent_poll_ids_globally: recent_poll_ids_globally.clone(),
    });

//    let seconds_since_epoch = SystemTime::now().duration_since(UNIX_EPOCH)
//        .expect("Time went backwards").as_secs();
//    println!("seconds_since_epoch: {}", seconds_since_epoch);

//    let naive_yesterday_same_time = NaiveDateTime::from_timestamp(
//        seconds_since_epoch as i64 - 24 * 60 * 60, 0);
//    let utc_yesterday: DateTime<Utc> = DateTime::from_utc(naive_yesterday_same_time, Utc);

    reset_date_hours();
    thread::spawn(move || {
        let mut system = System::new();
        let mut scheduler = JobScheduler::new();
        scheduler.add(Job::new("0 * * * * *".parse().unwrap(), || {
            reset_date_hours();
        }));
        scheduler.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
            check_mem_print_stats(&lru_cache_ref, &mut system);
        }));
        loop {
            scheduler.tick();
            thread::sleep(Duration::from_millis(1000));
        }
    });

    HttpServer::new(move || {
        App::new()
            .data(session.clone())
            .data(queries.clone())
            .data(lru_cache.clone())
// enable logger
//            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(|| async { "votecube-ui-read" }))
            .service(web::resource("/get/opinion/{poll_id}/{date}/{create_es}/{opinion_id}/{version}").route(web::get().to(get_opinion)))
            .service(web::resource("/get/thread/{poll_id}").route(web::get().to(get_thread)))
            .service(web::resource("/get/poll/{poll_id}").route(web::get().to(get_poll)))
            .service(web::resource("/list/opinions/{poll_id}/{period}/{create_es}").route(web::get().to(list_opinions)))
            .service(web::resource("/list/polls/recent").route(web::get().to(list_recent_polls)))
    })
        .bind("127.0.0.1:8444")?
        .run()
        .await
}
