#[macro_use]
extern crate cdrs;
extern crate cdrs_helpers_derive;
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
use job_scheduler::{Job, JobScheduler};
use lru_rs_mem::LruCache;
use serde_derive::Deserialize;
use sysinfo::{System, SystemExt};
use votecube_rust_lib::{encode_opinion_ids, encode_root_opinion_ids,
                        encode_poll_ids, get_partition_periods,
                        OpinionIdsStruct, PollIdStruct, RootOpinionIdsStruct};

type CurrentSession =
Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>;

type GetOpinionPreparedQuery = PreparedQuery;
type GetPollPreparedQuery = PreparedQuery;
type GetRootOpinionPreparedQuery = PreparedQuery;

#[derive(Deserialize)]
struct GetOpinionParams {
    opinion_id: u64,
    version: i16,
}

#[derive(Deserialize)]
struct GetPollParams {
    poll_id: u64,
}

#[derive(Deserialize)]
struct GetRootOpinionParams {
    root_opinion_id: u64,
    version: i32,
}

#[derive(Deserialize)]
struct ListOpinionIdsParams {
    root_opinion_id: u64,
    num_partition_periods_back: u8,
}

#[derive(Deserialize)]
struct ListRootOpinionIdsParams {
    poll_id: u64,
}

struct Queries {
    get_opinion_data_by_id: Arc<GetOpinionPreparedQuery>,
    list_opinion_ids_for_root_opinion_n_partition_period: Arc<GetOpinionPreparedQuery>,
    list_opinion_update_ids_for_root_opinion_n_partition_period: Arc<GetOpinionPreparedQuery>,
    get_poll_data_by_id: Arc<GetPollPreparedQuery>,
    get_root_opinion_data_by_id: Arc<GetRootOpinionPreparedQuery>,
    root_opinion_ids_by_poll_id: Arc<GetRootOpinionPreparedQuery>,
    get_period_poll_id_block_by_theme: Arc<GetPollPreparedQuery>,
}

static ROOT_OPINION_ID_MASK: u128 = 1 << 63;
static POLL_ID_MASK: u128 = 1 << 62;
static OPINION_ID_MASK: u128 = 0;

static EARLIEST_LIST_OPINIONS_PERIOD: u8 = 1;

static mut PARTITION_PERIODS: [u32; 7] = [
    0, 0, 0, 0, 0, 0, 0,
];

static mut NUM_LIST_OPINIONS_REQUESTS: u32 = 0;
static mut NUM_LIST_RECENT_POLLS_REQUESTS: u32 = 0;
static mut NUM_LIST_ROOT_OPINIONS_REQUESTS: u32 = 0;
static mut NUM_GET_OPINION_REQUESTS: u32 = 0;
static mut NUM_GET_POLL_REQUESTS: u32 = 0;
static mut NUM_GET_THREAD_REQUESTS: u32 = 0;

static OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 7;
static ROOT_OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 9;
static POLL_IDS_ROW_RESPONSE_MAX_SIZE_BYTES: usize = 11;

async fn get_opinion(
    _: HttpRequest,
    path: web::Path<GetOpinionParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_OPINION_REQUESTS += 1;
    }

    let cache_key: u128 = ((path.version as u128) << 64)
        + OPINION_ID_MASK + (path.opinion_id as u128);

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
            println!("Found Opinion in Cache");
            return Ok(HttpResponse::Ok()
                // By default cache for 1 day
                .header("Cache-Control", "public, max-age=86400")
//                .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
                .header("Content-Encoding", "gzip")
                .body(cache_entry.clone()));
        }
        None => {
            // Nothing to do
        }
    }

    let values_with_names = query_values! {
    "opinion_id" => path.opinion_id
    // Note version is not used in the query, its only there for caching purposes
    // if a version of an opinion updates then it will be a different URL
    };

    let rows = session.exec_with_values(
        &queries.get_opinion_data_by_id,
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
            // By default cache for 1 day
            .header("Cache-Control", "public, max-age=86400")
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
    path: web::Path<GetPollParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_POLL_REQUESTS += 1;
    }

    let cache_key = POLL_ID_MASK + (path.poll_id as u128);

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
//            println!("Found Poll in Cache");
            return Ok(HttpResponse::Ok()
                // By default cache for 1 day
                .header("Cache-Control", "public, max-age=86400")
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
        &queries.get_poll_data_by_id,
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
            // By default cache for 1 day
            .header("Cache-Control", "public, max-age=86400")
//            .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
            .header("Content-Encoding", "gzip")
            .body(bytes))
    } else {
        Ok(HttpResponse::Ok()
            .header("Cache-Control", "public, max-age=86400")
            .finish())
    }
}

async fn get_root_opinion(
    _: HttpRequest,
    path: web::Path<GetRootOpinionParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
    cache: web::Data<Arc<Mutex<LruCache>>>,
) -> Result<HttpResponse, Error> {
    unsafe {
        NUM_GET_THREAD_REQUESTS += 1;
    }

    let cache_key = ((path.version as u128) << 64)
        + ROOT_OPINION_ID_MASK + (path.root_opinion_id as u128);

    match cache.lock().unwrap().get(&cache_key) {
        Some(cache_entry) => {
            println!("Found Root Opinion in Cache");
            return Ok(HttpResponse::Ok()
                // By default cache for 1 day
                .header("Cache-Control", "public, max-age=86400")
//                .header("Expires", "Wed, 22 Oct 2025 07:28:00 GMT")
                .header("Content-Encoding", "gzip")
                .body(cache_entry.clone()));
        }
        None => {
            // Nothing to do
        }
    }

    let values_with_names = query_values! {
    "opinion_id" => path.root_opinion_id
    };

    let rows = session.exec_with_values(
        &queries.get_root_opinion_data_by_id,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    if rows.len() == 1 {
        let thread_row: &Row = &rows[0];
        let response;
        let blob_option: Option<Blob> = thread_row.by_name("data")
            .expect("data value");
        match blob_option {
            Some(data) => {
                let bytes = data.into_vec();
                cache.lock().unwrap().put(cache_key, bytes.clone());
                response = HttpResponse::Ok()
                    // By default cache for 1 day
                    .header("Cache-Control", "public, max-age=86400")
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

async fn list_opinion_ids(
    _: HttpRequest,
    path: web::Path<ListOpinionIdsParams>,
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

    if path.num_partition_periods_back > EARLIEST_LIST_OPINIONS_PERIOD {
        return Ok(HttpResponse::BadRequest()
            .header("Cache-Control", "public, max-age=86400")
            .finish());
    }

    let partition_period;
    unsafe {
        partition_period = PARTITION_PERIODS[path.num_partition_periods_back as usize];
    }

    let values_with_names = query_values! {
    "root_opinion_id" => path.root_opinion_id,
    "partition_period" => partition_period
    };

    let rows = session.exec_with_values(
        &queries.list_opinion_ids_for_root_opinion_n_partition_period,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    let mut response =
        Vec::with_capacity(rows.len() * OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);


    let mut opinion_ids: Vec<OpinionIdsStruct> = Vec::with_capacity(rows.len());
    for row in rows {
        let opinion_id = OpinionIdsStruct::try_from_row(row)
            .expect("into OpinionIdsStruct");
        opinion_ids.push(opinion_id);
    }
    response = encode_opinion_ids(&opinion_ids, response);

    return Ok(HttpResponse::Ok()
        .header("Cache-Control", "public, max-age=60")
//        .header("Transfer-Encoding", "identity")
        .body(response));
}

async fn list_opinion_updates(
    _: HttpRequest,
    path: web::Path<ListOpinionIdsParams>,
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

    if path.num_partition_periods_back > EARLIEST_LIST_OPINIONS_PERIOD {
        return Ok(HttpResponse::BadRequest()
            .header("Cache-Control", "public, max-age=86400")
            .finish());
    }

    let partition_period;
    unsafe {
        partition_period = PARTITION_PERIODS[path.num_partition_periods_back as usize];
    }

    let values_with_names = query_values! {
    "root_opinion_id" => path.root_opinion_id,
    "partition_period" => partition_period
    };

    let rows = session.exec_with_values(
        &queries.list_opinion_update_ids_for_root_opinion_n_partition_period,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    let mut response =
        Vec::with_capacity(rows.len() * OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);


    let mut opinion_ids: Vec<OpinionIdsStruct> = Vec::with_capacity(rows.len());
    for row in rows {
        let opinion_id = OpinionIdsStruct::try_from_row(row)
            .expect("into OpinionIdsStruct");
        opinion_ids.push(opinion_id);
    }
    response = encode_opinion_ids(&opinion_ids, response);

    return Ok(HttpResponse::Ok()
        .header("Cache-Control", "public, max-age=60")
//        .header("Transfer-Encoding", "identity")
        .body(response));
}


async fn list_root_opinion_ids(
    _: HttpRequest,
    path: web::Path<ListRootOpinionIdsParams>,
    session: web::Data<Arc<CurrentSession>>,
    queries: web::Data<Arc<Queries>>,
) -> Result<HttpResponse, Error> {
    // path.0 - poll_id
    // path.1 - period (0 || 1) 0 is the current day, 1 is the day before
    // path.2 - since epoch second (if more recent records are found)
//    println!("poll_id: {}, period: {}, create_es: {}", path.0, path.1, path.2);

    unsafe {
        NUM_LIST_ROOT_OPINIONS_REQUESTS += 1;
    }

    let values_with_names = query_values! {
    "poll_id" => path.poll_id
    };

    let rows = session.exec_with_values(
        &queries.root_opinion_ids_by_poll_id,
        values_with_names)
        .expect("query_with_values")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    let mut response =
        Vec::with_capacity(rows.len() * ROOT_OPINION_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);


    let mut root_opinion_ids: Vec<RootOpinionIdsStruct> = Vec::with_capacity(rows.len());
    for row in rows {
        let root_opinion_id = RootOpinionIdsStruct::try_from_row(row)
            .expect("into RootOpinionIdsStruct");
        root_opinion_ids.push(root_opinion_id);
    }
    response = encode_root_opinion_ids(&root_opinion_ids, response);

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
        query_date_hour_0 = PARTITION_PERIODS[0];
        query_date_hour_1 = PARTITION_PERIODS[1];
    }

    let query = &queries.get_period_poll_id_block_by_theme;
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
    let mut response = Vec::with_capacity(rows.len()
        * POLL_IDS_ROW_RESPONSE_MAX_SIZE_BYTES);

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

fn reset_partition_period(
    period_increment_minutes: i64,
) {
    unsafe {
        PARTITION_PERIODS = get_partition_periods(period_increment_minutes);
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
        if NUM_LIST_OPINIONS_REQUESTS > 0 {
            println!("list/rootOpinions {}", NUM_LIST_ROOT_OPINIONS_REQUESTS);
        }
        if NUM_LIST_RECENT_POLLS_REQUESTS > 0 {
            println!("list/polls/recent {}", NUM_LIST_RECENT_POLLS_REQUESTS);
        }

        NUM_GET_OPINION_REQUESTS = 0;
        NUM_GET_POLL_REQUESTS = 0;
        NUM_GET_THREAD_REQUESTS = 0;
        NUM_LIST_OPINIONS_REQUESTS = 0;
        NUM_LIST_ROOT_OPINIONS_REQUESTS = 0;
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
    let auth = StaticPasswordAuthenticator::new(
        &user, &password);

    let node =
        NodeTcpConfigBuilder::new("127.0.0.1:9042", auth).build();
    let cluster_config = ClusterTcpConfig(vec![node]);

    let session = Arc::new(
        new_session(&cluster_config,
                    RoundRobinSync::new()).expect("session should be created")
    );

    session.query("USE votecube;").expect("USE Keyspace error");

    let get_opinion_data_by_id: Arc<GetOpinionPreparedQuery> = Arc::new(
        session.prepare_tw(
            "SELECT data from opinions WHERE opinion_id = ?",
            false, false).unwrap()
    );

    let get_period_poll_id_block_by_theme: Arc<GetPollPreparedQuery> = Arc::new(
        session.prepare_tw(
            format!("{}{}","SELECT poll_ids from period_poll_id_blocks_by_theme ",
                "WHERE partition_period = ? AND age_suitability = ? AND theme_id = ?"),
            false, false).unwrap()
    );

    let get_poll_data_by_id: Arc<GetPollPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from polls WHERE poll_id = ?",
                           false, false).unwrap()
    );

    let get_root_opinion_data_by_id: Arc<GetRootOpinionPreparedQuery> = Arc::new(
        session.prepare_tw("SELECT data from root_opinions WHERE opinion_id = ?",
                           false, false).unwrap()
    );

    let list_opinion_ids_for_root_opinion_n_partition_period: Arc<GetOpinionPreparedQuery>
        = Arc::new(
        session.prepare_tw(
            format!("{}{}","SELECT opinion_id, version from period_opinion_ids ",
                "WHERE root_opinion_id = ? AND partition_period = ?"),
            false, false).unwrap()
    );

    let list_opinion_update_ids_for_root_opinion_n_partition_period: Arc<GetOpinionPreparedQuery>
        = Arc::new(
        session.prepare_tw(
            format!("{}{}","SELECT opinion_id, version from opinion_updates ",
                "WHERE root_opinion_id = ? AND partition_period = ?"),
            false, false).unwrap()
    );
    /*
        let list_recent_poll_ids_by_theme: Arc<GetPollPreparedQuery> = Arc::new(
            session.prepare_tw(
            "SELECT poll_id from period_poll_ids_by_theme " +
            "WHERE partition_period = ? AND theme_id = ?",
            false, false).unwrap()
        );
    */

    let root_opinion_ids_by_poll_id: Arc<GetRootOpinionPreparedQuery> = Arc::new(
        session.prepare_tw(
            "SELECT opinion_id, version from root_opinion_ids WHERE poll_id = ?",
            false, false).unwrap()
    );

    let free_ram_kb = System::new().get_free_memory();
    let free_ram = (free_ram_kb * 1024) as usize;

//    println!("Free RAM KB B4 Cache: {}", free_ram_kb);

    let lru_cache: Arc<Mutex<LruCache>> = Arc::new(Mutex::new(
        LruCache::new(free_ram, 100000000, 1000)));
    std::env::set_var("RUST_LOG", "actix_web=warn");
    env_logger::init();

    let lru_cache_ref = lru_cache.clone();

    let queries = Arc::new(Queries {
        get_opinion_data_by_id: get_opinion_data_by_id.clone(),
        get_period_poll_id_block_by_theme: get_period_poll_id_block_by_theme.clone(),
        get_poll_data_by_id: get_poll_data_by_id.clone(),
        get_root_opinion_data_by_id: get_root_opinion_data_by_id.clone(),
        list_opinion_ids_for_root_opinion_n_partition_period:
        list_opinion_ids_for_root_opinion_n_partition_period.clone(),
        list_opinion_update_ids_for_root_opinion_n_partition_period:
        list_opinion_update_ids_for_root_opinion_n_partition_period.clone(),
        root_opinion_ids_by_poll_id: root_opinion_ids_by_poll_id.clone(),
    });

//    let seconds_since_epoch = SystemTime::now().duration_since(UNIX_EPOCH)
//        .expect("Time went backwards").as_secs();
//    println!("seconds_since_epoch: {}", seconds_since_epoch);

//    let naive_yesterday_same_time = NaiveDateTime::from_timestamp(
//        seconds_since_epoch as i64 - 24 * 60 * 60, 0);
//    let utc_yesterday: DateTime<Utc> = DateTime::from_utc(naive_yesterday_same_time, Utc);

//    let partition_period_minutes = 15 as i64;
//    let partition_period_minutes = 10 as i64;
    let partition_period_minutes = 5 as i64;

    reset_partition_period(partition_period_minutes);
    thread::spawn(move || {
        let mut system = System::new();
        let mut scheduler = JobScheduler::new();
//        let cron_schedule = "0,15,30,45 * * * *";
//        let cron_schedule = "0,10,20,30,40,50 * * * *";
        let cron_schedule = "0 0,5,10,15,20,25,30,35,40,45,50,55 * * * *";
        scheduler.add(Job::new(cron_schedule.parse().unwrap(), || {
            reset_partition_period(partition_period_minutes);
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
            .service(web::resource("/get/opinion/{opinion_id}/{version}")
                .route(web::get().to(get_opinion)))
            .service(web::resource("/get/poll/{poll_id}")
                .route(web::get().to(get_poll)))
            .service(web::resource("/get/rootOpinion/{root_opinion_id}/{version}")
                .route(web::get().to(get_root_opinion)))
            .service(web::resource(
                "/list/opinions/{root_opinion_id}/{num_partition_periods_back}")
                .route(web::get().to(list_opinion_ids)))
            .service(web::resource(
                "/list/opinionUpdates/{root_opinion_id}/{num_partition_periods_back}")
                .route(web::get().to(list_opinion_updates)))
            .service(web::resource("/list/polls/recent")
                .route(web::get().to(list_recent_polls)))
            .service(web::resource("/list/rootOpinions/{poll_id}")
                .route(web::get().to(list_root_opinion_ids)))
    })
        .bind("127.0.0.1:8444")?
        .run()
        .await
}
