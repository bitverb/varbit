/// api mod
pub mod flash;
pub mod handler;
pub mod kafka;

use axum::{
    http::{HeaderValue, Method},
    routing::{get, post, put},
    Router,
};

use ::chrono::Local;
use axum::BoxError;
use flash::Whortleberry;
use schema::init_database;

use log::{error, info};

use std::{collections::HashMap, net::TcpListener};

use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
};

use crate::handler::{
    cancel_task, connect_testing, create_task, fetch_count, fetch_task_list, start_tasking,
    task_debug, task_debug_preview, update_task, AppState,
};

pub async fn start(app_conf: conf::app::AppConfig) -> anyhow::Result<()> {
    let state: AppState = AppState {
        conn: init_database(app_conf.data.db.clone()).await,
    };

    let cors: CorsLayer = CorsLayer::new()
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::OPTIONS,
        ])
        .allow_origin(AllowOrigin::list([
            "http://localhost:5173/*".parse::<HeaderValue>().unwrap(),
            "http://localhost:8080/*".parse::<HeaderValue>().unwrap(),
            "http://127.0.0.1:5173/*".parse::<HeaderValue>().unwrap(),
            "http://127.0.0.1:8080".parse::<HeaderValue>().unwrap(),
            "http://0.0.0.0:5173/*".parse::<HeaderValue>().unwrap(),
        ]));
    let limit: RequestBodyLimitLayer = RequestBodyLimitLayer::new(1024 * 1024 * 10);

    // let cors = CorsLayer::new()
    // .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
    // .allow_origin(Any)
    // .allow_credentials(false);

    let app = Router::new()
        .layer(cors.clone())
        .layer(limit)
        .route("/task/cancel", post(cancel_task))
        .route("/task/new", post(create_task))
        .route("/connect_testing", post(connect_testing))
        .route("/task/list", get(fetch_task_list).layer(cors.clone()))
        .route("/task/count", get(fetch_count).layer(cors.clone()))
        .route("/task/update", put(update_task))
        .route("/task/start", get(start_tasking))
        .route("/task/debug", post(task_debug))
        .route("/task/debug/preview", post(task_debug_preview))
        .fallback(handler_404)
        .with_state(state);

    info!("listen {}", app_conf.http.listen.to_owned());
    let listener = TcpListener::bind(app_conf.http.listen.to_owned())
        .expect(format!("unable to listen {}", app_conf.http.listen.to_owned()).as_str());
    axum::Server::from_tcp(listener)
        .expect("server bind  tcp error ")
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

pub async fn handler_404() -> Whortleberry<HashMap<String, String>> {
    let mut data = HashMap::new();
    data.insert(
        String::from("ts"),
        Local::now().format("%Y-%m-%d %H:%M:%S.%3f").to_string(),
    );
    data.insert(String::from("info"), "welcome use freebit".to_owned());

    Whortleberry {
        err_no: 404,
        err_msg: "ê hə, not found the specific resource!".to_string(),
        data,
    }
}

pub async fn time_out_handler(err: BoxError) -> Whortleberry<HashMap<String, String>> {
    let mut data: HashMap<String, String> = HashMap::new();
    data.insert(
        String::from("ts"),
        chrono::Utc::now().timestamp().to_string(),
    );
    data.insert(String::from("info"), "welcome".to_owned());
    error!("error info {:?}", err);
    if err.is::<tower::timeout::error::Elapsed>() {
        Whortleberry {
            err_no: 404,
            err_msg: "time out".to_string(),
            data,
        }
    } else {
        Whortleberry {
            err_no: 404,
            err_msg: "time out".to_string(),
            data,
        }
    }
}
