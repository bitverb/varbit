use axum::Json;
use axum_core::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Whortleberry<T> {
    pub err_no: i64,
    pub err_msg: String,
    pub data: T,
}

impl<T> IntoResponse for Whortleberry<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

