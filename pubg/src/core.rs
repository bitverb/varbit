use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Msg {
    pub g_id: String,             // g_id
    pub value: serde_json::Value, // msg value
}

impl Msg {
    pub fn new(g_id: String, value: serde_json::Value) -> Self {
        Msg { g_id, value }
    }
}
