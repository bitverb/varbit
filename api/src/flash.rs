
use serde::{Deserialize, Serialize};


#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Whortleberry <T>
where
    T: Serialize,
{
    pub err_no: i64,
    pub err_msg: String,
    pub data: T,
}
