// use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
struct BzResponse<T> {
    pub err_no: i64,
    pub err_msg: String,
    pub data: T,
}

// #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
// #[sea_orm(table_name = "user_info")]
// pub struct Model {
//     #[sea_orm(primary_key)]
//     pub id: i64,
//     #[sea_orm(column_name = "name")]

//     pub name: String,
// }

// // #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
// // #[sea_orm(table_name = "posts")]
// // pub struct Model {
// //     #[sea_orm(primary_key)]
// //     #[serde(skip_deserializing)]
// //     pub id: i32,
// //     pub title: String,
// //     #[sea_orm(column_type = "Text")]
// //     pub text: String,
// // }
// impl ActiveModelBehavior for ActiveModel {}

// #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
// #[sea_orm(table_name = "animal")]
// pub struct Animal {
//     #[sea_orm(primary_key)]
//     pub id: i32,
//     pub name: String,
//     pub age: i32,
//     pub species: String,
// }
