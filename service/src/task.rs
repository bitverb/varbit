/// this mod
///
pub mod json {
    use log::{debug, info, warn};
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};

    use crate::task::json::data_type::{ARRAY, BOOLEAN, NULL, NUMBER, OBJECT, STRING};

    const DEFAULT_MAX_DEPTH: i32 = -1;
    const ZERO_DEPTH: i32 = 0;

    #[derive(Debug, Deserialize, Serialize)]
    pub struct ChrysaetosBit {
        // split key
        sep: String,

        // walk max depth
        max_depth: i32,

        // ignore field
        ignore: HashSet<String>,

        // default value
        default_value: HashMap<String, serde_json::Value>,

        // fold
        fold: HashMap<String, serde_json::Value>,
    }

    impl ChrysaetosBit {
        pub fn new(sep: String, max_depth: i32) -> Self {
            Self {
                max_depth,
                sep,
                ignore: HashSet::new(),
                default_value: HashMap::new(),
                fold: HashMap::new(),
            }
        }

        fn format_key(&self, pre_key: String, key: &String, depth: i32) -> String {
            if depth == 0 {
                return key.clone();
            }
            format!("{}{}{}", pre_key, self.sep, key)
        }

        fn get_sep(&self) -> &String {
            // "".to_uppercase()
            &self.sep
        }
        /// parser json object like {}, []
        pub fn parse(&self, obj: &serde_json::Value) -> Vec<HashMap<String, serde_json::Value>> {
            debug!(
                "parse value {:?}",
                serde_json::to_string(obj).unwrap_or_default()
            );
            if obj.is_null() {
                warn!("obj is null {obj}");
                return vec![];
            }

            match obj {
                serde_json::Value::Array(_v) => {
                    self.parse_list(_v, &String::new(), &HashMap::new(), ZERO_DEPTH)
                }
                serde_json::Value::Object(_v) => {
                    self.parse_object(_v, &String::new(), &HashMap::new(), ZERO_DEPTH)
                }
                _b @ _ => {
                    warn!("parse root node maybe array or object {:#?}", obj);
                    vec![]
                }
            }
        }

        fn parse_list(
            &self,
            obj: &Vec<serde_json::Value>,
            pre_key: &String,
            curr: &HashMap<String, serde_json::Value>,
            depth: i32,
        ) -> Vec<HashMap<String, serde_json::Value>> {
            info!(
                "parse_list {:?}",
                serde_json::to_string(obj).unwrap_or_default()
            );
            if depth > self.max_depth && self.max_depth != DEFAULT_MAX_DEPTH {
                warn!(
                    "parse list depth({depth}) is over max_depth ({})",
                    self.max_depth
                );
                return vec![];
            }

            let mut tmp_result_list: Vec<HashMap<String, serde_json::Value>> = vec![];
            if obj.is_empty() {
                tmp_result_list.push(curr.clone());
                return tmp_result_list;
            }

            for oj in obj {
                match oj {
                    serde_json::Value::Object(_obj) => {
                        let mut result: Vec<HashMap<String, serde_json::Value>> =
                            self.parse_object(&_obj, pre_key, curr, depth + 1);
                        tmp_result_list.append(&mut result);
                    }
                    serde_json::Value::Array(_list) => {
                        // parser list
                        let mut result: Vec<HashMap<String, serde_json::Value>> = self.parse_list(
                            _list,
                            &self.format_key(pre_key.clone(), self.get_sep(), depth),
                            curr,
                            depth + 1,
                        );
                        tmp_result_list.append(&mut result);
                    }

                    _b @ _ => {
                        debug!("walk key:{:?} value:{:?}", pre_key.to_owned(), oj.clone());
                        let mut m: HashMap<String, serde_json::Value> = curr.clone();
                        m.insert(pre_key.clone(), oj.clone());
                        tmp_result_list.push(m);
                    }
                }
            }

            return tmp_result_list;
        }

        fn parse_object(
            &self,
            obj: &serde_json::Map<String, serde_json::Value>,
            pre_key: &String,
            curr: &HashMap<String, serde_json::Value>,
            depth: i32,
        ) -> Vec<HashMap<String, serde_json::Value>> {
            info!(
                "obj {:?} {depth}",
                serde_json::to_string(obj).unwrap_or_default()
            );
            let mut tmp_result_list: Vec<HashMap<String, serde_json::Value>> = vec![curr.clone()];
            if obj.is_empty() {
                warn!(
                    "parse_object {} obj is empty {:?}",
                    pre_key,
                    serde_json::to_string(obj).unwrap_or_default()
                );
                let mut tmp = curr.clone();
                tmp.insert(
                    pre_key.clone(),
                    serde_json::Value::Object(serde_json::Map::new()),
                );
                return vec![tmp];
            }
            for (key, value) in obj {
                debug!(
                    "parse_object pre_key:{pre_key},curr key:{key}, value:{}",
                    value.to_string()
                );
                match value {
                    serde_json::Value::Object(_obj) => {
                        let mut result_list: Vec<HashMap<String, serde_json::Value>> = Vec::new();
                        for x in &tmp_result_list {
                            let mut result: Vec<HashMap<String, serde_json::Value>> = self
                                .parse_object(
                                    _obj,
                                    &self.format_key(pre_key.clone(), key, depth),
                                    x,
                                    depth + 1,
                                );
                            result_list.append(&mut result);
                        }
                        tmp_result_list = result_list;
                    }
                    serde_json::Value::Array(_list) => {
                        let mut result_list: Vec<HashMap<String, serde_json::Value>> = Vec::new();
                        for x in &tmp_result_list {
                            let mut result: Vec<HashMap<String, serde_json::Value>> = self
                                .parse_list(
                                    _list,
                                    &self.format_key(pre_key.clone(), key, depth),
                                    x,
                                    depth + 1,
                                );
                            result_list.append(&mut result);
                        }
                        tmp_result_list = result_list;
                    }
                    _prima @ _ => {
                        for result in &mut tmp_result_list {
                            result.insert(
                                self.format_key(pre_key.clone(), key, depth),
                                _prima.clone(),
                            );
                        }
                    }
                }
            }
            return tmp_result_list;
        }
    }

    #[derive(Debug, Serialize)]
    pub struct ChrysaetosBitFlow {
        sep: String,
    }

    // data type
    mod data_type {
        // number like int, float
        pub(crate) const NUMBER: &'static str = "number";
        // boolean
        pub(crate) const BOOLEAN: &'static str = "boolean";
        // string
        pub(crate) const STRING: &'static str = "string";
        // array
        pub(crate) const OBJECT: &'static str = "object";
        // object
        pub(crate) const ARRAY: &'static str = "array";
        // null value
        pub(crate) const NULL: &'static str = "null";
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CRHRes {
        name: String,
        property_type: String,
        props: Vec<CRHRes>,
        son: Box<Option<CRHRes>>,
    }

    impl ChrysaetosBitFlow {
        pub fn new() -> Self {
            return ChrysaetosBitFlow::from_sep("".to_string());
        }
        pub fn from_sep(sep: String) -> Self {
            ChrysaetosBitFlow { sep }
        }
        pub fn property(&self, obj: serde_json::Value) -> CRHRes {
            info!("obj {}", obj.to_string());
            match obj {
                serde_json::Value::Object(m) => {
                    debug!("obj is {}", serde_json::json!(m).to_string());
                    CRHRes {
                        name: "".to_owned(),
                        property_type: OBJECT.to_string(),
                        props: vec![],
                        son: Box::new(Some(self.property_object(&m))),
                    }
                }
                serde_json::Value::Array(l) => {
                    debug!("l is {}", serde_json::json!(l).to_string());
                    CRHRes {
                        name: "".to_owned(),
                        property_type: ARRAY.to_string(),
                        props: vec![],
                        son: Box::new(Some(self.property_list(&l))),
                    }
                }
                serde_json::Value::Number(_num) => CRHRes {
                    name: "".to_owned(),
                    property_type: NUMBER.to_string(),
                    props: vec![],
                    son: Box::new(None),
                },
                serde_json::Value::Bool(_b) => CRHRes {
                    name: "".to_owned(),
                    property_type: BOOLEAN.to_string(),
                    props: vec![],
                    son: Box::new(None),
                },
                serde_json::Value::String(_s) => CRHRes {
                    name: "".to_owned(),
                    property_type: STRING.to_string(),
                    props: vec![],
                    son: Box::new(None),
                },

                serde_json::Value::Null => CRHRes {
                    name: "".to_owned(),
                    property_type: NULL.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                },
            }
        }

        // parser object property
        fn property_object(&self, obj: &serde_json::Map<String, serde_json::Value>) -> CRHRes {
            debug!("obj is {}", serde_json::json!(obj).to_string());
            let mut vc: Vec<CRHRes> = vec![];
            for (key, value) in obj {
                let data = match value {
                    serde_json::Value::Object(m) => CRHRes {
                        name: key.to_owned(),
                        property_type: OBJECT.to_owned(),
                        props: vec![],
                        son: Box::new(Some(self.property_object(&m))),
                    },
                    serde_json::Value::Array(l) => CRHRes {
                        name: key.to_owned(),
                        property_type: ARRAY.to_owned(),
                        props: vec![],
                        son: Box::new(Some(self.property_list(&l))),
                    },
                    serde_json::Value::String(_s) => CRHRes {
                        name: key.to_owned(),
                        property_type: STRING.to_owned(),
                        props: vec![],
                        son: Box::new(None),
                    },
                    serde_json::Value::Number(_n) => CRHRes {
                        name: key.to_owned(),
                        property_type: NUMBER.to_owned(),
                        props: vec![],
                        son: Box::new(None),
                    },
                    serde_json::Value::Bool(_b) => CRHRes {
                        name: key.to_owned(),
                        property_type: BOOLEAN.to_owned(),
                        props: vec![],
                        son: Box::new(None),
                    },
                    serde_json::Value::Null => CRHRes {
                        name: key.to_owned(),
                        property_type: NULL.to_owned(),
                        props: vec![],
                        son: Box::new(None),
                    },
                };
                vc.push(data);
            }
            CRHRes {
                name: "".to_string(),
                property_type: OBJECT.to_string(),
                props: vc,
                son: Box::new(None),
            }
        }

        fn property_list(&self, list: &Vec<serde_json::Value>) -> CRHRes {
            debug!("property_list is {}", serde_json::json!(list).to_string());
            if list.is_empty() {
                return CRHRes {
                    name: "".to_owned(),
                    property_type: NULL.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                };
            }

            let data = match list.first().unwrap() {
                serde_json::Value::Object(m) => CRHRes {
                    name: "".to_owned(),
                    property_type: OBJECT.to_owned(),
                    props: vec![],
                    son: Box::new(Some(self.property_object(&m))),
                },
                serde_json::Value::Array(l) => CRHRes {
                    name: "".to_owned(),
                    property_type: ARRAY.to_owned(),
                    props: vec![],
                    son: Box::new(Some(self.property_list(&l))),
                },
                serde_json::Value::String(_s) => CRHRes {
                    name: "".to_owned(),
                    property_type: STRING.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                },
                serde_json::Value::Number(_n) => CRHRes {
                    name: "".to_owned(),
                    property_type: NUMBER.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                },
                serde_json::Value::Bool(_b) => CRHRes {
                    name: "".to_owned(),
                    property_type: BOOLEAN.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                },
                serde_json::Value::Null => CRHRes {
                    name: "".to_owned(),
                    property_type: NULL.to_owned(),
                    props: vec![],
                    son: Box::new(None),
                },
            };

            CRHRes {
                name: "".to_owned(),
                property_type: ARRAY.to_owned(),
                props: vec![],
                son: Box::new(Some(data)),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use serde_json::json;

        use super::*;

        #[test]
        fn it_works() {
            let cry = ChrysaetosBit::new("_".to_owned().to_string(), 10);
            let res: Vec<HashMap<String, serde_json::Value>> = cry.parse(
                &serde_json::from_str(
                    r###"{
            "name":"ace"
        }"###
                        .to_owned()
                        .as_str(),
                )
                .unwrap(),
            );
            assert_eq!(res.len(), 1);

            let res: Vec<HashMap<String, serde_json::Value>> =
                cry.parse(&serde_json::from_str(r###"[true,true]"###.to_owned().as_str()).unwrap());
            assert_eq!(res.len(), 2);
            println!("res {:?}", json!(res));
        }
    }
}
