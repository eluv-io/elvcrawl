#![allow(dead_code, clippy::box_collection)]


use serde::{Deserialize, Serialize};
use serde_json::{Value};
use std::{cmp::min, error::Error};
use elvwasm::{ErrorKinds};



// Used to represent values in prefix path
#[derive(Clone, PartialEq, Eq)]
pub enum PrefixValue {
    ObjectItemAny,
    ArrayItemAny,
    Key(String),
}

impl PrefixValue {
    pub fn is_key(&self) -> bool {
        matches!(self, PrefixValue::Key(..))
    }
}

pub struct Prefix {
    prefix: Vec<PrefixValue>,
}

impl Prefix {
    pub fn get(&self, i: usize) -> &PrefixValue {
        &self.prefix[i]
    }

    pub fn from_jpath(jpath: &str) -> Prefix {
        let mut prefix: Vec<PrefixValue> = Vec::new();
        let otokens: Vec<&str> = jpath.split('/').collect();
        let mut start = 0;
        if (*otokens.get(start).unwrap()).eq("") {
            start = 1;
        }
        for &otokens in &otokens[start..] {
            let ltokens: Vec<&str> = otokens.split('[').collect();
            if ltokens[0] == "*" {
                prefix.push(PrefixValue::ObjectItemAny)
            } else if !ltokens[0].is_empty() {
                prefix.push(PrefixValue::Key(ltokens[0].to_string()));
            }

            for &ltoken in &ltokens[1..] {
                assert!(ltoken.ends_with(']'));

                let mut ltoken_slice = ltoken.chars();
                ltoken_slice.next_back();
                if ltoken_slice.as_str() != "*" {
                    prefix.push(PrefixValue::Key(ltoken_slice.as_str().to_string()));
                } else {
                    prefix.push(PrefixValue::ArrayItemAny)
                }
            }
        }
        Prefix { prefix }
    }

    pub fn to_jpath(&self) -> String {
        let mut str_elems: Vec<String> = Vec::new();
        for elem in &self.prefix {
            match elem {
                PrefixValue::Key(value) => str_elems.push(format!("/{}", value)),
                PrefixValue::ObjectItemAny => str_elems.push("/*".to_string()),
                PrefixValue::ArrayItemAny => str_elems.push("[*]".to_string()),
            }
        }
        if str_elems.is_empty() {
            return "/".to_string();
        }
        str_elems.join("")
    }

    pub fn common_ancestor(prefix_1: Prefix, prefix_2: Prefix) -> Prefix {
        let mut common_prefix: Vec<PrefixValue> = Vec::new();
        let min_length = min(prefix_1.prefix.len(), prefix_2.prefix.len());
        for i in 0..min_length {
            let mut is_match = false;
            let mut elem: Option<PrefixValue> = None;

            if *prefix_1.get(i) == *prefix_2.get(i) {
                is_match = true;
                elem = Some(prefix_1.get(i).clone());
            } else if *prefix_1.get(i) == PrefixValue::ObjectItemAny && prefix_2.get(i).is_key() {
                    is_match = true;
                    elem = Some(prefix_2.get(i).clone());
                } else if *prefix_2.get(i) == PrefixValue::ObjectItemAny && prefix_1.get(i).is_key() {
                    is_match = true;
                    elem = Some(prefix_1.get(i).clone());
                }
            if !is_match {
                break;
            }
            if let Some(e) = elem { common_prefix.push(e) }
        }
        Prefix {
            prefix: common_prefix,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct RootConfig {
    pub(crate) content: String,
    pub(crate) library: String,
}

#[derive(Deserialize, Clone)]
pub struct FabricPolicy {
    pub(crate) paths: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct FabricConfig {
    pub(crate) policy: FabricPolicy,
    pub(crate) root: RootConfig,
}

#[derive(Deserialize, Clone)]
pub struct FabricDocument {
    pub(crate) prefix: Box<String>,
}

pub struct Crawler {
    pub(crate) inception_url: String,
}

#[derive(Deserialize, Default)]
pub struct CrawlResult {
    //mdregistry, statistics, sorted_ids, sorted_hashes, raised_exceptions
    pub(crate) mdregistry: serde_json::map::Map<String, Value>,
    pub(crate) stats:  serde_json::map::Map<String, Value>,
    pub(crate) sorted_ids:  Vec<String>,
    pub(crate) sorted_hashes:  Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct IndexerConfig<> {
    #[serde(rename = "type")]
    pub(crate) indexer_type: Box<String>,
    pub(crate) document: FabricDocument,
    pub(crate) fields: Vec<FieldConfig>,
    pub(crate) fabric: FabricConfig,
}

impl Crawler{
    pub fn new(iurl:&str) -> Crawler{
        Crawler { inception_url: iurl.to_string() }
    }
    pub fn crawl(&self, _config:IndexerConfig) -> Result<CrawlResult, Box<dyn Error + Send + Sync>>{
        Ok(CrawlResult::default())
    }
}
impl IndexerConfig{

    /**
     * Given a string representing the JSON index config, returns
     * and IndexerConfig value with proper fields filled out.
     */
    pub fn parse_index_config(
        config_value: &Value,
    ) -> Result<IndexerConfig, Box<dyn Error + Send + Sync>> {
        // Read config string as serde_json Value

        // Parse config into IndexerConfig
        let indexer_config_val: &Value = &config_value["indexer"];
        let fabric_config_val: &Value = &config_value["fabric"];
        let indexer_arguments_val = &indexer_config_val["arguments"];
        let mut field_configs: Vec<FieldConfig> = Vec::new();
        let flds = match indexer_arguments_val["fields"].as_object(){
            Some(x) => x,
            None => return Err(ErrorKinds::NotExist("fields is not available in index").into())
        };
        for (field_name, field_value) in flds {
            field_configs.push(FieldConfig {
                name: field_name.to_string(),
                options: field_value["options"].clone(),
                field_type: serde_json::from_value(field_value["type"].clone())?,
                paths: serde_json::from_value(field_value["paths"].clone())?,
            });
        }
        Ok(IndexerConfig {
            fabric:serde_json::from_value(fabric_config_val.clone())?,
            indexer_type: serde_json::from_value(indexer_config_val["type"].clone())?,
            document: serde_json::from_value(indexer_arguments_val["document"].clone())?,
            fields: field_configs,
        })
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct FieldConfig {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) field_type: String,
    pub options: Value,
    pub paths: Vec<String>,
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    extern crate test_utils;
    use super::*;
//    use test_utils::test_metadata;
    use serde_json::json;
    use elvwasm::{BitcodeContext, Request};
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use crate::Indexer;

    fn strip_quotes(s :&mut str) -> String{
        s.replacen("\"", "", 2)
    }


    #[test]
    fn test_parse_index_config() -> () {
        let index_object_meta: Value = serde_json::from_str(test_utils::test_metadata::INDEX_CONFIG)
            .expect("Could not read index object into json value.");
        let config_value: &Value = &index_object_meta["indexer"]["config"];
        let indexer_config: IndexerConfig = IndexerConfig::parse_index_config(config_value)
            .expect("Could not parse indexer config.");

        /* Assert that indexer_config fields are correctly filled out. */
        assert_eq!(22, indexer_config.fields.len());
        assert_eq!("string", indexer_config.fields[0].field_type);
        assert_eq!(vec!["site_map.searchables.*.asset_metadata.asset_type"], indexer_config.fields[0].paths);
        assert_eq!(json!({ "stats": { "histogram": true } }), indexer_config.fields[0].options);
        assert_eq!("asset_type", indexer_config.fields[0].name);
        assert_eq!("metadata-text", indexer_config.indexer_type.as_ref());
        assert_eq!("ilib4M649Yi6tCTWpXgxch4i9RJvv4BQ", indexer_config.fabric.root.library);
        assert_eq!("iq__VjLkBkswLMrai3CfCUJtUfhWmZy", indexer_config.fabric.root.content);
        assert_eq!(vec!["/offerings/*"], indexer_config.fabric.policy.paths);
        assert_eq!(indexer_config.document.prefix.as_ref().to_string() , "/".to_string());
        assert_eq!(strip_quotes(config_value["indexer"]["arguments"]["document"]["prefix"].to_string().as_mut()), indexer_config.document.prefix.as_ref().to_string());
    }
    #[test]
    fn test_crawler() ->  () {
        let index_object_meta: Value = serde_json::from_str(test_utils::test_metadata::INDEX_CONFIG)
            .expect("Could not read index object into json value.");
        let config_value: &Value = &index_object_meta["indexer"]["config"];
        let indexer_config: IndexerConfig = IndexerConfig::parse_index_config(config_value)
            .expect("Could not parse indexer config.");
        let new_id = "id123".to_string();
        let req = &Request{
            id: new_id.clone(),
            jpc: "1.0".to_string(),
            method: "foo".to_string(),
            params: elvwasm::JpcParams {
                http: elvwasm::HttpParams {
                    headers: HashMap::<String, Vec<String>, RandomState>::new(),
                    path: "/".to_string(),
                    query: HashMap::<String, Vec<String>, RandomState>::new(),
                    verb: "GET".to_string(),
                    fragment: "".to_string(),
                    content_length: 0,
                    client_ip: "localhost".to_string(),
                    self_url: "localhost".to_string(),
                    proto: "".to_string(),
                    host: "somehost.com".to_string()
                }
            },
            q_info: elvwasm::QInfo { hash: "hqp_123".to_string(), id: new_id, qlib_id: "libfoo".to_string(), qtype: "hq_423234".to_string(), write_token: "tqw_5555".to_string() }
        };
        let mut bcc = BitcodeContext::new(req.clone());
        let idx = Indexer::new(&mut bcc, indexer_config.document.prefix.as_ref().to_string(), indexer_config.fields.clone()).expect("failed to create index");
        assert_eq!(&idx.fields.len(), &indexer_config.fields.len());
        let crawler = Crawler::new("");
        crawler.crawl(indexer_config).unwrap();

    }
}
