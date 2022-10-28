#![feature(backtrace)]
extern crate elvwasm;
extern crate serde_json;
extern crate serde;
extern crate petgraph;


pub mod crawler;
pub mod graph;
pub mod indexer;
pub mod searcher;
pub mod utils;



use elvwasm::{implement_bitcode_module, jpc, register_handler};
use serde_json::{json, Map, Value};
use crawler::FieldConfig;
use indexer::Indexer;

implement_bitcode_module!("crawl", do_crawl);


#[no_mangle]
fn do_crawl(bcc: &mut elvwasm::BitcodeContext) -> CallResult{
    let res = bcc.sqmd_get_json("/indexer/arguments/fields")?;
    let fields:Map<String, Value> = serde_json::from_slice(&res)?;
    let mut idx_fields = Vec::<crawler::FieldConfig>::new();
    for (field, val) in fields.into_iter(){
        let fc_cur = FieldConfig{
            name: field,
            options: serde_json::from_value(val["options"].clone()).unwrap(),
            field_type: serde_json::from_value(val["field_type"].clone()).unwrap(),
            paths: serde_json::from_value(val["paths"].clone()).unwrap()
        };

        idx_fields.push(fc_cur);
    }
    let _idx = Indexer::new(bcc, "idx".to_string(), idx_fields)?;
    let id = &bcc.request.id;

    let res_config = bcc.sqmd_get_json("/indexer/config")?;
    let fields_config: Value = serde_json::from_slice(&res_config)?;
    let _indexer_config = crawler::IndexerConfig::parse_index_config(&fields_config)?;


    bcc.make_success_json(&json!(
        {
            "headers" : "application/json",
            "body" : "SUCCESS",
            "result" : {"status" : "update complete"},
        }), id)
}