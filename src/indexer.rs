#![allow(dead_code, clippy::box_collection)]

use crate::{utils::extract_body, crawler};

use elvwasm::{BitcodeContext, ErrorKinds};
use serde_json::{json, Value};
use std::{error::Error, collections::HashMap};

pub struct Indexer {
    pub filepath: String,
    pub fields: Vec<crawler::FieldConfig>
}

impl Indexer {
    pub fn new(bcc: &mut BitcodeContext, filepath: String, fields: Vec<crawler::FieldConfig>) -> Result<Indexer, Box<dyn Error + Send + Sync>> {
        // Read request
        let http_p = &bcc.request.params.http;
        let query_params = &http_p.query;
        BitcodeContext::log(&format!(
            "In create_index hash={} headers={:#?} query params={:#?}",
            &bcc.request.q_info.hash, &http_p.headers, query_params
        ));
        let _id = &bcc.request.id;

        // Create index in directory
        let mut input_data = json!({
            "directory": "index" //TODO is this correct directory?
        });
        BitcodeContext::log("before BUILDER");
        bcc.new_index_builder(input_data)?;
        BitcodeContext::log("NEW INDEX BUILDER");

        // Add fields to schema builder
        for field_config in &fields {
            Indexer::add_field_to_schema(bcc, field_config)?;
        }

        // Build index
        input_data = json!({});
        bcc.builder_build(Some(input_data))?;

        Ok(Indexer { filepath, fields })
    }

    fn add_field_to_schema(bcc: &BitcodeContext, field_config: &crawler::FieldConfig) -> Result<(), Box<dyn Error + Send + Sync>>{ //TODO: Add support for other fields.
        let input_data;
        match field_config.field_type.as_str() {
            "text" => {
                input_data = json!({
                    "name": field_config.name,
                    "type": 1_u8, //FIXME this should be a TextOption
                    "stored": true,
                });
                let field_title_vec = bcc.builder_add_text_field(Some(input_data))?;
                let ft_json: serde_json::Value = serde_json::from_slice(&field_title_vec)?;
                match extract_body(ft_json) {
                    Some(o) => o.get("field").unwrap().as_u64(),
                    None => {
                        return Err(Box::new(ErrorKinds::BadHttpParams(
                            "could not find key document-create-id",
                        )))
                    }
                };
                BitcodeContext::log("ADDED TEXT FIELD.");
            }
            "string" => {
                input_data = json!({
                    "name": field_config.name,
                    "type": 1_u8, //FIXME this should be a TextOption. What is the right number here?
                    "stored": true,
                });
                let field_title_vec = bcc.builder_add_text_field(Some(input_data))?;
                let ft_json: serde_json::Value = serde_json::from_slice(&field_title_vec)?;
                match extract_body(ft_json) {
                    Some(o) => o.get("field").unwrap().as_u64(),
                    None => {
                        return Err(Box::new(ErrorKinds::BadHttpParams(
                            "could not find key document-create-id",
                        )))
                    }
                };
                BitcodeContext::log("ADDED STRING FIELD.");
            }
            _ => panic!("unknown field type"),
        }
        Ok(())
    }
}

struct Writer<'a> {
    bcc: &'a BitcodeContext,
    fields: HashMap<String, crawler::FieldConfig>
}

impl<'a> Writer<'a> {
    pub fn new(bcc: &'a BitcodeContext, fields: HashMap<String, crawler::FieldConfig>) -> Writer<'a>{
        Writer { bcc, fields }
    }

    pub fn index(&self, uid: &String, data: &Value, fields: &Value) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        assert!(fields.is_object());

        let response_vec = self.bcc.document_create(None)?;
        let response_val: serde_json::Value = serde_json::from_slice(&response_vec)?;
        let doc_id = match extract_body(response_val) {
            Some(o) => o.get("document-create-id").unwrap().as_u64(),
            None => {
                return self.bcc.make_error_with_kind(ErrorKinds::BadHttpParams(
                    "could not find key document-create-id",
                ))
            }
        }
        .unwrap();
        self.document_add_field("uid", "text", &json!(uid), doc_id)?;
        self.document_add_field("data", "text", data, doc_id)?;

        for field in fields.as_object().unwrap() {
            let field_type = &self.fields.get(field.0).unwrap().field_type;
            self.document_add_field(field.0, field_type, field.1, doc_id)?;
        }
        Ok(Vec::new())
    }

    fn document_add_field(
        &self,
        field_name: &str,
        field_type: &str,
        field_content: &Value,
        doc_id: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match field_type {
            "text" | "string" => {
                let input = json!({
                    "field": field_name,
                    "value": field_content.as_str(),
                    "doc": doc_id
                });
                self.bcc.document_add_text(Some(input))?;
            }
            _ => {
                return Err(Box::new(ErrorKinds::Invalid(
                    "invalid field type encountered",
                )))
            }
        }
        Ok(())
    }
}

 // The following are mearly intended to verify internal consistency.  There are no actual calls made
// but the tests verify that the json parsing of the http message is correct
#[cfg(test)]
mod tests{
    extern crate wapc;
    extern crate wapc_guest as guest;
    extern crate tantivy_jpc;

    extern crate wasmer;

    extern crate base64;
    extern crate serde;
    extern crate serde_derive;
    extern crate serde_json;
    extern crate json_dotpath;
    extern crate snailquote;
    extern crate tempdir;
    use std::sync::{Arc};
    use elvwasm::ErrorKinds;
    use std::fs::File;
    use std::io::BufReader;
    use json_dotpath::DotPaths;
    use test_utils::test_metadata::INDEX_CONFIG;
    use elvwasm::Request;
    use std::collections::hash_map::RandomState;
    use crate::{crawler};
    use std::collections::HashMap;
    use serde_json::Value;
    use elvwasm::BitcodeContext;


    use tantivy_jpc::tests::{FakeContext, TestDocument};


    use serde::{Deserialize, Serialize};
    pub static mut QFAB: MockFabric = MockFabric{
        fab : None,
        ctx: None,
        resp: vec![],
        docs: vec![]
    };

    macro_rules! output_raw_pointers {
        ($raw_ptr:ident, $raw_len:ident) => {
              unsafe { std::str::from_utf8(std::slice::from_raw_parts($raw_ptr, $raw_len)).unwrap_or("unable to convert")}
        }
      }

      #[no_mangle]
      pub extern "C" fn __console_log(ptr: *const u8, len: usize){
        let out_str = output_raw_pointers!(ptr,len);
        println!("console output : {}", out_str);
      }
      #[no_mangle]
      pub extern "C" fn __host_call(
        bd_ptr: *const u8,
        bd_len: usize,
        ns_ptr: *const u8,
        ns_len: usize,
        op_ptr: *const u8,
        op_len: usize,
        ptr: *const u8,
        len: usize,
        ) -> usize {
          let out_bd = output_raw_pointers!(bd_ptr, bd_len);
          let out_ns = output_raw_pointers!(ns_ptr, ns_len);
          let out_op = output_raw_pointers!(op_ptr, op_len);
          let out_ptr = output_raw_pointers!(ptr, len);
          println!("host call bd = {} ns = {} op = {}, ptr={}", out_bd, out_ns, out_op, out_ptr);
          let v = MockFabric::host_callback(0, out_bd, out_ns, out_op, out_ptr.as_bytes()).expect("host callback failed");
          unsafe{QFAB.resp = v;}
          1
      }
      #[no_mangle]
      pub extern "C" fn __host_response(ptr: *mut u8){
        println!("host __host_response ptr = {:?}", ptr);
        unsafe{std::ptr::copy(QFAB.resp.as_ptr(), ptr, QFAB.resp.len())}
      }

      #[no_mangle]
      pub extern "C" fn __host_response_len() -> usize{
        println!("host __host_response_len");
        unsafe{QFAB.resp.len()}
      }

      #[no_mangle]
      pub extern "C" fn __host_error_len() -> usize{
        println!("host __host_error_len");
        0
      }

      #[no_mangle]
      pub extern "C" fn __host_error(ptr: *const u8){
        println!("host __host_error ptr = {:?}", ptr);
      }

      #[no_mangle]
      pub extern "C" fn __guest_response(ptr: *const u8, len: usize){
        let out_resp = output_raw_pointers!(ptr,len);
        println!("host  __guest_response ptr = {}", out_resp);
      }

      #[no_mangle]
      pub extern "C" fn __guest_error(ptr: *const u8, len: usize){
        let out_error = output_raw_pointers!(ptr,len);
        println!("host  __guest_error ptr = {}", out_error);
      }

      #[no_mangle]
      pub extern "C" fn __guest_request(op_ptr: *const u8, ptr: *const u8){
        println!("host __guest_request op_ptr = {:?} ptr = {:?}", op_ptr, ptr);

      }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct RootMockFabric {
      pub library:Library,
      pub call:serde_json::Value,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Object {
      pub hash: String,
      pub id: String,
      pub qlib_id: String,
      #[serde(rename = "type")]
      pub qtype: String,
      pub write_token: String,
      pub meta : serde_json::Map<String, serde_json::Value>
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Library {
      pub id: String,
      pub objects: std::vec::Vec<Object>,
    }

    #[derive(Serialize, Deserialize,  Clone, Debug)]
    pub struct MockFabric{
        ctx: Option<FakeContext>,
        fab : Option<RootMockFabric>,
        resp: Vec<u8>,
        docs: Vec<TestDocument>
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct JPCRequest {
      pub jpc: String,
      pub params: serde_json::Map<String, serde_json::Value>
    }

    impl MockFabric{
        pub fn init(& mut self, path_to_json:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
            let file = File::open(path_to_json)?;
            let reader = BufReader::new(file);

            // Read the JSON contents of the file as an instance of `User`.
            let json_rep:RootMockFabric = serde_json::from_reader(reader)?;
            self.fab = Some(json_rep);
            return Ok("DONE".as_bytes().to_vec())
        }
        pub fn write_stream(&self, _json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in WriteStream");
            Ok("Not Implemented".as_bytes().to_vec())
        }
        pub fn sqmd_delete(&self, json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in SQMD delete");
            let j:JPCRequest = serde_json::from_str(json_rep)?;
            let path = j.params["path"].to_string();
            if  !path.is_empty(){
                let mut fab = self.fab.clone().unwrap();
                let p = &snailquote::unescape(&path).unwrap();
                let pp:String = p.chars().map(|x| match x {
                    '/' => '.',
                    _ => x
                }).collect();
                fab.library.objects[0].meta.dot_remove(&pp[1..])?;//{
                return Ok("DONE".as_bytes().to_vec())
            }else{
                println!("failed to find path argument");
            }
            Ok("FAILED".as_bytes().to_vec())
        }
        pub fn sqmd_set(&self, json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in SQMD set");
            let j:JPCRequest = serde_json::from_str(json_rep)?;
            let path = j.params["path"].to_string();
            let meta = j.params["meta"].to_string();
            if !path.is_empty(){
                let mut fab = self.fab.clone().unwrap();
                let p = &snailquote::unescape(&path).unwrap();
                let pp:String = p.chars().map(|x| match x {
                    '/' => '.',
                    _ => x
                }).collect();
                fab.library.objects[0].meta.dot_set(&pp[1..], meta)?;
                return Ok("DONE".as_bytes().to_vec())

            }else{
                println!("failed to find path argument");
            }
            Ok("FAILED".as_bytes().to_vec())
        }
        pub fn sqmd_get(&self, json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in SQMD get");
            let j:JPCRequest = serde_json::from_str(json_rep)?;
            let path = j.params["path"].to_string();
            if !path.is_empty(){
                let fab = self.fab.clone().unwrap();
                let p = &snailquote::unescape(&path).unwrap();
                let pp:String = p.chars().map(|x| match x {
                    '/' => '.',
                    _ => x
                }).collect();
                let gotten:Option<serde_json::Value> = fab.library.objects[0].meta.dot_get(&pp[1..])?;
                let ret = gotten.unwrap();
                println!("sqmd_get returning = {}", ret);
                return Ok(ret.to_string().as_bytes().to_vec())
            }else{
                println!("failed to find path argument");
            }
            Ok("FAILED".as_bytes().to_vec())
        }
        pub fn proxy_http(&self, _json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in ProxyHttp");
            let to_encode = r#"{"url" : {"type" : "application/json"}} "#.as_bytes();
            let enc = base64::encode(to_encode);
            Ok(format!(r#"{{"result": "{}"}}"#, enc).as_bytes().to_vec())
        }
        pub fn callback(&self, _json_rep:&str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            println!("in callback");
            let to_encode = r#"{"url" : {"type" : "application/json"}} "#.as_bytes();
            let enc = base64::encode(to_encode);
            Ok(format!(r#"{{"result": "{}"}}"#, enc).as_bytes().to_vec())
        }
        pub fn new_index_builder(&mut self, _dir:&str)-> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>>{
            self.ctx = Some(FakeContext::new());
            Ok(json!("DONE"))
        }
        pub fn archive_index_to_part(&mut self, _dir:&str)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn builder_add_text_field(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn builder_build(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn document_create(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn document_add_text(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn document_create_index(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn index_create_writer(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn index_writer_create_document(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn index_writer_commit(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn index_reader_builder_create(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn reader_builder_query_parser_create(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn query_parser_for_index(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn query_parser_parse_query(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn query_parser_search(&mut self)-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            Ok("DONE".as_bytes().to_vec())
        }
        pub fn host_callback(i_cb:u64, id:&str, context:&str, method:&str, pkg:&[u8])-> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>{
            let s_pkg = std::str::from_utf8(pkg)?;
            println!("In host callback, values i_cb = {} id = {} method = {} context = {}, pkg = {}", i_cb, id,method,context, s_pkg);
            match method {
                "SQMDGet" =>{
                   unsafe{ QFAB.sqmd_get(s_pkg) }
                }
                "TempDir" => {
                   let td = tempdir::TempDir::new("qbitcode")?;
                   let dir = td.path().to_str().unwrap();
                   let v = json!({"directory" : dir });
                   elvwasm::make_success_json(&v, id)
                }
                "SQMDSet" =>{
                    unsafe{ QFAB.sqmd_set(s_pkg) }
                 }
                "SQMDDelete" =>{
                    unsafe{ QFAB.sqmd_delete(s_pkg) }
                 }
                "Write" => {
                    unsafe{ QFAB.write_stream(s_pkg) }
                }
                "Callback" => {
                    unsafe{ QFAB.callback(s_pkg) }
                }
                "ProxyHttp" => {
                    unsafe{ QFAB.proxy_http(s_pkg) }
                }
                "NewIndexBuilder" => {
                    unsafe{
                        let v = QFAB.new_index_builder(s_pkg)?;
                        elvwasm::make_success_json(&v, id)
                    }
                }
                "ArchiveIndexToPart" => {
                    unsafe{ QFAB.archive_index_to_part("/tmp/foo") }
                }
                "BuilderAddTextField" => {
                    unsafe{
                        let v:&Value = &serde_json::from_slice(pkg)?;
                        let t = &mut QFAB.ctx.clone().unwrap();
                        let s = t.call_jpc("builder".to_string(), "add_text_field".to_string(), v["params"].clone(), true);
                        elvwasm::make_success_json(&json!({ "http" : { "body" : serde_json::from_slice::<Value>(&s).unwrap()}}), id)
                    }
                }
                "BuilderBuild" => {
                    unsafe{
                        let t = &mut QFAB.ctx.clone().unwrap();
                        let doc = t.build().unwrap();
                        QFAB.docs.append(vec![doc].as_mut());
                        elvwasm::make_success_json(&json!({"document-id": t.id}), id)
                    }
                }
                "DocumentCreate" => {
                    unsafe{ QFAB.document_create() }
                }
                "DocumentAddText" => {
                    unsafe{ QFAB.document_add_text() }
                }
                "DocumentCreateIndex" => {
                    unsafe{ QFAB.document_create_index() }
                }
                "IndexCreateWriter" => {
                    unsafe{ QFAB.index_create_writer() }
                }
                "IndexWriterAddDocument" => {
                    unsafe{ QFAB.index_writer_create_document() }
                }
                "IndexWriterCommit" => {
                    unsafe{ QFAB.index_writer_commit() }
                }
                "IndexReaderBuilderCreate" => {
                    unsafe{ QFAB.index_reader_builder_create() }
                }
                "ReaderBuilderQueryParserCreate" => {
                    unsafe{ QFAB.reader_builder_query_parser_create() }
                }
                "QueryParserForIndex" => {
                    unsafe{ QFAB.query_parser_for_index()}
                }
                "QueryParserParseQuery" => {
                    unsafe{ QFAB.query_parser_parse_query()}
                }
                "QueryParserSearch" => {
                    unsafe{ QFAB.query_parser_search()}
                }
                _ => {
                    Err(Box::new(ErrorKinds::NotExist("Method not handled")))
                }
            }
        }
    }

    struct WasmerHolder{
        _instance:wasmer::Instance
    }

    impl wapc::WebAssemblyEngineProvider for WasmerHolder{
        fn init(&mut self, _host: Arc<wapc::ModuleState>) -> std::result::Result<(), Box<dyn std::error::Error>>{
            Ok(())
        }
        fn call(&mut self, _op_length: i32, _msg_length: i32) -> std::result::Result<i32, Box<dyn std::error::Error>>{
            //.instance.store().engine.
            //self._instance.
            Ok(0)
        }
        fn replace(&mut self, _bytes: &[u8]) -> std::result::Result<(), Box<dyn std::error::Error>>{
            Ok(())
        }
    }

    use super::*;
    #[test]
    fn test_index() -> () {
        let index_object_meta: Value = serde_json::from_str(INDEX_CONFIG)
            .expect("Could not read index object into json value.");
        let config_value: &Value = &index_object_meta["indexer"]["config"];
        let indexer_config: crawler::IndexerConfig = crawler::IndexerConfig::parse_index_config(config_value)
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

    }
}

//}