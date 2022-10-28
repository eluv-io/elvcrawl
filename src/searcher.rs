use elvwasm::{BitcodeContext};
use guest::CallResult;


pub fn content_query(bcc: &mut BitcodeContext) -> CallResult {
    let searcher = Searcher { bcc };
    let http_p = &bcc.request.params.http;
    let qp = &http_p.query;
    BitcodeContext::log(&format!("In content_query hash={} headers={:#?} query params={:#?}",&bcc.request.q_info.hash, &http_p.headers, qp));
    searcher.query(qp["query"][0].as_str())?;
    Ok(Vec::new())
}

struct Searcher<'a> {
    bcc: &'a BitcodeContext,
}

impl<'a> Searcher<'a> {


    fn query(&self, query_str: &str) -> CallResult {
        // let hash_part_id_vec = self
        //     .bcc
        //     .sqmd_get_json(&format!("indexer/part/{}", part_name))?;
        // let hash_part_id = serde_json::from_slice(&hash_part_id_vec)?;
        self.bcc.index_reader_builder_create(None)?;

        self.bcc.reader_builder_query_parser_create(None)?;

        self.bcc.query_parser_for_index(Some(serde_json::from_str(r#"{ "fields" : ["title", "body"] } }"#)?))?;

        self.bcc.query_parser_parse_query(query_str)?;

        let _search_results = self.bcc.query_parser_search(None);

        Ok(Vec::new())
    }
}
