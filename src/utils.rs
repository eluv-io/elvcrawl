// pub fn unwrap<T>(e: Err, msg: &str) -> Response<T> {
//     info!("error={}", e);

//     Err(e)
// }
use serde_json::Value;

pub fn extract_body(v: Value) -> Option<Value> {
    let obj = match v.as_object() {
        Some(v) => v,
        None => return None,
    };
    let mut full_result = true;
    let res = match obj.get("result") {
        Some(m) => m,
        None => match obj.get("http") {
            Some(h) => {
                full_result = false;
                h
            }
            None => return None,
        },
    };
    if full_result {
        let http = match res.get("http") {
            Some(h) => h,
            None => return None,
        };
        return http.get("body").cloned();
    }
    res.get("body").cloned()
}
