// mod types { // do not include in mod definition file
//#![allow(dead_code)]
pub mod block;

// export block types
//pub use block::{ DataItem, NodeBlock};
pub use block::{
    DataItem, NodeCache, B, BID, BL, CNT, DT, GRAPH, ISNODE, IX, LB, LBL, LN, LS, N, ND, OP, OVB,
    OVB_BID, OVB_CUR, OVB_CNT, P, PARENT, PK, S, SB, SK, SK_, SN, SS, TUID, TY, XF, TARGET_UID, TARGET_ID, TARGET_BID,
};

use std::collections::{HashMap, HashSet};
//use std::borrow::Cow;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::iter::IntoIterator;
use std::slice::Iter;
use std::str::FromStr;
use std::sync::Arc;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
//use aws_sdk_dynamodb::primitives::Blob;

//&use aws_sdk_dynamodb as aws_sdk_dynamodb;
//use aws_config::aws_config;

//use aws_types::region::Region;
//use aws_sdk_dynamodb::config::{Builder, Config};
use aws_sdk_dynamodb::Client;

use uuid::{self, Builder, Uuid}; //, as_vec_string};

//use crate::lazy_static;

//use lazy_static::lazy_static;

//const LOGID: &str = "types";

// type FacetIdent : String; // type:attr:facet
//
//pub struct TyCache(HashMap<Ty, block::AttrBlock>);
// lazy_static! {
//     static ref TYIBLOCK: block::AttrItemBlock = async {
//         let mut m = db_load_types("movies".into()).await;
//         m.expect("no graph found for types")
//     };
// }

// lazy_static! {
//     static ref TYPREFIX: String = {
//         let mut m = get_type_prefix().expect("type prefix does not exist for requested graph");
//         m
//     };
// }

// lazy_static! {
//     static  ref CLIENT:  aws_sdk_dynamodb::Client = {
//         let mut m = make_dynamodb_client().expect("dynamodb client");
//         m
//     };
// }

// aws_sdk_dynamodb type conversion from AttributeValue to Rust type

pub fn uuid_to_av_lb(invec: &Vec<Uuid>) -> AttributeValue {
    let mut av: Vec<AttributeValue> = vec![];
    for v in invec {
        av.push(AttributeValue::B(Blob::new(v.clone().as_bytes())))
    }
    AttributeValue::L(av)
}

pub fn u8_to_av_ln(invec: &Vec<u8>) -> AttributeValue {
    let mut av: Vec<AttributeValue> = vec![];
    for v in invec {
        av.push(AttributeValue::N(v.to_string()))
    }
    AttributeValue::L(av)
}

pub fn u32_to_av_ln(invec: &Vec<u32>) -> AttributeValue {
    let mut av: Vec<AttributeValue> = vec![];
    for v in invec {
        av.push(AttributeValue::N(v.to_string()))
    }
    AttributeValue::L(av)
}

pub fn i32_to_av_ln(invec: &Vec<i32>) -> AttributeValue {
    let mut av: Vec<AttributeValue> = vec![];
    for v in invec {
        av.push(AttributeValue::N(v.to_string()))
    }
    AttributeValue::L(av)
}

pub fn as_string_trim_graph(val: AttributeValue) -> Option<String> {
    match val {
        AttributeValue::S(s) => {
            let s_ = match s.split('|').last() {
                Some(t) => t.to_owned(),
                None => s,
            };
            Some(s_)
        }
        AttributeValue::Null(b) => {
            if b == false {
                panic!("Got Null with bool of false")
            }
            None
        }
        _ => panic!("as_string(): Expected AttributeValue::S or ::NULL"),
    }
}

pub fn as_string(val: AttributeValue) -> Option<String> {
    match val {
        AttributeValue::S(s) => Some(s),
        AttributeValue::Null(b) => {
            if b == false {
                panic!("Got Null with bool of false")
            }
            None
        }
        _ => panic!("as_string(): Expected AttributeValue::S or ::NULL"),
    }
}

pub fn as_n(val: AttributeValue) -> Option<String> {
    match val {
        AttributeValue::N(s) => Some(s),
        AttributeValue::Null(b) => {
            if b == false {
                panic!("Got Null with bool of false")
            }
            None
        }
        _ => panic!("as_n(): Expected AttributeValue::N or ::NULL"),
    }
}

pub fn as_dt2(val: AttributeValue) -> Option<String> {
    let AttributeValue::S(s) = val else {
        panic!("as_dt2(): Expected AttributeValue::S")
    };
    Some(s)
}

pub fn as_float2(val: AttributeValue) -> Option<f64> {
    let AttributeValue::N(v) = val else {
        panic!("as_float2(): Expected AttributeValue::N")
    };
    //let Ok(f) = f64::from_str(s.as_str()) else {panic!("as_float2() : failed to convert String [{}] to f64",v)};
    let Ok(f) = v.as_str().parse::<f64>() else {
        panic!("as_float2() : failed to convert String [{:?}] to f64", v)
    };
    Some(f)
}

pub fn as_lf2(val: AttributeValue) -> Option<Vec<f64>> {
    let mut vs: Vec<f64> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_lf2(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_lf2: Expected AttributeValue::Bool")
        };
        let Ok(f) = f64::from_str(v.as_str()) else {
            panic!("as_lf2() : failed to convert String [{:?}] to f64", v)
        };
        vs.push(f);
    }
    Some(vs)
}

pub fn as_vi32(val: AttributeValue) -> Option<Vec<i32>> {
    let mut vs: Vec<i32> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i32::from_str(s.as_str()) else {
                        panic!("parse to i8 error in as_vi32 [{:?}]", s)
                    };
                    vs.push(i);
                }
                _ => {
                    panic!("as_vi32: Expected AttributeValue::N")
                }
            }
        }
    } else {
        panic!("as_vi32: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_vi8(val: AttributeValue) -> Option<Vec<i8>> {
    let mut vs: Vec<i8> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i8::from_str(s.as_str()) else {
                        panic!("parse to i8 error in as_vi8 [{:?}]", s)
                    };
                    vs.push(i);
                }
                _ => {
                    panic!("as_vi8: Expected AttributeValue::N")
                }
            }
        }
    } else {
        panic!("as_vi8: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_xf(val: AttributeValue) -> Option<Vec<i8>> {
    let mut vs: Vec<i8> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i8::from_str(s.as_str()) else {
                        panic!("parse to i8 error in as_xf [{:?}]", s)
                    };
                    vs.push(i);
                }
                _ => {
                    panic!("as_xf: Expected AttributeValue::N")
                }
            }
        }
    } else {
        panic!("as_xf: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_i8_2(val: AttributeValue) -> Option<i8> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i8::from_str(&v) else {
            panic!("as_i8_2() : failed to convert String [{:?}] to i8", v)
        };
        return Some(i);
    }
    None
}

pub fn as_u8_2(val: AttributeValue) -> Option<u8> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = u8::from_str(&v) else {
            panic!("as_u8_2() : failed to convert String [{:?}] to u8", v)
        };
        return Some(i);
    }
    None
}

pub fn as_i16_2(val: AttributeValue) -> Option<i16> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i16::from_str(v.as_str()) else {
            panic!("as_i16_2() : failed to convert String [{:?}] to i16", v)
        };
        return Some(i);
    }
    None
}

pub fn as_i32_2(val: AttributeValue) -> Option<i32> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i32::from_str(v.as_str()) else {
            panic!("as_i32_2() : failed to convert String [{:?}] to i32", v)
        };
        return Some(i);
    }
    None
}

pub fn as_i64_2(val: AttributeValue) -> Option<i64> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i64::from_str(v.as_str()) else {
            panic!("as_i64_2() : failed to convert String [{:?}] to i64", v)
        };
        return Some(i);
    }
    None
}

pub fn as_u32_2(val: AttributeValue) -> Option<u32> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = u32::from_str(v.as_str()) else {
            panic!("as_u32_2() : failed to convert String [{:?}] to u32", v)
        };
        return Some(i);
    }
    None
}

pub fn as_u64_2(val: AttributeValue) -> Option<u64> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = u64::from_str(v.as_str()) else {
            panic!("as_u64_2() : failed to convert String [{:?}] to u64", v)
        };
        return Some(i);
    }
    None
}

pub fn as_int2(val: AttributeValue) -> Option<i64> {
    if let AttributeValue::N(v) = val {
        let Ok(i) = i64::from_str(&v) else {
            panic!("as_int2() : failed to convert String [{}] to i64", v)
        };
        return Some(i);
    }
    // must be AttributeValue::Null
    None
}

pub fn as_li8(val: AttributeValue) -> Option<Vec<i8>> {
    let mut vs: Vec<i8> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_li8(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_li8(): Expected AttributeValue::N")
        };
        let Ok(f) = i8::from_str(v.as_str()) else {
            panic!("as_li8() : failed to convert String [{}] to i8", v)
        };
        vs.push(f);
    }
    Some(vs)
}

pub fn as_lu8(val: AttributeValue) -> Option<Vec<u8>> {
    let mut vs: Vec<u8> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_lu8(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_lu8(): Expected AttributeValue::N")
        };
        let Ok(f) = u8::from_str(v.as_str()) else {
            panic!("as_lu8() : failed to convert String [{}] to i8", v)
        };
        vs.push(f);
    }
    Some(vs)
}

pub fn as_li32(val: AttributeValue) -> Option<Vec<i32>> {
    let mut vs: Vec<i32> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_li32(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_li32: Expected AttributeValue::N")
        };
        let Ok(i) = i32::from_str(v.as_str()) else {
            panic!("as_li32() : failed to convert String [{}] to i32", v)
        };
        vs.push(i);
    }
    Some(vs)
}

pub fn as_lu32(val: AttributeValue) -> Option<Vec<u32>> {
    let mut vs: Vec<u32> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_lu32(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_lu32: Expected AttributeValue::N")
        };
        let Ok(i) = u32::from_str(v.as_str()) else {
            panic!("as_li32() : failed to convert String [{}] to u32", v)
        };
        vs.push(i);
    }
    Some(vs)
}

pub fn as_lusize(val: AttributeValue) -> Option<Vec<usize>> {
    let mut vs: Vec<usize> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_lusize(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_lusize: Expected AttributeValue::N")
        };
        let Ok(i) = usize::from_str(v.as_str()) else {
            panic!("as_lusize() : failed to convert String [{}] to i64", v)
        };
        vs.push(i);
    }
    Some(vs)
}

pub fn as_li64(val: AttributeValue) -> Option<Vec<i64>> {
    let mut vs: Vec<i64> = vec![];
    let AttributeValue::L(inner) = val else {
        panic!("as_li64(): Expected AttributeValue::L")
    };
    for s in inner {
        let AttributeValue::N(v) = s else {
            panic!("as_li64: Expected AttributeValue::N")
        };
        let Ok(f) = i64::from_str(v.as_str()) else {
            panic!("as_li64() : failed to convert String [{}] to i64", v)
        };
        vs.push(f);
    }
    Some(vs)
}

pub fn as_bool(val: AttributeValue) -> Option<bool> {
    match val {
        AttributeValue::Bool(bl) => Some(bl),
        AttributeValue::Null(bl) => {
            if bl == false {
                panic!("Got Null with bool of false")
            }
            None
        }
        _ => {
            panic!("as_bool(): Expected AttributeValue::Bool or ::Null")
        }
    }
}

pub fn as_blob2(val: AttributeValue) -> Option<Vec<u8>> {
    if let AttributeValue::B(blob) = val {
        return Some(blob.into_inner());
    }
    panic!("as_blob2: Expected AttributeValue::B");
}

pub fn as_lblob2(val: AttributeValue) -> Option<Vec<Vec<u8>>> {
    let mut vs: Vec<Vec<u8>> = vec![];
    if let AttributeValue::L(vb) = val {
        for v in vb {
            if let AttributeValue::B(blob) = v {
                vs.push(blob.into_inner());
            } else {
                panic!("as_lblob2: Expected AttributeValue::B");
            }
        }
    } else {
        panic!("as_lblob2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_luuid(val: AttributeValue) -> Option<Vec<Uuid>> {
    let mut vs: Vec<Uuid> = vec![];
    if let AttributeValue::L(vb) = val {
        for v in vb {
            if let AttributeValue::B(blob) = v {
                match <[u8; 16] as TryFrom<Vec<u8>>>::try_from(blob.into_inner()) {
                    Err(e) => {
                        panic!("TryFrom error in as_luuid: {:?}", e)
                    }
                    Ok(ary) => vs.push(Builder::from_bytes(ary).into_uuid()),
                }
            } else {
                panic!("as_lblob2: Expected AttributeValue::B");
            }
        }
    } else {
        panic!("as_lblob2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_uuid(val: AttributeValue) -> Option<Uuid> {
    if let AttributeValue::B(blob) = val {
        let uuid = match <[u8; 16] as TryFrom<Vec<u8>>>::try_from(blob.into_inner()) {
            Err(_) => {
                panic!("TryFrom error in as_uuid ")
            }
            Ok(ary) => Builder::from_bytes(ary).into_uuid(),
        };
        return Some(uuid);
    } else {
        panic!("as_uuid: Expected AttributeValue::B");
    }
}

pub fn as_i64(val: Option<&AttributeValue>, default: i64) -> i64 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i64>() {
                return n;
            }
        }
    }
    default
}

pub fn as_i32(val: Option<&AttributeValue>, default: i32) -> i32 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i32>() {
                return n;
            }
        }
    }
    default
}

pub fn as_i16(val: Option<&AttributeValue>, default: i16) -> i16 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<i16>() {
                return n;
            }
        }
    }
    default
}

pub fn as_lbool2(val: AttributeValue) -> Option<Vec<Option<bool>>> {
    let mut vs: Vec<Option<bool>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::Bool(bl) => vs.push(Some(bl)),
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_lbool2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_lbool2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_ln2(val: AttributeValue) -> Option<Vec<Option<String>>> {
    let mut vs: Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => vs.push(Some(s)),
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_ln2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_ln2: Expected AttributeValue::L");
    }
    Some(vs)
}

// for NULLABLE attributes - seel as_li8 for NON-NULLABLE
pub fn as_li8_2(val: AttributeValue) -> Option<Vec<Option<i8>>> {
    let mut vs: Vec<Option<i8>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i8::from_str(s.as_str()) else {
                        panic!("parse error in as_li8_2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_li8_2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_li8_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li16_2(val: AttributeValue) -> Option<Vec<Option<i16>>> {
    let mut vs: Vec<Option<i16>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i16::from_str(s.as_str()) else {
                        panic!("parse error in as_li16_2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_li16_2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_li16_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li32_2(val: AttributeValue) -> Option<Vec<Option<i32>>> {
    let mut vs: Vec<Option<i32>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i32::from_str(s.as_str()) else {
                        panic!("parse error in as_li32_2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_li32_2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_li32_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_li64_2(val: AttributeValue) -> Option<Vec<Option<i64>>> {
    let mut vs: Vec<Option<i64>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i64::from_str(s.as_str()) else {
                        panic!("parse error in as_li64_2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_li64_2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_li64_2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_lint2(val: AttributeValue) -> Option<Vec<Option<i64>>> {
    let mut vs: Vec<Option<i64>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = i64::from_str(s.as_str()) else {
                        panic!("parse error in as_lint2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_lint2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_lint2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_lfloat2(val: AttributeValue) -> Option<Vec<Option<f64>>> {
    let mut vs: Vec<Option<f64>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::N(s) => {
                    let Ok(i) = f64::from_str(&s) else {
                        panic!("parse error in as_lf64_2")
                    };
                    vs.push(Some(i));
                }
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_li16_2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_lfloat2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_lb2(val: AttributeValue) -> Option<Vec<Option<Vec<u8>>>> {
    let mut vs: Vec<Option<Vec<u8>>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::B(blob) => vs.push(Some(blob.as_ref().into())),
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_lb2: Expected AttributeValue::N"),
            }
        }
    } else {
        panic!("as_lb2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_ls2(val: AttributeValue) -> Option<Vec<Option<String>>> {
    let mut vs: Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::S(s) => vs.push(Some(s)),
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_ls2: Expected AttributeValue::S"),
            }
        }
    } else {
        panic!("as_ls2: Expected AttributeValue::L");
    }
    Some(vs)
}

pub fn as_ldt2(val: AttributeValue) -> Option<Vec<Option<String>>> {
    let mut vs: Vec<Option<String>> = vec![];
    if let AttributeValue::L(inner) = val {
        for v in inner {
            match v {
                AttributeValue::S(s) => vs.push(Some(s)),
                AttributeValue::Null(_) => vs.push(None),
                _ => panic!("as_ldt2: Expected AttributeValue::S"),
            }
        }
    } else {
        panic!("as_ldt2: Expected AttributeValue::L");
    }
    Some(vs)
}

#[derive(Debug)]
//pub struct SortK<'a>(&'a str);
pub struct SortK(String);

impl SortK {
    pub fn new(s: String) -> SortK {
        SortK(s)
    }
    pub fn string(&self) -> String {
        self.0.clone()
    }
    pub fn get_attr_sn(&self) -> &str {
        &self.0[self.0.rfind(':').unwrap() + 1..]
    }
    pub fn from_partition(&self) -> &str {
        &self.0[self.0.find('#').unwrap() + 1..]
    }
    pub fn get_partition(&self) -> &str {
        let p = &self.0[..self.0.rfind('#').unwrap()];
        &p[p.rfind('#').unwrap() + 1..]
    }
}

impl Clone for SortK {
    fn clone(&self) -> SortK {
        SortK::new(self.0.clone())
    }
}

impl<'a> From<String> for SortK {
    fn from(u: String) -> SortK {
        SortK::new(u)
    }
}

impl PartialEq for SortK {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SortK {}

impl Hash for SortK {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

struct Prefix(String);

impl Prefix {
    fn new() -> Self {
        Prefix(String::new())
    }
}

impl From<HashMap<String, AttributeValue>> for Prefix {
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        let mut prefix: Prefix = Prefix::new();
        let Some(p) = value.remove("SortK") else {
            panic!("Cannot remove 'Sortke' from HashMap")
        };
        let Some(s) = as_string(p) else {
            panic!("expected Some for as_string() got None")
        };
        prefix.0 = s;
        prefix
    }
}

#[derive(Debug)]
pub struct NodeType {
    // previous name TyName
    short: String,
    long: String,
    reference: bool,
    attrs: Option<block::AttrBlock>,
}

impl<'a> IntoIterator for &'a NodeType {
    type Item = &'a block::AttrD;
    type IntoIter = Iter<'a, block::AttrD>;

    fn into_iter(self) -> Iter<'a, block::AttrD> {
        if let None = self.attrs {
            println!(
                "IntoIterator error: type {} [{}] has no attrs",
                self.long, self.short
            )
        }
        (self.attrs).as_ref().unwrap().0.iter()
    }
}

impl PartialEq for NodeType {
    fn eq(&self, other: &Self) -> bool {
        self.long_nm() == other.long_nm()
    }
}

impl Eq for NodeType {}

impl Hash for NodeType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.long.hash(state);
    }
}

impl NodeType {
    fn new() -> Self {
        NodeType {
            short: String::new(), // long type name
            long: String::new(),  // sort type name
            reference: false,     // is type a container for reference data (i.e. static data)
            attrs: None,          // type attributes
        }
    }

    pub fn long_nm(&self) -> &str {
        return &self.long;
    }
    pub fn short_nm(&self) -> &str {
        return &self.short;
    }

    pub fn is_reference(&self) -> bool {
        return self.reference;
    }

    pub fn get_long(&self) -> &str {
        return &self.long;
    }

    pub fn get_short(&self) -> &str {
        return &self.short;
    }

    pub fn is_atttr_nullable(&self, attr_sn: &str) -> bool {
        for attr in self {
            //for attr in self {
            if attr.c == attr_sn {
                return attr.nullable;
            }
        }
        panic!(
            "is_atttr_nullable() not cached for [{}] [{}]",
            attr_sn, self.long
        );
    }

    pub fn get_attr_nm(&self, attr_sn: &str) -> &str {
        // TODO: Result(&str,Error)

        for attr in self {
            if attr.c == attr_sn {
                return &attr.name;
            }
        }
        panic!(
            "get_attr_nm() - attribute [{}] not in node type [{}]",
            attr_sn, self.long
        );
    }

    pub fn add_rvs_edge(&self, name : &str) -> bool {

        for v in self {
            if v.dt.as_str() == "Nd" && name == v.name {
                return v.rvsEdge
            }
        }
        panic!("Expected to find edge {}",name);
    }

    pub fn get_attr_sn(&self, attr_nm: &str) -> &str {
        for attr in self {
            if attr.name == attr_nm {
                return &attr.c;
            }
        }
        panic!(
            "get_edge_attr() - attribute [{}] not found in node type [{}]",
            attr_nm, self.long
        );
    }

    pub fn get_scalars(&self) -> HashMap<String, Vec<&str>> {
        let mut scalar_partitions: HashMap<String, Vec<&str>> = HashMap::new();

        for v in self {
            if v.dt.as_str() != "Nd" && v.pg {
                scalar_partitions
                    .entry(v.p.clone())
                    .and_modify(|r| {
                        r.push(&v.c[..]);
                    })
                    .or_insert(vec![&v.c[..]]);
            }
        }
        scalar_partitions
    }

    pub fn get_edge_child_ty(&self, edge: &str) -> &str {
        //for attr in (&self.attrs).as_ref().unwrap().0.iter()  {
        for attr in self {
            if attr.name == edge && attr.dt == "Nd" {
                return &attr.ty;
            }
        }
        panic!(
            "get_edge_child_ty() - [{}] is not an edge attribute in node type [{}]",
            edge, self.long
        );
    }

    pub fn get_attr_dt(&self, attr_sn: &str) -> &str {
        for attr in self {
            if attr.c == attr_sn {
                return &attr.dt;
            }
        }
        panic!("get_attr_dt() not cached for [{}] [{}]", attr_sn, self.long);
    }

    pub fn edge_cnt(&self) -> usize {
        self.into_iter()
            .filter(|&v| v.dt == "Nd")
            .fold(0, |n, _| n + 1)
    }

    pub fn get_11(&self) -> Vec<&block::AttrD> {
        self.into_iter()
            .filter(|&block::AttrD { card: x, .. }| x == "1:1")
            .collect()
    }
}

// From used by both DataType and Graph queries - populated from GraphSS - GoGraph data types
impl From<HashMap<String, AttributeValue>> for NodeType {
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        // ownerships transferred from AttributeValue into NodeType
        let mut ty = NodeType::new();

        for (k, v) in value.drain() {
            match k.as_str() {
                "PKey" => {}
                "SortK" => ty.short = as_string(v).unwrap(),
                "Name" => ty.long = as_string(v).unwrap(),
                "Reference" => match as_bool(v) {
                    Some(bl) => ty.reference = bl,
                    None => {
                        panic!("From AV for NodeType expected bool got None")
                    }
                },
                "OvBs" => {}
                // block::ND => { match as_luuid(v) {
                //                   Some(u) => ty.nd = u,
                //                   None => panic!("From AV for Node, expected Vec<Uuid> got Null"),
                //                 }
                //             },
                // block::XF =>  { match as_li8(v) {
                //                 Some(u) => ty.xf = u,
                //                 None => panic!("From AV for Node, expected Vec<i8> got Null"),
                //                 }
                //             },
                // block::TY => {
                //         match as_string(v) {
                //             Some(s) => {ty.short=s},
                //             None => panic!("From AV for NodeType Ty, expected string got Null")
                //         }
                //      },
                _ => panic!("NodeType from impl: unexpected attribute got [{}]", k),
            }
        }
        ty
    }
}

pub struct NodeTypes(pub Vec<NodeType>);

impl<'a> NodeTypes {
    pub fn get(&self, ty_nm: &str) -> &NodeType {
        for ty in self.0.iter() {
            if ty.short == ty_nm {
                return ty;
            }
        }
        for ty in self.0.iter() {
            if ty.long == ty_nm {
                return ty;
            }
        }
        panic!("get error: Node type [{}] not found", ty_nm);
    }

    pub fn set_attrs(&'a mut self, ty_nm: String, attrs: block::AttrBlock) {
        //-> Result<(),Error> {

        for ty in self.0.iter_mut() {
            if ty.long == ty_nm {
                ty.attrs = Some(attrs);
                return;
            }
        }
        for ty in self.0.iter_mut() {
            if ty.short == ty_nm {
                ty.attrs = Some(attrs);
                return;
            }
        }
        //Error::Err("get error: Node type [{}] not found",ty_nm);
    }

    //                                    Types containing 11 attrs        Types containing types that contain 11.
    pub fn get_types_with_11_types(
        &self,
    ) -> (
        HashMap<&NodeType, Vec<&block::AttrD>>,
        HashMap<&NodeType, Vec<&block::AttrD>>,
    ) {
        let mut ty_with_11: HashMap<&NodeType, Vec<&block::AttrD>> = HashMap::new();
        let mut ty_with_ty_11: HashMap<&NodeType, Vec<&block::AttrD>> = HashMap::new();

        //self.0.iter().get_11().iter().filter(|&v| v.len() > 0).map(|&v| ty_with_11.insert(v.long))

        for t in self.0.iter() {
            let c = t.get_11();
            if c.len() > 0 {
                ty_with_11.insert(t, c);
            }
        }
        for node_ty in self.0.iter() {
            for a in node_ty {
                for (&k, _) in &ty_with_11 {
                    if a.ty == k.long_nm() {
                        ty_with_ty_11
                            .entry(node_ty)
                            .and_modify(|v| v.push(a))
                            .or_insert(vec![a]);
                    }
                }
            }
        }
        (ty_with_ty_11, ty_with_11)
    }
}

pub async fn fetch_graph_types(
    client: &aws_sdk_dynamodb::Client,
    graph: String,
    //mut tyall: block::AttrItemBlock,
) -> Result<(Arc<NodeTypes>, String), aws_sdk_dynamodb::Error> {
    let mut ty_c_: HashMap<String, block::AttrBlock> = HashMap::new();
    let table_name = "GoGraphSS";
    println!("Fetch Fetch Graph Short Name ....[{}]", &graph);
    // ================================================================
    // Fetch Graph Short Name (used as prefix in some PK values)
    // ================================================================
    let results = client
        .query()
        .table_name(table_name)
        .projection_expression("SortK")
        .key_condition_expression("PKey =  :pkey")
        .expression_attribute_values(":pkey", AttributeValue::S("#Graph".to_string()))
        .filter_expression("#nm = :graph")
        .expression_attribute_names("#nm", "Name") // x Name
        .expression_attribute_values(":graph", AttributeValue::S(graph.clone()))
        .send()
        .await?;

    // TODO: handle no-data-found
    let graph_prefix_wdot: String = if let Some(items) = results.items {
        if items.len() != 1 {
            panic!("graph prefix query expected 1 item got [{}]", items.len());
        }
        let mut ty_prefix: Prefix = items.into_iter().next().unwrap().into();
        ty_prefix.0.push('.');
        ty_prefix.0
    } else {
        panic!("graph prefix query returned Option::None");
    };
    println!("graph_prefix_wdot [{}]", graph_prefix_wdot);
    // ==============================
    // Fetch Node Types
    // ==============================
    println!("Fetch Node Types  ....");
    let mut tys = String::new();
    tys.push('#');
    tys.push_str(&graph_prefix_wdot);
    tys.push('T');

    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("#nm = :prefix")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(tys))
        .send()
        .await?;

    let nt: Vec<NodeType> = if let Some(items) = results.items {
        let ty_names_v: Vec<NodeType> = items.into_iter().map(|v| v.into()).collect();
        ty_names_v
    } else {
        vec![]
    };
    if nt.len() == 0 {
        panic!("fetch node type data failed. ")
    }
    let mut ty_cache = NodeTypes(nt);

    // also package as HashSet
    let mut set = HashSet::<String>::new();
    for k in &ty_cache.0 {
        set.insert(k.long.clone());
    }
    println!("Fetch Attributes for all Node Types...");
    // ===================================
    // Fetch Attributes for all Node Types
    // ===================================
    let results = client
        .scan()
        .table_name(table_name)
        .filter_expression("begins_with(#nm, :prefix)")
        .expression_attribute_names("#nm", "PKey")
        .expression_attribute_values(":prefix", AttributeValue::S(graph_prefix_wdot.clone()))
        .send()
        .await?;

    // TODO Handle error
    let ty_all = if let Some(items) = results.items {
        let ty_block_v: Vec<block::AttrItem> = items.into_iter().map(|v| v.into()).collect(); // dot takes mut v
        block::AttrItemBlock(ty_block_v)
    } else {
        block::AttrItemBlock(vec![])
    };
    if ty_all.0.len() == 0 {
        panic!("fetch attribute data failed.")
    }
    println!("package into NodeTypes cache...");
    // =================================
    // package into NodeTypes cache
    // =================================
    for v in ty_all.0 {
        //let mut a = block::AttrD::new();

        let ty_ = v.ty.clone().unwrap();

        let a: block::AttrD = if ty_[0..1].contains('[') {
            // equiv: *v.ty.index(0..1).   Use of Index trait which has generic that itself is a SliceIndex trait which uses Ranges...
            block::AttrD {
                name: v.attr.unwrap(), // TODO: v.clone_attr(),v.get_attr()
                dt: "Nd".to_string(),
                c: v.c.unwrap(),
                ty: ty_[1..ty_.len() - 1].to_string(), //nm.clone(), //"XX".to_string(),// v.ty.unwrap()[1..v.ty.unwrap().len() - 1].to_string(),
                p: v.p.unwrap(),
                pg: v.pg.unwrap_or(false),
                nullable: v.n.unwrap_or(false),
                //incP: v.incp,
                ix: v.ix.unwrap_or(String::new()),
                card: "1:N".to_string(),
                rvsEdge: v.rvsEdge.unwrap_or(false),
            }
        } else {
            // check if Ty is a tnown Type
            if set.contains(&ty_) {
                block::AttrD {
                    name: v.attr.unwrap(),
                    dt: "Nd".to_string(),
                    c: v.c.unwrap(),
                    ty: ty_,
                    p: v.p.unwrap(),
                    pg: v.pg.unwrap_or(false),
                    nullable: v.n.unwrap_or(false),
                    //incp: v.incp,
                    ix: v.ix.unwrap_or(String::new()),
                    card: "1:1".to_string(),
                    rvsEdge: v.rvsEdge.unwrap_or(false),
                }
            } else {
                // scalar
                block::AttrD {
                    name: v.attr.unwrap(),
                    dt: v.ty.unwrap(),
                    c: v.c.unwrap(),
                    p: v.p.unwrap(),
                    nullable: v.n.unwrap_or(false),
                    pg: v.pg.unwrap_or(false),
                    //incp: v.incp,
                    ix: v.ix.unwrap_or(String::new()),
                    card: "".to_string(),
                    ty: "".to_string(),
                    rvsEdge: v.rvsEdge.unwrap_or(false),
                }
            }
        };

        let mut nm = v.nm.unwrap();
        nm.drain(0..nm.find('.').unwrap() + 1);

        // group AttrD by v.nm (PK in query)
        if let Some(c) = ty_c_.get_mut(&nm[..]) {
            c.0.push(a);
        } else {
            ty_c_.insert(nm, block::AttrBlock(vec![a]));
        }
    }
    // repackage (& consume) ty_c_ into NodeTypes
    for (k, v) in ty_c_ {
        ty_cache.set_attrs(k, v);
    }
    println!("exit fetch_graph_types..");
    Ok((Arc::new(ty_cache), graph_prefix_wdot))
}