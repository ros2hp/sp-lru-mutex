#![warn(unused_variables)]
#![warn(dead_code)]
use super::{
    as_blob2, as_bool, as_float2, as_i16, as_i16_2, as_i32_2, as_i8_2, as_int2, as_lb2, as_lblob2,
    as_lbool2, as_ldt2, as_lfloat2, as_li16_2, as_li32_2, as_li8_2, as_lint2, as_ln2, as_ls2,
    as_luuid, as_n, as_string, as_string_trim_graph, as_uuid, as_vi32, as_vi8,
};
use aws_sdk_dynamodb::types::AttributeValue;
use std::str::FromStr;
#[warn(non_camel_case_types)]
use std::{collections::HashMap};
//mod block {. // do not include in mod definition file
use uuid::{self, Uuid}; //, as_vec_string};

//               Dynamo Attr
pub const PK: &str = "PK";
pub const SK: &str = "SK";
// Ty item
pub const GRAPH: &str = "graph";
pub const ISNODE: &str = "isNode";
pub const PARENT: &str = "Parent";
pub const IX: &str = "ix";
//const TY : &str = "Ty";
// attribute name - stored in P_S, P_N, P_Bl index
// scalars
//const N : &str = "N";
// const F : &str = "F";                // use N to feed P_N index
// const I : &str = "I";                // use N to feed P_N index
pub const N: &str = "N";
pub const S: &str = "S";
pub const BL: &str = "Bl";
pub const B: &str = "B";
pub const DT: &str = "DT";
pub const P: &str = "P"; // attribute name feeds P_S, P_N, P_? indexes
pub const TY: &str = "Ty"; // item type (GoGraph system type)
                           //pub const TYA : &str = "TyA";           // attribute type/storage type .e.g LF, LI, LBl etc
pub const E: &str = "E";
// scalar sets
pub const SB: &str = "SB";
pub const SN: &str = "SN";
pub const SS: &str = "SS";
pub const SBL: &str = "SBL";
// Edge
pub const CNT: &str = "Cnt"; // edge count
pub const ND: &str = "Nd";
pub const BID: &str = "Bid"; // Batch id in OvB otherwise 0
pub const XF: &str = "Xf"; // how to interrupt value in Nd: child node, OvB node, deleted node, etc
pub const OVB: &str = "OvB";

// Propagated Scalars, scalar lists (determined by SK value)
pub const LS: &str = "LS";
pub const LN: &str = "LN";
pub const LB: &str = "LB";
pub const LBL: &str = "LBl";
pub const LDT: &str = "LDT";
// overflow
pub const OP: &str = "OP"; // overflow parent UUID
                           // reverse
pub const TUID: &str = "TUID";
pub const COMMENT: &str = "Comment";
pub const TYIX: &str = "TyIx";

// Reverse edge
//pub const CNT: &str = "Cnt";
pub const TARGET_UID: &str = "Target_uid";
pub const TARGET_BID: &str = "Target_bid";
pub const TARGET_ID: &str = "Target_id";
// Reverse edge Overflow metadata - stored with child reverse edge only when OvB used
//pub const OVB : &str = "OVB";     // List of UUIDs for OvBs
pub const OVB_BID: &str = "OBID"; // List of current Batch ID in each OvB
//pub const OVB_ID: &str = "OID"; // List of current index for each Batch
pub const OVB_CUR: &str = "OCUR"; // ovb last used
pub const OVB_CNT: &str = "OCNT"; // cnt in current overflow batch in current ovb

// XF values
pub const CHILD: i8 = 1;
pub const CHILD_INUSE: i8 = 2;
pub const CHILD_DETACHED: i8 = 3;
pub const OVB_: i8 = 4;
pub const OVB_INUSE: i8 = 5;
pub const OVB_THRESHOLD_HIT: i8 = 6;

//pub static LOAD_PROJ : LazyLock<String> = LazyLock::new(||"OvB".to_string() +  "," + OVB_BID + "," + OVB_ID + "," + OVB_CUR);

#[derive(Debug)]
pub struct SK_(pub String);

impl<'a> SK_ {
    // get attribute short name
    pub fn attribute_sn(&'a self) -> &str {
        &self.0[self.0.rfind(':').unwrap() + 1..]
    }
    pub fn get_edge_sn(&self) -> &str {
        let sk = &self.0;
        let mut start = 0;
        let mut end = 0;
        let mut cnt: u8 = 0;
        for (i, v) in sk.chars().enumerate() {
            if v == '#' {
                //
                cnt += 1;
                if cnt == 2 {
                    start = i + 2;
                }
                if cnt == 3 {
                    end = i;
                    break;
                }
            }
        }
        if start == 0 || end == 0 {
            panic!("SK_::get_edge_sn() : malformed sk value [{}]", sk);
        }

        &sk[start..end]
    }
}

// DataItem  maps database attribute (and its associated type) to golang type.
// Used during the database fetch of node block data into the memory cache.
// ToStruct(<dataItem>) for Spanner and dynamodbattribute.UnmarshalListOfMaps(result.Items, &data) for dynamodb.
// Data in then used to populate the NV.value attribute in the UnmarshalCache() ie. from []DataItem -> NV.value
// via the DataItem Get methods
#[allow(dead_code)]
pub struct DataItem {
    pub pk: Vec<u8>, // pk,sk form composite key
    pub sk: SK_,
    // Ty item
    pub graph: Option<String>,
    pub is_node: Option<bool>,
    pub ix: Option<String>, // used during double-propagation load "X": not processed, "Y": processed
    // scalar types
    pub n: Option<String>, // number type. No conversion from db storage format. Used for bulk loading operations only.
    pub f: Option<f64>,    // Nullable type has None for Null values
    pub i: Option<i64>,    // Nullable type has None for Null values

    pub s: Option<String>,  // string
    pub bl: Option<bool>,   // boolean
    pub b: Option<Vec<u8>>, // byte array
    pub dt: Option<String>, // DateTime
    pub p: Option<String>,  // attribute name as used in P_S, P_N, P_B global indexes
    pub ty: Option<String>, // type of node long name [and attribute e.g. Pf#N>, P#D (persisted in db>, alternative to cache)
    //    pub tya : Option<String>,         // item (attribute) type short name>, I>, F>, S, SS, B, SB etc telss From(below) hos to interpret dataitem
    pub e: Option<String>, // used for attributes populating ElasticSearch
    // List for scalar and propagation
    // pub lf  : Option<Vec<Option<f64>>>,     // Vec<Option<?>> for potential for Null value for nullable scalar types. No null vallues for simple scalar list.
    // pub li  : Option<Vec<Option<i64>>>,
    pub ln: Option<Vec<Option<String>>>,
    pub ls: Option<Vec<Option<String>>>,
    pub lb: Option<Vec<Option<Vec<u8>>>>,
    pub lbl: Option<Vec<Option<bool>>>,
    pub ldt: Option<Vec<Option<String>>>,
    //    pub lnu : Option<Vec<bool>>,    // nullables only, otherwise None.  true associated value is null. Ingore entry. False: value is valid.
    // Set scalar
    pub sb: Option<Vec<Vec<u8>>>,
    pub sn: Option<Vec<String>>,
    pub sbl: Option<Vec<bool>>,
    pub ss: Option<Vec<String>>,
    //  Edge
    pub cnt: Option<i64>,      // edge count
    pub nd: Option<Vec<Uuid>>, //uuid.UID // list of node UIDs>, overflow block UIDs>, oveflow index UIDs
    pub xf: Option<Vec<i8>>, // flag: used in uid-predicate 1 : c-UID>, 2 : c-UID is soft deleted>, 3 : ovefflow UID>, 4 : overflow block full
    pub bid: Option<Vec<i32>>, // current maximum overflow batch id.
    pub ovb: Option<bool>,
    // overflow
    pub op: Option<Uuid>, // assoc Parent UUID
    // double propagation
    // reverse edge
    pub tuid: Option<Uuid>,
    pub tyix: Option<String>,
}

impl DataItem {
    // ********************************
    // Null value represented by None
    // ********************************
    pub fn new() -> Self {
        DataItem {
            pk: vec![], // try using zero values for type instead of using Option::None. see if this works.
            sk: SK_(String::new()),
            // node type item - maybe removed
            graph: None,
            is_node: None,
            ix: None,
            // scalars
            i: None, // internal, db uses attribute N
            f: None, // internal, db uses attribute N
            n: None, // copy of N - useful when no conversion is necessary
            s: None,
            bl: None,
            b: None,
            dt: None,
            p: None,
            ty: None, // node type
            //            tya: None,  // storage attribute used
            e: None,
            //            nul: false,
            // List scalar and propagated
            // lf: None,
            // li: None,
            ln: None,
            ls: None,
            lb: None,
            lbl: None,
            ldt: None,
            //           lnu: None,
            // Sets scalar
            sb: None,
            ss: None,
            sn: None,
            sbl: None,
            //edge
            cnt: None,
            nd: None,
            xf: None,
            bid: None,
            ovb: None,
            // overflow
            op: None,
            // reverse
            tuid: None,
            tyix: None,
        }
    }

    // unwrap methods
    pub fn get_sk(&self) -> &str {
        &self.sk.0[..]
        // let SK_(sk) = self.sk; // (ref sk) is defaulted
        // sk
    }
    pub fn get_graph(&self) -> &str {
        let Some(ref x) = self.graph else {
            panic!("get_graph(): expected String got None")
        };
        x
    }
    pub fn is_node(&self) -> bool {
        let Some(x) = self.is_node else {
            panic!("get_is_node(): expected bool got None")
        };
        x
    }

    pub fn get_s(&self) -> &str {
        let Some(ref x) = self.s else {
            panic!("get_n(): expected String got None")
        };
        x
    }
    pub fn get_mut_s(&mut self) -> &mut str {
        let Some(ref mut x) = self.s else {
            panic!("get_n(): expected String got None")
        };
        x
    }
    pub fn get_e(&self) -> &str {
        let Some(ref x) = self.e else {
            panic!("get_n(): expected String got None")
        };
        x
    }
    pub fn get_p(&self) -> &str {
        let Some(ref x) = self.p else {
            panic!("get_n(): expected String got None")
        };
        x
    }
    pub fn get_mut_p(&mut self) -> &mut str {
        let Some(ref mut x) = self.p else {
            panic!("get_n(): expected String got None")
        };
        x
    }
    // number
    // pub fn get_n(&self) -> &str {
    //     let Some(ref x) = self.n else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // // i8
    // pub fn get_i8(&self) -> &i8 {
    //     match self {
    //         None =>
    //     }
    //     let Some(ref x) = i8::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // pub fn get_mut_i8(&mut self) -> &mut i8 {
    //     let Some(ref mut x) =  i8::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // // i16
    // pub fn get_i16(&self) -> &i16 {
    //     let Some(ref x) = i16::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // pub fn get_mut_i16(&mut self) -> &mut i16 {
    //     let Some(ref mut x) =  i16::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // // i32
    // pub fn get_i32(&self) -> &i32 {
    //     let Some(ref x) = i32::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // pub fn get_mut_i32(&mut self) -> &mut i32 {
    //     let Some(ref mut x) =  i32::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // // i64
    // pub fn get_i64(&self) -> &i64 {
    //     let Some(ref x) = i64::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }
    // pub fn get_mut_i64(&mut self) -> &mut i64 {
    //     let Some(ref mut x) =  i64::from_str(&self.n) else { panic!("get_n(): expected i64 got None") };
    //     x
    // }

    // Li64, Li32, Li16, Li8
    pub fn get_mut_li64(&mut self, null_value: i64) -> Vec<i64> {
        let mut i_out: Vec<i64> = vec![];
        let Some(ref x) = self.ln else {
            panic!("get_n(): expected Vec<i64> got None")
        };
        for v in x {
            match v {
                None => i_out.push(null_value),
                Some(i_val) => match i64::from_str(i_val) {
                    Err(e) => {
                        panic!(
                            "Error in get_mut_li64() - failed to parse [{}] to i64. {}",
                            i_val, e
                        )
                    }
                    Ok(i_ok) => i_out.push(i_ok),
                },
            }
        }
        i_out
    }
    // Lf64
    // CNT
    // pub fn get_mut_cnt(&self) -> &Vec<Vec<u8>> {
    //     let Some(ref x) = self.nd else { panic!("get_n(): expected Vec<Vec<u8>> got None") };
    //     x
    // }
    // // Nd
    // pub fn get_nd(&self) -> &Vec<Vec<u8>> {
    //     let Some(ref x) = self.nd else { panic!("get_n(): expected Vec<Vec<u8>> got None") };
    //     x
    // }
    // pub fn get_mut_nd(&mut self) -> &mut Vec<Vec<u8>> {
    //     let Some(ref mut x) = self.nd else { panic!("get_n(): expected Vec<Vec<u8>> got None") };
    //     x
    // }
    // // Ls
    // pub fn get_ls(&self) -> &Vec<String> {
    //     let Some(ref x) = self.ls else { panic!("get_n(): expected Vec<String> got None") };
    //     x
    // }
    // pub fn get_mut_ls(&mut self) -> &mut Vec<String> {
    //     let Some(ref mut x) = self.ls else { panic!("get_n(): expected Vec<String> got None") };
    //     x
    // }
}

impl From<HashMap<String, AttributeValue>> for DataItem {
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        // zero allocations by transfering ownership from AttributeValue to AttrItem fields.
        // use into_iter() in query.

        let mut di = DataItem::new();

        // get SK first - to determine item type
        if let Some(v) = value.remove(SK) {
            di.sk = SK_(as_string(v).unwrap());
        }
        // get TyA  - to determine whether N attribute contains Int or Float values
        // if let Some(v) = value.remove(TYA) {
        //     di.tya = as_string(v);
        // }
        // if let Some(v) = value.remove(NUL) {
        //     di.nul = as_bool(v);
        // }

        for (k, v) in value {
            match k.as_str() {
                PK => {
                    di.pk = as_blob2(v).unwrap();
                }
                // node type item - maybe removed
                GRAPH => {
                    di.graph = as_string(v);
                }
                ISNODE => {
                    di.is_node = as_bool(v);
                }
                IX => {
                    di.ix = as_string(v);
                } // used during double-propagation load "X": not processed, "Y": processed
                // scalars
                N => di.n = as_n(v),
                P => di.p = as_string(v),
                S => di.s = as_string(v),
                BL => di.bl = as_bool(v),
                B => di.b = as_blob2(v),
                DT => di.dt = as_string(v),            // DateTime
                TY => di.ty = as_string_trim_graph(v), // type of node (stored with each scalar item)
                E => di.e = as_string(v),
                // lists
                LS => di.ls = as_ls2(v),
                LN => di.ln = as_ln2(v),
                LB => {
                    di.lb = as_lb2(v);
                }
                LBL => {
                    di.lbl = as_lbool2(v);
                }
                LDT => {
                    di.ldt = as_ldt2(v);
                }
                // sets scalar
                // SB => { di.sb= as_sb2(v) },
                // SS => { di.ss },
                // SN => { di.sn },
                // SBL=> { di.sbl },
                // edge
                CNT => {
                    di.cnt = as_int2(v);
                }
                ND => {
                    di.nd = as_luuid(v);
                } //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
                //XBL => { di.xbl = as_lbool2(v) }, // used for propagated child scalars (List data). True means associated child value is NULL (ie. is not defined)
                //               LNU => { di.lnu = as_lbool2(v); },
                XF => {
                    di.xf = as_vi8(v);
                } // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
                BID => {
                    di.bid = as_vi32(v);
                } // current maximum overflow batch id. Maps to the overflow item number in Overflow block e.g. A#G:S#:A#3 where Id is 3 meaning its the third item in the overflow block. Each item containing 500 or more UIDs in Lists.
                // overflow
                OP => {
                    di.op = as_uuid(v);
                } // parent UID in overflow blocks
                OVB => {}
                TYIX => {
                    di.tyix = as_string_trim_graph(v);
                }
                _ => panic!("unexpected attribute name in DataItem From impl [{}]", k),
            }
        }
        di
    }
}

pub struct NodeCache(pub HashMap<String, DataItem>);

//=============== AttrItem  ========================================================

#[derive(Debug)]
pub struct AttrItem {
    pub nm: Option<String>,     //`dynamodbav:"PKey"`  // type name
    pub attr: Option<String>,   //`dynamodbav:"SortK"` // attribute name
    pub ty: Option<String>,     // DataType
    pub f: Option<Vec<String>>, // facets name#DataType#CompressedIdentifer
    pub c: Option<String>,      // short name for attribute
    pub p: Option<String>, // data partition containig attribute data - TODO: is this obselete???
    pub pg: Option<bool>,  // true: propagate scalar data to parent
    pub n: Option<bool>, // NULLABLE. False : not null (attribute will always exist ie. be populated), True: nullable (attribute may not exist)
    //    pub cd: Option<i16>, // cardinality - NOT USED
    //    pub sz: Option<i16>, // average size of attribute data - NOT USED
    pub ix: Option<String>, // supported indexes: FT=Full Text (S type only), "x" combined with Ty will index in GSI Ty_Ix
                            //	pub incp: Vec<String>, // (optional). List of attributes to be propagated. If empty all scalars will be propagated.
                            //	cardinality string   // 1:N , 1:1
    pub rvsEdge: Option<bool>,
}

impl AttrItem {
    pub fn new() -> Self {
        AttrItem {
            nm: None,
            attr: None,
            ty: None,
            f: None,
            c: None,
            p: None,
            pg: None,
            n: None,
            //          cd : None,
            //          sz : None,
            ix: None,
            rvsEdge: None,
        }
    }
}

impl From<HashMap<String, AttributeValue>> for AttrItem {
    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        // zero allocations by transfering ownership from AttributeValue to AttrItem fields.
        // use into_iter() in query.

        let mut item = AttrItem::new();

        for (k, v) in value.drain() {
            match k.as_str() {
                "PKey" => item.nm = as_string(v),
                "SortK" => item.attr = as_string(v),
                "Ty" | "ty" => item.ty = as_string(v),
                "f" => item.ty = None,
                "C" => item.c = as_string(v),
                "P" => item.p = as_string(v),
                "Pg" => item.pg = as_bool(v),
                "N" => item.n = as_bool(v),
                //"Cd" => item.cd = as_string(v),
                //"Sz" => item.sz = as_string(v),
                "Ix" => item.ix = as_string(v),
                "IncP" => println!("IncP not used..."),
                COMMENT => {}
                "RvsEdge" => item.rvsEdge = as_bool(v),
                &_ => panic!("unexpected attribute name in AttrItem From impl [{}]", k),
            }
        }
        item
    }
}

pub struct AttrItemBlock(pub Vec<AttrItem>);

//=============== AttrD (D for derived-type) ========================================================

// type attribute-block-derived from AttrItem
#[derive(Debug, Clone)]
pub struct AttrD {
    pub name: String,   // Attribute Identfier
    pub dt: String, // Derived value. Attribute Data Type - Nd (for uid-pred attribute only), (then scalars) DT,I,F,S,LI,SS etc
    pub c: String,  // Attribute short identifier
    pub ty: String, // For edge only, the type it represents e.g "Person"
    pub p: String,  // data partition (aka shard) containing attribute
    pub nullable: bool, // true: nullable (attribute may not exist) false: not nullable
    pub pg: bool,   // true: propagate scalar data to parent
    //	pub incp : Vec<String>,
    pub ix: String, // index type
    pub card: String,
    pub rvsEdge: bool,   // sp load will populate a reverse edge on child node (defined on edge attribute of parent node)
}

impl AttrD {
    pub fn new() -> Self {
        AttrD {
            name: String::new(),
            dt: String::new(),
            c: String::new(),
            ty: String::new(), // edge only attr: child type long name
            p: String::new(),
            nullable: false, // true: nullable (attribute may not exist) false: not nullable
            pg: true,        // true: propagate scalar data to parent
            //	pub incp : Vec<String>,
            ix: String::new(),
            card: String::new(),
            rvsEdge: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttrBlock(pub Vec<AttrD>);

impl AttrBlock {
    pub fn get_edges_sn(&self) -> Vec<&str> {
        let mut predc: Vec<&str> = vec![];
        for v in &self.0 {
            if v.dt == "Nd" {
                predc.push(&v.c);
            }
        }
        predc
    }

    pub fn new() -> AttrBlock {
        AttrBlock(vec![])
    }
}