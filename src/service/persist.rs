// de-service persist. Make calls to service via mutex sync'd calls.
// queuing requests to persist running as a service is causing issues with 
// 1. bug in tokio not processing a submit request (for some reason) when running in "biased" mode
// 2. order of processing of requests to persist when not running in "biased" mode and use 
//     of random number generator to determine order. Causes no-data-found in map because
//     submit and complete requests not processed in order.
use crate::types;

use crate::node::RNode;
use crate::service::stats::{Waits,Event};
//use crate::{QueryMsg, RKey};
use crate::rkey::RKey;

use std::collections::{HashMap, HashSet, VecDeque};

use std::sync::Arc;

use std::mem;
//
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::operation::update_item::{UpdateItemError, UpdateItemOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use aws_smithy_runtime_api::client::result::SdkError;

use tokio::task;
use tokio::time::{self, Duration, Instant};
use tokio::sync::Mutex;

use uuid::Uuid;

const MAX_PRESIST_TASKS: usize = 2;


//  container for clients querying persist service
struct WaitClient(HashMap<RKey, tokio::sync::mpsc::Sender<bool>>);

impl WaitClient {
    fn new() -> Self {
        WaitClient(HashMap::new())
    }
}

pub struct Persist {
     //active: HashMap<RKey, Arc<Mutex<RNode>>>
     active: HashSet<RKey>
    ,queue: VecDeque<RKey>
    ,lookup: HashMap<RKey, Arc<Mutex<RNode>>>
    ,waiting_client: HashMap<RKey, tokio::sync::mpsc::Sender<()> >
    //
    ,dyn_client: DynamoClient
    ,table_name: String
    ,waits: Waits
}

impl Persist {

    pub fn new(dyn_client : DynamoClient
              , table_name : impl Into<String>
              , waits : Waits
    ) -> Arc<Mutex<Self>> {

        Arc::new(Mutex::new(
            Persist{
                active: HashSet::new(),
                queue: VecDeque::new(),
                lookup: HashMap::new(),
                waiting_client:  HashMap::new(),
                //
                dyn_client,
                table_name: table_name.into(),
                waits,
            }
            ))
    }
}

impl Persist {

    pub fn remove(&mut self, rkey: &RKey) {
        let mut ri = 0;
        let mut found = false;

        for (i,v) in self.queue.iter().enumerate() {
            if *v == *rkey {
                ri = i;
                found = true;
                break;
            }
        }
        if found {
            self.queue.remove(ri);
        }
    }

    fn spawn_write(&mut self
                , rkey: RKey
                , arc_node: Arc<Mutex<RNode>>
                , persist : Arc<Mutex<Persist>>
    ) {
        
        self.active.insert(rkey.clone());

        let dyn_client = self.dyn_client.clone();
        let table_name = self.table_name.clone();
        let waits= self.waits.clone();
        let persist_ = persist.clone();
    
        println!("PERSIST : persist_rnode task created");
        tokio::spawn(async move {
            // save Node data to db
            persist_rnode(
                &dyn_client
                ,table_name
                ,arc_node
                ,persist_
                ,waits
            )
            .await;
        });
    }


    pub fn submit(&mut self
        , rkey: RKey
        , arc_node: Arc<Mutex<RNode>>
        , persist : Arc<Mutex<Persist>>
    ) {
  
    // persisting arc_node for given rkey
    if self.active.len() >= MAX_PRESIST_TASKS {
        // maintain a FIFO of evicted nodes
        self.queue.push_front(rkey.clone());             
        self.lookup.insert(rkey.clone(), arc_node);
              
        println!("PERSIST: submit - max tasks reached add {:?} queue {} ",rkey, self.queue.len());

    } else {

        self.spawn_write(rkey, arc_node, persist);

    }               
    println!("PERSIST: submit - Exit");

    }

    pub fn inuse(&mut self, rkey: RKey, client_ch : tokio::sync::mpsc::Sender<()>) -> bool {

        if let Some(_) = self.active.get(&rkey) {
            // register for notification of persist completion.
            self.waiting_client.insert(rkey, client_ch);
            return true;
        } 
        if let Some(_) = self.lookup.get(&rkey) {
            // register for notification of persist completion.
            self.waiting_client.insert(rkey, client_ch);
            return true;
        } 
        false
    }


    async fn completed(&mut self
                ,rkey : RKey 
                ,persist : Arc<Mutex<Persist>>
    ) {
        println!("PERSIST: completed  for {:?} tasks [{}]",rkey, self.active.len());
    
        self.active.remove(&rkey);        

        // send ack to client if one is waiting on query channel
        //println!("PERSIST : send complete persist ACK to client - if registered. {:?}",persist_rkey);
        if let Some(client_ch) = self.waiting_client.get(&rkey) {
            //println!("PERSIST :   Yes.. ABOUT to send ACK to query that persist completed ");
            // send ack of completed persistion to waiting client
            if let Err(err) = client_ch.send(()).await {
                panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
            }
            //
            self.waiting_client.remove(&rkey);
            //println!("PERSIST  EVIct: ABOUT to send ACK to query that persist completed - DONE");
        }

        // process next node in  Pending Queue
        if let Some(queued_rkey) = self.queue.pop_back() {

            println!("PERSIST: start persist task from persist_q. {:?} tasks {} queue size {}",queued_rkey, self.active.len(), self.queue.len() );

            let Some(arc_node) = self.lookup.get(&queued_rkey) else {println!("Persist: Error - expected arc_node in Persisting {:?}",queued_rkey);
                                                                                        panic!("Persist: expected arc_node in Persisting {:?}",queued_rkey)};

            self.spawn_write(queued_rkey.clone(), arc_node.clone(), persist);
            self.lookup.remove(&queued_rkey);
        }

        println!("PERSIST finished completed msg:  key {:?}  ", rkey);
}

}

async fn persist_rnode(
    dyn_client: &DynamoClient,
    table_name_: impl Into<String>,
    arc_node: Arc<tokio::sync::Mutex<RNode>>,
    persist: Arc<Mutex<Persist>>, 
    waits : Waits,
) {
    // at this point, cache is source-of-truth updated with db values if edge exists.
    // use db cache values to decide nature of updates to db
    // Note for LIST_APPEND Dynamodb will scan the entire attribute value before appending, so List should be relatively small < 10000.
    let table_name: String = table_name_.into();
    let mut target_uid: Vec<AttributeValue>;
    let mut target_bid: Vec<AttributeValue>;
    let mut target_id: Vec<AttributeValue>;
    let mut update_expression: &str;
    

    let mut node = arc_node.lock().await;
    let rkey = RKey::new(node.node, node.rvs_sk.clone());
    println!("*PERSIST start rkey {:?}",rkey);
    let init_cnt = node.init_cnt as usize;
    let edge_cnt = node.target_uid.len() + init_cnt;
    
    if init_cnt <= crate::EMBEDDED_CHILD_NODES {
    
        println!("*PERSIST  ..init_cnt < EMBEDDED. {:?}", rkey);
    
        if node.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
            // consume all of node.target*
            target_uid = mem::take(&mut node.target_uid);
            target_id = mem::take(&mut node.target_id);
            target_bid = mem::take(&mut node.target_bid);
        } else {
            // consume portion of node.target*
            target_uid = node
                .target_uid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_uid, &mut node.target_uid);
            target_id = node
                .target_id
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_id, &mut node.target_id);
            target_bid = node
                .target_bid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_bid, &mut node.target_bid);
        }

        if init_cnt == 0 {
            // no data in db
            update_expression = "SET #cnt = :cnt, #target = :tuid,   #bid = :bid , #id = :id";
        } else {
            // append to existing data
           update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id,:id), #bid=list_append(#bid,:bid), #cnt = :cnt";
        }
        let before = Instant::now();
        //update edge_item
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(
                types::PK,
                AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
            )
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // reverse edge
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#target", types::TARGET_UID)
            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
            .expression_attribute_names("#id", types::TARGET_ID)
            .expression_attribute_values(":id", AttributeValue::L(target_id))
            .expression_attribute_names("#bid", types::TARGET_BID)
            .expression_attribute_values(":bid", AttributeValue::L(target_bid))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;
        waits.record(Event::PersistEmbedded, Instant::now().duration_since(before)).await;        
       
        handle_result(&rkey, result);

    }
    // consume the target_* fields by moving them into overflow batches and persisting the batch
    // note if node has been loaded from db must drive off ovb meta data which gives state of current 
    // population of overflwo batches

    println!("*PERSIST  node.target_uid.len()  {}    {:?}",node.target_uid.len(),rkey);
    while node.target_uid.len() > 0 {

        ////println!("PERSIST  logic target_uid > 0 value {}  {:?}", node.target_uid.len(), rkey );
    
        let mut target_uid: Vec<AttributeValue> = vec![];
        let mut target_bid: Vec<AttributeValue> = vec![];
        let mut target_id: Vec<AttributeValue> = vec![];
        let mut sk_w_bid : String;
        let event :Event ;

        match node.ocur {
            None => {
                // first OvB
                node.obcnt=crate::OV_MAX_BATCH_SIZE;  // force zero free space - see later.
                node.ocur = Some(0);
                continue;
                }
            Some(mut ocur) => {

                let batch_freespace = crate::OV_MAX_BATCH_SIZE - node.obcnt;
                if batch_freespace > 0 {
                
                    // consume last of node.target*
                    if node.target_uid.len() <= batch_freespace {
                    // consume all of node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                        
                    } else {
                        
                        // consume portion of node.target*
                        target_uid = node
                            .target_uid
                            .split_off(batch_freespace);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(batch_freespace);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(batch_freespace);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;

                    }                                        
                    update_expression = "SET #target=list_append(#target, :tuid), #bid=list_append(#bid, :bid), #id=list_append(#id, :id)";  
                    event = Event::PersistOvbAppend;
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
              
                } else {
                
                    // create a new batch optionally in a new OvB
                    if node.ovb.len() < crate::MAX_OV_BLOCKS {
                        // create a new OvB
                        node.ovb.push(Uuid::new_v4());
                        //node.ovb.push(AttributeValue::B(Blob::new(Uuid::new_v4() as bytes)));
                        node.obid.push(1);
                        node.obcnt = 0;
                        node.ocur = Some(node.ovb.len() as u8 - 1);
                     
                    } else {

                        // change current ovb (ie. block)
                        ocur+=1;
                        if ocur as usize == crate::MAX_OV_BLOCKS {
                                ocur = 0;
                        }
                        node.ocur = Some(ocur);
                        //println!("PERSIST   33 node.ocur, ocur {}  {}", node.ocur.unwrap(), ocur);
                        node.obid[ocur as usize] += 1;
                        node.obcnt = 0;
                    }                     
                    if node.target_uid.len() <= crate::OV_MAX_BATCH_SIZE {

                        // consume remaining node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                    
                    } else {

                        // consume leading portion of node.target*
                        target_uid = node.target_uid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;
                    }
                    // ================
                    // add OvB batches
                    // ================
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
    
                    update_expression = "SET #target = :tuid, #bid=:bid, #id = :id";
                    event = Event::PersistOvbSet;
                }
                // ================
                // add OvB batches
                // ================   
                let before = Instant::now();
                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(
                        types::PK,
                        AttributeValue::B(Blob::new(
                            node.ovb[node.ocur.unwrap() as usize].as_bytes(),
                        )),
                    )
                    .key(types::SK, AttributeValue::S(sk_w_bid.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#target", types::TARGET_UID)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#bid", types::TARGET_BID)
                    .expression_attribute_values(":bid", AttributeValue::L(target_bid))
                    .expression_attribute_names("#id", types::TARGET_ID)
                    .expression_attribute_values(":id", AttributeValue::L(target_id))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;
                waits.record(event, Instant::now().duration_since(before)).await;        
  
                handle_result(&rkey, result);
                //println!("PERSIST : batch written.....{:?}",rkey);
            }
        }
    } // end while
    // update OvB meta on edge predicate only if OvB are used.
    if node.ovb.len() > 0 {
        update_expression = "SET  #cnt = :cnt, #ovb = :ovb, #obid = :obid, #obcnt = :obcnt, #ocur = :ocur";

        let ocur = match node.ocur {
            None => 0,
            Some(v) => v,
        };
        let before = Instant::now();
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // OvB metadata
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#ovb", types::OVB)
            .expression_attribute_values(":ovb", types::uuid_to_av_lb(&node.ovb))
            .expression_attribute_names("#obid", types::OVB_BID)
            .expression_attribute_values(":obid", types::u32_to_av_ln(&node.obid))
            .expression_attribute_names("#obcnt", types::OVB_CNT)
            .expression_attribute_values(":obcnt", AttributeValue::N(node.obcnt.to_string()))
            .expression_attribute_names("#ocur", types::OVB_CUR)
            .expression_attribute_values(":ocur", AttributeValue::N(ocur.to_string()))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;
        waits.record(Event::PersistMeta, Instant::now().duration_since(before)).await;        
     
        handle_result(&rkey, result);
        
    }
    let mut persist_guard = persist.lock().await;   
    println!("*PERSIST execute complete - rkey {:?} active {} queue {}",rkey, persist_guard.active.len(), persist_guard.queue.len());
    persist_guard.completed(rkey.clone(), persist.clone()).await;

    println!("*PERSIST  Exit    {:?}", rkey);
}


fn handle_result(rkey: &RKey, result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>>) {
    match result {
        Ok(_out) => {
            ////println!("PERSIST  PRESIST Service: Persist successful update...")
        }
        Err(err) => match err {
            SdkError::ConstructionFailure(_cf) => {
                //println!("PERSIST   Persist Service: Persist  update error ConstructionFailure...")
            }
            SdkError::TimeoutError(_te) => {
                //println!("PERSIST   Persist Service: Persist  update error TimeoutError")
            }
            SdkError::DispatchFailure(_df) => {
                //println!("PERSIST   Persist Service: Persist  update error...DispatchFailure")
            }
            SdkError::ResponseError(_re) => {
                //println!("PERSIST   Persist Service: Persist  update error ResponseError")
            }
            SdkError::ServiceError(_se) => {
                panic!(" Persist Service: Persist  update error ServiceError {:?}",rkey);
            }
            _ => {}
        },
    }
}
