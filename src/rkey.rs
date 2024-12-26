use super::*;

use tokio::time::Instant;

use crate::cache::ReverseCache;
use crate::service::stats::{Waits,Event};

use service::lru::LruAction;

// Reverse_SK is the SK value for the Child of form R#<parent-node-type>#:<parent-edge-attribute-sn>
type ReverseSK = String;

#[derive(Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct RKey(pub Uuid, pub ReverseSK);

impl RKey {
    pub fn new(n: Uuid, reverse_sk: ReverseSK) -> RKey {
        RKey(n, reverse_sk)
    }

    pub async fn add_reverse_edge(&self
                            ,task : usize
                            ,dyn_client: &DynamoClient
                            ,table_name: &str
                            //
                            ,lru_ch : tokio::sync::mpsc::Sender<(usize, RKey, Instant, tokio::sync::mpsc::Sender<bool>, LruAction)>
                            ,persist : Arc<Mutex<Persist>>
                            ,cache : Arc<tokio::sync::Mutex<ReverseCache>>
                            //
                            ,target : &Uuid
                            ,bid: usize
                            ,id : usize
                            //
                            ,waits: Waits
    ) {
        //println!("{} ------------------------------------------------ {:?}",task, self);
        //println!("{} RKEY add_reverse_edge: about to lock cache  {:?} ",task, self);

        let (client_ch, mut srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1); 
  
        let before:Instant;  
        let mut cache_guard = cache.lock().await;
        match cache_guard.0.get(&self) {
            
            None => {
                //println!("{} RKEY add_reverse_edge: - Not Cached: rkey {:?}", task, self);
                // acquire lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let arc_rnode = RNode::new_with_key(self);
                // =============================
                // add to cache and release lock 
                // =============================
                //println!("{} RKEY None match - Insert into Cache rkey {:?}", task, self);
                cache_guard.0.insert(self.clone(), arc_rnode.clone());
                let mut rnode_guard = arc_rnode.lock().await;
                drop(cache_guard);
                // ======================
                // IS NODE BEING PERSISTED 
                // ======================
                self.wait_if_persisting(waits.clone(), persist).await;

                rnode_guard.load_OvB_metadata(dyn_client, table_name, self, task).await;
                rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                //
                before =Instant::now();
                if let Err(err) = lru_ch.send((task, self.clone(), before, client_ch, LruAction::Attach)).await {
                    panic!("Send on lru_attach_ch errored: {}", err);
                }   
                waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;    
                // sync'd: wait for operation to complete - just like using a mutex is synchronous with operation.
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::Attach,Instant::now().duration_since(before)).await; 
            }
            
            Some(rnode_) => {

                //println!("{} RKEY add_reverse_edge: - Cached rkey {:?}", task, self);
                // acquire lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let arc_rnode=rnode_.clone();
                drop(cache_guard);
                println!("{} RKEY add_reverse_edge: - Cached rkey about to lock rnode {:?}", task, self);
                // acqure lock on node. Concurrent task, Evict, may have lock.
                let mut rnode_guard = arc_rnode.lock().await;  
                // ======================
                // IS NODE BEING EVICTED 
                // ======================
                if rnode_guard.evicted {
                    // if so, must wait for the evict-persist process to complete - setup comms with persist.
                    println!("{} RKEY: node read from cache but detected it has been evicted....{:?}",task, self);
                    // ======================
                    // IS NODE BEING PERSISTED 
                    // ======================
                    self.wait_if_persisting(waits.clone(), persist).await;
                    // load node from database and attach to LRU
                    rnode_guard.load_OvB_metadata(dyn_client, table_name, self, task).await;
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
               
                    before =Instant::now();
                    if let Err(err)= lru_ch.send((task, self.clone(), before, client_ch, LruAction::Attach)).await {
                        panic!("Send on lru_attach_ch failed {}",err)
                    };
                    waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;
                    let _ = srv_resp_rx.recv().await;
                    waits.record(Event::Attach,Instant::now().duration_since(before)).await; 

                    rnode_guard.evicted=false;

                } else {
                                   
                    println!("{} RKEY add_reverse_edge: - in cache: true about add_reverse_edge {:?}", task, self);    
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);   
                    //println!("{} RKEY add_reverse_edge: - in cache: send to LRU move_to_head {:?}", task, self);
                    before =Instant::now();    
                    if let Err(err) = lru_ch.send((task, self.clone(), before, client_ch, LruAction::MoveToHead)).await {
                        panic!("Send on lru_move_to_head_ch failed {}",err)
                    };
                    waits.record(Event::LRUSendMove,Instant::now().duration_since(before)).await; 
                    let _ = srv_resp_rx.recv().await;
                    waits.record(Event::MoveToHead,Instant::now().duration_since(before)).await; 
                }
            }
        }
    }

    async fn wait_if_persisting(
        &self
        ,waits : Waits    
        ,persist: Arc<Mutex<Persist>> 
    )  {
        let (ch, mut persist_srv_resp_rx) = tokio::sync::mpsc::channel::<()>(1);
        let mut persist_guard = persist.lock().await;

        if persist_guard.inuse(self.clone(), ch) {
            drop(persist_guard);
            
            println!("RKEY add_reverse_edge: - wait for node to Persist {:?}",self);   
            let before = Instant::now();
            persist_srv_resp_rx.recv().await;
            waits.record(Event::ChanPersistWait, Instant::now().duration_since(before)).await;
        }
    }

}
