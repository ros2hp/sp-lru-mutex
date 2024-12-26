use super::*;

use crate::rkey::RKey;
use crate::node::RNode;
use crate::cache::ReverseCache;
use crate::service::persist::Persist;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::service::stats::{Waits,Event};

use aws_sdk_dynamodb::Client as DynamoClient;

use tokio::time::{sleep, Duration, Instant};
use tokio::sync::Mutex;

pub enum LruAction {
    Attach,
    MoveToHead,
}

// lru is used only to drive lru_entry eviction.
// the lru_entry cache is separate
#[derive(Clone)]
pub struct Entry{
    pub key: RKey,
    //
    pub next: Option<Arc<Mutex<Entry>>>,
    pub prev: Option<Weak<Mutex<Entry>>>,
}

impl Entry {
    fn new(rkey : RKey) -> Entry {
        Entry{key: rkey
            ,next: None
            ,prev: None
        }
    }
}

// impl Drop for Entry {
//         fn drop(&mut self) {
//         //println!("\nDROP LRU Entry {:?}\n",self.key);
//     }
// }

pub struct LRUevict {
    // ultimate number of entries in LRU
    capacity : usize,
    // current number of entries in LRU
    cnt : usize, 
    // pointer to Entry value in the LRU linked list for a RKey
    lookup : HashMap<RKey,Arc<Mutex<Entry>>>,
    //
    pub persist: Arc<Mutex<Persist>>,
    // record stat waits
    waits : Waits,
    //
    head: Option<Arc<Mutex<Entry>>>,
    tail: Option<Arc<Mutex<Entry>>>,
}
    // 371 |       let lru_server = tokio::task::spawn( async move { 
    //     |  __________________________________________^
    // 372 | |         loop {
    // 373 | |             tokio::select! {
    // 374 | |                 //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
    // ...   |
    // 438 | |         }               
    // 439 | |     }); 
    //     | |_____^ future created by async block is not `Send`
    // head: Option<Rc<RefCell<Entry>>>,
    // tail: Option<Rc<RefCell<Entry>>>,


// implement attach & move_to_head as trait methods.
// // Makes more sense however for these methods to be part of the LRUevict itself - just epxeriementing with traits.
// pub trait LRU {
//     fn attach(
//         &mut self, // , cache_guard:  &mut std::sync::MutexGuard<'_, ReverseCache>
//         rkey: RKey,
//     );

//     //pub async fn detach(&mut self
//     fn move_to_head(
//         &mut self,
//         rkey: RKey,
//     );
// }


impl LRUevict { //impl LRU for MutexGuard<'_, LRUevict> {
    
    pub fn new( cap: usize
                , dynamo_client : DynamoClient
                , table_name : impl Into<String>
                , waits : Waits
    ) -> Self {
        let persist = Persist::new(dynamo_client, table_name, waits.clone());
        LRUevict{
            capacity: cap,
            cnt : 0,
            lookup: HashMap::new(),
            persist,
            waits,
            //
            head: None,
            tail: None,
        }
    }
    
    
    async fn print(&self) {

        let mut entry = self.head.clone();
        println!("*** print LRU chain ***");
        let mut i = 0;
        while let Some(entry_) = entry {
            i+=1;
            let rkey = entry_.lock().await.key.clone();
            println!("LRU entry {} {:?}",i, rkey);
            entry = entry_.lock().await.next.clone();      
        }
        println!("*** print LRU chain DONE ***");
    }

    // prerequisite - lru_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRUevict if lock has been acquired via Arc<Mutex<LRU>>
    async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
        task : usize, 
        rkey : RKey,
        mut cache: Arc<Mutex<ReverseCache>>,
    ) {
        // calling routine (RKEY) is hold lock on RNode(RKEY)
        
        ////println!("   ");
        println!("{} LRU attach {:?}. ***********",task, rkey);
        //self.print().await;     
        let mut lc = 0;   
        while self.cnt >= self.capacity && lc < 3 {
        
            lc += 1;
            // ================================
            // Evict the tail entry in the LRU 
            // ================================
            println!("{} LRU: attach reached LRU capacity - evict tail  lru.cnt {}  lc {}  key {:?}", task, self.cnt, lc,  rkey);
            // unlink tail lru_entry from lru and notify evict service.
            // Clone REntry as about to purge it from cache.
            let lru_evict_entry = self.tail.as_ref().unwrap().clone();
            let mut evict_entry = lru_evict_entry.lock().await;
            println!("{} LRU attach evict node {:?} - about to acquire cache lock ",task, evict_entry.key);

            let before = Instant::now();
            let mut cache_guard = cache.lock().await;
            let Some(arc_evict_node_) = cache_guard.0.get(&evict_entry.key)  
                        else { println!("{} LRU: PANIC - attach evict processing: expect entry in cache {:?}",task, evict_entry.key);
                               panic!("LRU: attach evict processing: expect entry in cache {:?}",evict_entry.key)
                            };

            let arc_evict_node: Arc<Mutex<RNode>>= arc_evict_node_.clone();
            let tlock_result: Result<tokio::sync::MutexGuard<'_, RNode>, tokio::sync::TryLockError> = arc_evict_node.try_lock();
            //drop(cache_guard);
            // ==========================
            // acquire lock on evict node 
            // ==========================
            match tlock_result {

                Ok(mut evict_node_guard)  => {
                    evict_node_guard.evicted=true;
                    // ============================
                    // remove node from cache
                    // ============================
                    //let mut cache_guard = cache.lock().await;
                    println!("{} LRU attach evict - remove from Cache {:?}", task, evict_entry.key);
                    cache_guard.0.remove(&evict_entry.key);    
                    // ===================================
                    // detach evict entry from tail of LRU
                    // ===================================
                    match evict_entry.prev {
                            None => {panic!("LRU attach - evict_entry - expected prev got None")}
                            Some(ref v) => {
                                match v.upgrade() {
                                    None => {panic!("LRU attach - evict_entry - could not upgrade")}
                                    Some(new_tail) => {
                                        let mut new_tail_guard = new_tail.lock().await;
                                        new_tail_guard.next = None;
                                        self.tail = Some(new_tail.clone()); 
                                    }
                                }
                            }
                    }
                    evict_entry.prev=None;
                    evict_entry.next=None;
                    self.cnt-=1;
                    // ===================
                    // submit persist task
                    // ===================
                    {
                        let mut persist_guard = self.persist.lock().await;
                        persist_guard.submit(evict_entry.key.clone(), arc_evict_node.clone(), self.persist.clone());
                    }
                    // =====================
                    // remove from lru lookup 
                    // =====================
                    self.lookup.remove(&evict_entry.key);
                    println!("{} LRU attach - release node lock {:?}",task, rkey);
                } 
                Err(err) =>  {
                    // Abort eviction - as node is being accessed.
                    // TODO check if error is "node locked"
                    println!("{} 3x LRU attach - lock cannot be acquired - abort eviction {:?}",task, evict_entry.key);
                    break;
                }
            }
            self.waits.record(Event::LruEvictCacheLock, Instant::now().duration_since(before)).await;     
        }
        // ======================
        // attach to head of LRU
        // ======================
        let arc_new_entry = Arc::new(Mutex::new(Entry::new(rkey.clone())));
        match self.head.clone() {
            None => { 
                // empty LRU 
                //println!("LRU <<< attach: empty LRU set head and tail");     
                self.head = Some(arc_new_entry.clone());
                self.tail = Some(arc_new_entry.clone());
                }
            
            Some(e) => {
                let mut new_entry = arc_new_entry.lock().await;
                let mut old_head_guard = e.lock().await;
                // set old head prev to point to new entry
                old_head_guard.prev = Some(Arc::downgrade(&arc_new_entry));
                // set new entry next to point to old head entry & prev to NOne   
                new_entry.next = Some(e.clone());
                new_entry.prev = None;
                // set LRU head to point to new entry
                self.head=Some(arc_new_entry.clone());
                
                if let None = new_entry.next {
                    panic!("LRU INCONSISTENCY attach: expected Some for next but got NONE {:?}",rkey);
                }
                if let Some(_) = new_entry.prev {
                    panic!("LRU INCONSISTENCY attach: expected None for prev but got NONE {:?}",rkey);
                }
                if let None = new_entry.next {
                    panic!("LRU INCONSISTENCY attach: expected entry to have next but got NONE {:?}",rkey);
                }
                if let Some(_) = new_entry.prev {
                    panic!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",rkey);
                }
                if let None = old_head_guard.prev {
                    panic!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",rkey);
                }
            }
        }
        println!("{} LRU: attach - insert into lookup {:?}",task, rkey);
        self.lookup.insert(rkey, arc_new_entry);
        
        if let None = self.head {
                //println!("LRU INCONSISTENCY attach: expected LRU to have head but got NONE")
        }
        if let None = self.tail {
                //println!("LRU INCONSISTENCY attach: expected LRU to have tail but got NONE")
        }
        self.cnt+=1;
        println!("{} LRU: attach add cnt {}",task, self.cnt);
        //self.print().await;
    }
    
    
    // prerequisite - lru_entry has been confirmed to be in lru-cache.\/
    // to execute a method a lock has been taken out on the LRU
    async fn move_to_head(
        &mut self,
        task: usize, 
        rkey: RKey,
        mut cache: Arc<Mutex<ReverseCache>>,
    ) {  
        //println!("--------------");
        println!("{} LRU move_to_head {:?} ********",task, rkey);
        // abort if lru_entry is at head of lru
        match self.head {
            None => {
                panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let hd: RKey = v.lock().await.key.clone();
                if hd == rkey {
                    // rkey already at head
                    println!("{} LRU entry already at head - return {:?}",task, rkey);
                    return
                }    
            }
        }
        // lookup entry in map
        let lru_entry = match self.lookup.get(&rkey) {
            None => {  
                        // node has been evicted since rkeys.add_reverse_edge9) detected it was cached  - due to msg delay in LRU channel buffer
                        // attach instead.
                        self.attach(task, rkey, cache).await;
                        return;    
                    }
            Some(v) => v.clone()
        };
        {
            // DETACH the entry before attaching to LRU head
            
            let lru_entry_guard = lru_entry.lock().await;
            // NEW CODE to fix eviction and new request at same time on a Node
            if let None = lru_entry_guard.prev {
                ////println!("LRU INCONSISTENCY move_to_head: expected entry to have prev but got NONE {:?}",rkey);
                if let None = lru_entry_guard.next {
                    // should not happend
                    panic!("LRU move_to_head : got a entry with no prev or next set (ie. a new node) - some synchronisation gone astray")
                }
            }

            // check if moving tail entry
            if let None = lru_entry_guard.next {
            
                println!("{} LRU move_to_head detach tail entry {:?}",task, rkey);
                let prev_ = lru_entry_guard.prev.as_ref().unwrap();
                let Some(prev_upgrade) =  prev_.upgrade() else {panic!("at tail: failed to upgrade weak lru_entry {:?}",rkey)};
                let mut prev_guard = prev_upgrade.lock().await;
                prev_guard.next = None;
                self.tail = Some(lru_entry_guard.prev.as_ref().unwrap().upgrade().as_ref().unwrap().clone());
                
            } else {
                
                let prev_ = lru_entry_guard.prev.as_ref().unwrap();
                let Some(prev_upgrade) =  prev_.upgrade() else {panic!("failed to upgrade weak lru_entry {:?}",rkey)};
                let mut prev_guard = prev_upgrade.lock().await;
    
                let next_ = lru_entry_guard.next.as_ref().unwrap();
                let mut next_guard= next_.lock().await;

                prev_guard.next = Some(next_.clone());
                next_guard.prev = Some(Arc::downgrade(&prev_upgrade));           

            }
            println!("{} LRU move_to_head Detach complete...{:?}",task, rkey);
        }
        // ATTACH
        let mut lru_entry_guard = lru_entry.lock().await;
        lru_entry_guard.next = self.head.clone();
        
        // adjust old head entry previous pointer to new entry (aka LRU head)
        match self.head {
            None => {
                 panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let mut hd = v.lock().await;
                hd.prev = Some(Arc::downgrade(&lru_entry)); 
                println!("{} LRU move_to_head: attach old head {:?} prev to {:?} ",task, hd.key, lru_entry_guard.key);
            }
        }
        lru_entry_guard.prev = None;
        self.head = Some(lru_entry.clone());
        println!("{} LRU move_to_head complete...{:?}",task, rkey);
    }
}


pub fn start_service(
        mut cache: Arc<tokio::sync::Mutex<ReverseCache>>
        //
        ,mut lru_rx : tokio::sync::mpsc::Receiver<(usize, RKey, Instant,tokio::sync::mpsc::Sender<bool>, LruAction)>
        ,mut lru_flush_rx : tokio::sync::mpsc::Receiver<tokio::sync::mpsc::Sender<()>>
        ,mut lru_evict: LRUevict
) -> tokio::task::JoinHandle<()>  {
    // also consider tokio::task::spawn_blocking() which will create a OS thread and allocate task to it.
    //let mut lru_evict = lru::LRUevict::new(lru_capacity);

    let lru_server = tokio::task::spawn( async move { 
        println!("LRU service started....");
        loop {
            tokio::select! {
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                Some((task, rkey, sent_time, client_ch, action)) = lru_rx.recv() => {

                        match action {
                            LruAction::Attach => {
                                println!("{} delay {:?} LRU: action Attach {:?}",task, Instant::now().duration_since(sent_time).as_nanos(), rkey);
                                lru_evict.attach(task, rkey, cache.clone()).await;
                            }
                            LruAction::MoveToHead => {
                                println!("{} delay {:?} LRU: action move_to_head {:?}",task, Instant::now().duration_since(sent_time).as_nanos(), rkey);
                                lru_evict.move_to_head(task, rkey, cache.clone()).await;
                            }
                        }
                        // send response back to client...sync'd.
                        if let Err(err) = client_ch.send(true).await {
                            panic!("LRU action send to client_ch {} ",err);
                        };
                    }

                Some(client_ch) = lru_flush_rx.recv() => {
                               
                                println!("LRU service - flush lru ");
                                    let persist_ = lru_evict.persist.clone();
                                    let mut persist_guard = lru_evict.persist.lock().await;
                                    let mut entry = lru_evict.head;
                                    let mut lc = 0;
                                    let cache_guard = cache.lock().await;
                                    println!("LRU flush in progress..persist entries in LRU flust {} lru entries",lru_evict.cnt);
                                    while let Some(entry_) = entry {
                                            lc += 1;     
                                            let rkey = entry_.lock().await.key.clone();
                                            if let Some(arc_node) = cache_guard.0.get(&rkey) {

                                                println!("LRU: flush-persist  {} in cache now submit {:?}",lc, rkey);
                                                persist_guard.submit(rkey, arc_node.clone(), persist_.clone() );
                  
                                            } else {
                                                println!("LRU: flush-persist Error {} not in cache {:?}",lc, rkey);
                                            }
                                            println!("LRU: flush-persist get next...");
                                            entry = entry_.lock().await.next.clone();         
                                            println!("LRU: flush-persist .. got it");      
                                    }
                                    //sleep(Duration::from_millis(2000)).await;
                                    println!("LRU: flush-persist Send on client_ch ");
                                    if let Err(err) = client_ch.send(()).await {
                                        panic!("LRU send on client_ch {} ",err);
                                    };
                                    println!("LRU shutdown ");
                                    return (); 
                    }                      
            }    
        } 
        ()  
        //println!("LRU service shutdown....");          
    }); 

    lru_server
}  
