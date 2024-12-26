use std::collections::HashMap;
use std::sync::Arc;
use std::u128;

use tokio::time::{Duration, Instant};
use tokio::task;
#[derive(Eq, Hash, PartialEq, Debug)]
pub enum Event {
    // mutex waits
    LRUmutex,
    CacheMutex,
    // channel waits
    LRUSendAttach,
    LRUSendMove, 
    EvictWait,
    ChanPersistQuery,
    ChanPersistQueryResp,
    ChanPersistWait,
    TaskSend,
    TaskRecv,
    TaskRemainRecv,
    // Operations
    MoveToHead,
    Attach,
    // Dynamodb
    GetItem,
    PersistEmbedded,
    PersistOvbSet,
    PersistOvbAppend,
    PersistMeta,
    // Cache
    LruEvictCacheLock
}

#[derive(Clone)]
pub struct Waits{
    record_ch: tokio::sync::mpsc::Sender<(Event, Duration, Duration)>,
    record_dur: Instant,
}


impl Waits {    
    
//     pub fn new(send : tokio::sync::mpsc::Sender<(Event, Duration)> 
// )-> Arc<Waits> {
//     Arc::new(Waits{
//         record_ch : send,
//     })
// }

    pub fn new(send : tokio::sync::mpsc::Sender<(Event, Duration, Duration)> 
    )-> Waits {
        Waits{
            record_ch : send,
            record_dur: Instant::now(),
        }
    }


    pub async fn record(&self, e : Event, dur : Duration) {
        //println!("STATS: send {:?} {}",e, dur.as_nanos());
        if let Err(err) = self.record_ch.send((e, Instant::now().duration_since(self.record_dur),  dur)).await {
            println!("STATS: Error on send to record_ch {}",err);
        };
    }

}

// impl Clone for Waits {

//     fn clone(&self) -> Self {
//         //println!("STATS: clone");
//         Waits{ record_ch : self.record_ch.clone(), record_dur: self.record_dur.clone()}
//     }
// }

pub fn start_service(mut stats_rx : tokio::sync::mpsc::Receiver<(Event, Duration, Duration)>
                    ,mut shutdown_rx : tokio::sync::broadcast::Receiver<u8>
) -> task::JoinHandle<()> {

    println!("STATS: start stats service....");
    let mut waits_repo: HashMap<Event, Vec<(Duration, Duration)>> =  HashMap::new();

    let stats_server = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased; 
                Some((e, rec_dur, dur)) = stats_rx.recv() => {

                    println!("STATS: stats_rx {:?} {:?} {}",e, rec_dur, dur.as_nanos());
                    match waits_repo.get_mut(&e) {
                        Some(vec) => { vec.push((rec_dur, dur)); }
                        None => { waits_repo.insert(e,  vec!((rec_dur, dur))); }
                    }
                }

                // Stats does not respond to shutdown broadcast rather it is synchronised with 
                // Persist shutdown.
                _ = shutdown_rx.recv() => {
                    println!("STATS: Shutdown stats service....");
                    // ==========================
                    // close channel - then drain
                    // ==========================
                    stats_rx.close();
                    // =======================
                    // drain remaining values
                    // =======================
                    let mut cnt = 0;
                    while let Some((e, rec_dur, dur)) = stats_rx.recv().await {
                            cnt+=1;
                            println!("STATS: consume remaining...{}",cnt);
                            match waits_repo.get_mut(&e) {
                                Some(vec) => { vec.push((rec_dur, dur)); }
                                None => {break;}
                            }                        
                    }
                    println!("STATS: repo size {}",waits_repo.len());
                    for (k,v) in waits_repo.iter() { 
                        println!("WAITS: repo size {:?} len {}",k,v.len());
                    }
                    for (k,v) in waits_repo.iter() {
                        let len : u128 = v.len() as u128;
                        let mut s : u128 = 0;  
                        let zero_dur : Duration = Duration::from_nanos(0);                 
                        let mut max : Duration = Duration::from_nanos(0);
                        let mut min : Duration = Duration::from_nanos(u64::MAX);
                        let mut mean : u128 = 0;
                        for i in v {
                            s += i.1.as_nanos();
                            if i.1 > max {
                                max = i.1;
                
                            }
                            if i.1 < min && i.1 > zero_dur {
                                min=i.1;
                            }
                        }

                        mean = s/len;
                        println!("WAITS {:?} len {} mean {} min {} max {}", k, len, mean, min.as_nanos(), max.as_nanos());
                    }
                    break;
                }
            }
        }
        println!("STATS: EXIT");
    });

    stats_server
}