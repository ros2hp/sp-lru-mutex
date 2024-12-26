use super::*;


// =======================
//  Reverse Cache
// =======================
// cache responsibility is to synchronise access to db across multiple Tokio tasks on a single cache entry.
// The state of the node edge will determine the type of update required, either embedded or OvB.
// Each cache update will be saved to db to keep both in sync.
// All mutations of the cache hashmap need to be serialized.
pub struct ReverseCache(pub HashMap<RKey, Arc<tokio::sync::Mutex<RNode>>>);

impl ReverseCache {
    pub fn new() -> Arc<Mutex<ReverseCache>> {
        Arc::new(Mutex::new(ReverseCache(HashMap::new())))
    }

    // pub fn get(&mut self, rkey: &RKey) -> Option<Arc<tokio::sync::Mutex<RNode>>> {
    //     //self.0.get(rkey).and_then(|v| Some(Arc::clone(v)))
    //     match self.0.get(rkey) {
    //         None => None,
    //         Some(re) => Some(Arc::clone(re)),
    //     }
    // }

    // pub fn insert(&mut self, rkey: RKey, rnode: RNode) -> Arc<tokio::sync::Mutex<RNode>> {
    //     let arcnode = Arc::new(tokio::sync::Mutex::new(rnode));
    //     let y = arcnode.clone();
    //     self.0.insert(rkey, arcnode);
    //     y
    // }
}