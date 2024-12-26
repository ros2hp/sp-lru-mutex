
use std::collections::HashMap;
//use std::time::{Duration, Instant};
//
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::WriteRequest;
use aws_sdk_dynamodb::Client as DynamoClient;
//
// use crate::aws_smithy_runtime_api::client::result::SdkError;
// use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
//
use tokio::time::{sleep, Duration, Instant};
//use tokio::sync::mpsc;
use tokio::task;
//use tokio::runtime::Runtime;

const DYNAMO_BAT_SIZE: usize = 25;

// #[derive(Debug)]
// enum RetryBuf  {
//         A,
//         B,
// }

// impl RetryBuf  {
//     fn switch(&self) -> Self {
//         match self {
//             RetryBuf::A => RetryBuf::B,
//             RetryBuf::B => RetryBuf::A
//         }
//     }

//     fn switch_table(&self, tbl : String) -> String {
//         match self {
//             RetryBuf::A => {
//                 let mut tblnew = (&tbl[..tbl.len()-2]).to_owned();
//                 tblnew.push_str(".B");
//                 tblnew
//             },
//             RetryBuf::B => {
//                 let mut tblnew = (&tbl[..tbl.len()-2]).to_owned();
//                 tblnew.push_str(".A");
//                 tblnew
//             }
//         }
//     }
// }

pub fn start_service(
    dynamo_client: DynamoClient,
    mut retry_rx: tokio::sync::mpsc::Receiver<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
    table_name_: impl Into<String>,
) -> task::JoinHandle<u32> {
    let mut retries_available = false;
    let mut bat_w_req: Vec<aws_sdk_dynamodb::types::WriteRequest> = vec![];
    println!("retry service...started.");
    let mut retry_items: u32 = 0;
    let table_name = table_name_.into();
    let mut table_name_retry: String = String::new();
    table_name_retry = table_name.clone();
    table_name_retry.push_str(".retry");

    //let buf = RetryBuf::A;

    let mut start = Instant::now();

    let dynamo_client1 = dynamo_client.clone();
    let dynamo_client2 = dynamo_client.clone();
    let dynamo_client3 = dynamo_client.clone();
    let dynamo_client4 = dynamo_client.clone();

    println!(
        "start retry service: table [{}]  retry [{}]",
        table_name, table_name_retry
    );

    let retry_server = tokio::spawn(async move {
        loop {
            tokio::select! {
                //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning selct! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_channel.
                Some(wreq) = retry_rx.recv() => {

                    retries_available=true;

                    println!("retry service : batch received  - {} items",wreq.len());

                    for wr in wreq {

                        bat_w_req.push(wr);

                        if bat_w_req.len() == DYNAMO_BAT_SIZE {
                            bat_w_req = persist_dynamo_batch(&dynamo_client1, bat_w_req, &retry_ch, &table_name_retry).await;
                        }
                    }
                }

                _ = shutdown_ch.recv() => {

                        if !retries_available {
                            break
                        }
                        println!("retry shutdown....");
                        // apply workrequests not yet persisted
                        let mut retries_in_channel = true;
                        let mut lc : usize = 0;

                        while retries_in_channel {

                            // save remaining bat_w_req
                            if bat_w_req.len() > 0 {

                                println!("retry service shutting down : about to persist remaining batch - len : {}",bat_w_req.len() );
                                bat_w_req = persist_dynamo_batch(&dynamo_client2, bat_w_req, &retry_ch, table_name_retry.as_str()).await;
                                println!("retry service shutting down : persisted bactch");

                                // check if last persist generated unprocessed items  TODO: remove as retry nolonger sends unproc on channel
                                tokio::select! {
                                    biased;

                                    Some(mut wreq) = retry_rx.recv(), if !retry_rx.is_empty() => {
                                        lc += 1;
                                        if lc > 3 {
                                            panic!("Dynamodb batchwrite retry failed on 3rd attempt. Aborting...")
                                        }
                                        if wreq.len() == 0 {
                                            retries_in_channel=false;
                                        } else {
                                            println!("retry service shutting down : remaining batch has items, retry..{}",lc);
                                            bat_w_req.append(&mut wreq);
                                        }
                                        }
                                    else => {
                                        retries_in_channel=false;
                                        break;
                                    }
                                }

                            } else {
                                retries_in_channel=false;
                            }
                            println!("in retries_in_channel loop...")

                        }

                        sleep(Duration::from_millis(5000)).await;

                        // copy items from retry table to live table. Split between two tokio tasks:  scan-task -> channel -> load-task

                        // paginated scan populates batch, then load batch into live table and repeat until last_evaluated_key is None

                        let (write_ch, mut read_rx) = tokio::sync::mpsc::channel(1);


                        bat_w_req.clear();
                        let mut lek : Option<HashMap<String, AttributeValue>> = None;
                        start = Instant::now();

                        let scan_task = tokio::spawn(async move {
                            let mut more_items = true;
                            let mut first_time = true;
                            println!("retry scan_task started...");

                            while more_items {

                                //.select() - default to ALL_ATTRIBUTES
                                let result = if first_time {
                                        first_time=false;
                                        dynamo_client3
                                        .scan()
                                        .table_name(&table_name_retry)
                                        .limit(1000)
                                        .send()
                                        .await
                                    } else {
                                         dynamo_client3
                                        .scan()
                                        .table_name(&table_name_retry)
                                        .set_exclusive_start_key(lek)
                                        .limit(1000)
                                        .send()
                                        .await
                                    };

                                if let Err(err) = result {
                                    panic!("Error in scan of retries table: {}",err); // TODO replace panic
                                }
                                let scan_output = result.unwrap();

                                // send Option<Vec<HashMap<String, AttributeValue>>>
                                println!("scan_task: send on channel ... ");
                                write_ch.send(scan_output.items).await.unwrap();

                                if None == scan_output.last_evaluated_key {
                                    more_items=false;
                                } else {
                                    more_items=true;
                                }
                                lek=scan_output.last_evaluated_key;

                            }
                            println!("retry scan_task finished");

                            0
                        });

                        let load_task = tokio::spawn(async move {
                            println!("retry load_task started...");
                            loop {

                                let items = read_rx.recv().await;

                                // recv() returns None if the channel has been closed and there are no remaining messages in the channel’s buffer.
                                if None == items {
                                    println!("retry load_task channel read - None");
                                    break;
                                }

                                println!("retry load_task channel read");
                                //for item in items.unwrap() {
                                let mut n = items.unwrap().into_iter();
                                while let Some(item) = n.next() {
                                    // build a put_item

                                    // attributes of item
                                    for hm in item {

                                        let mut put =  aws_sdk_dynamodb::types::PutRequest::builder();
                                        for (k,v) in hm {
                                            put = put.item(k,v);
                                        }

                                        // build a WriteRequest
                                        match put.build() {
                                            Err(err) => {
                                                println!("error in write_request builder: {}",err);
                                            }

                                            Ok(req) =>  {
                                                bat_w_req.push(WriteRequest::builder().put_request(req).build());
                                            }
                                        }
                                        retry_items+=1;
                                        println!("load_task: items ...{} ",bat_w_req.len());

                                        // persist write requests only when dynamodb batch limit reached (25 writerequests).
                                        if bat_w_req.len() == DYNAMO_BAT_SIZE {
                                            bat_w_req = persist_dynamo_batch(&dynamo_client4, bat_w_req, &retry_ch, &table_name).await;
                                        }
                                    }
                                }
                            }

                            while bat_w_req.len() > 0 {
                                bat_w_req = persist_dynamo_batch(&dynamo_client4, bat_w_req, &retry_ch, &table_name).await;
                            }
                            retry_items

                        });

                        scan_task.await;
                        let result = load_task.await;
                        if let Err(err) = result {
                            panic!("Error in load task of retry: {}",err);
                        }
                        retry_items=result.unwrap();
                        println!("retry items transferred {}",retry_items);
                        // break out of loop
                        break;
                }
            }
            println!("retry service...looping.");
        }
        if retry_items > 0 {
            println!(
                "retry items transferred {} Duration: {:?}",
                retry_items,
                Instant::now().duration_since(start)
            );
        } else {
            println!("retry items transferred {}", retry_items);
        }
        println!("retry service...shutdown.");

        retry_items
    });

    retry_server
}

async fn persist_dynamo_batch(
    dynamo_client: &DynamoClient,
    bat_w_req: Vec<WriteRequest>,
    _retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    table_name: impl Into<String>,
) -> Vec<WriteRequest> {
    let bat_w_outp = dynamo_client
        .batch_write_item()
        .request_items(table_name, bat_w_req)
        .send()
        .await;

    match bat_w_outp {
        Err(err) => {
            panic!(
                "Error in Dynamodb batch write in persist_dynamo_batch() - {}",
                err
            );
        }
        Ok(resp) => {
            if resp.unprocessed_items.as_ref().unwrap().values().len() > 0 {
                // in the case of this single-table-design, unprocessed items will
                // be associated with one table
                for (_, v) in resp.unprocessed_items.unwrap() {
                    sleep(Duration::from_millis(5000)).await;

                    let new_bat_w_req: Vec<WriteRequest> = v;
                    return new_bat_w_req;
                }

                // TODO: aggregate batchwrite metrics in bat_w_output.
                // pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
                // pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
            }
        }
    }
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}
