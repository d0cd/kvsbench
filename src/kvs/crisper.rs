use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use crisper::proto::reqresp::{KeyRequest, KeyTuple, KeyResponse};
use crisper::proto::lattice::{LwwValue};
use prost::Message;
use std::str;
use trackable::error::Failed;


struct CrisperKVSClient {
    client_id: usize,
    rid: u64, 
    request_pusher: zmq::Socket,
    response_puller: zmq::Socket,
}

impl CrisperKVSClient {
    pub fn new(id: usize, context: &zmq::Context, endpoint: &str, bind_addr: &str) -> Result<Self> {
        let pusher = context.socket(zmq::PUSH).unwrap();
        pusher.connect(endpoint).unwrap();

        let puller = context.socket(zmq::PULL).unwrap();
        puller.bind(bind_addr).unwrap();

        Ok(CrisperKVSClient{
            client_id: id,
            rid: 0,
            request_pusher: pusher,
            response_puller: puller,
        })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let key_str = str::from_utf8(key).unwrap().to_string();
        let val: LwwValue = LwwValue {
            timestamp: self.rid,
            value: value.to_vec(),
        };
        let mut payload: Vec<u8> = Vec::new();
        val.encode(&mut payload).unwrap();
        let tup = KeyTuple {
            key: key_str,
            lattice_type: 1, // LWW lattice_type
            error: 0, // No error
            payload: payload,
            address_cache_size: 0,
            invalidate: false,
        };
        let req = KeyRequest {
            r#type: 2, // Put request
            tuples: vec![tup],
            response_address: String::from("tcp://localhost:7777"),
            request_id: self.rid.to_string(),
        };
        self.rid += 1;

        // Send request
        let mut buf: Vec<u8> = Vec::new();
        req.encode(&mut buf).unwrap();
        self.request_pusher.send(buf, 0).unwrap();

        let resp = self.response_puller.recv_bytes(0).unwrap();
        let key_resp: KeyResponse = Message::decode(resp.as_slice()).unwrap();
        Ok(Existence::new(true))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key_str = str::from_utf8(key).unwrap().to_string();
        let tup = KeyTuple {
            key: key_str,
            lattice_type: 1, // LWW lattice_type
            error: 0, // No error
            payload: Vec::new(),
            address_cache_size: 0,
            invalidate: false,
        };
        let req = KeyRequest {
            r#type: 1, // Get request
            tuples: vec![tup],
            response_address: String::from("tcp://localhost:7777"),
            request_id: self.rid.to_string(),
        };
        self.rid += 1;

        // Send request
        let mut buf: Vec<u8> = Vec::new();
        req.encode(&mut buf).unwrap();
        self.request_pusher.send(buf, 0).unwrap();

        // Wait for response
        // TODO: Need to make sure that have the right request id
        let resp = self.response_puller.recv_bytes(0).unwrap();
        let key_resp: KeyResponse = Message::decode(resp.as_slice()).unwrap();
        let mut payload = Vec::new();
        if key_resp.tuples.len() > 0 {
            payload = key_resp.tuples[0].payload.clone()
        }

        match key_resp.tuples.len() > 0 {
            false => Ok(None),
            true => {
                let payload = key_resp.tuples[0].payload.clone();
                let lww_val: LwwValue = Message::decode(payload.as_slice()).unwrap();
                Ok(Some(lww_val.value))
            }
        }
    }

    // Deletes are not allowed by Crisper
    fn delete(&mut self, _key: &[u8]) -> Result<Existence> {
        Ok(Existence::unknown())
    }
}


pub struct CrisperClientPool {
    num_clients: usize,
    clients: Vec<CrisperKVSClient>,
    curr_client: usize,
}

impl CrisperClientPool {
    pub fn new(num_clients: usize, endpoint: &str) -> Result<Self> {
        let context = zmq::Context::new();
        let mut clients = Vec::new();
        for i in 0..num_clients {
            let result = CrisperKVSClient::new(i, &context, endpoint, format!("tcp://*:{}", 7000 + i).as_str());
            match result {
                Ok(client) => clients.push(client),
                _ => ()
            };
        }
        println!("Created {} clients...", clients.len());
        Ok(CrisperClientPool {
            num_clients: clients.len(),
            clients: clients,
            curr_client: 0,
        })
    }

    // Round robin clients
    fn get_client(&mut self) -> &mut CrisperKVSClient {
        let client = self.clients.get_mut(self.curr_client).unwrap();
        self.curr_client = (self.curr_client + 1) % self.num_clients;
        client
    }
}


impl KeyValueStore for CrisperClientPool {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        return self.get_client().put(key, value);
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        return self.get_client().get(key);
    }

    // Deletes are not allowed by Crisper
    fn delete(&mut self, _key: &[u8]) -> Result<Existence> {
        Ok(Existence::unknown())
    }
}
