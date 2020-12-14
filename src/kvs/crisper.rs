use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use crisper::proto::reqresp::{KeyRequest, KeyTuple, KeyResponse};
use prost::Message;
use std::str;
use trackable::error::Failed;


pub struct CrisperKVSClient {
    request_pusher: zmq::Socket,
    response_puller: zmq::Socket,
    rid: usize, //TODO: Is there a concurrency issue
}

impl CrisperKVSClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        let context = zmq::Context::new();

        let pusher = context.socket(zmq::PUSH).unwrap();
        pusher.connect(endpoint).unwrap();

        let puller = context.socket(zmq::PULL).unwrap();
        puller.bind("tcp::/*:7777").unwrap();
        Ok(CrisperKVSClient{
            request_pusher: pusher,
            response_puller: puller,
            rid: 0,
        })
    }
}

impl KeyValueStore for CrisperKVSClient {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let key_str = str::from_utf8(key).unwrap().to_string();
        let tup = KeyTuple {
            key: key_str,
            lattice_type: 1, // LWW lattice_type
            error: 0, // No error
            payload: value.to_vec(),
            address_cache_size: 0,
            invalidate: false,
        };
        let req = KeyRequest {
            r#type: 2, // Put request
            tuples: vec![tup],
            response_address: String::from("tcp::/*:7777"),
            request_id: self.rid.to_string(),
        };
        self.rid += 1;

        // Send request
        let mut buf: Vec<u8> = Vec::new();
        req.encode(&mut buf).unwrap();
        self.request_pusher.send(buf, 0).unwrap();

        //TODO: Wait for a response?

        Ok(Existence::unknown())
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
            response_address: String::from("tcp::/*:7777"),
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

        Ok(Some(payload))
    }

    // Deletes are not allowed by Crisper
    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        Ok(Existence::unknown())
    }
}
