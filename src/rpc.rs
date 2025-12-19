// src/rpc.rs

use crate::xdr::{XdrCodec, XdrR, XdrW};
use anyhow::Result;
use tokio::net::UdpSocket;
use tracing::{debug, error, info};

pub const RPC_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy)]
pub enum MsgType {
    Call = 0,
    Reply = 1,
}

#[derive(Debug)]
pub struct RpcCall {
    pub xid: u32,
    pub prog: u32,
    pub vers: u32,
    pub procid: u32,
    // auth fields ignored for now
}

pub fn decode_call(pkt: &[u8]) -> Option<(RpcCall, usize)> {
    let mut r = XdrR::new(pkt);
    let xid = r.get_u32().ok()?;
    let mtype = r.get_u32().ok()?;
    if mtype != MsgType::Call as u32 {
        return None;
    }
    let rpcvers = r.get_u32().ok()?;
    if rpcvers != RPC_VERSION {
        return None;
    }
    let prog = r.get_u32().ok()?;
    let vers = r.get_u32().ok()?;
    let procid = r.get_u32().ok()?;
    // cred
    let _cred_flavor = r.get_u32().ok()?;
    let _cred_len = r.get_u32().ok()?;
    let _ = r.get_opaque().ok()?;
    // verf
    let _verf_flavor = r.get_u32().ok()?;
    let _verf_len = r.get_u32().ok()?;
    let _ = r.get_opaque().ok()?;
    Some((
        RpcCall {
            xid,
            prog,
            vers,
            procid,
        },
        r.pos,
    ))
}

pub fn rpc_accept_reply(xid: u32, accept_stat: u32, body: &[u8]) -> Vec<u8> {
    // minimal successful reply with void verf
    let mut w = XdrW::new();
    w.put_u32(xid);
    w.put_u32(MsgType::Reply as u32);
    w.put_u32(0); // MSG_ACCEPTED
    // verf
    w.put_u32(0); // AUTH_NULL
    w.put_u32(0);
    // accept stat
    w.put_u32(accept_stat); // 0 = SUCCESS
    let mut v = w.buf.to_vec();
    v.extend_from_slice(body);
    v
}

pub fn rpc_deny_reply(xid: u32, stat: u32) -> Vec<u8> {
    let mut w = XdrW::new();
    w.put_u32(xid);
    w.put_u32(MsgType::Reply as u32);
    w.put_u32(1); // MSG_DENIED
    w.put_u32(stat); // RPC_MISMATCH or AUTH_ERROR
    w.buf.to_vec()
}

pub fn build_rpc_call(xid: u32, prog: u32, vers: u32, procid: u32, body: &[u8]) -> Vec<u8> {
    let mut w = XdrW::new();

    w.put_u32(xid);
    w.put_u32(MsgType::Call as u32);
    w.put_u32(RPC_VERSION);

    w.put_u32(prog);
    w.put_u32(vers);
    w.put_u32(procid);

    // AUTH_NULL
    w.put_u32(0);
    w.put_u32(0);

    // verifier AUTH_NULL
    w.put_u32(0);
    w.put_u32(0);

    let mut v = w.buf.to_vec();
    v.extend_from_slice(body);
    v
}

async fn rpcbind_register(nfs_port: u16) -> Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    let rpcbind_addr = "127.0.0.1:111";

    let mut w = XdrW::new();
    w.put_u32(100003); // NFS program
    w.put_u32(2); // NFS v2
    w.put_u32(17); // IPPROTO_UDP
    w.put_u32(nfs_port as u32);

    let call = build_rpc_call(
        /* xid */ 0x12345678, 100000, // rpcbind
        2, 1, // RPCBPROC_SET
        &w.buf,
    );

    sock.send_to(&call, rpcbind_addr).await?;
    Ok(())
}

pub async fn rpcbind_register_udp(program: u32, version: u32, port: u16) -> Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    let rpcbind_addr = "127.0.0.1:111";

    let mut body = XdrW::new();
    body.put_u32(program); // e.g. 100003 (NFS)
    body.put_u32(version); // 2
    body.put_u32(17); // IPPROTO_UDP
    body.put_u32(port as u32);

    let xid = rand::random::<u32>();

    let call = build_rpc_call(
        xid, 100000, // rpcbind
        2, 1, // RPCBPROC_SET
        &body.buf,
    );

    sock.send_to(&call, rpcbind_addr).await?;
    Ok(())
}
