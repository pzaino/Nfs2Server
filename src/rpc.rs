// src/rpc.rs

use crate::xdr::{XdrR, XdrW};
use anyhow::Result;
use tokio::net::UdpSocket;
use tracing::debug;

pub const RPC_VERSION: u32 = 2;
pub const RPCBIND_PROGRAM: u32 = 100000;
pub const RPCBIND_VERSION: u32 = 2;
pub const RPCBPROC_SET: u32 = 1;

pub const IPPROTO_TCP: u32 = 6;
pub const IPPROTO_UDP: u32 = 17;

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
}

/// Decode an ONC RPC CALL message.
/// Returns the parsed call and the offset where the procedure arguments start.
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

    // cred: (flavor, length, bytes[length], pad)
    let _cred_flavor = r.get_u32().ok()?;
    let cred_len = r.get_u32().ok()? as usize;
    r.skip_bytes(cred_len).ok()?;

    // verf: (flavor, length, bytes[length], pad)
    let _verf_flavor = r.get_u32().ok()?;
    let verf_len = r.get_u32().ok()? as usize;
    r.skip_bytes(verf_len).ok()?;

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

/// Build a successful RPC ACCEPTED reply.
/// `body` must contain the procedure-specific XDR payload.
pub fn rpc_accept_reply(xid: u32, accept_stat: u32, body: &[u8]) -> Vec<u8> {
    let mut w = XdrW::new();

    w.put_u32(xid);
    w.put_u32(MsgType::Reply as u32);
    w.put_u32(0); // MSG_ACCEPTED

    // verifier: AUTH_NULL
    w.put_u32(0);
    w.put_u32(0);

    // accept status (0 = SUCCESS)
    w.put_u32(accept_stat);

    let mut v = w.buf.to_vec();
    v.extend_from_slice(body);
    v
}

/// Build an RPC CALL message.
/// Used for rpcbind registration.
pub fn build_rpc_call(xid: u32, prog: u32, vers: u32, procid: u32, body: &[u8]) -> Vec<u8> {
    let mut w = XdrW::new();

    w.put_u32(xid);
    w.put_u32(MsgType::Call as u32);
    w.put_u32(RPC_VERSION);

    w.put_u32(prog);
    w.put_u32(vers);
    w.put_u32(procid);

    // credentials: AUTH_NULL
    w.put_u32(0);
    w.put_u32(0);

    // verifier: AUTH_NULL
    w.put_u32(0);
    w.put_u32(0);

    let mut v = w.buf.to_vec();
    v.extend_from_slice(body);
    v
}

/// Register a program/version over UDP with rpcbind.
pub async fn rpcbind_register_udp(program: u32, version: u32, port: u16) -> Result<()> {
    rpcbind_register(program, version, IPPROTO_UDP, port).await
}

/// Register a program/version over TCP with rpcbind.
pub async fn rpcbind_register_tcp(program: u32, version: u32, port: u16) -> Result<()> {
    rpcbind_register(program, version, IPPROTO_TCP, port).await
}

async fn rpcbind_register(program: u32, version: u32, protocol: u32, port: u16) -> Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    let rpcbind_addr = "127.0.0.1:111";

    let mut body = XdrW::new();
    body.put_u32(program);
    body.put_u32(version);
    body.put_u32(protocol);
    body.put_u32(port as u32);

    let xid = rand::random::<u32>();

    let call = build_rpc_call(
        xid,
        RPCBIND_PROGRAM,
        RPCBIND_VERSION,
        RPCBPROC_SET,
        &body.buf,
    );

    debug!(program, version, protocol, port, "registering with rpcbind");

    sock.send_to(&call, rpcbind_addr).await?;
    Ok(())
}
