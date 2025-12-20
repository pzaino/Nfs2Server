// src/mountd.rs

use crate::{
    export::Exports,
    rpc::{decode_call, rpc_accept_reply},
    xdr::{XdrR, XdrW},
};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tracing::{info, warn};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type MountTable = Arc<Mutex<HashMap<String, Vec<u8>>>>;

// Mount v1
const MOUNT_PROG: u32 = 100005;
const MOUNT_VERS: u32 = 1;

#[derive(Clone)]
pub struct Mountd {
    exports: Exports,
    mounts: MountTable,
}

impl Mountd {
    pub fn new(exports: Exports, mounts: MountTable) -> Self {
        Self { exports, mounts }
    }

    /// Core mountd RPC handler (UDP + TCP)
    pub fn handle_call(&self, buf: &[u8]) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;

        if call.prog != MOUNT_PROG {
            return None;
        }
        // accept v1..v3
        if call.vers < MOUNT_VERS || call.vers > 3 {
            return None;
        }

        let mut r = XdrR::new(&buf[ofs..]);

        let reply = match call.procid {
            0 => {
                // NULL
                info!("mountd: NULL");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            1 => {
                // MNT
                let path = r.get_string().unwrap_or_default();
                info!(path = %path, "mountd: MNT");

                let allowed = self.exports.list().iter().any(|e| e.path == path); // PathBuf::from(&path));

                let mut w = XdrW::new();

                if allowed {
                    w.put_u32(0); // OK

                    let p = PathBuf::from(&path);
                    let fh = crate::nfs2::fh_from_path(&p);

                    info!(
                        "mountd: issuing FH for path={} len={} hex={}",
                        p.display(),
                        fh.len(),
                        hex::encode(&fh)
                    );

                    let fh = crate::nfs2::fh_from_path(&p);

                    self.mounts.lock().unwrap().insert(path.clone(), fh.clone());

                    w.put_opaque(&fh);
                    w.put_u32(0); // auth flavors = empty
                } else {
                    w.put_u32(13); // NFSERR_ACCES
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            3 => {
                // UMNT
                let _ = r.get_string();
                info!("mountd: UMNT");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            5 => {
                // EXPORT
                info!("mountd: EXPORT");

                let mut w = XdrW::new();

                let exports = self.exports.list();

                // export list (linked list)
                for ex in exports {
                    w.put_u32(1); // exportnode present
                    w.put_string(&ex.path.to_string_lossy());

                    // groups list (empty)
                    w.put_u32(0);
                }

                w.put_u32(0); // end of export list

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            p => {
                warn!(procid = p, "mountd: unsupported proc");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }
        };

        Some(reply)
    }

    /// UDP server
    pub async fn run_udp(self, sock: UdpSocket) {
        let local = sock.local_addr().ok();
        info!(?local, "mountd listening (UDP)");

        let mut buf = vec![0u8; 8192];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            info!(%peer, size = n, "mountd UDP request");

            if let Some(reply) = self.handle_call(&buf[..n])
                && let Err(e) = sock.send_to(&reply, peer).await
            {
                warn!(?e, %peer, "mountd UDP send failed");
            }
        }
    }

    /// TCP server (record-marked RPC)
    pub async fn run_tcp(self, listener: TcpListener) {
        let local = listener.local_addr().ok();
        info!(?local, "mountd listening (TCP)");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(?e, "mountd TCP accept failed");
                    continue;
                }
            };

            let this = self.clone();

            tokio::spawn(async move {
                info!(%peer, "mountd TCP connected");

                loop {
                    let mut hdr = [0u8; 4];
                    if stream.read_exact(&mut hdr).await.is_err() {
                        break;
                    }

                    let marker = u32::from_be_bytes(hdr);
                    let len = (marker & 0x7fff_ffff) as usize;

                    let mut buf = vec![0u8; len];
                    if stream.read_exact(&mut buf).await.is_err() {
                        break;
                    }

                    if let Some(reply) = this.handle_call(&buf) {
                        let mut out = Vec::with_capacity(4 + reply.len());
                        out.extend_from_slice(&(0x8000_0000u32 | reply.len() as u32).to_be_bytes());
                        out.extend_from_slice(&reply);

                        if stream.write_all(&out).await.is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                info!(%peer, "mountd TCP disconnected");
            });
        }
    }
}
