// src/mountd.rs

use crate::{
    export::Exports,
    rpc::{decode_call, rpc_accept_reply},
    xdr::{XdrCodec, XdrR, XdrW},
};
use std::path::PathBuf;
use tokio::net::UdpSocket;
use tracing::{info, warn};

// Mount v1 program 100005, version 1
// Procedures: 0 NULL, 1 MNT, 2 DUMP, 3 UMNT, 4 UMNTALL, 5 EXPORT

pub struct Mountd {
    exports: Exports,
}

impl Mountd {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }
}

impl Mountd {
    // CHANGED: take ownership of an already-bound socket
    pub async fn run(self, sock: UdpSocket, prog: u32, vers: u32) {
        let local = match sock.local_addr() {
            Ok(a) => a,
            Err(e) => {
                warn!(?e, "mountd failed to get local addr");
                return;
            }
        };

        info!(%local, prog, vers, "mountd listening");

        let mut buf = vec![0u8; 8192];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            if let Some((call, ofs)) = decode_call(&buf[..n]) {
                if call.prog != prog || call.vers != vers {
                    continue;
                }

                let mut r = XdrR::new(&buf[ofs..n]);

                let reply = match call.procid {
                    0 => {
                        // NULL
                        let w = XdrW::new();
                        rpc_accept_reply(call.xid, 0, &w.buf)
                    }

                    1 => {
                        // MNT(path)
                        let path = r.get_string().unwrap_or_default();

                        let ok = self
                            .exports
                            .list()
                            .iter()
                            .find(|e| e.path == PathBuf::from(&path));

                        let mut w = XdrW::new();

                        if ok.is_some() {
                            w.put_u32(0); // status OK

                            // filehandle 32 bytes
                            let fh = super::nfs2::fh_from_path(&path);
                            w.put_opaque(&fh);

                            // auth flavors list (empty)
                            w.put_u32(0);
                        } else {
                            w.put_u32(13); // PERMISSION
                        }

                        rpc_accept_reply(call.xid, 0, &w.buf)
                    }

                    3 => {
                        // UMNT(path)
                        let _ = r.get_string();
                        let w = XdrW::new();
                        rpc_accept_reply(call.xid, 0, &w.buf)
                    }

                    5 => {
                        // EXPORT
                        let mut w = XdrW::new();

                        for ex in self.exports.list() {
                            w.put_u32(1); // more exports follow (TRUE)
                            w.put_string(&ex.path.to_string_lossy());

                            // groups list (empty)
                            w.put_u32(0); // no groups
                        }

                        // terminate export list
                        w.put_u32(0); // no more exports (FALSE)

                        rpc_accept_reply(call.xid, 0, &w.buf)
                    }

                    _ => {
                        let w = XdrW::new();
                        rpc_accept_reply(call.xid, 0, &w.buf)
                    }
                };

                let _ = sock.send_to(&reply, peer).await;
            }
        }
    }
}
