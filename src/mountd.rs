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

#[derive(Clone)]
pub struct Mountd {
    exports: Exports,
}

impl Mountd {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }

    /// Handle a single mountd RPC call.
    /// Transport-independent: works for UDP and TCP.
    pub fn handle_call(&self, buf: &[u8]) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;

        if call.prog != 100005 || call.vers != 1 {
            return None;
        }

        let mut r = XdrR::new(&buf[ofs..]);

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
                    .any(|e| e.path == PathBuf::from(&path));

                let mut w = XdrW::new();

                if ok {
                    w.put_u32(0); // status OK

                    let fh = crate::nfs2::fh_from_path(&path);
                    w.put_opaque(&fh);

                    // auth flavors list (empty)
                    w.put_u32(0);
                } else {
                    w.put_u32(13); // NFSERR_ACCES
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
                let exports = self.exports.list();

                if exports.is_empty() {
                    // exports pointer = NULL
                    w.put_u32(0);
                } else {
                    // exports pointer = present
                    w.put_u32(1);

                    for ex in exports {
                        // exportnode pointer = present
                        w.put_u32(1);
                        w.put_string(&ex.path.to_string_lossy());

                        // groups list = NULL
                        w.put_u32(0);
                    }

                    // end of exportnode list
                    w.put_u32(0);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            _ => {
                // Unsupported procedure
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }
        };

        Some(reply)
    }

    /// Run mountd over UDP
    /// Run mountd over UDP
    pub async fn run(self, sock: UdpSocket, prog: u32, vers: u32) {
        let local = match sock.local_addr() {
            Ok(a) => a,
            Err(e) => {
                warn!(?e, "mountd failed to get local addr");
                return;
            }
        };

        info!(%local, prog, vers, "mountd listening (UDP)");

        let mut buf = vec![0u8; 8192];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            info!(
                peer = %peer,
                size = n,
                "mountd received UDP packet"
            );

            match decode_call(&buf[..n]) {
                None => {
                    warn!(
                        peer = %peer,
                        "mountd: decode_call failed"
                    );
                    continue;
                }
                Some((call, ofs)) => {
                    info!(
                        peer = %peer,
                        xid = call.xid,
                        prog = call.prog,
                        vers = call.vers,
                        procid = call.procid,
                        "mountd: RPC call decoded"
                    );

                    if call.prog != prog || call.vers != vers {
                        warn!(
                            peer = %peer,
                            prog = call.prog,
                            vers = call.vers,
                            "mountd: program/version mismatch"
                        );
                        continue;
                    }

                    let mut r = XdrR::new(&buf[ofs..n]);

                    let reply = match call.procid {
                        0 => {
                            info!("mountd: NULL proc");
                            let w = XdrW::new();
                            rpc_accept_reply(call.xid, 0, &w.buf)
                        }

                        1 => {
                            info!("mountd: MNT proc");
                            let path = r.get_string().unwrap_or_default();
                            info!(path = %path, "mountd: MNT path");

                            let ok = self
                                .exports
                                .list()
                                .iter()
                                .any(|e| e.path == PathBuf::from(&path));

                            let mut w = XdrW::new();

                            if ok {
                                w.put_u32(0);
                                let fh = crate::nfs2::fh_from_path(&path);
                                w.put_opaque(&fh);
                                w.put_u32(0);
                            } else {
                                w.put_u32(13);
                            }

                            rpc_accept_reply(call.xid, 0, &w.buf)
                        }

                        3 => {
                            info!("mountd: UMNT proc");
                            let _ = r.get_string();
                            let w = XdrW::new();
                            rpc_accept_reply(call.xid, 0, &w.buf)
                        }

                        5 => {
                            info!("mountd: EXPORT proc");
                            let mut w = XdrW::new();
                            let exports = self.exports.list();

                            if exports.is_empty() {
                                w.put_u32(0);
                            } else {
                                w.put_u32(1);
                                for ex in exports {
                                    info!(
                                        path = %ex.path.to_string_lossy(),
                                        "mountd: exporting path"
                                    );
                                    w.put_u32(1);
                                    w.put_string(&ex.path.to_string_lossy());
                                    w.put_u32(0);
                                }
                                w.put_u32(0);
                            }

                            rpc_accept_reply(call.xid, 0, &w.buf)
                        }

                        _ => {
                            warn!(procid = call.procid, "mountd: unsupported procedure");
                            let w = XdrW::new();
                            rpc_accept_reply(call.xid, 0, &w.buf)
                        }
                    };

                    if let Err(e) = sock.send_to(&reply, peer).await {
                        warn!(?e, peer = %peer, "mountd: send reply failed");
                    } else {
                        info!(
                            peer = %peer,
                            size = reply.len(),
                            "mountd: reply sent"
                        );
                    }
                }
            }
        }
    }
}
