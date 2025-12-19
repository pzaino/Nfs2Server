// src/main.rs

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::signal;
use tracing::{error, info};

mod export;
mod mountd;
mod nfs2;
mod rpc;
mod xdr;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let exports = export::Exports::new(vec![export::Export {
        path: "/tmp/nfs_export".into(),
        read_only: true,
        anon_uid: 65534,
        anon_gid: 65534,
        clients: vec![], // allow all for now
    }]);

    let mountd = mountd::Mountd::new(exports.clone());
    let nfsd = nfs2::Nfs2::new(exports);

    // Register programs and start UDP listeners
    let mut tasks = Vec::new();

    // rpcbind registration helper
    let nfs_socket = UdpSocket::bind("0.0.0.0:0").await?;
    let local_port = nfs_socket.local_addr()?.port();
    rpc::rpcbind_register_udp(100003, 2, local_port).await?;

    // Start mountd and nfsd
    tasks.push(tokio::spawn(mountd.run("0.0.0.0:0", 100005, 1))); // mount v1
    tasks.push(tokio::spawn(nfsd.run("0.0.0.0:0", 100003, 2))); // nfs v2

    info!("nfs2-rs started. Exports at /tmp/nfs_export. Press Ctrl+C to stop.");

    signal::ctrl_c().await?;
    info!("shutting down");

    for t in tasks {
        let _ = t.abort();
    }
    Ok(())
}
