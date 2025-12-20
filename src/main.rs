// src/main.rs

use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::net::{TcpListener, UdpSocket};
use tokio::signal;
use tracing::{debug, info, warn};

mod export;
mod mountd;
mod nfs2;
mod rpc;
mod xdr;

use crate::export::{Export, Exports};
use serde::Deserialize;

//
// ---- TOML exports parsing ----
//

#[derive(Debug, Deserialize)]
struct ExportsFile {
    export: Vec<ExportEntry>,
}

#[derive(Debug, Deserialize)]
struct ExportEntry {
    path: PathBuf,

    #[serde(default)]
    read_only: bool,

    #[serde(default = "default_anon_uid")]
    anon_uid: u32,

    #[serde(default = "default_anon_gid")]
    anon_gid: u32,

    #[serde(default)]
    clients: Vec<String>,
}

fn default_anon_uid() -> u32 {
    65534
}
fn default_anon_gid() -> u32 {
    65534
}

fn load_exports(path: &str) -> Result<Exports> {
    debug!(path, "checking exports file");

    if !Path::new(path).exists() {
        warn!(path, "exports file not found");
        return Ok(Exports::new(Vec::new()));
    }

    info!(path, "reading exports file");

    let data = fs::read_to_string(path)?;
    let parsed: ExportsFile = toml::from_str(&data)?;

    let exports = parsed
        .export
        .into_iter()
        .map(|e| Export {
            path: e.path,
            read_only: e.read_only,
            anon_uid: e.anon_uid,
            anon_gid: e.anon_gid,
            clients: e.clients,
        })
        .collect();

    Ok(Exports::new(exports))
}

//
// ---- main ----
//

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Nfs2Server starting");

    //
    // ---- Load exports ----
    //

    let exports = load_exports("./exports.toml")?;

    if exports.list().is_empty() {
        warn!("no exports configured");
    }

    //
    // ---- Initialise services ----
    //

    let mountd = mountd::Mountd::new(exports.clone());
    let nfsd = nfs2::Nfs2::new(exports);

    //
    // ---- Bind UDP sockets ----
    //

    let mountd_udp = UdpSocket::bind("0.0.0.0:0").await?;
    let mountd_udp_port = mountd_udp.local_addr()?.port();

    let nfs_udp = UdpSocket::bind("0.0.0.0:0").await?;
    let nfs_udp_port = nfs_udp.local_addr()?.port();

    //
    // ---- Bind TCP sockets ----
    //

    let mountd_tcp = TcpListener::bind("0.0.0.0:0").await?;
    let mountd_tcp_port = mountd_tcp.local_addr()?.port();

    let nfs_tcp = TcpListener::bind("0.0.0.0:0").await?;
    let nfs_tcp_port = nfs_tcp.local_addr()?.port();

    //
    // ---- Register with rpcbind ----
    //

    rpc::rpcbind_register_udp(100005, 1, mountd_udp_port).await?;
    rpc::rpcbind_register_udp(100003, 2, nfs_udp_port).await?;

    rpc::rpcbind_register_tcp(100005, 1, mountd_tcp_port).await?;
    rpc::rpcbind_register_tcp(100003, 2, nfs_tcp_port).await?;

    // mountd versions commonly queried by clients
    for v in [1u32, 2u32, 3u32] {
        rpc::rpcbind_register_udp(100005, v, mountd_udp_port).await?;
        rpc::rpcbind_register_tcp(100005, v, mountd_tcp_port).await?;
    }

    //
    // ---- Start servers ----
    //

    tokio::spawn(mountd.clone().run_udp(mountd_udp));
    tokio::spawn(mountd.run_tcp(mountd_tcp));

    tokio::spawn(nfsd.clone().run_udp(nfs_udp));
    tokio::spawn(nfsd.run_tcp(nfs_tcp));

    info!("nfs2-rs started");
    signal::ctrl_c().await?;
    info!("shutdown requested");

    Ok(())
}
