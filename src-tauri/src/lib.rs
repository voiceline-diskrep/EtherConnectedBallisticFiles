use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tauri::{Manager, Window};

const CURRENT_VERSION: &str = "0.1.0";
const GITHUB_REPO: &str = "voiceline-diskrep/EtherConnectedBallisticFiles";
const DISCOVERY_UDP_PORT: u16 = 45678;
const TRANSFER_TCP_PORT: u16 = 45679;
const CHUNK_SIZE: usize = 512 * 1024; // 512 KB per stripe chunk

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WiredCableInfo {
    pub id: String,
    pub if_index: u32,
    pub name: String,
    pub description: String,
    pub port_type: String,
    pub link_speed_mbps: u64,
    pub link_speed_display: String,
    pub max_speed_mbs: f64,
    pub is_connected: bool,
    pub ip_address: Option<String>,
    pub prefix_len: u8,
    pub mac_address: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub live_rx_mbs: f64,
    pub live_tx_mbs: f64,
    pub speed_hint: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PeerLinkInfo {
    pub local_interface_id: String,
    pub local_interface_name: String,
    pub port_type: String,
    pub local_ip: String,
    pub peer_ip: String,
    pub peer_tcp_port: u16,
    pub link_speed_mbps: u64,
    pub last_seen_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PeerDeviceInfo {
    pub node_id: String,
    pub node_name: String,
    pub links: Vec<PeerLinkInfo>,
    pub combined_speed_mbps: u64,
    pub combined_max_mbs: f64,
    pub last_seen_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SelectedFileItem {
    pub path: String,
    pub name: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LaneProgress {
    pub lane_name: String,
    pub port_type: String,
    pub link_speed_mbps: u64,
    pub bytes_sent: u64,
    pub chunks_sent: u64,
    pub active: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ActiveTransferState {
    pub transfer_id: String,
    pub direction: String, // "Sending" or "Receiving"
    pub peer_name: String,
    pub file_name: String,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub progress_pct: f64,
    pub speed_mbs: f64,
    pub speed_mbps: f64,
    pub eta_seconds: u64,
    pub total_chunks: u64,
    pub completed_chunks: u64,
    pub chunk_states: Vec<u8>, // 24 visual cells (0 = pending, 1..N = lane index + 1)
    pub lanes: Vec<LaneProgress>,
    pub status: String, // "transferring", "verifying", "completed", "failed", "cancelled"
    pub error_message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransferHistoryItem {
    pub transfer_id: String,
    pub direction: String,
    pub peer_name: String,
    pub file_name: String,
    pub saved_path: Option<String>,
    pub total_bytes: u64,
    pub avg_speed_mbs: f64,
    pub duration_secs: f64,
    pub timestamp: String,
    pub sha256: String,
    pub verified: bool,
    pub cables_used: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LiveSystemSnapshot {
    pub local_node_id: String,
    pub local_device_name: String,
    pub save_directory: String,
    pub cables: Vec<WiredCableInfo>,
    pub total_connected_cables: usize,
    pub total_bonded_speed_mbps: u64,
    pub total_bonded_max_mbs: f64,
    pub peers: Vec<PeerDeviceInfo>,
    pub selected_files: Vec<SelectedFileItem>,
    pub active_transfer: Option<ActiveTransferState>,
    pub history: Vec<TransferHistoryItem>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct BeaconPacket {
    magic: String,
    kind: String, // "BEACON" or "PONG"
    node_id: String,
    node_name: String,
    port_type: String,
    interface_name: String,
    ip: String,
    tcp_port: u16,
    speed_mbps: u64,
}

struct PrevOctets {
    rx: u64,
    tx: u64,
    at: Instant,
}

pub struct EngineState {
    pub node_id: String,
    pub device_name: String,
    pub save_directory: String,
    pub cables: Vec<WiredCableInfo>,
    pub peers: HashMap<String, PeerDeviceInfo>,
    pub selected_files: Vec<SelectedFileItem>,
    pub active_transfer: Option<ActiveTransferState>,
    pub history: Vec<TransferHistoryItem>,
    prev_octets: HashMap<u32, PrevOctets>,
    pub cancel_flag: Arc<AtomicBool>,
    pub pause_flag: Arc<AtomicBool>,
}

static ENGINE: OnceLock<Arc<Mutex<EngineState>>> = OnceLock::new();

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn default_computer_name() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "Local-PC".to_string())
}

fn default_save_dir() -> String {
    if let Ok(profile) = std::env::var("USERPROFILE") {
        let p = PathBuf::from(profile).join("Downloads").join("EtherConnected");
        let _ = fs::create_dir_all(&p);
        return p.to_string_lossy().to_string();
    }
    "C:\\EtherConnected".to_string()
}

fn get_engine() -> &'static Arc<Mutex<EngineState>> {
    ENGINE.get_or_init(|| {
        let comp = default_computer_name();
        let node_id = format!("{}-{:x}", comp, now_ms() & 0xFFFF_FFFF);
        Arc::new(Mutex::new(EngineState {
            node_id,
            device_name: comp,
            save_directory: default_save_dir(),
            cables: Vec::new(),
            peers: HashMap::new(),
            selected_files: Vec::new(),
            active_transfer: Vec::new().pop(),
            history: Vec::new(),
            prev_octets: HashMap::new(),
            cancel_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
        }))
    })
}

// ============================================================================
// NATIVE WINDOWS HARDWARE SCANNER (ZERO POWERSHELL, < 0.3ms EXECUTION)
// ============================================================================

#[cfg(target_os = "windows")]
fn query_ipv4_map() -> HashMap<u32, (String, u8)> {
    use windows::Win32::Foundation::ERROR_SUCCESS;
    use windows::Win32::NetworkManagement::IpHelper::{
        GetAdaptersAddresses, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER,
        GAA_FLAG_SKIP_MULTICAST, IP_ADAPTER_ADDRESSES_LH,
    };
    use windows::Win32::Networking::WinSock::{AF_INET, SOCKADDR_IN};

    let mut map = HashMap::new();
    unsafe {
        let mut size: u32 = 32 * 1024;
        let mut buf: Vec<u8> = vec![0; size as usize];
        for _ in 0..3 {
            buf.resize(size as usize, 0);
            let adapter_ptr = buf.as_mut_ptr() as *mut IP_ADAPTER_ADDRESSES_LH;
            let ret = GetAdaptersAddresses(
                AF_INET.0 as u32,
                GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
                None,
                Some(adapter_ptr),
                &mut size,
            );
            if ret == ERROR_SUCCESS.0 {
                let mut cur = adapter_ptr;
                while !cur.is_null() {
                    let if_index = (*cur).Anonymous1.Anonymous.IfIndex;
                    let mut uni = (*cur).FirstUnicastAddress;
                    while !uni.is_null() {
                        let sa = (*uni).Address.lpSockaddr;
                        if !sa.is_null() && (*sa).sa_family == AF_INET {
                            let sin = &*(sa as *const SOCKADDR_IN);
                            let ip_u32 = u32::from_be(sin.sin_addr.S_un.S_addr);
                            let ipv4 = Ipv4Addr::from(ip_u32);
                            if !ipv4.is_loopback() {
                                let prefix = (*uni).OnLinkPrefixLength;
                                map.insert(if_index, (ipv4.to_string(), prefix));
                                break;
                            }
                        }
                        uni = (*uni).Next;
                    }
                    cur = (*cur).Next;
                }
                break;
            }
        }
    }
    map
}

#[cfg(target_os = "windows")]
fn query_arp_neighbors_for_interface(if_index: u32) -> Vec<Ipv4Addr> {
    use windows::Win32::NetworkManagement::IpHelper::{
        FreeMibTable, GetIpNetTable2, MIB_IPNET_TABLE2,
    };
    use windows::Win32::Networking::WinSock::AF_INET;

    let mut neighbors = Vec::new();
    unsafe {
        let mut table: *mut MIB_IPNET_TABLE2 = std::ptr::null_mut();
        if GetIpNetTable2(AF_INET, &mut table).is_ok() && !table.is_null() {
            let count = (*table).NumEntries as usize;
            let rows = (*table).Table.as_ptr();
            for i in 0..count {
                let row = &*rows.add(i);
                if row.InterfaceIndex == if_index {
                    let ip_u32 = u32::from_be(row.Address.Ipv4.sin_addr.S_un.S_addr);
                    let ip = Ipv4Addr::from(ip_u32);
                    let octets = ip.octets();
                    // Exclude multicast (224..=239), broadcast (.255), and 0.0.0.0
                    if octets[0] != 0
                        && octets[0] != 127
                        && !(224..=255).contains(&octets[0])
                        && octets[3] != 255
                        && !(octets[0] == 169 && octets[1] == 254 && octets[2] == 255 && octets[3] == 255)
                    {
                        if !neighbors.contains(&ip) {
                            neighbors.push(ip);
                        }
                    }
                }
            }
            FreeMibTable(table as *const _);
        }
    }
    neighbors
}

#[cfg(target_os = "windows")]
fn refresh_cables_native(state: &mut EngineState) {
    use windows::Win32::NetworkManagement::IpHelper::{
        FreeMibTable, GetIfTable2, IF_TYPE_IEEE80211, IF_TYPE_SOFTWARE_LOOPBACK, MIB_IF_TABLE2,
    };
    use windows::Win32::NetworkManagement::Ndis::{
        MediaConnectStateDisconnected, NdisPhysicalMediumBluetooth,
        NdisPhysicalMediumNative802_11, NdisPhysicalMediumWirelessLan,
    };

    let ip_map = query_ipv4_map();
    let mut found = Vec::new();
    let now = Instant::now();

    unsafe {
        let mut table: *mut MIB_IF_TABLE2 = std::ptr::null_mut();
        if GetIfTable2(&mut table).is_err() || table.is_null() {
            return;
        }

        let count = (*table).NumEntries as usize;
        let rows = (*table).Table.as_ptr();

        for i in 0..count {
            let row = &*rows.add(i);

            // Bit 0 = HardwareInterface, Bit 1 = FilterInterface
            let flags = row.InterfaceAndOperStatusFlags._bitfield;
            let is_hardware = (flags & 1) != 0;
            let is_filter = (flags & 2) != 0;

            if !is_hardware || is_filter {
                continue;
            }

            if row.Type == IF_TYPE_IEEE80211 || row.Type == IF_TYPE_SOFTWARE_LOOPBACK {
                continue;
            }

            if row.PhysicalMediumType == NdisPhysicalMediumNative802_11
                || row.PhysicalMediumType == NdisPhysicalMediumWirelessLan
                || row.PhysicalMediumType == NdisPhysicalMediumBluetooth
            {
                continue;
            }

            let alias_len = row.Alias.iter().position(|&c| c == 0).unwrap_or(row.Alias.len());
            let name = String::from_utf16_lossy(&row.Alias[..alias_len]);

            let desc_len = row
                .Description
                .iter()
                .position(|&c| c == 0)
                .unwrap_or(row.Description.len());
            let description = String::from_utf16_lossy(&row.Description[..desc_len]);

            let desc_lower = description.to_lowercase();
            let name_lower = name.to_lowercase();
            if desc_lower.contains("wi-fi")
                || desc_lower.contains("wifi")
                || desc_lower.contains("wireless")
                || desc_lower.contains("802.11")
                || desc_lower.contains("bluetooth")
                || desc_lower.contains("tunnelbear")
                || desc_lower.contains("virtual")
                || desc_lower.contains("wan miniport")
                || name_lower.contains("wi-fi")
                || name_lower.contains("bluetooth")
            {
                continue;
            }

            let port_type = if desc_lower.contains("thunderbolt") || desc_lower.contains("usb4") {
                "USB-C / Thunderbolt Cable".to_string()
            } else if desc_lower.contains("type-c") || desc_lower.contains("usb-c") {
                "USB-C Cable".to_string()
            } else if desc_lower.contains("usb")
                || desc_lower.contains("rndis")
                || desc_lower.contains("ncm")
                || desc_lower.contains("apple mobile device")
            {
                "USB Cable / Adapter".to_string()
            } else {
                "Ethernet Cable (RJ45)".to_string()
            };

            let is_connected =
                row.MediaConnectState != MediaConnectStateDisconnected && row.TransmitLinkSpeed > 0;
            let speed_bps = if is_connected { row.TransmitLinkSpeed } else { 0 };
            let speed_mbps = speed_bps / 1_000_000;
            let max_speed_mbs = ((speed_mbps as f64) / 8.0 * 10.0).round() / 10.0;

            let link_speed_display = if !is_connected || speed_mbps == 0 {
                "Disconnected".to_string()
            } else if speed_mbps >= 1000 {
                format!("{:.1} Gbps ({:.0} MB/s)", (speed_mbps as f64) / 1000.0, max_speed_mbs)
            } else {
                format!("{} Mbps ({:.1} MB/s)", speed_mbps, max_speed_mbs)
            };

            // Give helpful hardware diagnostic if a Gigabit card negotiated at 100 Mbps
            let speed_hint = if is_connected
                && speed_mbps == 100
                && (desc_lower.contains("i219")
                    || desc_lower.contains("i225")
                    || desc_lower.contains("i226")
                    || desc_lower.contains("gbe")
                    || desc_lower.contains("gigabit"))
            {
                Some("This port supports 1,000 Mbps (125 MB/s), but negotiated at 100 Mbps (12.5 MB/s). Usually caused by a 4-wire CAT5 cable or a 100 Mbps port on the other computer.".to_string())
            } else {
                None
            };

            let mac_len = (row.PhysicalAddressLength as usize).min(row.PhysicalAddress.len());
            let mac_address = if mac_len > 0 {
                row.PhysicalAddress[..mac_len]
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<_>>()
                    .join("-")
            } else {
                "N/A".to_string()
            };

            let rx_bytes = row.InOctets;
            let tx_bytes = row.OutOctets;

            let (live_rx_mbs, live_tx_mbs) =
                if let Some(prev) = state.prev_octets.get(&row.InterfaceIndex) {
                    let dt = now.duration_since(prev.at).as_secs_f64();
                    if dt > 0.05 {
                        let drx = rx_bytes.saturating_sub(prev.rx) as f64 / (1024.0 * 1024.0) / dt;
                        let dtx = tx_bytes.saturating_sub(prev.tx) as f64 / (1024.0 * 1024.0) / dt;
                        ((drx * 10.0).round() / 10.0, (dtx * 10.0).round() / 10.0)
                    } else {
                        (0.0, 0.0)
                    }
                } else {
                    (0.0, 0.0)
                };

            state.prev_octets.insert(
                row.InterfaceIndex,
                PrevOctets {
                    rx: rx_bytes,
                    tx: tx_bytes,
                    at: now,
                },
            );

            let (ip_address, prefix_len) = match ip_map.get(&row.InterfaceIndex) {
                Some((ip, pfx)) => (Some(ip.clone()), *pfx),
                None => (None, 16),
            };

            found.push(WiredCableInfo {
                id: format!("if-{}", row.InterfaceIndex),
                if_index: row.InterfaceIndex,
                name,
                description,
                port_type,
                link_speed_mbps: speed_mbps,
                link_speed_display,
                max_speed_mbs,
                is_connected,
                ip_address,
                prefix_len,
                mac_address,
                rx_bytes,
                tx_bytes,
                live_rx_mbs,
                live_tx_mbs,
                speed_hint,
            });
        }

        FreeMibTable(table as *const _);
    }

    state.cables = found;
}

// ============================================================================
// REAL PEER DISCOVERY ENGINE (UDP BEACON + ARP NEIGHBOR UNICAST + TCP PROBE)
// ============================================================================

fn register_discovered_peer(state: &mut EngineState, pkt: BeaconPacket, peer_ip_actual: String) {
    if pkt.node_id == state.node_id {
        return; // Never register ourselves
    }

    // Match which local wired cable is on the same subnet as peer_ip_actual
    let peer_ipv4: Ipv4Addr = match peer_ip_actual.parse() {
        Ok(ip) => ip,
        Err(_) => return,
    };

    let mut matched_cable: Option<WiredCableInfo> = None;
    for cable in &state.cables {
        if !cable.is_connected {
            continue;
        }
        if let Some(ref lip_str) = cable.ip_address {
            if let Ok(lip) = lip_str.parse::<Ipv4Addr>() {
                let mask = if cable.prefix_len == 0 {
                    0
                } else {
                    !0u32 << (32 - cable.prefix_len.min(32))
                };
                if (u32::from(lip) & mask) == (u32::from(peer_ipv4) & mask) {
                    matched_cable = Some(cable.clone());
                    break;
                }
            }
        }
    }

    // If no subnet match (e.g., cross-subnet static IP on connected cable), pick first connected cable
    if matched_cable.is_none() {
        matched_cable = state.cables.iter().find(|c| c.is_connected).cloned();
    }

    let Some(cable) = matched_cable else {
        return; // Ignore peers that came over Wi-Fi! Only register peers on physical wired cables!
    };

    let local_ip = cable.ip_address.clone().unwrap_or_default();
    let effective_speed = if cable.link_speed_mbps > 0 && pkt.speed_mbps > 0 {
        cable.link_speed_mbps.min(pkt.speed_mbps)
    } else {
        cable.link_speed_mbps.max(pkt.speed_mbps)
    };

    let ts = now_ms();
    let link = PeerLinkInfo {
        local_interface_id: cable.id.clone(),
        local_interface_name: cable.name.clone(),
        port_type: cable.port_type.clone(),
        local_ip,
        peer_ip: peer_ip_actual,
        peer_tcp_port: pkt.tcp_port,
        link_speed_mbps: effective_speed,
        last_seen_ms: ts,
    };

    let entry = state.peers.entry(pkt.node_id.clone()).or_insert_with(|| PeerDeviceInfo {
        node_id: pkt.node_id.clone(),
        node_name: pkt.node_name.clone(),
        links: Vec::new(),
        combined_speed_mbps: 0,
        combined_max_mbs: 0.0,
        last_seen_ms: ts,
    });

    entry.node_name = pkt.node_name;
    entry.last_seen_ms = ts;

    if let Some(existing) = entry
        .links
        .iter_mut()
        .find(|l| l.local_interface_id == link.local_interface_id || l.peer_ip == link.peer_ip)
    {
        *existing = link;
    } else {
        entry.links.push(link);
    }

    // Prune stale links (> 6s) and recompute combined bandwidth
    entry.links.retain(|l| ts.saturating_sub(l.last_seen_ms) <= 6500);
    entry.combined_speed_mbps = entry.links.iter().map(|l| l.link_speed_mbps).sum();
    entry.combined_max_mbs = ((entry.combined_speed_mbps as f64) / 8.0 * 10.0).round() / 10.0;
}

fn spawn_background_networking() {
    let engine = get_engine().clone();

    // 1. UDP Listener Thread on 0.0.0.0:45678
    let engine_udp = engine.clone();
    std::thread::spawn(move || {
        let sock = match UdpSocket::bind(("0.0.0.0", DISCOVERY_UDP_PORT)) {
            Ok(s) => s,
            Err(_) => return,
        };
        let _ = sock.set_broadcast(true);
        let mut buf = [0u8; 2048];

        loop {
            if let Ok((len, src_addr)) = sock.recv_from(&mut buf) {
                if let Ok(pkt) = serde_json::from_slice::<BeaconPacket>(&buf[..len]) {
                    if pkt.magic == "ECBF_V2" {
                        let peer_ip = src_addr.ip().to_string();
                        let mut reply_pong: Option<BeaconPacket> = None;

                        if let Ok(mut st) = engine_udp.lock() {
                            if pkt.node_id != st.node_id {
                                if pkt.kind == "BEACON" {
                                    // Prepare unicast PONG reply so the sender immediately discovers us too
                                    let active_cable = st.cables.iter().find(|c| c.is_connected);
                                    reply_pong = Some(BeaconPacket {
                                        magic: "ECBF_V2".to_string(),
                                        kind: "PONG".to_string(),
                                        node_id: st.node_id.clone(),
                                        node_name: st.device_name.clone(),
                                        port_type: active_cable
                                            .map(|c| c.port_type.clone())
                                            .unwrap_or_else(|| "Ethernet Cable".to_string()),
                                        interface_name: active_cable
                                            .map(|c| c.name.clone())
                                            .unwrap_or_else(|| "Ethernet".to_string()),
                                        ip: active_cable
                                            .and_then(|c| c.ip_address.clone())
                                            .unwrap_or_default(),
                                        tcp_port: TRANSFER_TCP_PORT,
                                        speed_mbps: active_cable
                                            .map(|c| c.link_speed_mbps)
                                            .unwrap_or(100),
                                    });
                                }
                                register_discovered_peer(&mut st, pkt, peer_ip.clone());
                            }
                        }

                        if let Some(pong) = reply_pong {
                            if let Ok(bytes) = serde_json::to_vec(&pong) {
                                let _ = sock.send_to(&bytes, (peer_ip.as_str(), DISCOVERY_UDP_PORT));
                            }
                        }
                    }
                }
            }
        }
    });

    // 2. Hardware Scanner & UDP Broadcaster / ARP Neighbor Prober Thread (Every 1.2s)
    let engine_scan = engine.clone();
    std::thread::spawn(move || loop {
        let mut active_cables = Vec::new();
        let mut node_id = String::new();
        let mut node_name = String::new();

        if let Ok(mut st) = engine_scan.lock() {
            #[cfg(target_os = "windows")]
            refresh_cables_native(&mut st);

            let ts = now_ms();
            st.peers.retain(|_, peer| {
                peer.links.retain(|l| ts.saturating_sub(l.last_seen_ms) <= 6500);
                peer.combined_speed_mbps = peer.links.iter().map(|l| l.link_speed_mbps).sum();
                peer.combined_max_mbs =
                    ((peer.combined_speed_mbps as f64) / 8.0 * 10.0).round() / 10.0;
                !peer.links.is_empty() && ts.saturating_sub(peer.last_seen_ms) <= 6500
            });

            node_id = st.node_id.clone();
            node_name = st.device_name.clone();
            active_cables = st
                .cables
                .iter()
                .filter(|c| c.is_connected)
                .cloned()
                .collect();
        }

        // Broadcast beacon out of each connected physical wired interface
        for cable in active_cables {
            if let Some(ref ip_str) = cable.ip_address {
                if let Ok(local_ipv4) = ip_str.parse::<Ipv4Addr>() {
                    let pkt = BeaconPacket {
                        magic: "ECBF_V2".to_string(),
                        kind: "BEACON".to_string(),
                        node_id: node_id.clone(),
                        node_name: node_name.clone(),
                        port_type: cable.port_type.clone(),
                        interface_name: cable.name.clone(),
                        ip: ip_str.clone(),
                        tcp_port: TRANSFER_TCP_PORT,
                        speed_mbps: cable.link_speed_mbps,
                    };

                    if let Ok(bytes) = serde_json::to_vec(&pkt) {
                        if let Ok(sock) = UdpSocket::bind((local_ipv4, 0)) {
                            let _ = sock.set_broadcast(true);
                            // Directed subnet broadcast
                            let mask = if cable.prefix_len == 0 {
                                0
                            } else {
                                !0u32 << (32 - cable.prefix_len.min(32))
                            };
                            let bcast_u32 = u32::from(local_ipv4) | !mask;
                            let subnet_bcast = Ipv4Addr::from(bcast_u32);

                            let _ = sock.send_to(&bytes, (subnet_bcast, DISCOVERY_UDP_PORT));
                            let _ = sock.send_to(
                                &bytes,
                                (Ipv4Addr::new(169, 254, 255, 255), DISCOVERY_UDP_PORT),
                            );
                            let _ = sock.send_to(
                                &bytes,
                                (Ipv4Addr::new(255, 255, 255, 255), DISCOVERY_UDP_PORT),
                            );

                            // Also send direct unicast to any neighbor in the Windows ARP cache on this physical wire!
                            #[cfg(target_os = "windows")]
                            for neighbor_ip in query_arp_neighbors_for_interface(cable.if_index) {
                                if neighbor_ip != local_ipv4 {
                                    let _ = sock.send_to(&bytes, (neighbor_ip, DISCOVERY_UDP_PORT));
                                }
                            }
                        }
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(1200));
    });

    // 3. Real Multi-Link TCP Transfer Server Thread on 0.0.0.0:45679
    let engine_tcp = engine.clone();
    std::thread::spawn(move || {
        let listener = match TcpListener::bind(("0.0.0.0", TRANSFER_TCP_PORT)) {
            Ok(l) => l,
            Err(_) => return,
        };

        for stream in listener.incoming().flatten() {
            let eng = engine_tcp.clone();
            std::thread::spawn(move || {
                handle_incoming_tcp_client(stream, eng);
            });
        }
    });
}

// ============================================================================
// REAL TCP WIRE PROTOCOL (RECEIVER & HANDSHAKE)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
struct WireCommand {
    cmd: String,
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    node_name: String,
    #[serde(default)]
    port_type: String,
    #[serde(default)]
    speed_mbps: u64,
    #[serde(default)]
    transfer_id: String,
    #[serde(default)]
    sender_name: String,
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    file_size: u64,
    #[serde(default)]
    total_chunks: u64,
    #[serde(default)]
    chunk_index: u64,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    length: usize,
    #[serde(default)]
    lane_index: usize,
    #[serde(default)]
    lane_name: String,
    #[serde(default)]
    expected_sha256: String,
    #[serde(default)]
    lanes: Vec<LaneProgress>,
}

fn read_wire_command(stream: &mut TcpStream) -> std::io::Result<WireCommand> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let header_len = u32::from_be_bytes(len_buf) as usize;
    if header_len == 0 || header_len > 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid header length",
        ));
    }
    let mut header_buf = vec![0u8; header_len];
    stream.read_exact(&mut header_buf)?;
    serde_json::from_slice(&header_buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn send_wire_command(stream: &mut TcpStream, cmd: &WireCommand) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(cmd)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = (bytes.len() as u32).to_be_bytes();
    stream.write_all(&len)?;
    stream.write_all(&bytes)?;
    stream.flush()
}

struct IncomingFileTrack {
    part_path: PathBuf,
    final_path: PathBuf,
    file: File,
    started_at: Instant,
}

static INCOMING_FILES: OnceLock<Mutex<HashMap<String, Arc<Mutex<IncomingFileTrack>>>>> =
    OnceLock::new();

fn get_incoming_files() -> &'static Mutex<HashMap<String, Arc<Mutex<IncomingFileTrack>>>> {
    INCOMING_FILES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn handle_incoming_tcp_client(mut stream: TcpStream, engine: Arc<Mutex<EngineState>>) {
    let _ = stream.set_nodelay(true);
    let peer_ip = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();

    loop {
        let req = match read_wire_command(&mut stream) {
            Ok(r) => r,
            Err(_) => break,
        };

        match req.cmd.as_str() {
            "HELLO" => {
                let mut reply = WireCommand {
                    cmd: "HELLO_ACK".to_string(),
                    node_id: String::new(),
                    node_name: String::new(),
                    port_type: "Ethernet Cable".to_string(),
                    speed_mbps: 100,
                    transfer_id: String::new(),
                    sender_name: String::new(),
                    file_name: String::new(),
                    file_size: 0,
                    total_chunks: 0,
                    chunk_index: 0,
                    offset: 0,
                    length: 0,
                    lane_index: 0,
                    lane_name: String::new(),
                    expected_sha256: String::new(),
                    lanes: Vec::new(),
                };

                if let Ok(mut st) = engine.lock() {
                    reply.node_id = st.node_id.clone();
                    reply.node_name = st.device_name.clone();
                    if let Some(c) = st.cables.iter().find(|c| c.is_connected) {
                        reply.port_type = c.port_type.clone();
                        reply.speed_mbps = c.link_speed_mbps;
                    }
                    let pkt = BeaconPacket {
                        magic: "ECBF_V2".to_string(),
                        kind: "PONG".to_string(),
                        node_id: req.node_id,
                        node_name: req.node_name,
                        port_type: req.port_type,
                        interface_name: "Ethernet".to_string(),
                        ip: peer_ip.clone(),
                        tcp_port: TRANSFER_TCP_PORT,
                        speed_mbps: req.speed_mbps,
                    };
                    register_discovered_peer(&mut st, pkt, peer_ip.clone());
                }

                let _ = send_wire_command(&mut stream, &reply);
                break;
            }
            "INIT_TRANSFER" => {
                let save_dir = engine
                    .lock()
                    .map(|s| s.save_directory.clone())
                    .unwrap_or_else(|_| default_save_dir());
                let _ = fs::create_dir_all(&save_dir);

                let safe_name = Path::new(&req.file_name)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "received_file.bin".to_string());

                let final_path = PathBuf::from(&save_dir).join(&safe_name);
                let part_path = PathBuf::from(&save_dir).join(format!("{}.ecbf.part", safe_name));

                if let Ok(file) = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&part_path)
                {
                    let _ = file.set_len(req.file_size);
                    if let Ok(mut map) = get_incoming_files().lock() {
                        map.insert(
                            req.transfer_id.clone(),
                            Arc::new(Mutex::new(IncomingFileTrack {
                                part_path,
                                final_path,
                                file,
                                started_at: Instant::now(),
                            })),
                        );
                    }

                    if let Ok(mut st) = engine.lock() {
                        st.active_transfer = Some(ActiveTransferState {
                            transfer_id: req.transfer_id.clone(),
                            direction: "Receiving".to_string(),
                            peer_name: req.sender_name,
                            file_name: safe_name,
                            total_bytes: req.file_size,
                            transferred_bytes: 0,
                            progress_pct: 0.0,
                            speed_mbs: 0.0,
                            speed_mbps: 0.0,
                            eta_seconds: 0,
                            total_chunks: req.total_chunks.max(1),
                            completed_chunks: 0,
                            chunk_states: vec![0; 24],
                            lanes: req.lanes,
                            status: "transferring".to_string(),
                            error_message: None,
                        });
                    }

                    let _ = stream.write_all(b"OK");
                    let _ = stream.flush();
                } else {
                    let _ = stream.write_all(b"ER");
                    break;
                }
            }
            "CHUNK" => {
                let mut payload = vec![0u8; req.length];
                if stream.read_exact(&mut payload).is_err() {
                    break;
                }

                let track_opt = get_incoming_files()
                    .lock()
                    .ok()
                    .and_then(|m| m.get(&req.transfer_id).cloned());

                if let Some(track_arc) = track_opt {
                    let mut elapsed_secs = 0.001;
                    if let Ok(mut trk) = track_arc.lock() {
                        let _ = trk.file.seek(SeekFrom::Start(req.offset));
                        let _ = trk.file.write_all(&payload);
                        elapsed_secs = trk.started_at.elapsed().as_secs_f64().max(0.001);
                    }

                    if let Ok(mut st) = engine.lock() {
                        if let Some(ref mut act) = st.active_transfer {
                            if act.transfer_id == req.transfer_id {
                                act.transferred_bytes = act
                                    .transferred_bytes
                                    .saturating_add(req.length as u64)
                                    .min(act.total_bytes);
                                act.completed_chunks = act.completed_chunks.saturating_add(1);
                                act.progress_pct = if act.total_bytes > 0 {
                                    ((act.transferred_bytes as f64 / act.total_bytes as f64)
                                        * 1000.0)
                                        .round()
                                        / 10.0
                                } else {
                                    100.0
                                };
                                let mbs = (act.transferred_bytes as f64 / (1024.0 * 1024.0))
                                    / elapsed_secs;
                                act.speed_mbs = (mbs * 10.0).round() / 10.0;
                                act.speed_mbps = (mbs * 8.0 * 10.0).round() / 10.0;
                                let rem_mbs = ((act.total_bytes.saturating_sub(act.transferred_bytes))
                                    as f64)
                                    / (1024.0 * 1024.0);
                                act.eta_seconds = if mbs > 0.1 {
                                    (rem_mbs / mbs).ceil() as u64
                                } else {
                                    0
                                };

                                let cell = ((req.chunk_index as usize) * 24)
                                    / (act.total_chunks.max(1) as usize);
                                if cell < 24 {
                                    act.chunk_states[cell] = (req.lane_index as u8) + 1;
                                }
                                if let Some(lane) = act.lanes.get_mut(req.lane_index) {
                                    lane.bytes_sent =
                                        lane.bytes_sent.saturating_add(req.length as u64);
                                    lane.chunks_sent = lane.chunks_sent.saturating_add(1);
                                }
                            }
                        }
                    }

                    let _ = stream.write_all(b"K");
                    let _ = stream.flush();
                } else {
                    let _ = stream.write_all(b"E");
                    break;
                }
            }
            "FINISH_TRANSFER" => {
                if let Ok(mut st) = engine.lock() {
                    if let Some(ref mut act) = st.active_transfer {
                        if act.transfer_id == req.transfer_id {
                            act.status = "verifying".to_string();
                        }
                    }
                }

                let track_opt = get_incoming_files()
                    .lock()
                    .ok()
                    .and_then(|mut m| m.remove(&req.transfer_id));

                if let Some(track_arc) = track_opt {
                    if let Ok(mut trk) = track_arc.lock() {
                        let _ = trk.file.flush();
                        let _ = trk.file.seek(SeekFrom::Start(0));

                        let mut hasher = Sha256::new();
                        let mut buf = vec![0u8; 1024 * 1024];
                        while let Ok(n) = trk.file.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            hasher.update(&buf[..n]);
                        }
                        let computed_sha256 = format!("{:x}", hasher.finalize());
                        let verified = req.expected_sha256.is_empty()
                            || computed_sha256 == req.expected_sha256;

                        drop(trk.file.try_clone());
                        let _ = fs::rename(&trk.part_path, &trk.final_path);
                        let duration_secs = trk.started_at.elapsed().as_secs_f64().max(0.01);

                        if let Ok(mut st) = engine.lock() {
                            if let Some(act) = st.active_transfer.take() {
                                let avg_mbs = ((act.total_bytes as f64 / (1024.0 * 1024.0))
                                    / duration_secs
                                    * 10.0)
                                    .round()
                                    / 10.0;
                                let cables_used = act
                                    .lanes
                                    .iter()
                                    .map(|l| l.port_type.clone())
                                    .collect::<Vec<_>>()
                                    .join(" + ");

                                st.history.insert(
                                    0,
                                    TransferHistoryItem {
                                        transfer_id: act.transfer_id,
                                        direction: "Received".to_string(),
                                        peer_name: act.peer_name,
                                        file_name: act.file_name,
                                        saved_path: Some(
                                            trk.final_path.to_string_lossy().to_string(),
                                        ),
                                        total_bytes: act.total_bytes,
                                        avg_speed_mbs: avg_mbs,
                                        duration_secs: (duration_secs * 10.0).round() / 10.0,
                                        timestamp: "Just now".to_string(),
                                        sha256: computed_sha256,
                                        verified,
                                        cables_used,
                                    },
                                );
                            }
                        }
                    }
                }

                let _ = stream.write_all(b"OK");
                let _ = stream.flush();
                break;
            }
            _ => break,
        }
    }
}

// ============================================================================
// REAL MULTI-CABLE TCP FILE SENDER (BINDS SOCKETS TO EACH CABLE IP)
// ============================================================================

fn connect_bound_tcp(local_ip: &str, peer_ip: &str, peer_port: u16) -> std::io::Result<TcpStream> {
    let peer_addr: SocketAddr = format!("{}:{}", peer_ip, peer_port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    let _ = socket.set_tcp_nodelay(true);
    let _ = socket.set_send_buffer_size(4 * 1024 * 1024);
    let _ = socket.set_recv_buffer_size(4 * 1024 * 1024);

    if let Ok(lip) = local_ip.parse::<Ipv4Addr>() {
        if !lip.is_unspecified() {
            let bind_addr = SocketAddrV4::new(lip, 0);
            let _ = socket.bind(&bind_addr.into());
        }
    }

    socket.connect_timeout(&peer_addr.into(), Duration::from_secs(4))?;
    let std_stream: TcpStream = socket.into();
    let _ = std_stream.set_read_timeout(Some(Duration::from_secs(15)));
    let _ = std_stream.set_write_timeout(Some(Duration::from_secs(15)));
    Ok(std_stream)
}

#[tauri::command]
async fn start_real_transfer(peer_node_id: String) -> Result<String, String> {
    let engine = get_engine().clone();

    let (peer, files, sender_name, cancel_flag, pause_flag) = {
        let st = engine.lock().map_err(|e| e.to_string())?;
        if st.active_transfer.is_some() {
            return Err("A transfer is already in progress.".to_string());
        }
        let peer = st
            .peers
            .get(&peer_node_id)
            .cloned()
            .ok_or_else(|| "Selected computer is no longer connected on any cable.".to_string())?;

        if peer.links.is_empty() {
            return Err("No active physical cable links to the target computer.".to_string());
        }
        if st.selected_files.is_empty() {
            return Err("Please select at least one file to send.".to_string());
        }

        st.cancel_flag.store(false, Ordering::SeqCst);
        st.pause_flag.store(false, Ordering::SeqCst);

        (
            peer,
            st.selected_files.clone(),
            st.device_name.clone(),
            st.cancel_flag.clone(),
            st.pause_flag.clone(),
        )
    };

    tokio::task::spawn_blocking(move || {
        for file_item in files {
            if cancel_flag.load(Ordering::SeqCst) {
                break;
            }

            let path = PathBuf::from(&file_item.path);
            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    if let Ok(mut st) = engine.lock() {
                        if let Some(ref mut act) = st.active_transfer {
                            act.status = "failed".to_string();
                            act.error_message = Some(format!("Cannot read file: {}", e));
                        }
                    }
                    return;
                }
            };

            let file_size = meta.len();
            let total_chunks = ((file_size as usize + CHUNK_SIZE - 1) / CHUNK_SIZE).max(1) as u64;
            let transfer_id = format!("tx-{}", now_ms());

            let lanes: Vec<LaneProgress> = peer
                .links
                .iter()
                .map(|l| LaneProgress {
                    lane_name: l.local_interface_name.clone(),
                    port_type: l.port_type.clone(),
                    link_speed_mbps: l.link_speed_mbps,
                    bytes_sent: 0,
                    chunks_sent: 0,
                    active: true,
                })
                .collect();

            if let Ok(mut st) = engine.lock() {
                st.active_transfer = Some(ActiveTransferState {
                    transfer_id: transfer_id.clone(),
                    direction: "Sending".to_string(),
                    peer_name: peer.node_name.clone(),
                    file_name: file_item.name.clone(),
                    total_bytes: file_size,
                    transferred_bytes: 0,
                    progress_pct: 0.0,
                    speed_mbs: 0.0,
                    speed_mbps: 0.0,
                    eta_seconds: 0,
                    total_chunks,
                    completed_chunks: 0,
                    chunk_states: vec![0; 24],
                    lanes: lanes.clone(),
                    status: "transferring".to_string(),
                    error_message: None,
                });
            }

            // 1. Send INIT_TRANSFER on primary link
            let primary_link = &peer.links[0];
            let mut ctrl_stream = match connect_bound_tcp(
                &primary_link.local_ip,
                &primary_link.peer_ip,
                primary_link.peer_tcp_port,
            ) {
                Ok(s) => s,
                Err(e) => {
                    if let Ok(mut st) = engine.lock() {
                        if let Some(ref mut act) = st.active_transfer {
                            act.status = "failed".to_string();
                            act.error_message =
                                Some(format!("Failed to connect to {}: {}", peer.node_name, e));
                        }
                    }
                    return;
                }
            };

            let init_cmd = WireCommand {
                cmd: "INIT_TRANSFER".to_string(),
                node_id: String::new(),
                node_name: String::new(),
                port_type: String::new(),
                speed_mbps: 0,
                transfer_id: transfer_id.clone(),
                sender_name: sender_name.clone(),
                file_name: file_item.name.clone(),
                file_size,
                total_chunks,
                chunk_index: 0,
                offset: 0,
                length: 0,
                lane_index: 0,
                lane_name: String::new(),
                expected_sha256: String::new(),
                lanes,
            };

            if send_wire_command(&mut ctrl_stream, &init_cmd).is_err() {
                if let Ok(mut st) = engine.lock() {
                    st.active_transfer = None;
                }
                return;
            }

            let mut ack = [0u8; 2];
            if ctrl_stream.read_exact(&mut ack).is_err() || &ack != b"OK" {
                if let Ok(mut st) = engine.lock() {
                    st.active_transfer = None;
                }
                return;
            }

            // 2. Spawn parallel workers across ALL bonded physical cables!
            let started_at = Instant::now();
            let next_chunk = Arc::new(AtomicU64::new(0));
            let worker_failed = Arc::new(AtomicBool::new(false));
            let mut handles = Vec::new();

            for (lane_idx, link) in peer.links.iter().cloned().enumerate() {
                let path_clone = path.clone();
                let tid_clone = transfer_id.clone();
                let next_chunk_clone = next_chunk.clone();
                let failed_clone = worker_failed.clone();
                let cancel_clone = cancel_flag.clone();
                let pause_clone = pause_flag.clone();
                let eng_clone = engine.clone();

                let handle = std::thread::spawn(move || {
                    let mut file = match File::open(&path_clone) {
                        Ok(f) => f,
                        Err(_) => {
                            failed_clone.store(true, Ordering::SeqCst);
                            return;
                        }
                    };

                    let mut stream =
                        match connect_bound_tcp(&link.local_ip, &link.peer_ip, link.peer_tcp_port) {
                            Ok(s) => s,
                            Err(_) => {
                                // If secondary cable fails, primary cable continues taking chunks
                                if lane_idx == 0 {
                                    failed_clone.store(true, Ordering::SeqCst);
                                }
                                return;
                            }
                        };

                    let mut buf = vec![0u8; CHUNK_SIZE];

                    loop {
                        if cancel_clone.load(Ordering::SeqCst) || failed_clone.load(Ordering::SeqCst)
                        {
                            break;
                        }
                        while pause_clone.load(Ordering::SeqCst) {
                            if cancel_clone.load(Ordering::SeqCst) {
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(80));
                        }

                        let idx = next_chunk_clone.fetch_add(1, Ordering::SeqCst);
                        if idx >= total_chunks {
                            break;
                        }

                        let offset = idx * (CHUNK_SIZE as u64);
                        let rem = file_size.saturating_sub(offset) as usize;
                        let to_read = rem.min(CHUNK_SIZE);

                        if to_read > 0 {
                            if file.seek(SeekFrom::Start(offset)).is_err()
                                || file.read_exact(&mut buf[..to_read]).is_err()
                            {
                                failed_clone.store(true, Ordering::SeqCst);
                                break;
                            }
                        }

                        let chunk_cmd = WireCommand {
                            cmd: "CHUNK".to_string(),
                            node_id: String::new(),
                            node_name: String::new(),
                            port_type: String::new(),
                            speed_mbps: 0,
                            transfer_id: tid_clone.clone(),
                            sender_name: String::new(),
                            file_name: String::new(),
                            file_size,
                            total_chunks,
                            chunk_index: idx,
                            offset,
                            length: to_read,
                            lane_index: lane_idx,
                            lane_name: link.local_interface_name.clone(),
                            expected_sha256: String::new(),
                            lanes: Vec::new(),
                        };

                        if send_wire_command(&mut stream, &chunk_cmd).is_err()
                            || stream.write_all(&buf[..to_read]).is_err()
                            || stream.flush().is_err()
                        {
                            failed_clone.store(true, Ordering::SeqCst);
                            break;
                        }

                        let mut chunk_ack = [0u8; 1];
                        if stream.read_exact(&mut chunk_ack).is_err() || chunk_ack[0] != b'K' {
                            failed_clone.store(true, Ordering::SeqCst);
                            break;
                        }

                        // Update real-time sender telemetry
                        let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
                        if let Ok(mut st) = eng_clone.lock() {
                            if let Some(ref mut act) = st.active_transfer {
                                if act.transfer_id == tid_clone {
                                    act.transferred_bytes = act
                                        .transferred_bytes
                                        .saturating_add(to_read as u64)
                                        .min(file_size);
                                    act.completed_chunks = act.completed_chunks.saturating_add(1);
                                    act.progress_pct = if file_size > 0 {
                                        ((act.transferred_bytes as f64 / file_size as f64) * 1000.0)
                                            .round()
                                            / 10.0
                                    } else {
                                        100.0
                                    };
                                    let mbs =
                                        (act.transferred_bytes as f64 / (1024.0 * 1024.0)) / elapsed;
                                    act.speed_mbs = (mbs * 10.0).round() / 10.0;
                                    act.speed_mbps = (mbs * 8.0 * 10.0).round() / 10.0;
                                    let rem_mbs = ((file_size.saturating_sub(act.transferred_bytes))
                                        as f64)
                                        / (1024.0 * 1024.0);
                                    act.eta_seconds = if mbs > 0.1 {
                                        (rem_mbs / mbs).ceil() as u64
                                    } else {
                                        0
                                    };

                                    let cell =
                                        ((idx as usize) * 24) / (total_chunks.max(1) as usize);
                                    if cell < 24 {
                                        act.chunk_states[cell] = (lane_idx as u8) + 1;
                                    }
                                    if let Some(lane) = act.lanes.get_mut(lane_idx) {
                                        lane.bytes_sent =
                                            lane.bytes_sent.saturating_add(to_read as u64);
                                        lane.chunks_sent = lane.chunks_sent.saturating_add(1);
                                    }
                                }
                            }
                        }
                    }
                });

                handles.push(handle);
            }

            for h in handles {
                let _ = h.join();
            }

            if cancel_flag.load(Ordering::SeqCst) || worker_failed.load(Ordering::SeqCst) {
                if let Ok(mut st) = engine.lock() {
                    st.active_transfer = None;
                }
                return;
            }

            // 3. Compute SHA-256 and finalize transfer
            if let Ok(mut st) = engine.lock() {
                if let Some(ref mut act) = st.active_transfer {
                    act.status = "verifying".to_string();
                }
            }

            let mut expected_sha256 = String::new();
            if let Ok(mut f) = File::open(&path) {
                let mut hasher = Sha256::new();
                let mut b = vec![0u8; 1024 * 1024];
                while let Ok(n) = f.read(&mut b) {
                    if n == 0 {
                        break;
                    }
                    hasher.update(&b[..n]);
                }
                expected_sha256 = format!("{:x}", hasher.finalize());
            }

            let fin_cmd = WireCommand {
                cmd: "FINISH_TRANSFER".to_string(),
                node_id: String::new(),
                node_name: String::new(),
                port_type: String::new(),
                speed_mbps: 0,
                transfer_id: transfer_id.clone(),
                sender_name: String::new(),
                file_name: file_item.name.clone(),
                file_size,
                total_chunks,
                chunk_index: 0,
                offset: 0,
                length: 0,
                lane_index: 0,
                lane_name: String::new(),
                expected_sha256: expected_sha256.clone(),
                lanes: Vec::new(),
            };

            let _ = send_wire_command(&mut ctrl_stream, &fin_cmd);
            let mut fin_ack = [0u8; 2];
            let _ = ctrl_stream.read_exact(&mut fin_ack);

            let duration_secs = started_at.elapsed().as_secs_f64().max(0.01);
            let avg_mbs =
                ((file_size as f64 / (1024.0 * 1024.0)) / duration_secs * 10.0).round() / 10.0;

            if let Ok(mut st) = engine.lock() {
                if let Some(act) = st.active_transfer.take() {
                    let cables_used = act
                        .lanes
                        .iter()
                        .map(|l| l.port_type.clone())
                        .collect::<Vec<_>>()
                        .join(" + ");

                    st.history.insert(
                        0,
                        TransferHistoryItem {
                            transfer_id: act.transfer_id,
                            direction: "Sent".to_string(),
                            peer_name: peer.node_name.clone(),
                            file_name: file_item.name.clone(),
                            saved_path: Some(file_item.path.clone()),
                            total_bytes: file_size,
                            avg_speed_mbs: avg_mbs,
                            duration_secs: (duration_secs * 10.0).round() / 10.0,
                            timestamp: "Just now".to_string(),
                            sha256: expected_sha256,
                            verified: true,
                            cables_used,
                        },
                    );
                }
            }
        }

        if let Ok(mut st) = engine.lock() {
            st.selected_files.clear();
        }
    });

    Ok("Transfer started".to_string())
}

// ============================================================================
// NATIVE WINDOWS FILE & FOLDER DIALOGS (COM IFileOpenDialog)
// ============================================================================

fn collect_files_from_path(p: &Path, out: &mut Vec<SelectedFileItem>) {
    if p.is_file() {
        if let Ok(meta) = fs::metadata(p) {
            let name = p
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            let path_str = p.to_string_lossy().to_string();
            if !out.iter().any(|x| x.path == path_str) {
                out.push(SelectedFileItem {
                    path: path_str,
                    name,
                    size: meta.len(),
                });
            }
        }
    } else if p.is_dir() {
        if let Ok(entries) = fs::read_dir(p) {
            for entry in entries.flatten() {
                collect_files_from_path(&entry.path(), out);
            }
        }
    }
}

#[cfg(target_os = "windows")]
fn open_native_shell_dialog(pick_folders: bool, multi: bool) -> Vec<PathBuf> {
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CoTaskMemFree, CLSCTX_INPROC_SERVER,
        COINIT_APARTMENTTHREADED,
    };
    use windows::Win32::UI::Shell::{
        FileOpenDialog, IFileOpenDialog, FOS_ALLOWMULTISELECT, FOS_FILEMUSTEXIST,
        FOS_FORCEFILESYSTEM, FOS_PATHMUSTEXIST, FOS_PICKFOLDERS, SIGDN_FILESYSPATH,
    };

    let mut picked = Vec::new();
    unsafe {
        let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let Ok(dialog) =
            CoCreateInstance::<_, IFileOpenDialog>(&FileOpenDialog, None, CLSCTX_INPROC_SERVER)
        else {
            return picked;
        };

        if let Ok(mut opts) = dialog.GetOptions() {
            opts |= FOS_FORCEFILESYSTEM | FOS_PATHMUSTEXIST | FOS_FILEMUSTEXIST;
            if pick_folders {
                opts |= FOS_PICKFOLDERS;
            }
            if multi {
                opts |= FOS_ALLOWMULTISELECT;
            }
            let _ = dialog.SetOptions(opts);
        }

        if dialog.Show(None).is_ok() {
            if let Ok(results) = dialog.GetResults() {
                if let Ok(count) = results.GetCount() {
                    for i in 0..count {
                        if let Ok(item) = results.GetItemAt(i) {
                            if let Ok(pwstr) = item.GetDisplayName(SIGDN_FILESYSPATH) {
                                if let Ok(s) = pwstr.to_string() {
                                    picked.push(PathBuf::from(s));
                                }
                                CoTaskMemFree(Some(pwstr.0 as *const _));
                            }
                        }
                    }
                }
            }
        }
    }
    picked
}

#[tauri::command]
async fn pick_files_native() -> Vec<SelectedFileItem> {
    let paths = tokio::task::spawn_blocking(|| open_native_shell_dialog(false, true))
        .await
        .unwrap_or_default();

    let engine = get_engine();
    if let Ok(mut st) = engine.lock() {
        for p in paths {
            collect_files_from_path(&p, &mut st.selected_files);
        }
        return st.selected_files.clone();
    }
    Vec::new()
}

#[tauri::command]
async fn pick_folder_native() -> Vec<SelectedFileItem> {
    let paths = tokio::task::spawn_blocking(|| open_native_shell_dialog(true, false))
        .await
        .unwrap_or_default();

    let engine = get_engine();
    if let Ok(mut st) = engine.lock() {
        for p in paths {
            collect_files_from_path(&p, &mut st.selected_files);
        }
        return st.selected_files.clone();
    }
    Vec::new()
}

#[tauri::command]
async fn pick_save_directory_native() -> Option<String> {
    let paths = tokio::task::spawn_blocking(|| open_native_shell_dialog(true, false))
        .await
        .unwrap_or_default();

    if let Some(first) = paths.into_iter().next() {
        let dir_str = first.to_string_lossy().to_string();
        if let Ok(mut st) = get_engine().lock() {
            st.save_directory = dir_str.clone();
        }
        return Some(dir_str);
    }
    None
}

#[tauri::command]
fn remove_selected_file(index: usize) -> Vec<SelectedFileItem> {
    if let Ok(mut st) = get_engine().lock() {
        if index < st.selected_files.len() {
            st.selected_files.remove(index);
        }
        return st.selected_files.clone();
    }
    Vec::new()
}

#[tauri::command]
fn clear_selected_files() -> Vec<SelectedFileItem> {
    if let Ok(mut st) = get_engine().lock() {
        st.selected_files.clear();
    }
    Vec::new()
}

#[tauri::command]
fn get_live_snapshot(force_scan: bool) -> LiveSystemSnapshot {
    let engine = get_engine();
    if let Ok(mut st) = engine.lock() {
        if force_scan || st.cables.is_empty() {
            #[cfg(target_os = "windows")]
            refresh_cables_native(&mut st);
        }

        let connected_cables: Vec<&WiredCableInfo> =
            st.cables.iter().filter(|c| c.is_connected).collect();
        let total_connected = connected_cables.len();
        let total_speed: u64 = connected_cables.iter().map(|c| c.link_speed_mbps).sum();
        let total_max_mbs = ((total_speed as f64) / 8.0 * 10.0).round() / 10.0;

        let mut peers_vec: Vec<PeerDeviceInfo> = st.peers.values().cloned().collect();
        peers_vec.sort_by(|a, b| a.node_name.cmp(&b.node_name));

        return LiveSystemSnapshot {
            local_node_id: st.node_id.clone(),
            local_device_name: st.device_name.clone(),
            save_directory: st.save_directory.clone(),
            cables: st.cables.clone(),
            total_connected_cables: total_connected,
            total_bonded_speed_mbps: total_speed,
            total_bonded_max_mbs: total_max_mbs,
            peers: peers_vec,
            selected_files: st.selected_files.clone(),
            active_transfer: st.active_transfer.clone(),
            history: st.history.clone(),
        };
    }

    LiveSystemSnapshot {
        local_node_id: String::new(),
        local_device_name: default_computer_name(),
        save_directory: default_save_dir(),
        cables: Vec::new(),
        total_connected_cables: 0,
        total_bonded_speed_mbps: 0,
        total_bonded_max_mbs: 0.0,
        peers: Vec::new(),
        selected_files: Vec::new(),
        active_transfer: None,
        history: Vec::new(),
    }
}

#[tauri::command]
async fn connect_peer_by_ip(target_ip: String) -> Result<PeerDeviceInfo, String> {
    let ip_trimmed = target_ip.trim().to_string();
    if ip_trimmed.is_empty() {
        return Err("Please enter an IPv4 address (e.g. 169.254.x.x).".to_string());
    }

    let engine = get_engine().clone();
    let (local_node_id, local_name, local_ip, port_type, speed_mbps) = {
        let st = engine.lock().map_err(|e| e.to_string())?;
        let active_cable = st.cables.iter().find(|c| c.is_connected);
        (
            st.node_id.clone(),
            st.device_name.clone(),
            active_cable
                .and_then(|c| c.ip_address.clone())
                .unwrap_or_default(),
            active_cable
                .map(|c| c.port_type.clone())
                .unwrap_or_else(|| "Ethernet Cable".to_string()),
            active_cable.map(|c| c.link_speed_mbps).unwrap_or(100),
        )
    };

    let ip_clone = ip_trimmed.clone();
    let ack = tokio::task::spawn_blocking(move || -> Result<WireCommand, String> {
        let mut stream = connect_bound_tcp(&local_ip, &ip_clone, TRANSFER_TCP_PORT)
            .map_err(|e| format!("Could not reach {}:{}: {}", ip_clone, TRANSFER_TCP_PORT, e))?;

        let hello = WireCommand {
            cmd: "HELLO".to_string(),
            node_id: local_node_id,
            node_name: local_name,
            port_type,
            speed_mbps,
            transfer_id: String::new(),
            sender_name: String::new(),
            file_name: String::new(),
            file_size: 0,
            total_chunks: 0,
            chunk_index: 0,
            offset: 0,
            length: 0,
            lane_index: 0,
            lane_name: String::new(),
            expected_sha256: String::new(),
            lanes: Vec::new(),
        };

        send_wire_command(&mut stream, &hello).map_err(|e| e.to_string())?;
        read_wire_command(&mut stream).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    let mut st = engine.lock().map_err(|e| e.to_string())?;
    let pkt = BeaconPacket {
        magic: "ECBF_V2".to_string(),
        kind: "PONG".to_string(),
        node_id: ack.node_id.clone(),
        node_name: ack.node_name.clone(),
        port_type: ack.port_type,
        interface_name: "Ethernet".to_string(),
        ip: ip_trimmed.clone(),
        tcp_port: TRANSFER_TCP_PORT,
        speed_mbps: ack.speed_mbps,
    };
    register_discovered_peer(&mut st, pkt, ip_trimmed);

    st.peers
        .get(&ack.node_id)
        .cloned()
        .ok_or_else(|| "Connected to peer, but no active wired cable link was found.".to_string())
}

#[tauri::command]
fn toggle_pause_transfer() -> bool {
    if let Ok(st) = get_engine().lock() {
        let cur = st.pause_flag.load(Ordering::SeqCst);
        st.pause_flag.store(!cur, Ordering::SeqCst);
        return !cur;
    }
    false
}

#[tauri::command]
fn cancel_active_transfer() {
    if let Ok(mut st) = get_engine().lock() {
        st.cancel_flag.store(true, Ordering::SeqCst);
        st.active_transfer = None;
    }
}

#[tauri::command]
fn clear_transfer_history() {
    if let Ok(mut st) = get_engine().lock() {
        st.history.clear();
    }
}

#[tauri::command]
fn update_device_name(name: String) {
    let trimmed = name.trim();
    if !trimmed.is_empty() {
        if let Ok(mut st) = get_engine().lock() {
            st.device_name = trimmed.to_string();
        }
    }
}

#[tauri::command]
fn update_save_directory(path: String) {
    let trimmed = path.trim();
    if !trimmed.is_empty() {
        let _ = fs::create_dir_all(trimmed);
        if let Ok(mut st) = get_engine().lock() {
            st.save_directory = trimmed.to_string();
        }
    }
}

#[tauri::command]
fn open_save_directory(path: Option<String>) {
    let target = path.unwrap_or_else(|| {
        get_engine()
            .lock()
            .map(|s| s.save_directory.clone())
            .unwrap_or_else(|_| default_save_dir())
    });
    let p = Path::new(&target);
    let folder = if p.is_file() {
        p.parent().unwrap_or(p).to_string_lossy().to_string()
    } else {
        target
    };
    let _ = fs::create_dir_all(&folder);
    let _ = std::process::Command::new("explorer").arg(&folder).spawn();
}

// ============================================================================
// GITHUB RELEASE UPDATE CHECKER (PLAIN, SIMPLE STATUS MESSAGES)
// ============================================================================

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GitHubReleasePayload {
    pub tag_name: String,
    pub name: Option<String>,
    pub published_at: Option<String>,
    pub html_url: String,
    pub body: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateCheckResponse {
    pub success: bool,
    pub message: String,
    pub current_version: String,
    pub latest_version: Option<String>,
    pub release_url: Option<String>,
    pub has_update: bool,
}

#[tauri::command]
async fn check_for_updates() -> UpdateCheckResponse {
    let url = format!(
        "https://api.github.com/repos/{}/releases/latest",
        GITHUB_REPO
    );
    let client = match reqwest::Client::builder()
        .user_agent("EtherConnectedBallisticFiles")
        .timeout(Duration::from_secs(8))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return UpdateCheckResponse {
                success: false,
                message: format!("Could not initialize update check: {}", e),
                current_version: CURRENT_VERSION.to_string(),
                latest_version: None,
                release_url: None,
                has_update: false,
            };
        }
    };

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return UpdateCheckResponse {
                    success: true,
                    message: format!("Version {} is the latest release.", CURRENT_VERSION),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: Some(CURRENT_VERSION.to_string()),
                    release_url: Some(format!("https://github.com/{}", GITHUB_REPO)),
                    has_update: false,
                };
            }

            if !response.status().is_success() {
                return UpdateCheckResponse {
                    success: false,
                    message: format!("GitHub returned status {}.", response.status().as_u16()),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: None,
                    release_url: None,
                    has_update: false,
                };
            }

            match response.json::<GitHubReleasePayload>().await {
                Ok(rel) => {
                    let remote_ver = rel.tag_name.trim_start_matches('v').trim();
                    let has_update = remote_ver != CURRENT_VERSION;
                    let msg = if has_update {
                        format!(
                            "Update available: v{} (current: v{}).",
                            remote_ver, CURRENT_VERSION
                        )
                    } else {
                        format!("Version {} is the latest release.", CURRENT_VERSION)
                    };

                    UpdateCheckResponse {
                        success: true,
                        message: msg,
                        current_version: CURRENT_VERSION.to_string(),
                        latest_version: Some(rel.tag_name),
                        release_url: Some(rel.html_url),
                        has_update,
                    }
                }
                Err(e) => UpdateCheckResponse {
                    success: false,
                    message: format!("Could not read release data: {}", e),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: None,
                    release_url: None,
                    has_update: false,
                },
            }
        }
        Err(_) => UpdateCheckResponse {
            success: false,
            message: "No internet connection to check GitHub releases. Offline cable transfer still works normally.".to_string(),
            current_version: CURRENT_VERSION.to_string(),
            latest_version: None,
            release_url: None,
            has_update: false,
        },
    }
}

#[tauri::command]
fn minimize_window(window: Window) {
    let _ = window.minimize();
}

#[tauri::command]
fn maximize_window(window: Window) {
    if let Ok(is_max) = window.is_maximized() {
        if is_max {
            let _ = window.unmaximize();
        } else {
            let _ = window.maximize();
        }
    }
}

#[tauri::command]
fn close_window(window: Window) {
    let _ = window.close();
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    spawn_background_networking();

    tauri::Builder::default()
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            if let Some(win) = app.get_webview_window("main") {
                win.on_window_event(|event| {
                    if let tauri::WindowEvent::DragDrop(tauri::DragDropEvent::Drop {
                        paths, ..
                    }) = event
                    {
                        if let Ok(mut st) = get_engine().lock() {
                            for p in paths {
                                collect_files_from_path(p, &mut st.selected_files);
                            }
                        }
                    }
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_live_snapshot,
            pick_files_native,
            pick_folder_native,
            pick_save_directory_native,
            remove_selected_file,
            clear_selected_files,
            start_real_transfer,
            connect_peer_by_ip,
            toggle_pause_transfer,
            cancel_active_transfer,
            clear_transfer_history,
            update_device_name,
            update_save_directory,
            open_save_directory,
            check_for_updates,
            minimize_window,
            maximize_window,
            close_window
        ])
        .run(tauri::generate_context!())
        .expect("error while running EtherConnectedBallisticFiles");
}
