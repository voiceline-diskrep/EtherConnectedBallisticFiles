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

const CURRENT_VERSION: &str = "0.1.1";
const GITHUB_REPO: &str = "voiceline-diskrep/EtherConnectedBallisticFiles";
const DISCOVERY_UDP_PORT: u16 = 45678;
const TRANSFER_TCP_PORT: u16 = 45679;
const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MB per stripe chunk

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
    #[serde(default)]
    pub local_if_index: u32,
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
    #[serde(default)]
    pub batch_total_files: usize,
    #[serde(default)]
    pub batch_completed_files: usize,
    #[serde(default)]
    pub batch_total_bytes: u64,
    #[serde(default)]
    pub batch_transferred_bytes: u64,
    #[serde(default)]
    pub batch_progress_pct: f64,
    #[serde(default)]
    pub queued_files_count: usize,
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
    pub transfer_queue: std::collections::VecDeque<SelectedFileItem>,
    pub batch_completed_files: usize,
    pub batch_completed_bytes: u64,
    pub batch_total_files: usize,
    pub batch_total_bytes: u64,
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
            transfer_queue: std::collections::VecDeque::new(),
            batch_completed_files: 0,
            batch_completed_bytes: 0,
            batch_total_files: 0,
            batch_total_bytes: 0,
            active_transfer: Vec::new().pop(),
            history: Vec::new(),
            prev_octets: HashMap::new(),
            cancel_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
        }))
    })
}

fn compute_batch_pct(transferred: u64, total: u64) -> f64 {
    if total > 0 {
        (((transferred as f64 / total as f64) * 1000.0).round() / 10.0).min(100.0)
    } else {
        100.0
    }
}

fn enqueue_files_into_active_transfer(st: &mut EngineState, new_files: Vec<SelectedFileItem>) {
    if new_files.is_empty() {
        return;
    }
    let added_count = new_files.len();
    let added_bytes: u64 = new_files.iter().map(|f| f.size).sum();
    for f in new_files {
        st.transfer_queue.push_back(f);
    }
    st.batch_total_files = st.batch_total_files.saturating_add(added_count);
    st.batch_total_bytes = st.batch_total_bytes.saturating_add(added_bytes);
    let q_len = st.transfer_queue.len();
    let b_total_files = st.batch_total_files;
    let b_total_bytes = st.batch_total_bytes;
    if let Some(ref mut act) = st.active_transfer {
        act.batch_total_files = b_total_files;
        act.batch_total_bytes = b_total_bytes;
        act.queued_files_count = q_len;
        act.batch_progress_pct = compute_batch_pct(act.batch_transferred_bytes, b_total_bytes);
    }
}

fn update_chunk_states(
    chunk_states: &mut [u8],
    completed_chunks: u64,
    total_chunks: u64,
    lane_index: usize,
) {
    let total = total_chunks.max(1) as usize;
    let filled = if completed_chunks >= total_chunks {
        chunk_states.len()
    } else {
        (((completed_chunks as usize) * chunk_states.len()) / total).min(chunk_states.len())
    };
    let lane_val = (lane_index as u8) + 1;
    for cell in &mut chunk_states[..filled] {
        if *cell == 0 {
            *cell = lane_val;
        }
    }
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

            let raw_speed_bps = row.TransmitLinkSpeed.max(row.ReceiveLinkSpeed);
            let is_connected = row.MediaConnectState != MediaConnectStateDisconnected;
            let speed_bps = if is_connected { raw_speed_bps } else { 0 };
            let speed_mbps = speed_bps / 1_000_000;
            let max_speed_mbs = ((speed_mbps as f64) / 8.0 * 10.0).round() / 10.0;

            let link_speed_display = if !is_connected {
                "Disconnected".to_string()
            } else if speed_mbps == 0 {
                "Negotiating Link...".to_string()
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

fn find_cable_for_peer(
    cables: &[WiredCableInfo],
    peer_ip: &str,
    local_ip_hint: Option<&str>,
) -> Option<WiredCableInfo> {
    if let Some(lip) = local_ip_hint {
        if !lip.is_empty() && lip != "0.0.0.0" {
            if let Some(c) = cables
                .iter()
                .find(|c| c.is_connected && c.ip_address.as_deref() == Some(lip))
            {
                return Some(c.clone());
            }
        }
    }

    if let Ok(peer_ipv4) = peer_ip.parse::<Ipv4Addr>() {
        // 1. Check ARP neighbor table per interface FIRST so two APIPA (169.254.0.0/16) cables stay distinct
        #[cfg(target_os = "windows")]
        for cable in cables {
            if !cable.is_connected {
                continue;
            }
            if query_arp_neighbors_for_interface(cable.if_index).contains(&peer_ipv4) {
                return Some(cable.clone());
            }
        }

        // 2. Fallback to subnet match
        for cable in cables {
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
                        return Some(cable.clone());
                    }
                }
            }
        }
    }

    // 3. Fallback to first connected cable (e.g. cross-subnet static IP)
    cables.iter().find(|c| c.is_connected).cloned()
}

fn register_discovered_peer(state: &mut EngineState, pkt: BeaconPacket, peer_ip_actual: String) {
    if pkt.node_id == state.node_id {
        return; // Never register ourselves
    }

    if peer_ip_actual.parse::<Ipv4Addr>().is_err() {
        return;
    }

    let Some(cable) = find_cable_for_peer(&state.cables, &peer_ip_actual, None) else {
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
        local_if_index: cable.if_index,
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

    if let Some(existing) = entry.links.iter_mut().find(|l| {
        (l.local_interface_id == link.local_interface_id && l.peer_ip == link.peer_ip)
            || l.local_interface_id == link.local_interface_id
    }) {
        *existing = link;
    } else {
        entry.links.push(link);
    }

    // Prune stale links (> 6s) and recompute combined bandwidth
    entry.links.retain(|l| ts.saturating_sub(l.last_seen_ms) <= 6500);
    entry.combined_speed_mbps = entry.links.iter().map(|l| l.link_speed_mbps).sum();
    entry.combined_max_mbs = ((entry.combined_speed_mbps as f64) / 8.0 * 10.0).round() / 10.0;
}

fn ensure_public_firewall_rules() {
    #[cfg(target_os = "windows")]
    std::thread::spawn(|| {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;

        // 1. Update any existing Windows Firewall "Query User" rules for this app from Private-only to Any (Domain, Private, Public)
        // 2. Add explicit Public/Private/Domain allow rules for UDP 45678 and TCP 45679 if elevated
        let ps_script = r#"
$ErrorActionPreference = 'SilentlyContinue'
Get-NetFirewallRule | Where-Object { $_.DisplayName -match 'ether_connected_ballistic_files|EtherConnectedBallisticFiles' } | Set-NetFirewallRule -Profile Any -Action Allow -Enabled True
New-NetFirewallRule -DisplayName 'EtherConnectedBallisticFiles Discovery UDP' -Direction Inbound -Protocol UDP -LocalPort 45678 -Profile Any -Action Allow -Enabled True
New-NetFirewallRule -DisplayName 'EtherConnectedBallisticFiles Transfer TCP' -Direction Inbound -Protocol TCP -LocalPort 45679 -Profile Any -Action Allow -Enabled True
"#;
        let _ = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", ps_script])
            .creation_flags(CREATE_NO_WINDOW)
            .output();
    });
}

fn spawn_background_networking() {
    ensure_public_firewall_rules();
    let engine = get_engine().clone();

    // 1. UDP Listener Thread on 0.0.0.0:45678 (plus Multicast groups 224.0.0.251 & 239.255.255.250)
    let engine_udp = engine.clone();
    std::thread::spawn(move || {
        let sock = match UdpSocket::bind(("0.0.0.0", DISCOVERY_UDP_PORT)) {
            Ok(s) => s,
            Err(_) => return,
        };
        let _ = sock.set_broadcast(true);
        let _ = sock.join_multicast_v4(&Ipv4Addr::new(224, 0, 0, 251), &Ipv4Addr::UNSPECIFIED);
        let _ = sock.join_multicast_v4(&Ipv4Addr::new(239, 255, 255, 250), &Ipv4Addr::UNSPECIFIED);
        let mut buf = [0u8; 2048];

        loop {
            if let Ok((len, src_addr)) = sock.recv_from(&mut buf) {
                if let Ok(pkt) = serde_json::from_slice::<BeaconPacket>(&buf[..len]) {
                    if pkt.magic == "ECBF_V2" {
                        let peer_ip = src_addr.ip().to_string();
                        let mut reply_pong: Option<BeaconPacket> = None;
                        let mut tcp_reverse_hello: Option<(String, Option<u32>, String, String, String, u64)> =
                            None;

                        if let Ok(mut st) = engine_udp.lock() {
                            if pkt.node_id != st.node_id {
                                let active_cable = find_cable_for_peer(&st.cables, &peer_ip, None);
                                let lip = active_cable
                                    .as_ref()
                                    .and_then(|c| c.ip_address.clone())
                                    .unwrap_or_default();
                                let if_idx = active_cable.as_ref().map(|c| c.if_index);
                                let ptype = active_cable
                                    .as_ref()
                                    .map(|c| c.port_type.clone())
                                    .unwrap_or_else(|| "Ethernet Cable".to_string());
                                let ifname = active_cable
                                    .as_ref()
                                    .map(|c| c.name.clone())
                                    .unwrap_or_else(|| "Ethernet".to_string());
                                let spd = active_cable
                                    .as_ref()
                                    .map(|c| c.link_speed_mbps)
                                    .unwrap_or(100);

                                if pkt.kind == "BEACON" {
                                    // Prepare unicast PONG reply AND reverse TCP HELLO so the sender immediately discovers us
                                    // even if the sender's Windows Firewall drops inbound UDP on "Unidentified / Public Network"!
                                    reply_pong = Some(BeaconPacket {
                                        magic: "ECBF_V2".to_string(),
                                        kind: "PONG".to_string(),
                                        node_id: st.node_id.clone(),
                                        node_name: st.device_name.clone(),
                                        port_type: ptype.clone(),
                                        interface_name: ifname,
                                        ip: lip.clone(),
                                        tcp_port: TRANSFER_TCP_PORT,
                                        speed_mbps: spd,
                                    });
                                    tcp_reverse_hello = Some((
                                        lip,
                                        if_idx,
                                        st.node_id.clone(),
                                        st.device_name.clone(),
                                        ptype,
                                        spd,
                                    ));
                                }
                                register_discovered_peer(&mut st, pkt, peer_ip.clone());
                            }
                        }

                        if let Some(pong) = reply_pong {
                            if let Ok(bytes) = serde_json::to_vec(&pong) {
                                // Reply to both src_addr (in case of stateful NAT/firewall pinhole) AND port 45678
                                let _ = sock.send_to(&bytes, src_addr);
                                let _ = sock.send_to(&bytes, (peer_ip.as_str(), DISCOVERY_UDP_PORT));
                            }
                        }

                        if let Some((lip, if_idx, nid, nname, ptype, spd)) = tcp_reverse_hello {
                            let pip = peer_ip.clone();
                            std::thread::spawn(move || {
                                if let Ok(mut stream) = connect_bound_tcp_fast(
                                    &lip,
                                    &pip,
                                    TRANSFER_TCP_PORT,
                                    350,
                                    if_idx,
                                ) {
                                    let hello = WireCommand {
                                        cmd: "HELLO".to_string(),
                                        node_id: nid,
                                        node_name: nname,
                                        port_type: ptype,
                                        speed_mbps: spd,
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
                                        ..Default::default()
                                    };
                                    let _ = send_wire_command(&mut stream, &hello);
                                }
                            });
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
            let local_ipv4 = cable
                .ip_address
                .as_deref()
                .and_then(|s| s.parse::<Ipv4Addr>().ok())
                .unwrap_or(Ipv4Addr::UNSPECIFIED);

            let pkt = BeaconPacket {
                magic: "ECBF_V2".to_string(),
                kind: "BEACON".to_string(),
                node_id: node_id.clone(),
                node_name: node_name.clone(),
                port_type: cable.port_type.clone(),
                interface_name: cable.name.clone(),
                ip: cable.ip_address.clone().unwrap_or_default(),
                tcp_port: TRANSFER_TCP_PORT,
                speed_mbps: cable.link_speed_mbps,
            };

            if let Ok(bytes) = serde_json::to_vec(&pkt) {
                let bind_ip = if local_ipv4.is_unspecified() {
                    Ipv4Addr::UNSPECIFIED
                } else {
                    local_ipv4
                };

                if let Ok(sock) = UdpSocket::bind((bind_ip, 0)) {
                    let _ = sock.set_broadcast(true);

                    // 1. Multicast discovery group (punches through Windows Public Firewall "Core Networking" rules!)
                    let _ = sock.send_to(
                        &bytes,
                        (Ipv4Addr::new(224, 0, 0, 251), DISCOVERY_UDP_PORT),
                    );
                    let _ = sock.send_to(
                        &bytes,
                        (Ipv4Addr::new(239, 255, 255, 250), DISCOVERY_UDP_PORT),
                    );

                    // 2. Directed subnet broadcast + APIPA link-local broadcast + Global broadcast
                    if !local_ipv4.is_unspecified() {
                        let mask = if cable.prefix_len == 0 {
                            0
                        } else {
                            !0u32 << (32 - cable.prefix_len.min(32))
                        };
                        let bcast_u32 = u32::from(local_ipv4) | !mask;
                        let subnet_bcast = Ipv4Addr::from(bcast_u32);
                        let _ = sock.send_to(&bytes, (subnet_bcast, DISCOVERY_UDP_PORT));
                    }

                    let _ = sock.send_to(
                        &bytes,
                        (Ipv4Addr::new(169, 254, 255, 255), DISCOVERY_UDP_PORT),
                    );
                    let _ = sock.send_to(
                        &bytes,
                        (Ipv4Addr::new(255, 255, 255, 255), DISCOVERY_UDP_PORT),
                    );

                    // 3. Direct unicast UDP + Bidirectional TCP HELLO to every neighbor on this physical wire!
                    // Outbound TCP punches a stateful hole in Windows Firewall so even if Inbound UDP is blocked
                    // on "Public / Unidentified Network", both PC and Laptop immediately discover each other!
                    #[cfg(target_os = "windows")]
                    for neighbor_ip in query_arp_neighbors_for_interface(cable.if_index) {
                        if neighbor_ip != local_ipv4 {
                            let _ = sock.send_to(&bytes, (neighbor_ip, DISCOVERY_UDP_PORT));

                            let eng_probe = engine_scan.clone();
                            let lip_str = cable.ip_address.clone().unwrap_or_default();
                            let nip_str = neighbor_ip.to_string();
                            let nid = node_id.clone();
                            let nname = node_name.clone();
                            let ptype = cable.port_type.clone();
                            let spd = cable.link_speed_mbps;
                            let if_idx = cable.if_index;

                            std::thread::spawn(move || {
                                if let Ok(mut stream) = connect_bound_tcp_fast(
                                    &lip_str,
                                    &nip_str,
                                    TRANSFER_TCP_PORT,
                                    350,
                                    Some(if_idx),
                                ) {
                                    let hello = WireCommand {
                                        cmd: "HELLO".to_string(),
                                        node_id: nid,
                                        node_name: nname,
                                        port_type: ptype,
                                        speed_mbps: spd,
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
                                        ..Default::default()
                                    };
                                    if send_wire_command(&mut stream, &hello).is_ok() {
                                        if let Ok(ack) = read_wire_command(&mut stream) {
                                            if ack.cmd == "HELLO_ACK" && !ack.node_id.is_empty() {
                                                if let Ok(mut st) = eng_probe.lock() {
                                                    let ack_pkt = BeaconPacket {
                                                        magic: "ECBF_V2".to_string(),
                                                        kind: "PONG".to_string(),
                                                        node_id: ack.node_id,
                                                        node_name: ack.node_name,
                                                        port_type: ack.port_type,
                                                        interface_name: "Ethernet".to_string(),
                                                        ip: nip_str.clone(),
                                                        tcp_port: TRANSFER_TCP_PORT,
                                                        speed_mbps: ack.speed_mbps,
                                                    };
                                                    register_discovered_peer(&mut st, ack_pkt, nip_str);
                                                }
                                            }
                                        }
                                    }
                                }
                            });
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

#[derive(Serialize, Deserialize, Debug, Default)]
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
    #[serde(default)]
    batch_total_files: usize,
    #[serde(default)]
    batch_completed_files: usize,
    #[serde(default)]
    batch_total_bytes: u64,
    #[serde(default)]
    batch_bytes_before: u64,
}

fn read_wire_command_into(
    stream: &mut TcpStream,
    header_buf: &mut Vec<u8>,
) -> std::io::Result<WireCommand> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let header_len = u32::from_be_bytes(len_buf) as usize;
    if header_len == 0 || header_len > 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid header length",
        ));
    }
    header_buf.resize(header_len, 0);
    stream.read_exact(&mut header_buf[..header_len])?;
    serde_json::from_slice(&header_buf[..header_len])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn read_wire_command(stream: &mut TcpStream) -> std::io::Result<WireCommand> {
    let mut header_buf = Vec::with_capacity(512);
    read_wire_command_into(stream, &mut header_buf)
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
    file: Mutex<File>,
    started_at: Instant,
    total_bytes: u64,
    total_chunks: u64,
    batch_total_bytes: u64,
    batch_bytes_before: u64,
    bytes_received: AtomicU64,
    chunks_received: AtomicU64,
    last_ui_sync_ms: AtomicU64,
    lane_bytes: [AtomicU64; 16],
    lane_chunks: [AtomicU64; 16],
}

static INCOMING_FILES: OnceLock<Mutex<HashMap<String, Arc<IncomingFileTrack>>>> = OnceLock::new();

fn get_incoming_files() -> &'static Mutex<HashMap<String, Arc<IncomingFileTrack>>> {
    INCOMING_FILES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn handle_incoming_tcp_client(mut stream: TcpStream, engine: Arc<Mutex<EngineState>>) {
    let _ = stream.set_nodelay(true);
    {
        let sock_ref = socket2::SockRef::from(&stream);
        let _ = sock_ref.set_recv_buffer_size(4 * 1024 * 1024);
        let _ = sock_ref.set_send_buffer_size(4 * 1024 * 1024);
    }
    let peer_ip = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let local_ip_hint = stream.local_addr().ok().map(|a| a.ip().to_string());

    let mut header_buf: Vec<u8> = Vec::with_capacity(512);
    let mut payload_buf: Vec<u8> = Vec::new();
    let mut cached_track: Option<(String, Arc<IncomingFileTrack>)> = None;

    loop {
        let req = match read_wire_command_into(&mut stream, &mut header_buf) {
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
                    ..Default::default()
                };

                if let Ok(mut st) = engine.lock() {
                    reply.node_id = st.node_id.clone();
                    reply.node_name = st.device_name.clone();
                    if let Some(c) =
                        find_cable_for_peer(&st.cables, &peer_ip, local_ip_hint.as_deref())
                    {
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
                    let batch_total_files = req.batch_total_files.max(1);
                    let batch_completed_files = req.batch_completed_files;
                    let batch_total_bytes = req.batch_total_bytes.max(req.file_size);
                    let batch_bytes_before = req.batch_bytes_before;
                    let batch_progress_pct = if batch_total_bytes > 0 {
                        compute_batch_pct(batch_bytes_before, batch_total_bytes)
                    } else {
                        0.0
                    };
                    let queued_files_count =
                        batch_total_files.saturating_sub(batch_completed_files.saturating_add(1));

                    if let Ok(mut map) = get_incoming_files().lock() {
                        map.insert(
                            req.transfer_id.clone(),
                            Arc::new(IncomingFileTrack {
                                part_path,
                                final_path,
                                file: Mutex::new(file),
                                started_at: Instant::now(),
                                total_bytes: req.file_size,
                                total_chunks: req.total_chunks.max(1),
                                batch_total_bytes,
                                batch_bytes_before,
                                bytes_received: AtomicU64::new(0),
                                chunks_received: AtomicU64::new(0),
                                last_ui_sync_ms: AtomicU64::new(0),
                                lane_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
                                lane_chunks: std::array::from_fn(|_| AtomicU64::new(0)),
                            }),
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
                            batch_total_files,
                            batch_completed_files,
                            batch_total_bytes,
                            batch_transferred_bytes: batch_bytes_before,
                            batch_progress_pct,
                            queued_files_count,
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
                if payload_buf.len() < req.length {
                    payload_buf.resize(req.length, 0);
                }
                if req.length > 0 && stream.read_exact(&mut payload_buf[..req.length]).is_err() {
                    break;
                }

                let track_opt = if let Some((ref tid, ref arc)) = cached_track {
                    if tid == &req.transfer_id {
                        Some(arc.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
                .or_else(|| {
                    let found = get_incoming_files()
                        .lock()
                        .ok()
                        .and_then(|m| m.get(&req.transfer_id).cloned());
                    if let Some(ref arc) = found {
                        cached_track = Some((req.transfer_id.clone(), arc.clone()));
                    }
                    found
                });

                if let Some(track) = track_opt {
                    let write_ok = if let Ok(mut f) = track.file.lock() {
                        f.seek(SeekFrom::Start(req.offset)).is_ok()
                            && (req.length == 0 || f.write_all(&payload_buf[..req.length]).is_ok())
                    } else {
                        false
                    };
                    if !write_ok {
                        break;
                    }

                    let total_rx = track
                        .bytes_received
                        .fetch_add(req.length as u64, Ordering::Relaxed)
                        .saturating_add(req.length as u64)
                        .min(track.total_bytes);
                    let total_chk = track
                        .chunks_received
                        .fetch_add(1, Ordering::Relaxed)
                        .saturating_add(1);
                    if req.lane_index < 16 {
                        track.lane_bytes[req.lane_index]
                            .fetch_add(req.length as u64, Ordering::Relaxed);
                        track.lane_chunks[req.lane_index].fetch_add(1, Ordering::Relaxed);
                    }

                    let now = now_ms();
                    let prev_sync = track.last_ui_sync_ms.load(Ordering::Relaxed);
                    let should_sync = now.saturating_sub(prev_sync) >= 100
                        || total_chk >= track.total_chunks;
                    if should_sync
                        && track
                            .last_ui_sync_ms
                            .compare_exchange(prev_sync, now, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                    {
                        let elapsed_secs = track.started_at.elapsed().as_secs_f64().max(0.001);
                        if let Ok(mut st) = engine.lock() {
                            if let Some(ref mut act) = st.active_transfer {
                                if act.transfer_id == req.transfer_id {
                                    act.transferred_bytes = total_rx;
                                    act.completed_chunks = total_chk;
                                    act.progress_pct = if act.total_bytes > 0 {
                                        ((total_rx as f64 / act.total_bytes as f64) * 1000.0)
                                            .round()
                                            / 10.0
                                    } else {
                                        100.0
                                    };
                                    let mbs =
                                        (total_rx as f64 / (1024.0 * 1024.0)) / elapsed_secs;
                                    act.speed_mbs = (mbs * 10.0).round() / 10.0;
                                    act.speed_mbps = (mbs * 8.0 * 10.0).round() / 10.0;
                                    let rem_mbs = ((act.total_bytes.saturating_sub(total_rx))
                                        as f64)
                                        / (1024.0 * 1024.0);
                                    act.eta_seconds = if mbs > 0.1 {
                                        (rem_mbs / mbs).ceil() as u64
                                    } else {
                                        0
                                    };

                                    let b_transferred = track
                                        .batch_bytes_before
                                        .saturating_add(total_rx)
                                        .min(track.batch_total_bytes.max(total_rx));
                                    act.batch_transferred_bytes = b_transferred;
                                    act.batch_progress_pct =
                                        compute_batch_pct(b_transferred, track.batch_total_bytes);

                                    update_chunk_states(
                                        &mut act.chunk_states,
                                        total_chk,
                                        act.total_chunks,
                                        req.lane_index,
                                    );
                                    for (li, lane) in act.lanes.iter_mut().enumerate() {
                                        if li < 16 {
                                            lane.bytes_sent =
                                                track.lane_bytes[li].load(Ordering::Relaxed);
                                            lane.chunks_sent =
                                                track.lane_chunks[li].load(Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            "LANE_DONE" => {
                if let Some((_, ref track)) = cached_track {
                    if let Ok(mut f) = track.file.lock() {
                        let _ = f.flush();
                    }
                }
                let _ = stream.write_all(b"OK");
                let _ = stream.flush();
                break;
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

                if let Some(track) = track_opt {
                    if let Ok(mut f) = track.file.lock() {
                        let _ = f.flush();
                        let _ = f.seek(SeekFrom::Start(0));

                        let mut hasher = Sha256::new();
                        let mut buf = vec![0u8; 4 * 1024 * 1024];
                        while let Ok(n) = f.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            hasher.update(&buf[..n]);
                        }
                        let computed_sha256 = format!("{:x}", hasher.finalize());
                        let verified = req.expected_sha256.is_empty()
                            || computed_sha256 == req.expected_sha256;

                        drop(f.try_clone());
                        let _ = fs::rename(&track.part_path, &track.final_path);
                        let duration_secs = track.started_at.elapsed().as_secs_f64().max(0.01);

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
                                            track.final_path.to_string_lossy().to_string(),
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

fn connect_bound_tcp_fast(
    local_ip: &str,
    peer_ip: &str,
    peer_port: u16,
    timeout_ms: u64,
    if_index: Option<u32>,
) -> std::io::Result<TcpStream> {
    let peer_addr: SocketAddr = format!("{}:{}", peer_ip, peer_port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    let _ = socket.set_tcp_nodelay(true);
    let _ = socket.set_send_buffer_size(4 * 1024 * 1024);
    let _ = socket.set_recv_buffer_size(4 * 1024 * 1024);

    #[cfg(target_os = "windows")]
    if let Some(idx) = if_index {
        if idx > 0 {
            use std::os::windows::io::AsRawSocket;
            use windows::Win32::Networking::WinSock::{
                setsockopt, IPPROTO_IP, IP_UNICAST_IF, SOCKET,
            };
            let val = idx.to_be_bytes();
            unsafe {
                let _ = setsockopt(
                    SOCKET(socket.as_raw_socket() as usize),
                    IPPROTO_IP.0,
                    IP_UNICAST_IF as i32,
                    Some(&val),
                );
            }
        }
    }

    if let Ok(lip) = local_ip.parse::<Ipv4Addr>() {
        if !lip.is_unspecified() {
            let bind_addr = SocketAddrV4::new(lip, 0);
            let _ = socket.bind(&bind_addr.into());
        }
    }

    socket.connect_timeout(&peer_addr.into(), Duration::from_millis(timeout_ms))?;
    let std_stream: TcpStream = socket.into();
    let rw_timeout = timeout_ms.max(2000);
    let _ = std_stream.set_read_timeout(Some(Duration::from_millis(rw_timeout)));
    let _ = std_stream.set_write_timeout(Some(Duration::from_millis(rw_timeout)));
    Ok(std_stream)
}

fn connect_bound_tcp(
    local_ip: &str,
    peer_ip: &str,
    peer_port: u16,
    if_index: Option<u32>,
) -> std::io::Result<TcpStream> {
    let std_stream = connect_bound_tcp_fast(local_ip, peer_ip, peer_port, 4000, if_index)?;
    let _ = std_stream.set_read_timeout(Some(Duration::from_secs(15)));
    let _ = std_stream.set_write_timeout(Some(Duration::from_secs(15)));
    Ok(std_stream)
}

#[tauri::command]
async fn start_real_transfer(peer_node_id: String) -> Result<String, String> {
    let engine = get_engine().clone();

    let (peer, sender_name, cancel_flag, pause_flag) = {
        let mut st = engine.lock().map_err(|e| e.to_string())?;
        if st.active_transfer.is_some() {
            if st.selected_files.is_empty() {
                return Err("A transfer is already in progress.".to_string());
            }
            let files_to_add = std::mem::take(&mut st.selected_files);
            enqueue_files_into_active_transfer(&mut st, files_to_add);
            return Ok("Added to active transfer queue".to_string());
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

        let files = std::mem::take(&mut st.selected_files);
        let total_files = files.len();
        let total_bytes: u64 = files.iter().map(|f| f.size).sum();
        st.transfer_queue.clear();
        for f in files {
            st.transfer_queue.push_back(f);
        }
        st.batch_completed_files = 0;
        st.batch_completed_bytes = 0;
        st.batch_total_files = total_files;
        st.batch_total_bytes = total_bytes;

        (
            peer,
            st.device_name.clone(),
            st.cancel_flag.clone(),
            st.pause_flag.clone(),
        )
    };

    tokio::task::spawn_blocking(move || {
        loop {
            if cancel_flag.load(Ordering::SeqCst) {
                break;
            }

            let next_item = if let Ok(mut st) = engine.lock() {
                st.transfer_queue.pop_front()
            } else {
                None
            };
            let Some(file_item) = next_item else {
                break;
            };

            let path = PathBuf::from(&file_item.path);
            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    if let Ok(mut st) = engine.lock() {
                        if let Some(ref mut act) = st.active_transfer {
                            act.status = "failed".to_string();
                            act.error_message = Some(format!("Cannot read file: {}", e));
                        }
                        st.transfer_queue.clear();
                        st.batch_completed_files = 0;
                        st.batch_completed_bytes = 0;
                        st.batch_total_files = 0;
                        st.batch_total_bytes = 0;
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

            let (b_total_files, b_completed_files, b_total_bytes, b_bytes_before) =
                if let Ok(mut st) = engine.lock() {
                    let b_total_files = st.batch_total_files.max(1);
                    let b_completed_files = st.batch_completed_files;
                    let b_total_bytes = st.batch_total_bytes.max(file_size);
                    let b_bytes_before = st.batch_completed_bytes;
                    let q_count = st.transfer_queue.len();
                    let b_pct = if b_total_bytes > 0 {
                        compute_batch_pct(b_bytes_before, b_total_bytes)
                    } else {
                        0.0
                    };
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
                        batch_total_files: b_total_files,
                        batch_completed_files: b_completed_files,
                        batch_total_bytes: b_total_bytes,
                        batch_transferred_bytes: b_bytes_before,
                        batch_progress_pct: b_pct,
                        queued_files_count: q_count,
                    });
                    (b_total_files, b_completed_files, b_total_bytes, b_bytes_before)
                } else {
                    (1, 0, file_size, 0)
                };

            // 1. Send INIT_TRANSFER on primary link
            let primary_link = &peer.links[0];
            let mut ctrl_stream = match connect_bound_tcp(
                &primary_link.local_ip,
                &primary_link.peer_ip,
                primary_link.peer_tcp_port,
                Some(primary_link.local_if_index),
            ) {
                Ok(s) => s,
                Err(e) => {
                    if let Ok(mut st) = engine.lock() {
                        if let Some(ref mut act) = st.active_transfer {
                            act.status = "failed".to_string();
                            act.error_message =
                                Some(format!("Failed to connect to {}: {}", peer.node_name, e));
                        }
                        st.transfer_queue.clear();
                        st.batch_completed_files = 0;
                        st.batch_completed_bytes = 0;
                        st.batch_total_files = 0;
                        st.batch_total_bytes = 0;
                    }
                    return;
                }
            };

            let init_cmd = WireCommand {
                cmd: "INIT_TRANSFER".to_string(),
                transfer_id: transfer_id.clone(),
                sender_name: sender_name.clone(),
                file_name: file_item.name.clone(),
                file_size,
                total_chunks,
                lanes,
                batch_total_files: b_total_files,
                batch_completed_files: b_completed_files,
                batch_total_bytes: b_total_bytes,
                batch_bytes_before: b_bytes_before,
                ..Default::default()
            };

            if send_wire_command(&mut ctrl_stream, &init_cmd).is_err() {
                if let Ok(mut st) = engine.lock() {
                    st.active_transfer = None;
                    st.transfer_queue.clear();
                    st.batch_completed_files = 0;
                    st.batch_completed_bytes = 0;
                    st.batch_total_files = 0;
                    st.batch_total_bytes = 0;
                }
                return;
            }

            let mut ack = [0u8; 2];
            if ctrl_stream.read_exact(&mut ack).is_err() || &ack != b"OK" {
                if let Ok(mut st) = engine.lock() {
                    st.active_transfer = None;
                    st.transfer_queue.clear();
                    st.batch_completed_files = 0;
                    st.batch_completed_bytes = 0;
                    st.batch_total_files = 0;
                    st.batch_total_bytes = 0;
                }
                return;
            }

            // 2. Spawn parallel workers across ALL bonded physical cables!
            let started_at = Instant::now();
            let next_chunk = Arc::new(AtomicU64::new(0));
            let worker_failed = Arc::new(AtomicBool::new(false));
            let total_sent_bytes = Arc::new(AtomicU64::new(0));
            let total_sent_chunks = Arc::new(AtomicU64::new(0));
            let last_ui_sync_ms = Arc::new(AtomicU64::new(0));
            let lane_bytes: Arc<[AtomicU64; 16]> =
                Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
            let lane_chunks: Arc<[AtomicU64; 16]> =
                Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
            let single_lane = peer.links.len() == 1;
            let in_flight_sha256: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
            let mut handles = Vec::new();

            for (lane_idx, link) in peer.links.iter().cloned().enumerate() {
                let path_clone = path.clone();
                let tid_clone = transfer_id.clone();
                let next_chunk_clone = next_chunk.clone();
                let failed_clone = worker_failed.clone();
                let cancel_clone = cancel_flag.clone();
                let pause_clone = pause_flag.clone();
                let eng_clone = engine.clone();
                let total_bytes_atomic = total_sent_bytes.clone();
                let total_chunks_atomic = total_sent_chunks.clone();
                let sync_ms_atomic = last_ui_sync_ms.clone();
                let lane_bytes_clone = lane_bytes.clone();
                let lane_chunks_clone = lane_chunks.clone();
                let sha_out_clone = in_flight_sha256.clone();

                let handle = std::thread::spawn(move || {
                    let mut file = match File::open(&path_clone) {
                        Ok(f) => f,
                        Err(_) => {
                            failed_clone.store(true, Ordering::SeqCst);
                            return;
                        }
                    };

                    let mut stream = match connect_bound_tcp(
                        &link.local_ip,
                        &link.peer_ip,
                        link.peer_tcp_port,
                        Some(link.local_if_index),
                    ) {
                        Ok(s) => s,
                        Err(_) => {
                            // If secondary cable fails, mark lane inactive so UI & remaining lanes reflect it
                            if lane_idx == 0 {
                                failed_clone.store(true, Ordering::SeqCst);
                            } else if let Ok(mut st) = eng_clone.lock() {
                                if let Some(ref mut act) = st.active_transfer {
                                    if act.transfer_id == tid_clone {
                                        if let Some(lane) = act.lanes.get_mut(lane_idx) {
                                            lane.active = false;
                                        }
                                    }
                                }
                            }
                            return;
                        }
                    };

                    // Pre-allocate reusable frame buffer: [4-byte hdr_len][JSON header][chunk_bytes]
                    let mut frame_buf: Vec<u8> = Vec::with_capacity(4 + 512 + CHUNK_SIZE);
                    let mut stream_hasher = if single_lane {
                        Some(Sha256::new())
                    } else {
                        None
                    };
                    let mut chunk_cmd = WireCommand {
                        cmd: "CHUNK".to_string(),
                        transfer_id: tid_clone.clone(),
                        file_size,
                        total_chunks,
                        lane_index: lane_idx,
                        lane_name: link.local_interface_name.clone(),
                        ..Default::default()
                    };

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

                        chunk_cmd.chunk_index = idx;
                        chunk_cmd.offset = offset;
                        chunk_cmd.length = to_read;

                        frame_buf.clear();
                        frame_buf.resize(4, 0);
                        if serde_json::to_writer(&mut frame_buf, &chunk_cmd).is_err() {
                            failed_clone.store(true, Ordering::SeqCst);
                            break;
                        }
                        let hdr_len = (frame_buf.len() - 4) as u32;
                        frame_buf[..4].copy_from_slice(&hdr_len.to_be_bytes());

                        let payload_start = frame_buf.len();
                        if to_read > 0 {
                            frame_buf.resize(payload_start + to_read, 0);
                            if file.seek(SeekFrom::Start(offset)).is_err()
                                || file
                                    .read_exact(&mut frame_buf[payload_start..payload_start + to_read])
                                    .is_err()
                            {
                                failed_clone.store(true, Ordering::SeqCst);
                                break;
                            }
                            if let Some(ref mut hasher) = stream_hasher {
                                hasher.update(&frame_buf[payload_start..payload_start + to_read]);
                            }
                        }

                        // Pipelined coalesced write — zero per-chunk flush or stop-and-wait ACK
                        if stream.write_all(&frame_buf).is_err() {
                            failed_clone.store(true, Ordering::SeqCst);
                            break;
                        }

                        let cur_bytes = total_bytes_atomic
                            .fetch_add(to_read as u64, Ordering::Relaxed)
                            .saturating_add(to_read as u64)
                            .min(file_size);
                        let cur_chunks = total_chunks_atomic
                            .fetch_add(1, Ordering::Relaxed)
                            .saturating_add(1);
                        if lane_idx < 16 {
                            lane_bytes_clone[lane_idx].fetch_add(to_read as u64, Ordering::Relaxed);
                            lane_chunks_clone[lane_idx].fetch_add(1, Ordering::Relaxed);
                        }

                        let now = now_ms();
                        let prev = sync_ms_atomic.load(Ordering::Relaxed);
                        let should_sync =
                            now.saturating_sub(prev) >= 100 || cur_chunks >= total_chunks;
                        if should_sync
                            && sync_ms_atomic
                                .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                        {
                            let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
                            if let Ok(mut st) = eng_clone.lock() {
                                let b_total_files = st.batch_total_files.max(1);
                                let b_completed_files = st.batch_completed_files;
                                let b_total_bytes = st.batch_total_bytes.max(file_size);
                                let b_transferred = st
                                    .batch_completed_bytes
                                    .saturating_add(cur_bytes)
                                    .min(b_total_bytes.max(cur_bytes));
                                let b_pct = compute_batch_pct(b_transferred, b_total_bytes);
                                let q_count = st.transfer_queue.len();

                                if let Some(ref mut act) = st.active_transfer {
                                    if act.transfer_id == tid_clone {
                                        act.transferred_bytes = cur_bytes;
                                        act.completed_chunks = cur_chunks;
                                        act.progress_pct = if file_size > 0 {
                                            ((cur_bytes as f64 / file_size as f64) * 1000.0)
                                                .round()
                                                / 10.0
                                        } else {
                                            100.0
                                        };
                                        let mbs =
                                            (cur_bytes as f64 / (1024.0 * 1024.0)) / elapsed;
                                        act.speed_mbs = (mbs * 10.0).round() / 10.0;
                                        act.speed_mbps = (mbs * 8.0 * 10.0).round() / 10.0;
                                        let rem_mbs = ((file_size.saturating_sub(cur_bytes))
                                            as f64)
                                            / (1024.0 * 1024.0);
                                        act.eta_seconds = if mbs > 0.1 {
                                            (rem_mbs / mbs).ceil() as u64
                                        } else {
                                            0
                                        };
                                        act.batch_total_files = b_total_files;
                                        act.batch_completed_files = b_completed_files;
                                        act.batch_total_bytes = b_total_bytes;
                                        act.batch_transferred_bytes = b_transferred;
                                        act.batch_progress_pct = b_pct;
                                        act.queued_files_count = q_count;

                                        update_chunk_states(
                                            &mut act.chunk_states,
                                            cur_chunks,
                                            total_chunks,
                                            lane_idx,
                                        );
                                        for (li, lane) in act.lanes.iter_mut().enumerate() {
                                            if li < 16 {
                                                lane.bytes_sent = lane_bytes_clone[li]
                                                    .load(Ordering::Relaxed);
                                                lane.chunks_sent = lane_chunks_clone[li]
                                                    .load(Ordering::Relaxed);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Single end-of-lane handshake to ensure all pipelined chunks are flushed to disk
                    if !cancel_clone.load(Ordering::SeqCst) && !failed_clone.load(Ordering::SeqCst)
                    {
                        chunk_cmd.cmd = "LANE_DONE".to_string();
                        chunk_cmd.length = 0;
                        if send_wire_command(&mut stream, &chunk_cmd).is_ok() {
                            let mut lane_ack = [0u8; 2];
                            if stream.read_exact(&mut lane_ack).is_err() || &lane_ack != b"OK" {
                                failed_clone.store(true, Ordering::SeqCst);
                            }
                        } else {
                            failed_clone.store(true, Ordering::SeqCst);
                        }
                    }

                    if let Some(hasher) = stream_hasher {
                        if !cancel_clone.load(Ordering::SeqCst)
                            && !failed_clone.load(Ordering::SeqCst)
                        {
                            if let Ok(mut guard) = sha_out_clone.lock() {
                                *guard = Some(format!("{:x}", hasher.finalize()));
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
                    st.transfer_queue.clear();
                    st.batch_completed_files = 0;
                    st.batch_completed_bytes = 0;
                    st.batch_total_files = 0;
                    st.batch_total_bytes = 0;
                }
                return;
            }

            // 3. Compute SHA-256 (reuse in-flight hash if single lane) and finalize transfer
            if let Ok(mut st) = engine.lock() {
                if let Some(ref mut act) = st.active_transfer {
                    act.status = "verifying".to_string();
                }
            }

            let mut expected_sha256 = in_flight_sha256
                .lock()
                .ok()
                .and_then(|mut g| g.take())
                .unwrap_or_default();
            if expected_sha256.is_empty() {
                if let Ok(mut f) = File::open(&path) {
                    let mut hasher = Sha256::new();
                    let mut b = vec![0u8; 4 * 1024 * 1024];
                    while let Ok(n) = f.read(&mut b) {
                        if n == 0 {
                            break;
                        }
                        hasher.update(&b[..n]);
                    }
                    expected_sha256 = format!("{:x}", hasher.finalize());
                }
            }

            let fin_cmd = WireCommand {
                cmd: "FINISH_TRANSFER".to_string(),
                transfer_id: transfer_id.clone(),
                file_name: file_item.name.clone(),
                file_size,
                total_chunks,
                expected_sha256: expected_sha256.clone(),
                ..Default::default()
            };

            let _ = send_wire_command(&mut ctrl_stream, &fin_cmd);
            let mut fin_ack = [0u8; 2];
            let _ = ctrl_stream.read_exact(&mut fin_ack);

            let duration_secs = started_at.elapsed().as_secs_f64().max(0.01);
            let avg_mbs =
                ((file_size as f64 / (1024.0 * 1024.0)) / duration_secs * 10.0).round() / 10.0;

            if let Ok(mut st) = engine.lock() {
                st.batch_completed_files = st.batch_completed_files.saturating_add(1);
                st.batch_completed_bytes = st.batch_completed_bytes.saturating_add(file_size);
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
            st.transfer_queue.clear();
            st.batch_completed_files = 0;
            st.batch_completed_bytes = 0;
            st.batch_total_files = 0;
            st.batch_total_bytes = 0;
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
        let is_sending = st
            .active_transfer
            .as_ref()
            .map(|a| a.direction == "Sending")
            .unwrap_or(false);
        if is_sending {
            let mut newly_added = Vec::new();
            for p in paths {
                collect_files_from_path(&p, &mut newly_added);
            }
            enqueue_files_into_active_transfer(&mut st, newly_added);
        } else {
            for p in paths {
                collect_files_from_path(&p, &mut st.selected_files);
            }
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
        let is_sending = st
            .active_transfer
            .as_ref()
            .map(|a| a.direction == "Sending")
            .unwrap_or(false);
        if is_sending {
            let mut newly_added = Vec::new();
            for p in paths {
                collect_files_from_path(&p, &mut newly_added);
            }
            enqueue_files_into_active_transfer(&mut st, newly_added);
        } else {
            for p in paths {
                collect_files_from_path(&p, &mut st.selected_files);
            }
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
    let (local_node_id, local_name, local_ip, if_index, port_type, speed_mbps) = {
        let st = engine.lock().map_err(|e| e.to_string())?;
        let active_cable = find_cable_for_peer(&st.cables, &ip_trimmed, None);
        (
            st.node_id.clone(),
            st.device_name.clone(),
            active_cable
                .as_ref()
                .and_then(|c| c.ip_address.clone())
                .unwrap_or_default(),
            active_cable.as_ref().map(|c| c.if_index),
            active_cable
                .as_ref()
                .map(|c| c.port_type.clone())
                .unwrap_or_else(|| "Ethernet Cable".to_string()),
            active_cable
                .as_ref()
                .map(|c| c.link_speed_mbps)
                .unwrap_or(100),
        )
    };

    let ip_clone = ip_trimmed.clone();
    let ack = tokio::task::spawn_blocking(move || -> Result<WireCommand, String> {
        let mut stream = connect_bound_tcp(&local_ip, &ip_clone, TRANSFER_TCP_PORT, if_index)
            .map_err(|e| format!("Could not reach {}:{}: {}", ip_clone, TRANSFER_TCP_PORT, e))?;

        let hello = WireCommand {
            cmd: "HELLO".to_string(),
            node_id: local_node_id,
            node_name: local_name,
            port_type,
            speed_mbps,
            ..Default::default()
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
        st.transfer_queue.clear();
        st.batch_completed_files = 0;
        st.batch_completed_bytes = 0;
        st.batch_total_files = 0;
        st.batch_total_bytes = 0;
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
                            let is_sending = st
                                .active_transfer
                                .as_ref()
                                .map(|a| a.direction == "Sending")
                                .unwrap_or(false);
                            if is_sending {
                                let mut newly_added = Vec::new();
                                for p in paths {
                                    collect_files_from_path(p, &mut newly_added);
                                }
                                enqueue_files_into_active_transfer(&mut st, newly_added);
                            } else {
                                for p in paths {
                                    collect_files_from_path(p, &mut st.selected_files);
                                }
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
