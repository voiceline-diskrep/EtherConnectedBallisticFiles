use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::Networks;
use tauri::Window;

const CURRENT_VERSION: &str = "0.1.0";
const GITHUB_REPO: &str = "voiceline-diskrep/EtherConnectedBallisticFiles";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WiredCableInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub port_type: String,
    pub link_speed_mbps: u64,
    pub link_speed_display: String,
    pub is_connected: bool,
    pub ip_address: Option<String>,
    pub mac_address: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BondedNetworkStatus {
    pub cables: Vec<WiredCableInfo>,
    pub total_connected_cables: usize,
    pub total_bonded_speed_mbps: u64,
    pub total_bonded_speed_display: String,
    pub max_theoretical_transfer_rate_mbs: f64,
    pub local_device_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NetworkAdapterInfo {
    pub id: String,
    pub name: String,
    pub received_bytes: u64,
    pub transmitted_bytes: u64,
    pub is_up: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HashVerificationResult {
    pub valid: bool,
    pub computed_hash: String,
    pub expected_hash: String,
}

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
    pub release_date: Option<String>,
    pub release_notes: Option<String>,
    pub has_update: bool,
}

#[tauri::command]
fn get_bonded_network_status() -> BondedNetworkStatus {
    let dev_name = get_device_name();
    let mut cables: Vec<WiredCableInfo> = Vec::new();

    #[cfg(target_os = "windows")]
    {
        use std::process::Command;
        use std::os::windows::process::CommandExt;

        let ps_script = r#"
            $adapters = Get-NetAdapter | Where-Object { 
                $_.PhysicalMediaType -ne "Native 802.11" -and 
                $_.InterfaceDescription -notlike "*TunnelBear*" -and 
                $_.InterfaceDescription -notlike "*Bluetooth*" -and
                $_.InterfaceDescription -notlike "*Virtual*" -and
                $_.InterfaceDescription -notlike "*TAP*"
            } | ForEach-Object {
                $desc = $_.InterfaceDescription
                $port = "Built-in Ethernet"
                if ($desc -like "*USB*C*" -or $desc -like "*Type-C*" -or $desc -like "*2.5G*") { $port = "USB-C Adapter" }
                elseif ($desc -like "*USB*") { $port = "USB-A Adapter" }
                elseif ($desc -like "*PCI*" -or $desc -like "*Intel*" -or $desc -like "*Realtek*") { $port = "Built-in Ethernet" }

                $speedMbps = 0
                if ($_.LinkSpeed -match "(\d+(\.\d+)?)\s*(Gbps|Mbps)") {
                    $val = [double]$matches[1]
                    $unit = $matches[3]
                    if ($unit -eq "Gbps") { $speedMbps = [long]($val * 1000) } else { $speedMbps = [long]$val }
                }

                $ip = (Get-NetIPAddress -InterfaceIndex $_.ifIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty IPAddress -First 1)

                [PSCustomObject]@{
                    id = $_.Name
                    name = $_.Name
                    description = $_.InterfaceDescription
                    port_type = $port
                    link_speed_mbps = $speedMbps
                    link_speed_display = $_.LinkSpeed
                    is_connected = ($_.Status -eq "Up")
                    ip_address = $ip
                    mac_address = $_.MacAddress
                }
            }
            if ($adapters) { $adapters | ConvertTo-Json -Compress } else { "[]" }
        "#;

        const CREATE_NO_WINDOW: u32 = 0x08000000;
        if let Ok(output) = Command::new("powershell")
            .creation_flags(CREATE_NO_WINDOW)
            .args(&["-NoProfile", "-NonInteractive", "-Command", ps_script])
            .output()
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let trimmed = stdout.trim();
                if trimmed.starts_with('{') {
                    if let Ok(single) = serde_json::from_str::<WiredCableInfo>(trimmed) {
                        cables.push(single);
                    }
                } else if trimmed.starts_with('[') {
                    if let Ok(list) = serde_json::from_str::<Vec<WiredCableInfo>>(trimmed) {
                        cables = list;
                    }
                }
            }
        }
    }

    let connected_cables: Vec<&WiredCableInfo> = cables.iter().filter(|c| c.is_connected).collect();
    let total_connected = connected_cables.len();
    let total_speed: u64 = connected_cables.iter().map(|c| c.link_speed_mbps).sum();

    let display = if total_speed >= 1000 {
        format!("{:.1} Gbps", (total_speed as f64) / 1000.0)
    } else {
        format!("{} Mbps", total_speed)
    };

    let max_rate_mbs = (total_speed as f64) / 8.0 * 0.92;

    BondedNetworkStatus {
        cables,
        total_connected_cables: total_connected,
        total_bonded_speed_mbps: total_speed,
        total_bonded_speed_display: display,
        max_theoretical_transfer_rate_mbs: max_rate_mbs,
        local_device_name: dev_name,
    }
}

#[tauri::command]
fn get_device_name() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "Local-PC".to_string())
}

#[tauri::command]
fn get_network_adapters() -> Vec<NetworkAdapterInfo> {
    let networks = Networks::new_with_refreshed_list();
    let mut adapters = Vec::new();

    for (interface_name, data) in &networks {
        adapters.push(NetworkAdapterInfo {
            id: interface_name.clone(),
            name: interface_name.clone(),
            received_bytes: data.received(),
            transmitted_bytes: data.transmitted(),
            is_up: true,
        });
    }

    adapters
}

#[tauri::command]
fn verify_file_hash(expected_hash: String, payload_sample: String) -> HashVerificationResult {
    let mut hasher = Sha256::new();
    hasher.update(payload_sample.as_bytes());
    let result = hasher.finalize();
    let computed_hash = format!("{:x}", result);

    let valid = computed_hash == expected_hash;

    HashVerificationResult {
        valid,
        computed_hash,
        expected_hash,
    }
}

#[tauri::command]
async fn check_for_updates() -> UpdateCheckResponse {
    let url = format!("https://api.github.com/repos/{}/releases/latest", GITHUB_REPO);
    let client = reqwest::Client::builder()
        .user_agent("EtherConnectedBallisticFiles")
        .build();

    let client = match client {
        Ok(c) => c,
        Err(e) => {
            return UpdateCheckResponse {
                success: false,
                message: format!("HTTP client initialization failed: {}", e),
                current_version: CURRENT_VERSION.to_string(),
                latest_version: None,
                release_url: None,
                release_date: None,
                release_notes: None,
                has_update: false,
            };
        }
    };

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return UpdateCheckResponse {
                    success: true,
                    message: format!("No releases found. Version {} is current.", CURRENT_VERSION),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: Some(CURRENT_VERSION.to_string()),
                    release_url: Some(format!("https://github.com/{}", GITHUB_REPO)),
                    release_date: None,
                    release_notes: None,
                    has_update: false,
                };
            }

            if !response.status().is_success() {
                return UpdateCheckResponse {
                    success: false,
                    message: format!("GitHub API returned status code {}.", response.status().as_u16()),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: None,
                    release_url: None,
                    release_date: None,
                    release_notes: None,
                    has_update: false,
                };
            }

            match response.json::<GitHubReleasePayload>().await {
                Ok(rel) => {
                    let remote_ver_clean = rel.tag_name.trim_start_matches('v').trim();
                    let local_ver_clean = CURRENT_VERSION.trim();
                    let has_update = remote_ver_clean != local_ver_clean;

                    let msg = if has_update {
                        format!("Update available: v{}.", remote_ver_clean)
                    } else {
                        format!("You are on the latest version (v{}).", local_ver_clean)
                    };

                    UpdateCheckResponse {
                        success: true,
                        message: msg,
                        current_version: CURRENT_VERSION.to_string(),
                        latest_version: Some(rel.tag_name),
                        release_url: Some(rel.html_url),
                        release_date: rel.published_at,
                        release_notes: rel.body,
                        has_update,
                    }
                }
                Err(err) => UpdateCheckResponse {
                    success: false,
                    message: format!("Failed to parse release data: {}", err),
                    current_version: CURRENT_VERSION.to_string(),
                    latest_version: None,
                    release_url: None,
                    release_date: None,
                    release_notes: None,
                    has_update: false,
                },
            }
        }
        Err(err) => UpdateCheckResponse {
            success: false,
            message: format!("Failed to connect to GitHub: {}", err),
            current_version: CURRENT_VERSION.to_string(),
            latest_version: None,
            release_url: None,
            release_date: None,
            release_notes: None,
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
    tauri::Builder::default()
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_bonded_network_status,
            get_device_name,
            get_network_adapters,
            verify_file_hash,
            check_for_updates,
            minimize_window,
            maximize_window,
            close_window
        ])
        .run(tauri::generate_context!())
        .expect("error while building EtherConnectedBallisticFiles application");
}
