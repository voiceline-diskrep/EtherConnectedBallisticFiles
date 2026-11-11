use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::Networks;
use tauri::Window;

const CURRENT_VERSION: &str = "0.1.0";
const GITHUB_REPO: &str = "voiceline-diskrep/EtherConnectedBallisticFiles";

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
