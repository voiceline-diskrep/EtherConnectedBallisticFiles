use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::Networks;
use tauri::Window;

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
            minimize_window,
            maximize_window,
            close_window
        ])
        .run(tauri::generate_context!())
        .expect("error while building EtherConnectedBallisticFiles application");
}
