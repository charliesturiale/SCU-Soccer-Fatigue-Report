#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
use tauri::Manager;

fn main() {
  tauri::Builder::default()
    .plugin(tauri_plugin_store::Builder::default().build())
    .plugin(tauri_plugin_fs::init())
    .plugin(tauri_plugin_shell::init()) 
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}

