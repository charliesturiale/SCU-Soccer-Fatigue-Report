// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri_plugin_store;
use tauri_plugin_stronghold;

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_store::Builder::new().build())
        .plugin(tauri_plugin_stronghold::Builder::new(|pass| todo!()).build())
        .plugin(tauri_plugin_stronghold::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
