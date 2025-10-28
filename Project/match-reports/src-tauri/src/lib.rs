use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  tauri::Builder::default()
    .setup(|app| {
      let salt_path = app
        .path()
        .app_local_data_dir()
        .expect("no app data dir")
        .join("salt.txt");

      app.handle()
        .plugin(tauri_plugin_store::Builder::default().build())?;
      Ok(())
    })
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}

