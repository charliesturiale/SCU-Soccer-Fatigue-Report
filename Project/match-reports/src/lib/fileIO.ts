// src/lib/fileIO.ts
export async function writeJsonToData(relPath: string, data: unknown) {
    if (!("__TAURI__" in window)) return; // no-op in plain web dev

    // Try v2 plugin-fs first, then v1 api/fs
    let writeTextFile: (path: string, contents: string, opts?: Record<string, unknown>) => Promise<void>;
    let BaseDirectory: { Resource: number; AppData: number };

    try {
        const fs = await import("@tauri-apps/plugin-fs");     // v2
        writeTextFile = fs.writeTextFile;
        BaseDirectory = fs.BaseDirectory;
    } catch {
        const fs = await import("@tauri-apps/plugin-fs");        // v1
        writeTextFile = fs.writeTextFile;
        BaseDirectory = fs.BaseDirectory;
    }

    await writeTextFile(
        relPath,
        JSON.stringify(data, null, 2),
        { dir: BaseDirectory.Resource } // or BaseDirectory.AppData if you prefer
    );
}
