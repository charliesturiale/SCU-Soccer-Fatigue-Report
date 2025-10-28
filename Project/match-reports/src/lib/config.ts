// src/lib/config.ts
import { Store } from "@tauri-apps/plugin-store";

let storeP: Promise<Store> | null = null;
export async function getStore() {
    if (!storeP) storeP = Store.load("config.json"); // lives in app-data dir
    return storeP;
}

export const KEYMAP = "apiKeys"; // we’ll keep all keys under this object

export async function listKeys(): Promise<string[]> {
    const store = await getStore();
    const map = (await store.get<Record<string, string>>(KEYMAP)) ?? {};
    return Object.keys(map).sort((a, b) => a.localeCompare(b));
}

export async function getKey(name: string): Promise<string | null> {
    const store = await getStore();
    const map = (await store.get<Record<string, string>>(KEYMAP)) ?? {};
    return map[name] ?? null;
}

export async function setKey(name: string, value: string): Promise<void> {
    const store = await getStore();
    const map = (await store.get<Record<string, string>>(KEYMAP)) ?? {};
    map[name] = value;
    await store.set(KEYMAP, map);
    await store.save();
}

export async function deleteKey(name: string): Promise<void> {
    const store = await getStore();
    const map = (await store.get<Record<string, string>>(KEYMAP)) ?? {};
    delete map[name];
    await store.set(KEYMAP, map);
    await store.save();
}

export async function exportConfig(): Promise<Record<string, string>> {
    const store = await getStore();
    return (await store.get<Record<string, string>>(KEYMAP)) ?? {};
}

export async function importConfig(obj: Record<string, string>) {
    const store = await getStore();
    await store.set(KEYMAP, obj);
    await store.save();
}

// src/lib/config.ts (append these helpers)
export async function exportSecretsToDisk(
    relPath = "data/secrets.json"
): Promise<void> {
    const isTauri = "__TAURI__" in window;
    if (!isTauri) return; // no-op if running in plain browser dev

    const store = await getStore();
    const secrets = (await store.get<Record<string, string>>(KEYMAP)) ?? {};

    const [{ writeTextFile }, { BaseDirectory }] = await Promise.all([
        import("@tauri-apps/plugin-fs"),
        import("@tauri-apps/api/path")
    ]);

    // write relative to app's current working dir (project root when dev; app dir when prod)
    await writeTextFile(relPath, JSON.stringify(secrets, null, 2), {
        baseDir: BaseDirectory.Resource, // if this doesn't suit, use BaseDirectory.App or resolve an absolute path
    });
}

