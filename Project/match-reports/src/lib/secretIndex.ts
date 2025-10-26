// src/lib/secretIndex.ts
import { Store } from "@tauri-apps/plugin-store";

let storePromise: Promise<Store> | null = null;

async function getStore(): Promise<Store> {
    if (!storePromise) {
        // The file will live in your app-data dir automatically
        storePromise = Store.load(".app-settings.json");
    }
    return storePromise;
}

const KEY = "secret_index"; // string[]

export async function listSecretNames(): Promise<string[]> {
    const store = await getStore();
    return (await store.get<string[]>(KEY)) ?? [];
}

export async function addSecretName(name: string): Promise<void> {
    const store = await getStore();
    const names = new Set(await listSecretNames());
    names.add(name);
    await store.set(KEY, Array.from(names));
    await store.save();
}

export async function removeSecretName(name: string): Promise<void> {
    const store = await getStore();
    const names = new Set(await listSecretNames());
    names.delete(name);
    await store.set(KEY, Array.from(names));
    await store.save();
}

export async function renameSecretName(oldName: string, newName: string): Promise<void> {
    const store = await getStore();
    const names = new Set(await listSecretNames());
    if (names.has(oldName)) names.delete(oldName);
    names.add(newName);
    await store.set(KEY, Array.from(names));
    await store.save();
}
