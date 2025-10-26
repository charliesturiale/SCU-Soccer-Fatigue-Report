// src/lib/secrets.ts
import { Stronghold } from "@tauri-apps/plugin-stronghold";
import { appDataDir } from "@tauri-apps/api/path";

let stronghold: Stronghold | null = null;
const CLIENT_NAME = "match-reports";
const VAULT_FILENAME = "vault.hold";
// TODO: replace this for prod (prompt or derive per-user)
const DEV_PASSWORD = "change-me-in-production";

async function getClient() {
    if (!stronghold) {
        const dir = await appDataDir();
        const vaultPath = `${dir}${VAULT_FILENAME}`;
        stronghold = await Stronghold.load(vaultPath, DEV_PASSWORD);
    }
    try {
        return await stronghold.loadClient(CLIENT_NAME);
    } catch {
        return await stronghold.createClient(CLIENT_NAME);
    }
}

export async function saveSecret(name: string, value: string): Promise<void> {
    const client = await getClient();
    const store = client.getStore();
    await store.insert(name, Array.from(new TextEncoder().encode(value)));
    await stronghold!.save();
}

export async function readSecret(name: string): Promise<string | null> {
    const client = await getClient();
    const store = client.getStore();
    const data = await store.get(name).catch(() => null);
    return data ? new TextDecoder().decode(new Uint8Array(data)) : null;
}

export async function deleteSecret(name: string): Promise<void> {
    const client = await getClient();
    const store = client.getStore();
    await store.remove(name);
    await stronghold!.save();
}

