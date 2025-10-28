// src/lib/appConfigExport.ts
import { API_URLS } from "../config/endpoints";
import { writeJsonToData } from "./fileIO";
// src/lib/config.ts (secrets)
import { getStore, KEYMAP } from "./config"; // whatever you named it

export async function exportAppConfigToDisk() {
    await writeJsonToData("data/app-config.json", API_URLS);
}

export async function exportSecretsToDisk() {
    const store = await getStore();
    const secrets = (await store.get<Record<string, string>>(KEYMAP)) ?? {};
    await writeJsonToData("data/secrets.json", secrets);
}