// src/config/endpoints.ts
export const API_URLS = {
    catapultBase: "https://api.catapultsports.com",
    valdAuth: "https://security.valdperformance.com/connect/token",
    valdForceDecks: "https://api.valdperformance.com/forcedecks",
    valdNordBord: "https://api.valdperformance.com/nordbord",
    filmroomApi: "https://api.filmroom.us", // example
    // add as many as you like…
};

export type ApiUrls = typeof API_URLS;

import { exportAppConfigToDisk } from "../lib/appConfigExport.ts";

export async function exportApiUrlsToDisk() {
    await exportAppConfigToDisk(); // writes to data/app-config.json
}
