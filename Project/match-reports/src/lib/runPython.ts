// runPython.ts
import { Command } from "@tauri-apps/plugin-shell";

export async function runPython(scriptRelPath: string, args: string[] = []) {
    if (!("__TAURI__" in window)) throw new Error("Not running in Tauri");

    // try your venv first, then system python
    const candidates = ["pyvenv", "python", "py"]; // names defined in capabilities below

    let lastErr: unknown = null;
    for (const name of candidates) {
        try {
            const cmd = Command.create(name, [scriptRelPath, ...args], { cwd: "." });
            const out = await cmd.execute();
            return out; // { code, stdout, stderr }
        } catch (e) {
            lastErr = e;
        }
    }
    throw lastErr ?? new Error("No allowed python command worked.");
}

