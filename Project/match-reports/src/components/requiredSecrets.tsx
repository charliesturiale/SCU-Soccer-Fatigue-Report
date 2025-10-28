import { useEffect, useMemo, useState, useCallback } from "react";
import { exportSecretsToDisk } from "../lib/config";
import {
    setKey as saveSecret,
    getKey as readSecret,
} from "../lib/config";

/** 🧱 Add/Change required fields here. */
const REQUIRED_KEYS = [
    {
        key: "MSOC_CATAPULT_KEY",
        label: "Men’s Soccer — Catapult API Key",
        help: "Paste the Catapult API key for Men’s Soccer.",
        required: true,
    },
    {
        key: "MSOC_VALD_KEY",
        label: "Men’s Soccer — VALD API Key",
        help: "Paste the VALD API key for Men’s Soccer.",
        required: true,
    },
    // Example: add more teams or vendors later
    // { key: "WSOC_CATAPULT_KEY", label: "Women’s Soccer — Catapult API Key", help: "…", required: true },
    // { key: "GOLF_VALD_KEY", label: "Golf — VALD API Key", help: "…", required: false },
] as const;

type FieldDef = typeof REQUIRED_KEYS[number];

function errMsg(err: unknown): string {
    if (err instanceof Error) return err.message;
    try { return JSON.stringify(err); } catch { return String(err); }
}

export default function RequiredSecrets() {
    const [values, setValues] = useState<Record<string, string>>({});
    const [dirty, setDirty] = useState<Record<string, boolean>>({});
    const [busy, setBusy] = useState<Record<string, boolean>>({});
    const [busyAll, setBusyAll] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);

    // load once
    const loadAll = useCallback(async () => {
        setMsg(null);
        const next: Record<string, string> = {};
        try {
            for (const f of REQUIRED_KEYS) {
                const v = await readSecret(f.key);
                next[f.key] = v ?? "";
            }
            setValues(next);
            setDirty({});
        } catch (e) {
            setMsg(`Failed to load secrets: ${errMsg(e)}`);
        }
    }, []);

    useEffect(() => { void loadAll(); }, [loadAll]);

    const missingRequired = useMemo(() => {
        return REQUIRED_KEYS
            .filter(f => f.required && !(values[f.key] && values[f.key].trim().length > 0))
            .map(f => f.label);
    }, [values]);

    async function saveOne(field: FieldDef) {
        const k = field.key;
        const v = (values[k] ?? "").trim();
        if (field.required && !v) {
            setMsg(`"${field.label}" is required.`);
            return;
        }
        setBusy(s => ({ ...s, [k]: true }));
        try {
            await saveSecret(k, v);
            setDirty(s => ({ ...s, [k]: false }));
            setMsg(`Saved "${field.label}".`);
        } catch (e) {
            setMsg(`Failed to save "${field.label}": ${errMsg(e)}`);
        } finally {
            setBusy(s => ({ ...s, [k]: false }));
        }
    }

    async function saveAll() {
        setBusyAll(true);
        setMsg(null);
        try {
            for (const f of REQUIRED_KEYS) {
                const k = f.key;
                const v = (values[k] ?? "").trim();
                if (f.required && !v) {
                    throw new Error(`"${f.label}" is required.`);
                }
                await saveSecret(k, v);
            }
            setDirty({});
            setMsg("All keys saved.");
        } catch (e) {
            setMsg(errMsg(e));
        } finally {
            setBusyAll(false);
        }
    }

    return (
        <div className="space-y-6">
            <header className="space-y-1">
                <h2 className="text-xl font-semibold">Team API Keys</h2>
                <p className="text-sm text-gray-600">
                    Enter the required API keys below. You can update them anytime.
                </p>
            </header>

            <div className="rounded-xl border">
                {REQUIRED_KEYS.map((f) => {
                    const k = f.key;
                    const isBusy = !!busy[k];
                    const isDirty = !!dirty[k];
                    const val = values[k] ?? "";

                    return (
                        <div key={k} className="p-4 border-b last:border-b-0">
                            <div className="flex items-center justify-between gap-4 mb-2">
                                <label className="font-medium">
                                    {f.label} {f.required && <span className="text-red-600">*</span>}
                                </label>
                                <span className="text-xs font-mono text-gray-500">{k}</span>
                            </div>

                            {/* {f.help && <p className="text-sm text-gray-600 mb-2">{f.help}</p>} */}

                            <div className="flex items-center gap-2">
                                <input
                                    className="flex-1 border rounded px-2 py-1 font-mono"
                                    type="password"
                                    placeholder={f.required ? "Required" : "Optional"}
                                    value={val}
                                    onChange={(e) => {
                                        const next = e.target.value;
                                        setValues(s => ({ ...s, [k]: next }));
                                        setDirty(s => ({ ...s, [k]: true }));
                                    }}
                                    onKeyDown={(e) => {
                                        if (e.key === "Enter") void saveOne(f);
                                    }}
                                    disabled={isBusy || busyAll}
                                />
                                <button
                                    className="text-sm px-3 py-1 border rounded"
                                    onClick={() => void saveOne(f)}
                                    disabled={isBusy || busyAll || (f.required && !val.trim())}
                                    title={isDirty ? "Save changes" : "Saved"}
                                >
                                    {isBusy ? "Saving…" : isDirty ? "Save" : "Saved"}
                                </button>
                            </div>
                        </div>
                    );
                })}
            </div>

            <div className="flex items-center justify-between">
                <div className="text-sm text-gray-700">
                    {missingRequired.length > 0 ? (
                        <span>
                            Missing required: {missingRequired.join(", ")}
                        </span>
                    ) : (
                        <span>All required fields are filled.</span>
                    )}
                </div>
                <button
                    className="px-4 py-2 border rounded"
                    onClick={() => void saveAll()}
                    disabled={busyAll || missingRequired.length > 0}
                >
                    {busyAll ? "Saving…" : "Save all"}
                </button>
                <button
                    className="px-3 py-1 border rounded"
                    onClick={() => exportSecretsToDisk().then(() => setMsg("Exported to data/secrets.json"))}
                >
                    Export for Python
                </button>
            </div>

            {msg && <div className="text-sm text-blue-700">{msg}</div>}
        </div>
    );
}
