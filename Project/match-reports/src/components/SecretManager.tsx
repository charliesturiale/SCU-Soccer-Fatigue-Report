// src/components/SecretManager.tsx
import { useEffect, useMemo, useState } from "react";
import { saveSecret, readSecret, deleteSecret } from "../lib/secrets";
import { listSecretNames, addSecretName, removeSecretName, renameSecretName } from "../lib/secretIndex";

type Mode = "idle" | "reveal";

function errMsg(err: unknown): string {
    if (err instanceof Error) return err.message;
    try { return JSON.stringify(err); } catch { return String(err); }
}

export default function SecretManager() {
    const [names, setNames] = useState<string[]>([]);
    const [filter, setFilter] = useState("");
    const [newName, setNewName] = useState("");
    const [newValue, setNewValue] = useState("");
    const [busy, setBusy] = useState(false);
    const [message, setMessage] = useState<string | null>(null);
    const [reveal, setReveal] = useState<Record<string, Mode>>({});
    const [editingValue, setEditingValue] = useState<Record<string, string>>({});
    const [renaming, setRenaming] = useState<Record<string, string>>({});

    const filtered = useMemo(
        () => names.filter(n => n.toLowerCase().includes(filter.toLowerCase())).sort((a, b) => a.localeCompare(b)),
        [names, filter]
    );

    async function refresh() {
        setNames(await listSecretNames());
    }
    useEffect(() => { void refresh(); }, []);

    async function handleAdd() {
        const name = newName.trim();
        const value = newValue;
        if (!name) return setMessage("Please provide a secret name.");
        if (!value) return setMessage("Please provide a secret value.");
        setBusy(true);
        try {
            await saveSecret(name, value);
            await addSecretName(name);
            setNewName(""); setNewValue("");
            await refresh();
            setMessage(`Saved "${name}".`);
        } catch (err: unknown) {
            setMessage(`Failed to save: ${errMsg(err)}`);
        } finally { setBusy(false); }
    }

    async function handleReveal(name: string) {
        setBusy(true);
        try {
            const v = await readSecret(name);
            setEditingValue(s => ({ ...s, [name]: v ?? "" }));
            setReveal(s => ({ ...s, [name]: "reveal" }));
        } catch (err: unknown) {
            setMessage(`Failed to read "${name}": ${errMsg(err)}`);
        } finally { setBusy(false); }
    }

    async function handleUpdate(name: string) {
        const v = editingValue[name] ?? "";
        if (!v) return setMessage("Value cannot be empty.");
        setBusy(true);
        try {
            await saveSecret(name, v);
            setMessage(`Updated "${name}".`);
            setReveal(s => ({ ...s, [name]: "idle" }));
        } catch (err: unknown) {
            setMessage(`Failed to update "${name}": ${errMsg(err)}`);
        } finally { setBusy(false); }
    }

    async function handleDelete(name: string) {
        if (!confirm(`Delete secret "${name}"? This cannot be undone.`)) return;
        setBusy(true);
        try {
            await deleteSecret(name);
            await removeSecretName(name);
            setReveal(s => { const c = { ...s }; delete c[name]; return c; });
            setEditingValue(s => { const c = { ...s }; delete c[name]; return c; });
            setRenaming(s => { const c = { ...s }; delete c[name]; return c; });
            await refresh();
            setMessage(`Deleted "${name}".`);
        } catch (err: unknown) {
            setMessage(`Failed to delete "${name}": ${errMsg(err)}`);
        } finally { setBusy(false); }
    }

    function startRename(name: string) {
        setRenaming(s => ({ ...s, [name]: name }));
    }

    async function commitRename(name: string) {
        const newN = (renaming[name] || "").trim();
        if (!newN) return setMessage("New name cannot be empty.");
        if (newN === name) { setRenaming(s => { const c = { ...s }; delete c[name]; return c; }); return; }

        setBusy(true);
        try {
            const v = await readSecret(name);
            if (v == null) throw new Error("Could not read existing secret.");
            await saveSecret(newN, v);
            await deleteSecret(name);
            await renameSecretName(name, newN);
            setRenaming(s => { const c = { ...s }; delete c[name]; return c; });
            await refresh();
            setMessage(`Renamed "${name}" → "${newN}".`);
        } catch (err: unknown) {
            setMessage(`Rename failed: ${errMsg(err)}`);
        } finally { setBusy(false); }
    }

    return (
        <div className="space-y-6">
            <h2 className="text-xl font-semibold">Secrets</h2>

            {/* Create new */}
            <div className="rounded-xl border p-4 space-y-3">
                <div className="font-medium">Add a new secret</div>
                <div className="flex gap-2">
                    <input
                        className="min-w-48 flex-1 border rounded px-2 py-1"
                        placeholder="Secret name (e.g. CATAPULT_API_KEY)"
                        value={newName}
                        onChange={e => setNewName(e.target.value)}
                    />
                    <input
                        className="flex-1 border rounded px-2 py-1"
                        placeholder="Value"
                        value={newValue}
                        onChange={e => setNewValue(e.target.value)}
                        type="password"
                    />
                    <button disabled={busy} onClick={() => void handleAdd()} className="border rounded px-3 py-1">
                        Save
                    </button>
                </div>
            </div>

            {/* Search */}
            <div className="flex items-center gap-2">
                <input
                    className="border rounded px-2 py-1 flex-1"
                    placeholder="Filter by name…"
                    value={filter}
                    onChange={e => setFilter(e.target.value)}
                />
                <button className="border rounded px-3 py-1" onClick={() => void refresh()} disabled={busy}>Refresh</button>
            </div>

            {/* List */}
            <div className="rounded-xl border divide-y">
                {filtered.length === 0 ? (
                    <div className="p-4 text-sm text-gray-600">No secrets yet.</div>
                ) : filtered.map(name => {
                    const mode = reveal[name] ?? "idle";
                    const isRenaming = renaming[name] !== undefined;
                    return (
                        <div key={name} className="p-3 flex flex-col gap-2">
                            <div className="flex items-center gap-2">
                                {!isRenaming ? (
                                    <>
                                        <div className="font-mono text-sm flex-1 break-all">{name}</div>
                                        <button className="text-sm px-2 py-1 border rounded" onClick={() => startRename(name)}>Rename</button>
                                    </>
                                ) : (
                                    <>
                                        <input
                                            className="border rounded px-2 py-1 font-mono text-sm flex-1"
                                            value={renaming[name] || ""}
                                            onChange={e => setRenaming(s => ({ ...s, [name]: e.target.value }))}
                                        />
                                        <button className="text-sm px-2 py-1 border rounded" onClick={() => void commitRename(name)} disabled={busy}>Save</button>
                                        <button className="text-sm px-2 py-1 border rounded" onClick={() => {
                                            setRenaming(s => { const c = { ...s }; delete c[name]; return c; });
                                        }}>Cancel</button>
                                    </>
                                )}
                            </div>

                            <div className="flex items-center gap-2">
                                {mode === "reveal" ? (
                                    <input
                                        className="border rounded px-2 py-1 flex-1 font-mono"
                                        value={editingValue[name] ?? ""}
                                        onChange={e => setEditingValue(s => ({ ...s, [name]: e.target.value }))}
                                        type="text"
                                    />
                                ) : (
                                    <input className="border rounded px-2 py-1 flex-1 font-mono" value="••••••••••" readOnly />
                                )}

                                {mode === "reveal" ? (
                                    <>
                                        <button className="text-sm px-2 py-1 border rounded" onClick={() => void handleUpdate(name)} disabled={busy}>Update</button>
                                        <button className="text-sm px-2 py-1 border rounded" onClick={() => setReveal(s => ({ ...s, [name]: "idle" }))}>Hide</button>
                                    </>
                                ) : (
                                    <button className="text-sm px-2 py-1 border rounded" onClick={() => void handleReveal(name)} disabled={busy}>Reveal</button>
                                )}

                                <button className="text-sm px-2 py-1 border rounded" onClick={() => void handleDelete(name)} disabled={busy}>Delete</button>
                            </div>
                        </div>
                    );
                })}
            </div>

            {message && <div className="text-sm text-blue-700">{message}</div>}
        </div>
    );
}
