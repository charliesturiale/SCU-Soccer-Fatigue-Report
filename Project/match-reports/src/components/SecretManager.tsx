// src/components/SecretManager.tsx
import { useEffect, useMemo, useState, useCallback } from "react";
import {
    setKey as saveSecret,
    getKey as readSecret,
    deleteKey as deleteSecret,
    listKeys as listSecretNames,
} from "../lib/config";

type Mode = "idle" | "reveal";

function errMsg(err: unknown): string {
    if (err instanceof Error) return err.message;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
}

export default function SecretManager() {
    const [names, setNames] = useState<string[]>([]);
    const [filter, setFilter] = useState("");
    const [newName, setNewName] = useState("");
    const [newValue, setNewValue] = useState("");
    const [busyGlobal, setBusyGlobal] = useState(false);
    const [busyRow, setBusyRow] = useState<Record<string, boolean>>({});
    const [message, setMessage] = useState<string | null>(null);
    const [reveal, setReveal] = useState<Record<string, Mode>>({});
    const [editingValue, setEditingValue] = useState<Record<string, string>>({});
    const [renaming, setRenaming] = useState<Record<string, string | undefined>>({});

    const filtered = useMemo(
        () =>
            names
                .filter((n) => n.toLowerCase().includes(filter.toLowerCase()))
                .sort((a, b) => a.localeCompare(b)),
        [names, filter]
    );

    const refresh = useCallback(async () => {
        try {
            const ks = await listSecretNames();
            setNames(ks ?? []);
        } catch (e) {
            setMessage(`Failed to load secrets: ${errMsg(e)}`);
        }
    }, []);

    useEffect(() => {
        void refresh();
    }, [refresh]);

    // --- per-row busy helpers ---
    const setRowBusy = (name: string, v: boolean) =>
        setBusyRow((s) => ({ ...s, [name]: v }));
    const isRowBusy = (name: string) => !!busyRow[name];

    // --- Add ---
    async function handleAdd() {
        const name = newName.trim();
        const value = newValue;
        if (!name) return setMessage("Please provide a secret name.");
        if (!value) return setMessage("Please provide a secret value.");
        if (names.includes(name)) return setMessage(`"${name}" already exists.`);

        setBusyGlobal(true);
        try {
            await saveSecret(name, value);
            setNewName("");
            setNewValue("");
            await refresh();
            setMessage(`Saved "${name}".`);
        } catch (err: unknown) {
            setMessage(`Failed to save: ${errMsg(err)}`);
        } finally {
            setBusyGlobal(false);
        }
    }

    // --- Reveal/Update/Delete ---
    async function handleReveal(name: string) {
        setRowBusy(name, true);
        try {
            const v = await readSecret(name);
            setEditingValue((s) => ({ ...s, [name]: v ?? "" }));
            setReveal((s) => ({ ...s, [name]: "reveal" }));
        } catch (err: unknown) {
            setMessage(`Failed to read "${name}": ${errMsg(err)}`);
        } finally {
            setRowBusy(name, false);
        }
    }

    async function handleUpdate(name: string) {
        const v = editingValue[name] ?? "";
        if (!v) return setMessage("Value cannot be empty.");
        setRowBusy(name, true);
        try {
            await saveSecret(name, v);
            setMessage(`Updated "${name}".`);
            setReveal((s) => ({ ...s, [name]: "idle" }));
        } catch (err: unknown) {
            setMessage(`Failed to update "${name}": ${errMsg(err)}`);
        } finally {
            setRowBusy(name, false);
        }
    }

    async function handleDelete(name: string) {
        if (!window.confirm(`Delete secret "${name}"? This cannot be undone.`)) return;
        setRowBusy(name, true);
        try {
            await deleteSecret(name);
            // Optimistic local cleanup to keep state tidy:
            setReveal((s) => {
                const c = { ...s };
                delete c[name];
                return c;
            });
            setEditingValue((s) => {
                const c = { ...s };
                delete c[name];
                return c;
            });
            setRenaming((s) => {
                const c = { ...s };
                delete c[name];
                return c;
            });
            await refresh();
            setMessage(`Deleted "${name}".`);
        } catch (err: unknown) {
            setMessage(`Failed to delete "${name}": ${errMsg(err)}`);
        } finally {
            setRowBusy(name, false);
        }
    }

    // --- Rename (copy -> new key, then delete old) ---
    async function renameSecretName(oldName: string, newNameInput: string) {
        const newName = newNameInput.trim();
        if (!newName) {
            setMessage("New name cannot be empty.");
            return;
        }
        if (newName === oldName) {
            // no-op: just exit rename mode
            setRenaming((s) => {
                const c = { ...s };
                delete c[oldName];
                return c;
            });
            return;
        }
        if (names.includes(newName)) {
            setMessage(`"${newName}" already exists.`);
            return;
        }

        setRowBusy(oldName, true);
        try {
            const val = await readSecret(oldName);
            if (val == null) throw new Error("Original secret value not found.");
            await saveSecret(newName, val);
            await deleteSecret(oldName);

            // Move local state under new name to keep UI consistent:
            setReveal((s) => {
                const c = { ...s };
                c[newName] = c[oldName] ?? "idle";
                delete c[oldName];
                return c;
            });
            setEditingValue((s) => {
                const c = { ...s };
                c[newName] = c[oldName] ?? "";
                delete c[oldName];
                return c;
            });
            setRenaming((s) => {
                const c = { ...s };
                delete c[oldName];
                return c;
            });

            await refresh();
            setMessage(`Renamed "${oldName}" → "${newName}".`);
        } catch (err: unknown) {
            setMessage(`Failed to rename "${oldName}": ${errMsg(err)}`);
        } finally {
            setRowBusy(oldName, false);
        }
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
                        onChange={(e) => setNewName(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") void handleAdd();
                        }}
                        disabled={busyGlobal}
                    />
                    <input
                        className="flex-1 border rounded px-2 py-1"
                        placeholder="Value"
                        value={newValue}
                        onChange={(e) => setNewValue(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") void handleAdd();
                        }}
                        type="password"
                        disabled={busyGlobal}
                    />
                    <button
                        disabled={busyGlobal}
                        onClick={() => void handleAdd()}
                        className="border rounded px-3 py-1"
                    >
                        {busyGlobal ? "Saving…" : "Save"}
                    </button>
                </div>
            </div>

            {/* Search */}
            <div className="flex items-center gap-2">
                <input
                    className="border rounded px-2 py-1 flex-1"
                    placeholder="Filter by name…"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                />
                <button
                    className="border rounded px-3 py-1"
                    onClick={() => void refresh()}
                    disabled={busyGlobal}
                >
                    Refresh
                </button>
            </div>

            {/* List */}
            <div className="rounded-xl border divide-y">
                {filtered.length === 0 ? (
                    <div className="p-4 text-sm text-gray-600">No secrets yet.</div>
                ) : (
                    filtered.map((name) => {
                        const mode = reveal[name] ?? "idle";
                        const isRenaming = renaming[name] !== undefined;
                        const rowBusy = isRowBusy(name);
                        return (
                            <div key={name} className="p-3 flex flex-col gap-2">
                                {/* Header / Rename */}
                                <div className="flex items-center gap-2">
                                    {!isRenaming ? (
                                        <>
                                            <div className="font-mono text-sm flex-1 break-all">{name}</div>
                                            <button
                                                className="text-sm px-2 py-1 border rounded"
                                                onClick={() =>
                                                    setRenaming((s) => ({ ...s, [name]: name }))
                                                }
                                                disabled={rowBusy}
                                            >
                                                Rename
                                            </button>
                                        </>
                                    ) : (
                                        <>
                                            <input
                                                className="border rounded px-2 py-1 font-mono text-sm flex-1"
                                                value={renaming[name] || ""}
                                                onChange={(e) =>
                                                    setRenaming((s) => ({ ...s, [name]: e.target.value }))
                                                }
                                                onKeyDown={(e) => {
                                                    if (e.key === "Enter") {
                                                        void renameSecretName(name, renaming[name] || "");
                                                    } else if (e.key === "Escape") {
                                                        setRenaming((s) => {
                                                            const c = { ...s };
                                                            delete c[name];
                                                            return c;
                                                        });
                                                    }
                                                }}
                                                disabled={rowBusy}
                                            />
                                            <button
                                                className="text-sm px-2 py-1 border rounded"
                                                onClick={() => void renameSecretName(name, renaming[name] || "")}
                                                disabled={rowBusy}
                                            >
                                                Save
                                            </button>
                                            <button
                                                className="text-sm px-2 py-1 border rounded"
                                                onClick={() =>
                                                    setRenaming((s) => {
                                                        const c = { ...s };
                                                        delete c[name];
                                                        return c;
                                                    })
                                                }
                                                disabled={rowBusy}
                                            >
                                                Cancel
                                            </button>
                                        </>
                                    )}
                                </div>

                                {/* Value / Actions */}
                                <div className="flex items-center gap-2">
                                    {mode === "reveal" ? (
                                        <input
                                            className="border rounded px-2 py-1 flex-1 font-mono"
                                            value={editingValue[name] ?? ""}
                                            onChange={(e) =>
                                                setEditingValue((s) => ({ ...s, [name]: e.target.value }))
                                            }
                                            type="text"
                                            disabled={rowBusy}
                                        />
                                    ) : (
                                        <input
                                            className="border rounded px-2 py-1 flex-1 font-mono"
                                            value="••••••••••"
                                            readOnly
                                        />
                                    )}

                                    {mode === "reveal" ? (
                                        <>
                                            <button
                                                className="text-sm px-2 py-1 border rounded"
                                                onClick={() => void handleUpdate(name)}
                                                disabled={rowBusy}
                                            >
                                                {rowBusy ? "Updating…" : "Update"}
                                            </button>
                                            <button
                                                className="text-sm px-2 py-1 border rounded"
                                                onClick={() => setReveal((s) => ({ ...s, [name]: "idle" }))}
                                                disabled={rowBusy}
                                            >
                                                Hide
                                            </button>
                                        </>
                                    ) : (
                                        <button
                                            className="text-sm px-2 py-1 border rounded"
                                            onClick={() => void handleReveal(name)}
                                            disabled={rowBusy}
                                        >
                                            {rowBusy ? "Loading…" : "Reveal"}
                                        </button>
                                    )}

                                    <button
                                        className="text-sm px-2 py-1 border rounded"
                                        onClick={() => void handleDelete(name)}
                                        disabled={rowBusy}
                                    >
                                        {rowBusy ? "Deleting…" : "Delete"}
                                    </button>
                                </div>
                            </div>
                        );
                    })
                )}
            </div>

            {message && <div className="text-sm text-blue-700">{message}</div>}
        </div>
    );
}
