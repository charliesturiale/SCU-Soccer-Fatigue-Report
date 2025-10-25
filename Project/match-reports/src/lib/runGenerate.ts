// src/lib/runGenerate.ts

export async function runGenerate(
    args: string[],
    onLine: (l: string) => void
): Promise<number> {
    onLine(`(mock) reports ${args.join(" ")}`);
    // simulate some progress
    await new Promise(r => setTimeout(r, 300));
    onLine("(mock) finished");
    return 0; // success
}