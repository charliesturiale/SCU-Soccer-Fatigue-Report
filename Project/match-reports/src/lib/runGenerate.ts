// src/lib/runGenerate.ts
import { runPython } from "./runPython";

export async function runGenerate(
    args: string[],
    onLine: (l: string) => void
): Promise<number> {
    try {
        onLine(`Running: server/generate.py ${args.join(" ")}`);

        const result = await runPython("../server/generate.py", args);

        // Output stdout line by line
        if (result.stdout) {
            result.stdout.split("\n").forEach(line => {
                if (line.trim()) onLine(line);
            });
        }

        // Output stderr if any
        if (result.stderr) {
            result.stderr.split("\n").forEach(line => {
                if (line.trim()) onLine(`[stderr] ${line}`);
            });
        }

        return result.code ?? 0;
    } catch (error) {
        onLine(`Error: ${error instanceof Error ? error.message : String(error)}`);
        return 1; // failure
    }
}