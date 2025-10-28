// src/App.tsx
import { useState } from "react";
import { runGenerate } from "./lib/runGenerate";
// import SecretManager from "./components/SecretManager";
import RequiredSecrets from "./components/requiredSecrets";
import { exportSecretsToDisk } from "./lib/config";
import { exportAppConfigToDisk } from "./lib/appConfigExport";
import { runPython } from "./lib/runPython";

export default function App() {
  const [log, setLog] = useState<string[]>([]);

  const addLog = (l: string) => setLog(s => [...s, l]);

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-bold">Match Report Toolkit</h1>

      <div className="space-x-2">
        <button onClick={async () => {
          await exportSecretsToDisk();
          await exportAppConfigToDisk();
          // optional: kick an ingest
          const res = await runPython("etl/ingest_vald.py", ["MSOC"]);
          console.log(res.stdout);
        }}>
          Prep data pipeline
        </button>
        <button
          onClick={async () => {
            addLog("Building player profiles…");
            const code = await runGenerate(["build-profiles", "--window-days", "42"], addLog);
            addLog(code === 0 ? "Profiles built ✅" : `Build failed (code ${code})`);
          }}
        >
          Build Player Profiles
        </button>

        <button
          onClick={async () => {
            addLog("Generating report…");
            const code = await runGenerate(["generate", "--match-date", "2025-10-24"], addLog);
            addLog(code === 0 ? "Report done ✅" : `Report failed (code ${code})`);
          }}
        >
          Generate Report PDF
        </button>
        <RequiredSecrets />
        {/* <SecretManager /> */}
      </div>

      <pre className="bg-black text-green-300 p-3 h-64 overflow-auto">{log.join("\n")}</pre>
    </div>
  );
}

