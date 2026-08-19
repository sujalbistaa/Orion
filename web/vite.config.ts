import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Dev server proxies /api and friends to the local Orion API server so the
// console can be developed with `npm run dev` while `orion-server
// -enable-console=false` serves the real API on 127.0.0.1:7070 (see
// hack/dev.sh). In production the console is served by orion-server itself
// (go:embed of dist/), so requests are same-origin and no proxy is involved.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:7070",
      "/healthz": "http://127.0.0.1:7070",
      "/readyz": "http://127.0.0.1:7070",
      "/metrics": "http://127.0.0.1:7070",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
  },
});
