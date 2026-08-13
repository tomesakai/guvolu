import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 查询服务仅绑定本机
const QUERY_SERVICE_ORIGIN = "http://127.0.0.1:8721";
const DEV_SERVER_PORT = 5173;
const TOKEN_HEADER = "X-Guvolu-Token";

const HERE = dirname(fileURLToPath(import.meta.url));
const TOKEN_FILE = resolve(HERE, "..", "logs", "ui-token.txt");

// 令牌只在服务端读取，浏览器不接触
function readUiToken(): string {
  try {
    return readFileSync(TOKEN_FILE, "utf-8").trim();
  } catch {
    console.warn(`[guvolu] 未读到令牌文件 ${TOKEN_FILE}，请先启动查询服务`);
    return "";
  }
}

const uiToken = readUiToken();

export default defineConfig({
  plugins: [react()],
  server: {
    port: DEV_SERVER_PORT,
    strictPort: true,
    proxy: {
      "/api": {
        target: QUERY_SERVICE_ORIGIN,
        changeOrigin: false,
        headers: { [TOKEN_HEADER]: uiToken },
      },
    },
  },
  build: {
    target: "es2022",
    sourcemap: false,
  },
});
