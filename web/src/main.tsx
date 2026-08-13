import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
// 中文字形本地打包，运行时零外链
import "@fontsource/noto-sans-sc/400.css";
import "@fontsource/noto-sans-sc/500.css";
import "./tokens.css";
import "./style.css";

const container = document.getElementById("root");
if (container === null) {
  // 根节点缺失即中止，不带病渲染
  throw new Error("页面根节点缺失");
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
