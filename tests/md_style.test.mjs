// Markdown 样式校验，见 W-08
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

// 扫描排除目录，含外部资料与运行数据
const EXCLUDE = new Set([
  ".git", "__pycache__", ".venv", ".venv-gpu", "venv", "node_modules",
  ".pytest_cache", ".mypy_cache", ".ruff_cache", "dist",
  "data", "logs",
  "gpu-factor-mining-v1.1", "outsides", "worktrees",
]);

const CJK = /[\u4e00-\u9fff]/;
const HEADING = /^(#{1,6})\s+\S/;
const RULE = /^-{3,}$/;
const SEP_CELL = /^:?-{1,}:?$/;
const BULLET = /^\s*[*+]\s+\S/;

function walk(dir, exts, out = []) {
  for (const name of readdirSync(dir)) {
    if (EXCLUDE.has(name)) continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, exts, out);
    else if (exts.some((e) => name.endsWith(e))) out.push(p);
  }
  return out;
}

const rel = (p) => relative(ROOT, p).split("\\").join("/");
const mdFiles = () => walk(ROOT, [".md"]).sort();
const mjsFiles = () => walk(ROOT, [".mjs", ".ts", ".tsx"]).sort();

// 解析围栏与文件头信息块
function scan(path) {
  const raw = readFileSync(path, "utf8");
  const lines = raw.split("\n");
  let fmEnd = -1;
  if (lines[0] === "---") {
    for (let i = 1; i < lines.length; i += 1) {
      if (lines[i] === "---") { fmEnd = i; break; }
    }
  }
  const fence = new Array(lines.length).fill(false);
  const inside = new Array(lines.length).fill(false);
  let open = false;
  for (let i = fmEnd + 1; i < lines.length; i += 1) {
    if (lines[i].trim().startsWith("```")) {
      fence[i] = true;
      open = !open;
    } else {
      inside[i] = open;
    }
  }
  return { raw, lines, fmEnd, fence, inside, path };
}

const plain = (f, i) =>
  i > f.fmEnd && !f.fence[i] && !f.inside[i];

// 正文不得出现水平分割线
function checkRules() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    f.lines.forEach((line, i) => {
      if (plain(f, i) && RULE.test(line.trim())) {
        errs.push(`${rel(p)}:${i + 1}: 多余的水平分割线`);
      }
    });
  }
  return errs;
}

// 标题层级与前后空行
function checkHeadings() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    let prev = 0;
    f.lines.forEach((line, i) => {
      if (!plain(f, i)) return;
      const m = HEADING.exec(line);
      if (!m) return;
      const level = m[1].length;
      if (prev && level > prev + 1) {
        errs.push(`${rel(p)}:${i + 1}: 标题层级跳级 ${prev} 到 ${level}`);
      }
      prev = level;
      const top = i === 0 || i === f.fmEnd + 1;
      if (!top && f.lines[i - 1] !== "") {
        errs.push(`${rel(p)}:${i + 1}: 标题前缺空行`);
      }
      if (i + 1 < f.lines.length && f.lines[i + 1] !== "") {
        errs.push(`${rel(p)}:${i + 1}: 标题后缺空行`);
      }
    });
  }
  return errs;
}

// 表格列数一致且含分隔行
function checkTables() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    const isRow = (i) =>
      plain(f, i) && f.lines[i].trim().startsWith("|");
    const cells = (line) => {
      const t = line.trim();
      return t.slice(1, t.endsWith("|") ? -1 : undefined).split("|");
    };
    let i = 0;
    while (i < f.lines.length) {
      if (!isRow(i)) { i += 1; continue; }
      let j = i;
      while (j < f.lines.length && isRow(j)) j += 1;
      const block = f.lines.slice(i, j);
      const at = `${rel(p)}:${i + 1}`;
      if (block.length < 3) {
        errs.push(`${at}: 表格行数不足`);
      } else if (!cells(block[1]).every((c) => SEP_CELL.test(c.trim()))) {
        errs.push(`${at}: 表格缺少分隔行`);
      }
      const n = cells(block[0]).length;
      block.forEach((row, k) => {
        if (cells(row).length !== n) {
          errs.push(`${rel(p)}:${i + k + 1}: 表格列数不一致`);
        }
      });
      if (i > 0 && f.lines[i - 1] !== "") errs.push(`${at}: 表格前缺空行`);
      if (j < f.lines.length && f.lines[j] !== "") {
        errs.push(`${rel(p)}:${j}: 表格后缺空行`);
      }
      i = j;
    }
  }
  return errs;
}

// 围栏配对且带语言标注
function checkFences() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    const marks = [];
    f.lines.forEach((line, i) => { if (f.fence[i]) marks.push(i); });
    if (marks.length % 2 !== 0) {
      errs.push(`${rel(p)}: 代码围栏未配对`);
      continue;
    }
    for (let k = 0; k < marks.length; k += 2) {
      const [a, b] = [marks[k], marks[k + 1]];
      if (!/^```[A-Za-z0-9_-]+$/.test(f.lines[a].trim())) {
        errs.push(`${rel(p)}:${a + 1}: 围栏缺语言标注`);
      }
      if (f.lines[b].trim() !== "```") {
        errs.push(`${rel(p)}:${b + 1}: 结束围栏应为三个反引号`);
      }
      if (a > 0 && f.lines[a - 1] !== "") {
        errs.push(`${rel(p)}:${a + 1}: 围栏前缺空行`);
      }
      if (b + 1 < f.lines.length && f.lines[b + 1] !== "") {
        errs.push(`${rel(p)}:${b + 1}: 围栏后缺空行`);
      }
    }
  }
  return errs;
}

// 空白字符与文件结尾
function checkWhitespace() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    let blank = 0;
    f.lines.forEach((line, i) => {
      if (/[ \t]$/.test(line)) errs.push(`${rel(p)}:${i + 1}: 行尾空白`);
      if (line.includes("\t")) errs.push(`${rel(p)}:${i + 1}: 含制表符`);
      blank = line === "" ? blank + 1 : 0;
      if (blank > 1) errs.push(`${rel(p)}:${i + 1}: 连续空行`);
    });
    if (!f.raw.endsWith("\n") || f.raw.endsWith("\n\n")) {
      errs.push(`${rel(p)}: 文件应以单个换行结尾`);
    }
  }
  return errs;
}

// 无序列表统一用短横线
function checkBullets() {
  const errs = [];
  for (const p of mdFiles()) {
    const f = scan(p);
    f.lines.forEach((line, i) => {
      if (plain(f, i) && BULLET.test(line)) {
        errs.push(`${rel(p)}:${i + 1}: 无序列表应用短横线`);
      }
    });
  }
  return errs;
}

// 脚本注释中文且不超二十字
function checkComments() {
  const errs = [];
  for (const p of mjsFiles()) {
    readFileSync(p, "utf8").split("\n").forEach((line, i) => {
      const t = line.trim();
      if (!t.startsWith("//")) return;
      const text = t.slice(2).trim();
      if (!text) return;
      // 三斜线指令与工具指令跳过
      if (text.startsWith("/") || text.startsWith("@")) return;
      if (text.length > 20) errs.push(`${rel(p)}:${i + 1}: 注释超过二十字`);
      if (!CJK.test(text)) errs.push(`${rel(p)}:${i + 1}: 注释非中文`);
    });
  }
  return errs;
}

const CHECKS = [
  ["水平分割线", checkRules],
  ["标题", checkHeadings],
  ["表格", checkTables],
  ["代码围栏", checkFences],
  ["空白字符", checkWhitespace],
  ["列表标记", checkBullets],
  ["脚本注释", checkComments],
];

for (const [name, fn] of CHECKS) {
  test(name, () => {
    const errs = fn();
    assert.deepEqual(errs, [], `\n${errs.join("\n")}`);
  });
}

export { CHECKS };
