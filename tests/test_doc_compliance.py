"""文档合规校验，规则见 SKILLS.md 第 10 章。"""
import re
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs" / "00-rules-registry.md"

# 扫描排除目录，含外部引入资料
EXCLUDE = {".git", "__pycache__", ".venv", "venv", "node_modules",
           ".pytest_cache", ".mypy_cache", ".ruff_cache",
           "gpu-factor-mining-v1.1", "outsides"}

DEF_RE = re.compile(r"^\s*(?:\|\s*|-\s+)\*\*(TBD|[TRCDXGUAW])-(\d{2})", re.M)
REF_RE = re.compile(r"\b(TBD|[TRCDXGUAW])-(\d{2})\b")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)#\s]+)(?:#[^)]*)?\)")
# 感叹号与问号，全角及半角
_MARKS = "".join(map(chr, (0xFF01, 0xFF1F, 0x21, 0x3F)))
BAD_PUNCT_RE = re.compile("[" + _MARKS + "]{2,}")
REF_ONLY_RE = re.compile(
    r"^(TBD|[TRCDXGUAW])-\d{2}([、,;\s]+(TBD|[TRCDXGUAW])-\d{2})*$")
CJK_RE = re.compile(r"[一-鿿]")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-")
# 注释检查跳过标记
COMMENT_SKIP = ("noqa", "type:", "coding", "fmt:", "!/")


def repo_files(suffix):
    out = []
    for p in ROOT.rglob(f"*{suffix}"):
        if not EXCLUDE.intersection(p.relative_to(ROOT).parts):
            out.append(p)
    return sorted(out)


def read(p):
    return p.read_text(encoding="utf-8")


def rel(p):
    return p.relative_to(ROOT).as_posix()


def registry_tables():
    domains, docs, section = {}, {}, ""
    for line in read(REGISTRY).splitlines():
        if line.startswith("## "):
            section = line
            continue
        if not line.startswith("|") or "---" in line:
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if "编号域" in section and len(cells) >= 5 and cells[0] != "前缀":
            dep = cells[4]
            depset = (set() if dep in ("无", "") else
                      {int(x.split("-")[-1]) for x in re.split("[、,]", dep)})
            domains[cells[0]] = (cells[2], int(cells[3]), depset)
        elif "文档清单" in section and len(cells) >= 3 and cells[0] != "路径":
            docs[cells[0]] = cells[1]
    return domains, docs


def check_registry():
    """编号定义与登记册同步（W-01）"""
    errs, cache = [], {}
    domains, _ = registry_tables()
    for prefix, (owner, mx, dep) in domains.items():
        if owner not in cache:
            found = {}
            for m in DEF_RE.finditer(read(ROOT / owner)):
                found.setdefault(m.group(1), set()).add(int(m.group(2)))
            cache[owner] = found
        got = cache[owner].get(prefix, set())
        want = set(range(1, mx + 1)) - dep
        if got != want:
            errs.append(f"{owner}: {prefix} 编号不同步"
                        f" 缺{sorted(want - got)} 多{sorted(got - want)}")
    return errs


def check_refs():
    """规则引用必须已登记（W-01）"""
    errs = []
    domains, _ = registry_tables()
    for p in repo_files(".md") + repo_files(".py"):
        for m in REF_RE.finditer(read(p)):
            prefix, num = m.group(1), int(m.group(2))
            if prefix not in domains:
                errs.append(f"{rel(p)}: 引用未登记编号域 {m.group(0)}")
            elif not 1 <= num <= domains[prefix][1]:
                errs.append(f"{rel(p)}: 引用越界编号 {m.group(0)}")
    return errs


def check_links():
    """文档内部链接必须可达（W-03）"""
    errs = []
    for p in repo_files(".md"):
        for m in LINK_RE.finditer(read(p)):
            t = m.group(1)
            if t.startswith(("http://", "https://", "mailto:")):
                continue
            if not (p.parent / t).resolve().exists():
                errs.append(f"{rel(p)}: 链接失效 {t}")
    return errs


def check_inventory():
    """文档清单完整且命名合规（W-02）"""
    errs = []
    _, docs = registry_tables()
    actual = {rel(p) for p in repo_files(".md")}
    for path, kind in docs.items():
        if path not in actual:
            errs.append(f"清单登记的文档不存在 {path}")
        if kind not in ("长期维护", "时效快照"):
            errs.append(f"{path}: 类别非法 {kind}")
        if kind == "时效快照" and not DATE_RE.match(Path(path).name):
            errs.append(f"{path}: 时效快照缺日期前缀")
    for path in sorted(actual - set(docs)):
        errs.append(f"文档未登记 {path}")
    return errs


def bad_char(ch):
    o = ord(ch)
    return (0x1F000 <= o <= 0x1FAFF or 0x2600 <= o <= 0x27BF
            or 0x2B00 <= o <= 0x2BFF or o == 0xFE0F)


def check_symbols():
    """禁用符号与重复标点（W-06）"""
    errs = []
    for p in repo_files(".md") + repo_files(".py"):
        for i, line in enumerate(read(p).splitlines(), 1):
            bad = {c for c in line if bad_char(c)}
            if bad:
                errs.append(f"{rel(p)}:{i}: 禁用符号 {''.join(sorted(bad))}")
            if BAD_PUNCT_RE.search(line):
                errs.append(f"{rel(p)}:{i}: 重复标点")
    return errs


def check_comments():
    """注释中文且不超二十字（W-04）"""
    errs = []
    for p in repo_files(".py"):
        with open(p, "rb") as f:
            toks = list(tokenize.tokenize(f.readline))
        for tok in toks:
            if tok.type != tokenize.COMMENT:
                continue
            text = tok.string.lstrip("#").strip()
            if not text or any(s in text for s in COMMENT_SKIP):
                continue
            if REF_ONLY_RE.match(text):
                continue
            if len(text) > 20:
                errs.append(f"{rel(p)}:{tok.start[0]}: 注释超过二十字")
            if not CJK_RE.search(text):
                errs.append(f"{rel(p)}:{tok.start[0]}: 注释非中文")
    return errs


CHECKS = [check_registry, check_refs, check_links,
          check_inventory, check_symbols, check_comments]


def test_registry():
    assert not check_registry(), "\n".join(check_registry())


def test_refs():
    assert not check_refs(), "\n".join(check_refs())


def test_links():
    assert not check_links(), "\n".join(check_links())


def test_inventory():
    assert not check_inventory(), "\n".join(check_inventory())


def test_symbols():
    assert not check_symbols(), "\n".join(check_symbols())


def test_comments():
    assert not check_comments(), "\n".join(check_comments())


if __name__ == "__main__":
    total = []
    for fn in CHECKS:
        for e in fn():
            total.append(e)
            print(e)
    print(f"共 {len(total)} 项违规")
    raise SystemExit(1 if total else 0)
