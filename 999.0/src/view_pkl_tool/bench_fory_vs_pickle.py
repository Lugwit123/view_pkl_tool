"""
pyfory vs pickle (protocol 2/4/5) 性能对比测试

运行示例（按本机环境二选一）:
  Python 3.11（lugwit）:
    & D:/TD_Depot/Software/Lugwit_syncPlug/lugwit_insapp/python_env/lugwit_python.exe bench_fory_vs_pickle.py
  Python 3.15（仓库内 wuwo/py_env）:
    & <trayapp>/wuwo/py_env/python.exe bench_fory_vs_pickle.py
  例: & D:/TD_Depot/Software/Lugwit_syncPlug/lugwit_insapp/trayapp/wuwo/py_env/python.exe bench_fory_vs_pickle.py

说明: 3.15 环境若只有 pickle 列，多为当前解释器装不上 PyPI 的 pyfory wheel，与脚本无关。
"""
import platform
import timeit
import pickle
import sys
from dataclasses import dataclass, field
from typing import Any, List, Dict

# pickle 各协议版本说明:
#   2 - Python 2.3+，兼容性最好
#   4 - Python 3.4+，支持大对象(>4GB)，更高效
#   5 - Python 3.8+，支持 out-of-band buffer (numpy 零拷贝)
PICKLE_PROTOCOLS = {
    "pkl-p2": 2,
    "pkl-p4": 4,
    "pkl-p5": 5,
}

try:
    import pyfory
    HAS_FORY = True
except ImportError:
    HAS_FORY = False


def _python_is_prerelease() -> bool:
    return sys.version_info.releaselevel != "final"


def _print_startup_notes() -> None:
    """启动时说明：预发布 Python 的基准意义、pyfory 缺失原因。"""
    if _python_is_prerelease():
        print(
            f"[NOTE] 当前 Python {platform.python_version()} 为预发布版；"
            "性能与正式版会有差异，本脚本数字仅作同机对比参考。"
        )
    if not HAS_FORY:
        print(
            "[WARN] 未能 import pyfory，仅输出 pickle 各协议列。"
            "可尝试: pip install pyfory"
        )
        if _python_is_prerelease():
            print(
                "[NOTE] 预发布 Python 常无 pyfory 官方 wheel，pip 可能装不上。"
                "要与 Fory 对比请用已支持版本（见 PyPI）或源码构建。"
            )

# ── 测试数据 ──────────────────────────────────────────────────────
@dataclass
class Inner:
    x: float = 1.0
    y: float = 2.0
    label: str = "inner"

@dataclass
class ComplexObj:
    name: str = "test"
    values: List[float] = field(default_factory=lambda: list(range(100)))
    meta: Dict[str, Any] = field(default_factory=lambda: {f"k{i}": f"v{i}" for i in range(20)})
    nested: Inner = field(default_factory=Inner)

DATASETS = {
    "small dict  (50 keys)":   {f"key{i}": f"value{i}" for i in range(50)},
    "medium dict (500 keys)":  {f"key{i}": f"value{i}" for i in range(500)},
    "large dict  (5000 keys)": {f"key{i}": f"value{i}" for i in range(5000)},
    "list of ints (10000)":    list(range(10000)),
    "nested list":             [[i * j for j in range(50)] for i in range(50)],
    "dataclass obj":           ComplexObj(),
}

REPEAT = 5000

# ── 工具函数 ──────────────────────────────────────────────────────
def fmt(seconds: float) -> str:
    return f"{seconds*1000:.1f}ms"

def speedup(base: float, fast: float) -> str:
    if fast <= 0:
        return "N/A"
    ratio = base / fast
    tag = "faster" if ratio >= 1.0 else "slower"
    return f"{ratio:.2f}x {tag}"

# ── 主测试 ────────────────────────────────────────────────────────
def run_bench():
    _print_startup_notes()

    if HAS_FORY:
        fory = pyfory.Fory(xlang=False, ref=True, strict=False)

    # 列头: 数据集 | 指标 | pkl-p2 | pkl-p4 | pkl-p5 | Fory | vs pkl-p5
    col_w = 14
    header = (f"{'数据集':<22} {'指标':<8}"
              f" {'pkl-p2':>{col_w}} {'pkl-p4':>{col_w}} {'pkl-p5':>{col_w}}"
              + (f" {'Fory':>{col_w}} {'vs pkl-p5':>{col_w}}" if HAS_FORY else ""))
    sep = "-" * len(header)
    print(sep)
    print(header)
    print(sep)

    for name, obj in DATASETS.items():
        rows: dict[str, dict] = {"序列化": {}, "反序列化": {}, "体积(B)": {}}

        for label, proto in PICKLE_PROTOCOLS.items():
            b = pickle.dumps(obj, protocol=proto)
            rows["序列化"][label]  = timeit.timeit(lambda p=proto: pickle.dumps(obj, protocol=p), number=REPEAT)
            rows["反序列化"][label] = timeit.timeit(lambda _b=b: pickle.loads(_b), number=REPEAT)
            rows["体积(B)"][label]  = len(b)

        if HAS_FORY:
            fb = fory.dumps(obj)
            rows["序列化"]["fory"]  = timeit.timeit(lambda: fory.dumps(obj), number=REPEAT)
            rows["反序列化"]["fory"] = timeit.timeit(lambda: fory.loads(fb), number=REPEAT)
            rows["体积(B)"]["fory"]  = len(fb)

        for metric, vals in rows.items():
            is_size = metric == "体积(B)"
            def cell(v):
                return f"{v:>{col_w}}" if is_size else f"{fmt(v):>{col_w}}"

            line = (f"{name:<22} {metric:<8}"
                    f" {cell(vals['pkl-p2'])} {cell(vals['pkl-p4'])} {cell(vals['pkl-p5'])}")
            if HAS_FORY:
                base = vals["pkl-p5"]
                fast = vals["fory"]
                line += f" {cell(fast)} {speedup(base, fast):>{col_w}}"
            print(line)
        print()

    print(sep)
    print(f"Python {sys.version.split()[0]}  |  每项重复: {REPEAT} 次")
    if HAS_FORY:
        print(f"pyfory {pyfory.__version__}  |  对比基准: pkl-p5")

if __name__ == "__main__":
    run_bench()
