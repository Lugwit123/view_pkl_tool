"""
PKL 文件查看器 - 支持查看通用 pickle 对象
用法: python main.py [pkl_file]
"""

import ctypes
import datetime
import io
import json
import logging
import math
import pickle
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, TypeVar

from PySide6.QtCore import QByteArray, QBuffer, QIODevice, QThread, QTimer, Qt, Signal, Slot
from PySide6.QtUiTools import QUiLoader
from PySide6.QtGui import (
    QColor,
    QFont,
    QIcon,
    QTextBlockFormat,
    QTextCursor,
    QTextOption,
)
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

_QW = TypeVar("_QW", bound=QWidget)


def _require_ui_child(parent: QWidget, typ: type[_QW], name: str) -> _QW:
    w = parent.findChild(typ, name)
    if w is None:
        raise RuntimeError(
            f"UI 缺少控件 {name!r}（类型 {getattr(typ, '__name__', typ)}）"
        )
    return w


def _apply_ui_layout_after_load(central: QWidget) -> None:
    """补回从 .ui 去掉的 stretch / 分割尺寸（避免旧 uic 不识别 layoutStretch 等属性）。"""
    root_ly = central.layout()
    if isinstance(root_ly, QVBoxLayout) and root_ly.count() >= 2:
        root_ly.setStretch(0, 0)
        root_ly.setStretch(1, 1)

    browse = central.findChild(QWidget, "browseTab")
    if browse is not None:
        bv = browse.layout()
        if isinstance(bv, QVBoxLayout) and bv.count() >= 3:
            bv.setStretch(0, 0)
            bv.setStretch(1, 0)
            bv.setStretch(2, 1)

    hist_col = central.findChild(QWidget, "historyColumn")
    if hist_col is not None:
        hv = hist_col.layout()
        if isinstance(hv, QVBoxLayout) and hv.count() >= 3:
            hv.setStretch(0, 0)
            hv.setStretch(1, 1)
            hv.setStretch(2, 0)

    log_tab = central.findChild(QWidget, "logTab")
    if log_tab is not None:
        lv = log_tab.layout()
        if isinstance(lv, QVBoxLayout) and lv.count() >= 2:
            lv.setStretch(0, 0)
            lv.setStretch(1, 1)

    sp = central.findChild(QSplitter, "mainSplitter")
    if sp is not None:
        sp.setSizes([220, 680, 360])


_MAX_CHILDREN = 200
_DETAIL_MAX_CHARS = 50_000
_PLACEHOLDER = "__placeholder__"
_HISTORY_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer_history.json"
_HISTORY_MAX = 20
_LOG_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer.log"

_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# 树搜索：下拉「字段名」= 键列去重；后台线程遍历整棵对象图（含未展开节点），匹配后定位到树节点。
_SEARCH_ALL_FIELDS = "【全部字段】"
_SEARCH_DEFAULT_FIELD = "name"
_DEEP_SEARCH_BATCH = 256
_DEEP_SEARCH_YIELD_NODES = 6000
_KEYS_COLLECT_YIELD_NODES = 8000


class GenericPickleObject:
    """在缺失原始类定义时兜底承载对象状态。"""

    __missing_module__ = "unknown_module"
    __missing_class__ = "unknown_class"

    def __init__(self, *args, **kwargs) -> None:
        self.__dict__["_state"] = {}

    def __setstate__(self, state):
        if isinstance(state, dict):
            self.__dict__.update(state)
            self.__dict__["_state"] = state
        else:
            self.__dict__["_state"] = {"__raw_state__": repr(state)}

    def __repr__(self) -> str:
        module_name = getattr(self.__class__, "__missing_module__", "unknown_module")
        class_name = getattr(self.__class__, "__missing_class__", "unknown_class")
        return f"<GenericPickleObject {module_name}.{class_name}>"


class TolerantUnpickler(pickle.Unpickler):
    """缺模块时降级，避免必须依赖缓存文件里的完整 Python 结构体。"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._missing_class_cache: dict[tuple[str, str], type] = {}

    @staticmethod
    def _safe_type_name(module: str, name: str) -> str:
        raw = f"Missing_{module}_{name}"
        return "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in raw)

    def find_class(self, module, name):
        try:
            return super().find_class(module, name)
        except Exception:
            key = (module, name)
            cached = self._missing_class_cache.get(key)
            if cached is not None:
                return cached

            missing_type = type(
                self._safe_type_name(module, name),
                (GenericPickleObject,),
                {
                    "__missing_module__": module,
                    "__missing_class__": name,
                    "__module__": "view_pkl_tool.missing_types",
                },
            )
            self._missing_class_cache[key] = missing_type
            return missing_type


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("pkl_viewer")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger
    file_handler = logging.FileHandler(str(_LOG_FILE), encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


_LOGGER = _build_logger()


def _log_exception(message: str) -> None:
    _LOGGER.exception(message)


class _History:
    def __init__(self) -> None:
        self._paths: list[str] = []
        _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        try:
            if _HISTORY_FILE.exists():
                data = json.loads(_HISTORY_FILE.read_text(encoding="utf-8"))
                self._paths = [p for p in data if isinstance(p, str)]
        except Exception:
            traceback.print_exc()
            _log_exception(f"加载历史记录失败: {_HISTORY_FILE}")
            self._paths = []

    def _save(self) -> None:
        try:
            _HISTORY_FILE.write_text(
                json.dumps(self._paths, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            traceback.print_exc()
            _log_exception(f"保存历史记录失败: {_HISTORY_FILE}")

    def push(self, path: str) -> None:
        path = str(Path(path).resolve())
        if path in self._paths:
            self._paths.remove(path)
        self._paths.insert(0, path)
        self._paths = self._paths[:_HISTORY_MAX]
        self._save()

    def remove(self, path: str) -> None:
        path = str(Path(path).resolve())
        if path in self._paths:
            self._paths.remove(path)
            self._save()

    def all(self) -> list[str]:
        return list(self._paths)


class _LoadThread(QThread):
    done: Signal = Signal(object, str, bool)
    error: Signal = Signal(str)

    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = path

    def run(self) -> None:
        try:
            raw = Path(self._path).read_bytes()
            used_fallback = False
            try:
                obj = pickle.loads(raw)
            except Exception:
                obj = TolerantUnpickler(io.BytesIO(raw)).load()
                used_fallback = True
            self.done.emit(obj, type(obj).__name__, used_fallback)
        except Exception as e:
            traceback.print_exc()
            _log_exception(f"加载 PKL 失败: {self._path}")
            self.error.emit(str(e))


def _type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    # For placeholder objects created during tolerant unpickling, only show
    # struct/class name to keep the type column compact.
    if isinstance(value, GenericPickleObject):
        class_name = getattr(value.__class__, "__missing_class__", type(value).__name__)
        return class_name
    return type(value).__name__


def _node_label(value: Any) -> str:
    if isinstance(value, dict):
        return f"dict  ({len(value)})"
    if isinstance(value, (list, tuple)):
        t = "list" if isinstance(value, list) else "tuple"
        return f"{t}  ({len(value)})"
    # Missing-type fallback objects: keep value column compact and readable.
    if isinstance(value, GenericPickleObject):
        return _type_label(value)
    if hasattr(value, "__fields__"):
        return f"{type(value).__name__}  ({len(value.__fields__)} fields)"
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return f"{type(value).__name__}  ({len(vars(value))} attrs)"
    s = str(value)
    return s[:120] + "..." if len(s) > 120 else s


def _is_container(value: Any) -> bool:
    if isinstance(value, (dict, list, tuple)):
        return True
    if hasattr(value, "__fields__"):
        return True
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return True
    return False


def _iter_children(value: Any):
    if hasattr(value, "__fields__"):
        for field_name in value.__fields__:
            yield field_name, getattr(value, field_name, None)
        return
    if isinstance(value, dict):
        yield from ((str(k), v) for k, v in value.items())
        return
    if isinstance(value, (list, tuple)):
        yield from ((f"[{i}]", v) for i, v in enumerate(value))
        return
    if hasattr(value, "__dict__") and not isinstance(value, type):
        yield from ((k, v) for k, v in vars(value).items())


def _child_count(value: Any) -> int:
    if hasattr(value, "__fields__"):
        return len(value.__fields__)
    if isinstance(value, (dict, list, tuple)):
        return len(value)
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return len(vars(value))
    return 0


def _add_children(parent: QTreeWidgetItem, value: Any) -> None:
    count = 0
    total = _child_count(value)
    for key, child_val in _iter_children(value):
        if count >= _MAX_CHILDREN:
            remaining = total - _MAX_CHILDREN
            more = QTreeWidgetItem(parent, ["...", f"{remaining} more items", ""])
            more.setForeground(1, QColor("#888"))
            more.setData(0, Qt.ItemDataRole.UserRole, ("__more__", value, _MAX_CHILDREN))
            break
        _make_node(parent, key, child_val)
        count += 1


def _row_matches_search(field: str, needle: str, key: str, value: Any) -> bool:
    """与树节点一致的匹配规则：指定字段时键列须等于 field；否则在三列文本中搜子串。"""
    k0 = key
    k1 = _node_label(value)
    k2 = _type_label(value)
    if field != _SEARCH_ALL_FIELDS and k0 != field:
        return False
    n = needle.strip().lower()
    if not n:
        return field != _SEARCH_ALL_FIELDS
    blob = f"{k0}\n{k1}\n{k2}".lower()
    return n in blob


def _tree_expand_lazy_placeholder(tree: QTreeWidget, item: QTreeWidgetItem) -> bool:
    """若 item 下仅有懒加载占位子节点，则展开为真实子项。返回是否执行了展开。"""
    if item.childCount() != 1:
        return False
    child = item.child(0)
    if child.data(0, Qt.ItemDataRole.UserRole) != _PLACEHOLDER:
        return False
    item.removeChild(child)
    value = item.data(0, Qt.ItemDataRole.UserRole)
    if value is not None and value != _PLACEHOLDER:
        tree.setUpdatesEnabled(False)
        _add_children(item, value)
        tree.setUpdatesEnabled(True)
    return True


def _tree_expand_one_more_chunk(tree: QTreeWidget, item: QTreeWidgetItem) -> bool:
    """展开 item 下第一个「… 还有更多」分页块。返回是否执行了展开。"""
    for i in range(item.childCount()):
        ch = item.child(i)
        meta = ch.data(0, Qt.ItemDataRole.UserRole)
        if isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__":
            _, parent_value, offset = meta
            item.removeChild(ch)
            tree.setUpdatesEnabled(False)
            count = 0
            for key, child_val in _iter_children(parent_value):
                if count < offset:
                    count += 1
                    continue
                if count >= offset + _MAX_CHILDREN:
                    total = _child_count(parent_value)
                    remaining = total - count
                    more = QTreeWidgetItem(item, ["...", f"{remaining} more items", ""])
                    more.setForeground(1, QColor("#888"))
                    more.setData(0, Qt.ItemDataRole.UserRole, ("__more__", parent_value, count))
                    break
                _make_node(item, key, child_val)
                count += 1
            tree.setUpdatesEnabled(True)
            return True
    return False


def _make_node(parent: QTreeWidgetItem | QTreeWidget, key: str, value: Any) -> QTreeWidgetItem:
    label = _node_label(value)
    tname = _type_label(value)
    node = QTreeWidgetItem(parent, [key, label, tname])
    if isinstance(value, GenericPickleObject):
        node.setForeground(1, QColor("#ff6b6b"))
        node.setForeground(2, QColor("#ff6b6b"))
    else:
        node.setForeground(2, QColor("#569cd6"))
    if _is_container(value):
        if not isinstance(value, GenericPickleObject):
            node.setForeground(1, QColor("#888"))
        node.setData(0, Qt.ItemDataRole.UserRole, value)
        placeholder = QTreeWidgetItem(node, ["", "", ""])
        placeholder.setData(0, Qt.ItemDataRole.UserRole, _PLACEHOLDER)
    else:
        if not isinstance(value, GenericPickleObject):
            node.setForeground(1, QColor("#4ec9b0"))
    return node


class _CollectFieldKeysThread(QThread):
    """后台遍历对象图，收集所有出现过的子键名（用于字段下拉框）。"""

    keys_delta = Signal(int, object)
    finished_ok = Signal(int)

    def __init__(self, root: Any, gen_at_start: int, get_tree_gen: Any) -> None:
        super().__init__()
        self._root = root
        self._gen_at_start = gen_at_start
        self._get_tree_gen = get_tree_gen

    def run(self) -> None:
        if not _is_container(self._root):
            if self._get_tree_gen() == self._gen_at_start and not self.isInterruptionRequested():
                self.finished_ok.emit(self._gen_at_start)
            return
        acc: set[str] = set()
        seen: set[int] = set()
        n = 0

        def cancelled() -> bool:
            return self.isInterruptionRequested() or self._get_tree_gen() != self._gen_at_start

        def walk(val: Any) -> None:
            nonlocal n
            if cancelled():
                return
            if not _is_container(val):
                return
            vid = id(val)
            if vid in seen:
                return
            seen.add(vid)
            for key, child in _iter_children(val):
                if cancelled():
                    return
                acc.add(key)
                n += 1
                if n % _KEYS_COLLECT_YIELD_NODES == 0:
                    self.keys_delta.emit(self._gen_at_start, set(acc))
                    acc.clear()
                if _is_container(child):
                    walk(child)

        try:
            walk(self._root)
            if acc and not cancelled():
                self.keys_delta.emit(self._gen_at_start, set(acc))
            if not cancelled():
                self.finished_ok.emit(self._gen_at_start)
        except Exception:
            traceback.print_exc()
            _log_exception("后台收集字段名失败")


class _DeepSearchThread(QThread):
    """后台深度遍历对象图，按字段名 + 子串规则收集匹配节点的路径。"""

    paths_batch = Signal(int, object)
    finished_ok = Signal(int, int)
    failed = Signal(int, str)

    def __init__(
        self,
        root: Any,
        field: str,
        needle: str,
        job_gen: int,
        get_job_gen: Any,
    ) -> None:
        super().__init__()
        self._root = root
        self._field = field
        self._needle = needle
        self._job_gen_at_start = job_gen
        self._get_job_gen = get_job_gen

    def run(self) -> None:
        if not _is_container(self._root):
            if self._get_job_gen() == self._job_gen_at_start and not self.isInterruptionRequested():
                self.finished_ok.emit(self._job_gen_at_start, 0)
            return
        batch: list[tuple[str, ...]] = []
        total = 0
        nodes = 0
        seen: set[int] = set()

        def cancelled() -> bool:
            return self.isInterruptionRequested() or self._get_job_gen() != self._job_gen_at_start

        def flush() -> None:
            nonlocal batch
            if batch and not cancelled():
                self.paths_batch.emit(self._job_gen_at_start, list(batch))
            batch = []

        def walk(val: Any, path: tuple[str, ...]) -> None:
            nonlocal total, nodes, batch
            if cancelled():
                return
            if not _is_container(val):
                return
            vid = id(val)
            if vid in seen:
                return
            seen.add(vid)
            for key, child in _iter_children(val):
                if cancelled():
                    return
                nodes += 1
                if nodes % _DEEP_SEARCH_YIELD_NODES == 0:
                    flush()
                sub = path + (key,)
                if _row_matches_search(self._field, self._needle, key, child):
                    batch.append(sub)
                    total += 1
                    if len(batch) >= _DEEP_SEARCH_BATCH:
                        flush()
                if _is_container(child):
                    walk(child, sub)

        try:
            walk(self._root, ("root",))
            flush()
            if not cancelled():
                self.finished_ok.emit(self._job_gen_at_start, total)
        except Exception as e:
            traceback.print_exc()
            self.failed.emit(self._job_gen_at_start, str(e))


class _HistoryRow(QWidget):
    """一行历史记录。

    QLabel 的 setWordWrap 只在「词边界」折行，无空格的长文件名不会换行。
    使用只读 QTextEdit + QTextOption.WrapAnywhere，可在任意字符处折行。
    """

    def __init__(self, path: str, viewer: "PklViewer") -> None:
        super().__init__()
        self._viewer = viewer
        self._path = path
        layout = QVBoxLayout(self)
        layout.setContentsMargins(2, 1, 2, 1)
        layout.setSpacing(0)

        self._text = QTextEdit()
        self._text.setReadOnly(True)
        self._text.setFrameShape(QFrame.Shape.NoFrame)
        self._text.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._text.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._text.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        opt = QTextOption()
        opt.setWrapMode(QTextOption.WrapMode.WrapAnywhere)
        self._text.document().setDefaultTextOption(opt)
        self._text.setPlainText(Path(path).name)
        self._text.document().setDocumentMargin(0)
        self._text.setViewportMargins(0, 0, 0, 0)
        self._text.setStyleSheet("padding: 0px; margin: 0px; border: none;")
        cur = QTextCursor(self._text.document())
        cur.select(QTextCursor.SelectionType.Document)
        bf = QTextBlockFormat()
        bf.setTopMargin(0)
        bf.setBottomMargin(0)
        cur.mergeBlockFormat(bf)
        self._text.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._text.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum
        )
        # 点击交给整行处理（选中 / 双击打开），避免 QTextEdit 抢走鼠标
        self._text.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(self._text, 0)

        # 文件大小 + 修改时间副标题
        self._meta_label = QLabel(self._build_meta(path))
        self._meta_label.setStyleSheet(
            "color: #888; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
        )
        self._meta_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(self._meta_label, 0)

        self.setToolTip(path)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        QTimer.singleShot(0, self._sync_text_height)

    def path(self) -> str:
        return self._path

    def _sync_text_height(self) -> None:
        w = self._text.width()
        if w <= 0:
            w = max(self.width() - 12, 40)
        self._text.document().setTextWidth(float(max(w - 2, 40)))
        doc_h = math.ceil(self._text.document().size().height())
        # 文档高度常含额外行距，略压一点避免 item 之间空白过大
        self._text.setFixedHeight(max(doc_h + 2, 20))

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._sync_text_height()

    @staticmethod
    def _build_meta(path: str) -> str:
        """返回「文件大小 + 修改时间」副标题；文件不存在时给出提示。"""
        p = Path(path)
        if not p.exists():
            return "（文件不存在或已移动）"
        try:
            stat = p.stat()
            size = stat.st_size
            if size < 1024:
                size_str = f"{size} B"
            elif size < 1024 * 1024:
                size_str = f"{size / 1024:.1f} KB"
            else:
                size_str = f"{size / 1024 / 1024:.1f} MB"
            mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
            mtime_str = mtime.strftime("%Y-%m-%d %H:%M:%S")
            return f"大小 {size_str}  ·  修改时间 {mtime_str}"
        except OSError:
            return "（无法读取文件信息）"

    def apply_style(self, *, selected: bool, missing: bool) -> None:
        PklViewer._apply_history_text_style(self._text, selected=selected, missing=missing)
        if missing:
            self._meta_label.setStyleSheet(
                "color: #555; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
            )
        elif selected:
            self._meta_label.setStyleSheet(
                "color: #aaa; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
            )
        else:
            self._meta_label.setStyleSheet(
                "color: #888; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
            )

    def sync_font_from_list_container(self) -> None:
        parent = self.parentWidget()
        if parent is not None:
            self._text.setFont(parent.font())
            self._sync_text_height()

    def refresh_meta(self) -> None:
        """重新读取磁盘上的大小与修改时间并更新副标题。"""
        self._meta_label.setText(self._build_meta(self._path))
        missing = not Path(self._path).exists()
        selected = self._viewer._history_selected_row is self
        self.apply_style(selected=selected, missing=missing)

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        self._viewer._select_history_row(self)
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:  # type: ignore[override]
        self._viewer._select_history_row(self)
        self._viewer._on_history_path_open(self._path)
        super().mouseDoubleClickEvent(event)


class PklViewer(QMainWindow):
    def __init__(self, initial_file: str | None = None) -> None:
        super().__init__()
        self.setWindowTitle("PKL Viewer")
        self.resize(1300, 700)
        self._current_obj: Any = None
        self._load_thread: _LoadThread | None = None
        self._dot_timer = QTimer(self)
        self._dot_timer.setInterval(400)
        self._dot_timer.timeout.connect(self._on_dot_tick)
        self._dot_count = 0
        self._loading_path = ""
        self._history = _History()
        self._history_selected_row: _HistoryRow | None = None
        self._search_matches: list[QTreeWidgetItem] = []
        self._search_index: int = 0
        self._tree_async_gen: int = 0
        self._search_job_gen: int = 0
        self._keys_thread: _CollectFieldKeysThread | None = None
        self._deep_search_thread: _DeepSearchThread | None = None
        self._all_field_keys_from_obj: set[str] = set()
        self._search_default_name_pending: bool = True
        self._search_combo_user_picked: bool = False
        self._deep_search_running: bool = False
        self._search_first_nav_pending: bool = False
        self._rebuild_combo_timer = QTimer(self)
        self._rebuild_combo_timer.setSingleShot(True)
        self._rebuild_combo_timer.setInterval(200)
        self._rebuild_combo_timer.timeout.connect(self._on_rebuild_combo_timeout)
        self._load_ui()
        self._refresh_history_list()
        if initial_file and Path(initial_file).exists():
            self._load_file(initial_file)

    @staticmethod
    def _apply_history_text_style(editor: QTextEdit, *, selected: bool, missing: bool) -> None:
        if missing:
            editor.setStyleSheet(
                "color: #666; background: transparent; border: none; padding: 0px; margin: 0px;"
            )
            return
        if selected:
            editor.setStyleSheet(
                "color: #d4d4d4; background: #37373d; border: none; padding: 0px; margin: 0px;"
            )
        else:
            editor.setStyleSheet(
                "color: #d4d4d4; background: transparent; border: none; padding: 0px; margin: 0px;"
            )

    def _select_history_row(self, row: _HistoryRow | None) -> None:
        self._history_selected_row = row
        layout = self._history_inner.layout()
        if layout is None:
            return
        for i in range(layout.count()):
            item = layout.itemAt(i)
            w = item.widget() if item is not None else None
            if isinstance(w, _HistoryRow):
                missing = not Path(w.path()).exists()
                w.apply_style(selected=w is row, missing=missing)

    @staticmethod
    def _history_row_from_descendant(w: QWidget | None) -> _HistoryRow | None:
        while w is not None:
            if isinstance(w, _HistoryRow):
                return w
            w = w.parentWidget()
        return None

    def _load_ui(self) -> None:
        """从 view_pkl_tool.ui 加载界面；布局与样式在 .ui 中维护。"""
        ui_path = Path(__file__).resolve().parent / "view_pkl_tool.ui"
        if not ui_path.is_file():
            raise RuntimeError(f"UI 文件不存在: {ui_path}")
        raw = QByteArray(ui_path.read_bytes())
        buf = QBuffer()
        buf.setData(raw)
        if not buf.open(QIODevice.OpenModeFlag.ReadOnly):
            raise RuntimeError(f"无法读取 UI 内容: {ui_path}")
        loader = QUiLoader()
        try:
            central = loader.load(buf, None)
        finally:
            buf.close()
        if central is None:
            raise RuntimeError(
                f"加载 UI 失败: {ui_path} — {loader.errorString()}"
            )
        self.setCentralWidget(central)
        _apply_ui_layout_after_load(central)

        self._path_label = _require_ui_child(central, QLabel, "pathLabel")
        _require_ui_child(central, QPushButton, "openButton").clicked.connect(self._on_open)
        _require_ui_child(central, QPushButton, "reloadButton").clicked.connect(
            self._on_reload
        )
        _require_ui_child(central, QPushButton, "refreshMetaButton").clicked.connect(
            self._on_refresh_log_and_file_meta
        )
        _require_ui_child(central, QPushButton, "removeFromListButton").clicked.connect(
            self._on_history_remove
        )

        self._main_tabs = _require_ui_child(central, QTabWidget, "mainTabs")
        self._tree = _require_ui_child(central, QTreeWidget, "treeWidget")
        self._detail = _require_ui_child(central, QTextEdit, "detailText")
        self._history_scroll = _require_ui_child(central, QScrollArea, "historyScroll")
        self._history_inner = _require_ui_child(central, QWidget, "historyInner")
        inner_layout = self._history_inner.layout()
        if not isinstance(inner_layout, QVBoxLayout):
            raise RuntimeError("UI: historyInner 应为 QVBoxLayout")
        self._history_layout = inner_layout
        self._history_inner.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )
        self._history_inner.customContextMenuRequested.connect(
            self._on_history_context_menu
        )

        self._search_field_combo = _require_ui_child(
            central, QComboBox, "searchFieldCombo"
        )
        self._search_field_combo.activated.connect(self._on_search_field_user_activated)
        self._search_line = _require_ui_child(central, QLineEdit, "searchLine")
        self._search_line.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Fixed,
        )
        self._search_line.returnPressed.connect(self._on_tree_search_next)
        self._search_btn = _require_ui_child(central, QPushButton, "searchButton")
        self._search_btn.clicked.connect(self._on_tree_search)
        self._search_next_btn = _require_ui_child(
            central, QPushButton, "searchNextButton"
        )
        self._search_next_btn.clicked.connect(self._on_tree_search_next)

        self._log_view = _require_ui_child(central, QTextEdit, "logView")
        log_hint = _require_ui_child(central, QLabel, "logHintLabel")
        log_hint.setText(f"日志文件：{_LOG_FILE}")

        hdr = self._tree.header()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._tree.itemExpanded.connect(self._on_item_expanded)
        self._tree.currentItemChanged.connect(self._on_item_selected)
        self._detail.setFont(QFont("Consolas", 12))
        self._log_view.setFont(QFont("Consolas", 10))

        self._reload_app_log_view()

        self._status = QStatusBar()
        self.setStatusBar(self._status)
        self.setStyleSheet("QMainWindow { background: #252526; }")

    def _stop_keys_thread(self) -> None:
        t = self._keys_thread
        if t is None:
            return
        if t.isRunning():
            t.requestInterruption()
            t.wait(500)
        self._keys_thread = None

    def _stop_deep_search_thread(self) -> None:
        t = self._deep_search_thread
        if t is None:
            return
        if t.isRunning():
            t.requestInterruption()
            t.wait(500)
        self._deep_search_thread = None

    @Slot(int)
    def _on_search_field_user_activated(self, _index: int) -> None:
        self._search_combo_user_picked = True

    def _maybe_select_default_search_field(self) -> None:
        if self._search_combo_user_picked or not self._search_default_name_pending:
            return
        combo = self._search_field_combo
        idx = combo.findText(_SEARCH_DEFAULT_FIELD, Qt.MatchFlag.MatchExactly)
        if idx < 0:
            return
        combo.blockSignals(True)
        combo.setCurrentIndex(idx)
        combo.blockSignals(False)
        self._search_default_name_pending = False

    def _start_collect_keys_thread(self, obj: Any) -> None:
        self._stop_keys_thread()
        if not _is_container(obj):
            self._schedule_rebuild_search_field_combo()
            self._maybe_select_default_search_field()
            return
        gen = self._tree_async_gen
        th = _CollectFieldKeysThread(obj, gen, lambda: self._tree_async_gen)
        self._keys_thread = th
        th.keys_delta.connect(self._on_field_keys_delta)
        th.finished_ok.connect(self._on_field_keys_finished)
        th.start()

    @Slot(int, object)
    def _on_field_keys_delta(self, gen: int, delta: object) -> None:
        if gen != self._tree_async_gen or not isinstance(delta, set):
            return
        self._all_field_keys_from_obj.update(str(x) for x in delta)
        # 下拉框仅由后台键收集驱动更新；不扫描树节点
        self._schedule_rebuild_search_field_combo()

    @Slot(int)
    def _on_field_keys_finished(self, gen: int) -> None:
        if gen != self._tree_async_gen:
            return
        self._schedule_rebuild_search_field_combo()
        self._maybe_select_default_search_field()

    def _schedule_rebuild_search_field_combo(self) -> None:
        """合并多次触发（展开/后台收集）后再重建，避免频繁全树扫描卡 UI。"""
        self._rebuild_combo_timer.start()

    def _on_rebuild_combo_timeout(self) -> None:
        self._rebuild_search_field_combo()

    def _find_direct_child_for_search(
        self, parent: QTreeWidgetItem, key: str
    ) -> QTreeWidgetItem | None:
        tree = self._tree
        _tree_expand_lazy_placeholder(tree, parent)
        while True:
            for i in range(parent.childCount()):
                ch = parent.child(i)
                if ch.data(0, Qt.ItemDataRole.UserRole) == _PLACEHOLDER:
                    continue
                meta = ch.data(0, Qt.ItemDataRole.UserRole)
                if isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__":
                    continue
                if ch.text(0) == key:
                    return ch
            if not _tree_expand_one_more_chunk(tree, parent):
                return None

    def _find_tree_item_for_path(self, path: tuple[str, ...]) -> QTreeWidgetItem | None:
        tree = self._tree
        if tree.topLevelItemCount() == 0:
            return None
        item = tree.topLevelItem(0)
        if item is None:
            return None
        if not path or item.text(0) != path[0]:
            return None
        for seg in path[1:]:
            nxt = self._find_direct_child_for_search(item, seg)
            if nxt is None:
                return None
            item = nxt
        return item

    def _rebuild_search_field_combo(self) -> None:
        """用后台收集到的键名重建字段下拉框（不扫描树，避免展开卡顿）。"""
        cur = self._search_field_combo.currentText()
        self._search_field_combo.blockSignals(True)
        self._search_field_combo.clear()
        self._search_field_combo.addItem(_SEARCH_ALL_FIELDS)
        keys: set[str] = set(self._all_field_keys_from_obj)
        for k in sorted(keys, key=lambda s: (s.lower(), s)):
            self._search_field_combo.addItem(k)
        self._search_field_combo.blockSignals(False)
        idx = self._search_field_combo.findText(cur, Qt.MatchFlag.MatchExactly)
        if idx >= 0:
            self._search_field_combo.setCurrentIndex(idx)
        elif not self._search_combo_user_picked:
            self._maybe_select_default_search_field()

    def _tree_item_matches(self, field: str, needle: str, item: QTreeWidgetItem) -> bool:
        """与后台深度搜索使用同一套规则（有绑定值时用对象算标签）。"""
        meta = item.data(0, Qt.ItemDataRole.UserRole)
        if meta == _PLACEHOLDER:
            return False
        if isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__":
            return False
        k0 = item.text(0)
        k1 = item.text(1)
        k2 = item.text(2)
        if k0.strip() == "" and k1.strip() == "" and k2.strip() == "":
            return False
        if meta is not None and meta != _PLACEHOLDER:
            return _row_matches_search(field, needle, k0, meta)
        if field != _SEARCH_ALL_FIELDS and k0 != field:
            return False
        n = needle.strip().lower()
        if not n:
            return field != _SEARCH_ALL_FIELDS
        blob = f"{k0}\n{k1}\n{k2}".lower()
        return n in blob

    def _ensure_tree_item_visible(self, item: QTreeWidgetItem) -> None:
        chain: list[QTreeWidgetItem] = []
        p = item.parent()
        while p is not None:
            chain.append(p)
            p = p.parent()
        for x in reversed(chain):
            x.setExpanded(True)
        self._tree.scrollToItem(item)
        self._tree.setCurrentItem(item)

    def _goto_search_match(self, index: int) -> None:
        if not self._search_matches:
            return
        index %= len(self._search_matches)
        self._search_index = index
        self._ensure_tree_item_visible(self._search_matches[index])

    def _on_tree_search(self) -> None:
        if self._current_obj is None:
            self._status.showMessage("请先加载 PKL 文件", 3000)
            return
        field = self._search_field_combo.currentText()
        needle = self._search_line.text()
        if field == _SEARCH_ALL_FIELDS and not needle.strip():
            self._status.showMessage("选择「全部字段」时请至少输入一个搜索子串", 3500)
            return

        self._stop_deep_search_thread()
        self._search_job_gen += 1
        job_gen = self._search_job_gen
        self._search_matches.clear()
        self._search_index = 0
        self._search_first_nav_pending = True

        th = _DeepSearchThread(
            self._current_obj,
            field,
            needle,
            job_gen,
            lambda: self._search_job_gen,
        )
        self._deep_search_thread = th
        self._deep_search_running = True
        th.paths_batch.connect(self._on_deep_search_paths_batch)
        th.finished_ok.connect(self._on_deep_search_finished)
        th.failed.connect(self._on_deep_search_failed)
        th.start()
        self._status.showMessage("深度搜索中（后台遍历整棵对象）…", 0)

    def _on_tree_search_next(self) -> None:
        if self._search_matches:
            self._search_index = (self._search_index + 1) % len(self._search_matches)
            self._goto_search_match(self._search_index)
            return
        if self._deep_search_running:
            self._status.showMessage("深度搜索尚未完成，请稍候再试「下一个」", 2500)
            return
        self._on_tree_search()

    @Slot(int, object)
    def _on_deep_search_paths_batch(self, gen: int, paths: object) -> None:
        if gen != self._search_job_gen or not isinstance(paths, list):
            return
        resolved: list[QTreeWidgetItem] = []
        for p in paths:
            if not isinstance(p, tuple):
                continue
            path = tuple(str(x) for x in p)
            it = self._find_tree_item_for_path(path)
            if it is not None:
                resolved.append(it)
        if not resolved:
            return
        self._search_matches.extend(resolved)
        if self._search_first_nav_pending:
            self._search_first_nav_pending = False
            self._goto_search_match(0)
        n = len(self._search_matches)
        self._status.showMessage(f"深度搜索中… 已定位 {n} 项（后台继续）", 1500)

    @Slot(int, int)
    def _on_deep_search_finished(self, gen: int, total: int) -> None:
        if gen != self._search_job_gen:
            return
        self._deep_search_running = False
        if total == 0:
            self._status.showMessage("未找到匹配项", 4000)
            return
        located = len(self._search_matches)
        if located == 0:
            self._status.showMessage(
                f"对象中共 {total} 处匹配，但无法在树中定位到节点", 6000
            )
            return
        if located < total:
            self._status.showMessage(
                f"深度搜索完成：共 {total} 处匹配，已定位 {located} 项；回车或「下一个」跳转",
                5000,
            )
            return
        self._status.showMessage(
            f"深度搜索完成，共 {total} 项；回车或「下一个」在已定位项间跳转", 5000
        )

    @Slot(int, str)
    def _on_deep_search_failed(self, gen: int, msg: str) -> None:
        if gen != self._search_job_gen:
            return
        self._deep_search_running = False
        self._status.showMessage(f"深度搜索失败: {msg}", 6000)
        print(f"[PKL Viewer] 深度搜索失败: {msg}", file=sys.stderr, flush=True)

    def _reload_app_log_view(self) -> None:
        """从磁盘加载 pkl_viewer 日志到「日志」标签页（过大则只读尾部）。"""
        max_tail = 512_000
        try:
            if not _LOG_FILE.exists():
                self._log_view.setPlainText("（尚无日志文件）")
                return
            size = _LOG_FILE.stat().st_size
            with _LOG_FILE.open("rb") as f:
                if size > max_tail:
                    f.seek(-max_tail, 2)
                    raw = f.read()
                    nl = raw.find(b"\n")
                    raw = raw[nl + 1 :] if nl >= 0 else raw
                    head = f"...（日志较大，仅显示末尾约 {max_tail // 1024} KB）...\n\n"
                else:
                    raw = f.read()
                    head = ""
            self._log_view.setPlainText(head + raw.decode("utf-8", errors="replace"))
            cur = self._log_view.textCursor()
            cur.movePosition(QTextCursor.MoveOperation.End)
            self._log_view.setTextCursor(cur)
        except OSError as e:
            self._log_view.setPlainText(f"读取日志失败: {e}")

    def _refresh_history_rows_meta(self) -> None:
        layout = self._history_layout
        for i in range(layout.count()):
            item = layout.itemAt(i)
            w = item.widget() if item is not None else None
            if isinstance(w, _HistoryRow):
                w.refresh_meta()

    def _on_refresh_log_and_file_meta(self) -> None:
        self._reload_app_log_view()
        self._refresh_history_rows_meta()
        self._status.showMessage("已刷新运行日志与「最近打开」中的文件大小、修改时间", 3000)

    def _refresh_history_list(self) -> None:
        self._history_selected_row = None
        layout = self._history_layout
        while layout.count():
            li = layout.takeAt(0)
            if li is None:
                continue
            w = li.widget()
            if w is not None:
                w.deleteLater()
            del li
        for path in self._history.all():
            row = _HistoryRow(path, self)
            missing = not Path(path).exists()
            row.apply_style(selected=False, missing=missing)
            layout.addWidget(row)
            row.sync_font_from_list_container()
        layout.addStretch(1)

    def _on_history_path_open(self, path: str) -> None:
        if Path(path).exists():
            self._load_file(path)
        else:
            self._status.showMessage(f"文件不存在: {path}", 4000)

    def _on_history_remove(self) -> None:
        if self._history_selected_row is None:
            self._status.showMessage("请先在列表中点选一项", 2000)
            return
        self._history.remove(self._history_selected_row.path())
        self._refresh_history_list()

    def _on_history_context_menu(self, pos) -> None:
        row = self._history_row_from_descendant(self._history_inner.childAt(pos))
        if row is None:
            return
        self._select_history_row(row)
        menu = QMenu(self)
        menu.setStyleSheet(
            "QMenu { background: #2d2d2d; color: #d4d4d4; }"
            " QMenu::item:selected { background: #264f78; }"
        )
        open_act = menu.addAction("打开")
        reopen_select_act = menu.addAction("重新打开选择文件...")
        open_folder_act = menu.addAction("打开所在文件夹")
        remove_act = menu.addAction("从列表移除")
        action = menu.exec(self._history_inner.mapToGlobal(pos))
        if action == open_act:
            self._on_history_path_open(row.path())
        elif action == reopen_select_act:
            self._on_reopen_select_file(row.path())
        elif action == open_folder_act:
            self._open_folder(row.path())
        elif action == remove_act:
            self._history.remove(row.path())
            self._refresh_history_list()

    def _on_reopen_select_file(self, selected_path: str) -> None:
        initial_dir = ""
        if selected_path:
            selected = Path(selected_path)
            initial_dir = str(selected.parent)
        path, _ = QFileDialog.getOpenFileName(
            self,
            "重新选择 PKL 文件",
            initial_dir,
            "Pickle 文件 (*.pkl);;所有文件 (*)",
        )
        if path:
            self._load_file(path)

    def _open_folder(self, path: str) -> None:
        """在资源管理器中打开并选中该文件；文件不存在则打开其所在目录。"""
        p = Path(path)
        parent = p.parent
        try:
            if sys.platform.startswith("win"):
                if p.is_file():
                    subprocess.Popen(
                        ["explorer", "/select,", str(p.resolve())],
                        close_fds=True,
                    )
                elif p.is_dir():
                    subprocess.Popen(
                        ["explorer", str(p.resolve())],
                        close_fds=True,
                    )
                elif parent.is_dir():
                    subprocess.Popen(
                        ["explorer", str(parent.resolve())],
                        close_fds=True,
                    )
                else:
                    self._status.showMessage("无法打开：路径无效", 3000)
            elif sys.platform == "darwin":
                if p.is_file():
                    subprocess.Popen(["open", "-R", str(p)], close_fds=True)
                elif p.is_dir():
                    subprocess.Popen(["open", str(p)], close_fds=True)
                elif parent.is_dir():
                    subprocess.Popen(["open", str(parent)], close_fds=True)
                else:
                    self._status.showMessage("无法打开：路径无效", 3000)
            else:
                if p.is_dir():
                    subprocess.Popen(["xdg-open", str(p.resolve())], close_fds=True)
                elif parent.is_dir():
                    subprocess.Popen(["xdg-open", str(parent.resolve())], close_fds=True)
                else:
                    self._status.showMessage("无法打开：路径无效", 3000)
        except OSError as e:
            traceback.print_exc()
            _log_exception(f"打开所在文件夹失败: {path}")
            self._status.showMessage(f"打开文件夹失败: {e}", 4000)

    def _on_open(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "选择 PKL 文件", "", "Pickle 文件 (*.pkl);;所有文件 (*)"
        )
        if path:
            self._load_file(path)

    def _on_reload(self) -> None:
        path = self._path_label.toolTip()
        if path:
            self._load_file(path)

    def _load_file(self, path: str) -> None:
        if self._load_thread and self._load_thread.isRunning():
            self._load_thread.quit()
            self._load_thread.wait(500)

        self._stop_deep_search_thread()
        self._stop_keys_thread()

        self._history.push(path)
        self._refresh_history_list()

        self._loading_path = path
        self._dot_count = 0
        self._dot_timer.start()
        self._status.showMessage(f"加载中: {Path(path).name} .", 0)
        self._tree.clear()
        self._detail.clear()

        thread = _LoadThread(path)
        thread.done.connect(lambda obj, t, fallback: self._on_loaded(obj, t, path, fallback))
        thread.error.connect(self._on_load_error)
        thread.start()
        self._load_thread = thread

    @Slot()
    def _on_dot_tick(self) -> None:
        self._dot_count = (self._dot_count + 1) % 4
        dots = "." * (self._dot_count + 1)
        self._status.showMessage(f"加载中: {Path(self._loading_path).name} {dots}", 0)

    @Slot(str)
    def _on_load_error(self, msg: str) -> None:
        self._dot_timer.stop()
        print(f"[PKL Viewer] 加载失败: {msg}", file=sys.stderr, flush=True)
        self._status.clearMessage()

    def _on_loaded(self, obj: Any, type_name: str, path: str, used_fallback: bool) -> None:
        self._dot_timer.stop()
        self._current_obj = obj
        self._path_label.setText(Path(path).name)
        self._path_label.setToolTip(path)
        self._status.showMessage("构建树中...", 0)
        QApplication.processEvents()
        self._refresh_tree(obj)
        suffix = " | 已使用兼容模式(缺失类已降级)" if used_fallback else ""
        self._status.showMessage(f"已加载: {path}  |  类型: {type_name}{suffix}", 0)

    def _refresh_tree(self, obj: Any) -> None:
        self._stop_deep_search_thread()
        self._stop_keys_thread()
        self._tree_async_gen += 1
        self._search_job_gen += 1
        self._deep_search_running = False
        self._all_field_keys_from_obj.clear()
        self._search_combo_user_picked = False
        self._search_default_name_pending = True

        self._tree.setUpdatesEnabled(False)
        self._tree.clear()
        _make_node(self._tree, "root", obj)
        self._tree.setUpdatesEnabled(True)
        if self._tree.topLevelItemCount() > 0:
            root_item = self._tree.topLevelItem(0)
            if root_item is not None:
                root_item.setExpanded(True)
        self._search_matches = []
        self._search_index = 0
        # 字段名下拉框仅由后台收集线程更新，这里不做重建/扫描
        self._maybe_select_default_search_field()
        self._start_collect_keys_thread(obj)

    def _on_item_expanded(self, item: QTreeWidgetItem) -> None:
        if _tree_expand_lazy_placeholder(self._tree, item):
            return
        if _tree_expand_one_more_chunk(self._tree, item):
            return

    def _on_item_selected(self, current: QTreeWidgetItem | None, _prev: QTreeWidgetItem | None) -> None:
        if current is None:
            return
        value = current.data(0, Qt.ItemDataRole.UserRole)
        if value is None or value == _PLACEHOLDER:
            self._detail.setPlainText(current.text(1))
            return
        if isinstance(value, tuple) and len(value) == 3 and value[0] == "__more__":
            self._detail.setPlainText("(more items placeholder)")
            return
        try:
            shallow: dict = {}
            for k, v in _iter_children(value):
                shallow[k] = {"value": _node_label(v), "type": _type_label(v)}
            text = json.dumps(shallow, ensure_ascii=False, indent=2, default=str)
            if len(text) > _DETAIL_MAX_CHARS:
                text = text[:_DETAIL_MAX_CHARS] + f"\n\n... (truncated, total {len(text)} chars)"
            self._detail.setPlainText(text)
        except Exception as e:
            traceback.print_exc()
            _log_exception("序列化详情视图失败")
            self._detail.setPlainText(f"序列化失败: {e}\n\n{current.text(1)}")


def _resolve_icon(app: "QApplication") -> QIcon:
    from PySide6.QtWidgets import QStyle
    candidates = [
        Path(__file__).resolve().parent / "icons" / "view_pkl_tool.ico",
        Path(__file__).resolve().parent / "icons" / "view_pkl_tool.png",
        Path(__file__).resolve().parent / "icons" / "view_pkl_tool.svg",
        Path(__file__).resolve().parent / "view_pkl_tool.ico",
        Path(__file__).resolve().parent / "view_pkl_tool.png",
        Path(__file__).resolve().parent / "view_pkl_tool.svg",
    ]
    for c in candidates:
        if c.exists():
            icon = QIcon(str(c))
            if not icon.isNull():
                return icon
    return app.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)


def main() -> None:
    if sys.platform.startswith("win"):
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                "Lugwit.view_pkl_tool"
            )
        except Exception:
            pass
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    icon = _resolve_icon(app)
    app.setWindowIcon(icon)
    initial = sys.argv[1] if len(sys.argv) > 1 else None
    win = PklViewer(initial)
    win.setWindowIcon(icon)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

