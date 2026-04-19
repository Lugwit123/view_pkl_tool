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
from typing import Any

from PySide6.QtCore import QThread, QTimer, Qt, Signal, Slot
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
    QFileDialog,
    QFrame,
    QHeaderView,
    QHBoxLayout,
    QLabel,
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

_MAX_CHILDREN = 200
_DETAIL_MAX_CHARS = 50_000
_PLACEHOLDER = "__placeholder__"
_HISTORY_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer_history.json"
_HISTORY_MAX = 20
_LOG_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer.log"

_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


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
        self._build_ui()
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

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(8, 8, 8, 4)
        root_layout.setSpacing(6)

        toolbar = QHBoxLayout()
        self._path_label = QLabel("未加载文件")
        self._path_label.setStyleSheet("color: #aaa; font-size: 12px;")
        open_btn = QPushButton("打开 PKL...")
        open_btn.setFixedWidth(100)
        open_btn.clicked.connect(self._on_open)
        reload_btn = QPushButton("重新加载")
        reload_btn.setFixedWidth(80)
        reload_btn.clicked.connect(self._on_reload)
        toolbar.addWidget(open_btn)
        toolbar.addWidget(reload_btn)
        toolbar.addWidget(self._path_label, stretch=1)
        root_layout.addLayout(toolbar)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        history_widget = QWidget()
        history_layout = QVBoxLayout(history_widget)
        history_layout.setContentsMargins(0, 0, 0, 0)
        history_layout.setSpacing(3)
        history_label = QLabel("最近打开")
        history_label.setStyleSheet("color: #888; font-size: 11px; padding: 2px 4px;")
        self._history_scroll = QScrollArea()
        self._history_scroll.setWidgetResizable(True)
        self._history_scroll.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )
        self._history_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._history_scroll.setStyleSheet(
            "QScrollArea { background: #252526; border: none; }"
        )
        self._history_inner = QWidget()
        self._history_inner.setStyleSheet("background: #252526;")
        self._history_layout = QVBoxLayout(self._history_inner)
        self._history_layout.setContentsMargins(1, 1, 1, 1)
        self._history_layout.setSpacing(0)
        self._history_scroll.setWidget(self._history_inner)
        self._history_inner.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._history_inner.customContextMenuRequested.connect(self._on_history_context_menu)
        remove_btn = QPushButton("从列表移除")
        remove_btn.setStyleSheet("font-size: 11px; padding: 3px;")
        remove_btn.clicked.connect(self._on_history_remove)
        history_layout.addWidget(history_label)
        history_layout.addWidget(self._history_scroll, stretch=1)
        history_layout.addWidget(remove_btn)
        splitter.addWidget(history_widget)

        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["键", "值", "类型"])
        header = self._tree.header()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._tree.setAlternatingRowColors(True)
        self._tree.setUniformRowHeights(True)
        self._tree.setStyleSheet(
            """
            QTreeWidget { background: #1e1e1e; color: #d4d4d4; border: none; font-size: 13px; }
            QTreeWidget::item:selected { background: #264f78; }
            QHeaderView::section { background: #2d2d2d; color: #ccc; padding: 4px; border: none; }
        """
        )
        self._tree.itemExpanded.connect(self._on_item_expanded)
        self._tree.currentItemChanged.connect(self._on_item_selected)
        splitter.addWidget(self._tree)

        self._detail = QTextEdit()
        self._detail.setReadOnly(True)
        self._detail.setFont(QFont("Consolas", 12))
        self._detail.setStyleSheet(
            "background: #1e1e1e; color: #ce9178; border: none; padding: 6px;"
        )
        splitter.addWidget(self._detail)
        # Keep detail pane narrower by default to prioritize tree browsing.
        splitter.setSizes([220, 680, 360])

        browse_page = QWidget()
        browse_layout = QVBoxLayout(browse_page)
        browse_layout.setContentsMargins(0, 0, 0, 0)
        browse_layout.setSpacing(4)
        browse_refresh_row = QHBoxLayout()
        refresh_info_btn = QPushButton("刷新")
        refresh_info_btn.setFixedWidth(72)
        refresh_info_btn.setToolTip(
            "重新读取运行日志，并更新「最近打开」中的文件大小与修改时间"
        )
        refresh_info_btn.clicked.connect(self._on_refresh_log_and_file_meta)
        browse_refresh_row.addWidget(refresh_info_btn)
        browse_refresh_row.addStretch(1)
        browse_layout.addLayout(browse_refresh_row)
        browse_layout.addWidget(splitter)

        log_page = QWidget()
        log_layout = QVBoxLayout(log_page)
        log_layout.setContentsMargins(0, 4, 0, 0)
        log_layout.setSpacing(4)
        log_hint = QLabel(f"日志文件：{_LOG_FILE}")
        log_hint.setStyleSheet("color: #666; font-size: 10px; padding: 2px 4px;")
        log_hint.setWordWrap(True)
        self._log_view = QTextEdit()
        self._log_view.setReadOnly(True)
        self._log_view.setFont(QFont("Consolas", 10))
        self._log_view.setStyleSheet(
            "background: #1e1e1e; color: #b5cea8; border: none; padding: 6px;"
        )
        log_layout.addWidget(log_hint)
        log_layout.addWidget(self._log_view, stretch=1)

        self._main_tabs = QTabWidget()
        self._main_tabs.setDocumentMode(True)
        self._main_tabs.setStyleSheet(
            """
            QTabWidget::pane { border: none; top: -1px; }
            QTabBar::tab { background: #2d2d2d; color: #aaa; padding: 6px 16px; margin-right: 2px; }
            QTabBar::tab:selected { background: #1e1e1e; color: #e0e0e0; }
            QTabBar::tab:hover:!selected { background: #353535; color: #ccc; }
        """
        )
        self._main_tabs.addTab(browse_page, "查看")
        self._main_tabs.addTab(log_page, "日志")
        root_layout.addWidget(self._main_tabs, stretch=1)

        self._reload_app_log_view()

        self._status = QStatusBar()
        self.setStatusBar(self._status)
        self.setStyleSheet("QMainWindow { background: #252526; }")

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
            w = li.widget()
            if w is not None:
                w.deleteLater()
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
        self._tree.setUpdatesEnabled(False)
        self._tree.clear()
        _make_node(self._tree, "root", obj)
        self._tree.setUpdatesEnabled(True)
        if self._tree.topLevelItemCount() > 0:
            root_item = self._tree.topLevelItem(0)
            if root_item is not None:
                root_item.setExpanded(True)

    def _on_item_expanded(self, item: QTreeWidgetItem) -> None:
        if item.childCount() == 1:
            child = item.child(0)
            if child.data(0, Qt.ItemDataRole.UserRole) == _PLACEHOLDER:
                item.removeChild(child)
                value = item.data(0, Qt.ItemDataRole.UserRole)
                if value is not None and value != _PLACEHOLDER:
                    self._tree.setUpdatesEnabled(False)
                    _add_children(item, value)
                    self._tree.setUpdatesEnabled(True)
                return

        for i in range(item.childCount()):
            child = item.child(i)
            meta = child.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__":
                _, parent_value, offset = meta
                item.removeChild(child)
                self._tree.setUpdatesEnabled(False)
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
                self._tree.setUpdatesEnabled(True)
                break

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

