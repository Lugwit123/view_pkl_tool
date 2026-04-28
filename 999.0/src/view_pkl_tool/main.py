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

try:
    import pyfory
    HAS_FORY = True
except ImportError:
    pyfory = None
    HAS_FORY = False
import sys
import traceback
import types
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
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QProgressDialog,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTextEdit,
    QToolButton,
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


# 单次展开/分页展示的子项数量。数值越大，展开越可能卡 UI。
_MAX_CHILDREN = 60
_BATCH_ADD_CHILDREN = 25
_DETAIL_MAX_CHARS = 50_000
_PLACEHOLDER = "__placeholder__"
_HISTORY_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer_history.json"
_DETAIL_PRESET_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer_detail_presets.json"
_SEARCH_FIELD_PRESET_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer_search_fields.json"
_HISTORY_MAX = 20
_LOG_FILE = Path(r"D:\Temp\pkl") / "pkl_viewer.log"

_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# 树搜索：下拉「字段名」= 键列去重；后台线程遍历整棵对象图（含未展开节点），匹配后定位到树节点。
_SEARCH_ALL_FIELDS = "【全部字段】"
_SEARCH_DEFAULT_FIELD = "name"
_SEARCH_FIXED_FIELDS: list[str] = ["name", "path", _SEARCH_ALL_FIELDS]
_DEEP_SEARCH_BATCH = 256
_DEEP_SEARCH_YIELD_NODES = 6000
_KEYS_COLLECT_YIELD_NODES = 8000

# 右侧“目录树”详情的安全上限，避免一次性渲染过大卡 UI
_DETAIL_SUBTREE_MAX_NODES = 3000
_DETAIL_SUBTREE_MAX_DEPTH = 20

# 子树展开下拉：只保留“级数”一个参数
_DETAIL_SUBTREE_LEVEL_PRESETS: list[str] = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "8",
    "10",
    "12",
    "15",
    "20",
]

_DEFAULT_DETAIL_PRESETS: list[dict[str, Any]] = [
    {
        "name": "常用目录",
        "show_text": True,
        "text_mode": "recursive",
        "show_tree": True,
        "fields": ["name", "id", "children"],
        "tree_columns": ["name", "id"],
    },
    {
        "name": "目录详情",
        "show_text": True,
        "text_mode": "recursive",
        "show_tree": True,
        "fields": ["name", "id", "parent_id", "children"],
        "tree_columns": ["name", "id"],
    },
    {
        "name": "文本摘要",
        "show_text": True,
        "text_mode": "shallow",
        "show_tree": False,
        "fields": None,
        "tree_columns": ["name", "id"],
    },
    {
        "name": "全部字段",
        "show_text": True,
        "text_mode": "recursive",
        "show_tree": True,
        "fields": None,
        "tree_columns": ["name", "id"],
    },
]


def _normalize_field_list(text: str) -> list[str] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    normalized = (
        raw.replace("；", ",")
        .replace(";", ",")
        .replace("，", ",")
        .replace("\n", ",")
        .replace(" ", ",")
    )
    fields = [x.strip() for x in normalized.split(",") if x.strip()]
    if not fields:
        return None
    if any(x in ("*", "全部", "all", "ALL") for x in fields):
        return None
    result: list[str] = []
    seen: set[str] = set()
    for field in fields:
        if field not in seen:
            result.append(field)
            seen.add(field)
    return result


def _field_list_to_text(fields: list[str] | None) -> str:
    return "*" if fields is None else ",".join(fields)


def _normalize_search_field_list(raw: Any) -> list[str]:
    if isinstance(raw, str):
        text = raw
    elif isinstance(raw, list):
        text = ",".join(str(x) for x in raw if str(x).strip())
    else:
        text = ""
    fields = _normalize_field_list(text) or []
    result: list[str] = []
    seen: set[str] = set()
    for field in fields:
        name = str(field).strip()
        if not name or name == _SEARCH_ALL_FIELDS:
            continue
        if name not in seen:
            result.append(name)
            seen.add(name)
    return result


def _normalize_detail_preset(raw: Any) -> dict[str, Any]:
    src = raw if isinstance(raw, dict) else {}
    name = str(src.get("name") or "未命名预设").strip() or "未命名预设"
    text_mode = str(src.get("text_mode") or "recursive").strip().lower()
    if text_mode not in ("recursive", "shallow"):
        text_mode = "recursive"
    tree_columns = _normalize_field_list(_field_list_to_text(src.get("tree_columns")))
    if not tree_columns:
        tree_columns = ["name", "id"]
    return {
        "name": name,
        "show_text": bool(src.get("show_text", True)),
        "text_mode": text_mode,
        "show_tree": bool(src.get("show_tree", True)),
        "fields": _normalize_field_list(_field_list_to_text(src.get("fields"))),
        "tree_columns": tree_columns[:2],
    }


def _clone_default_detail_presets() -> list[dict[str, Any]]:
    return [_normalize_detail_preset(item) for item in _DEFAULT_DETAIL_PRESETS]


class _DetailPresetEditorDialog(QDialog):
    def __init__(self, parent: QWidget, preset: dict[str, Any]) -> None:
        super().__init__(parent)
        self.setWindowTitle("编辑完整预设")
        self.resize(560, 420)

        layout = QVBoxLayout(self)
        hint = QLabel(
            "完整预设用于任意 PKL 预览：控制文本预览、目录树显示，以及要保留的字段。\n"
            "字段支持逗号/空格/换行分隔；输入 * 表示不过滤字段。"
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        form = QFormLayout()
        layout.addLayout(form)

        self.name_edit = QLineEdit(str(preset.get("name") or ""))
        form.addRow("预设名称", self.name_edit)

        self.show_text_check = QCheckBox("显示上方文本预览")
        self.show_text_check.setChecked(bool(preset.get("show_text", True)))
        form.addRow("文本预览", self.show_text_check)

        self.text_mode_combo = QComboBox()
        self.text_mode_combo.addItem("递归 JSON", "recursive")
        self.text_mode_combo.addItem("摘要", "shallow")
        idx = self.text_mode_combo.findData(str(preset.get("text_mode") or "recursive"))
        self.text_mode_combo.setCurrentIndex(idx if idx >= 0 else 0)
        form.addRow("文本模式", self.text_mode_combo)

        self.show_tree_check = QCheckBox("显示下方目录树")
        self.show_tree_check.setChecked(bool(preset.get("show_tree", True)))
        form.addRow("目录树", self.show_tree_check)

        self.fields_edit = QTextEdit()
        self.fields_edit.setPlaceholderText("例如: name,id,children 或 *")
        self.fields_edit.setPlainText(_field_list_to_text(preset.get("fields")))
        self.fields_edit.setMaximumHeight(90)
        form.addRow("文本字段", self.fields_edit)

        self.tree_columns_edit = QLineEdit(_field_list_to_text(preset.get("tree_columns")))
        self.tree_columns_edit.setPlaceholderText("例如: name,id")
        form.addRow("目录树列", self.tree_columns_edit)

        self.delete_requested = False
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        delete_btn = QPushButton("删除预设")
        buttons.addButton(delete_btn, QDialogButtonBox.ButtonRole.ResetRole)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        delete_btn.clicked.connect(self._on_delete_clicked)
        layout.addWidget(buttons)

    def _on_delete_clicked(self) -> None:
        self.delete_requested = True
        self.accept()

    def build_preset(self) -> dict[str, Any]:
        return _normalize_detail_preset(
            {
                "name": self.name_edit.text(),
                "show_text": self.show_text_check.isChecked(),
                "text_mode": self.text_mode_combo.currentData(),
                "show_tree": self.show_tree_check.isChecked(),
                "fields": _normalize_field_list(self.fields_edit.toPlainText()),
                "tree_columns": _normalize_field_list(self.tree_columns_edit.text()),
            }
        )


class _SearchFieldSettingsDialog(QDialog):
    def __init__(self, parent: QWidget, custom_fields: list[str]) -> None:
        super().__init__(parent)
        self.setWindowTitle("字段名设置")
        self.resize(520, 360)

        layout = QVBoxLayout(self)
        hint = QLabel(
            "字段名下拉框会始终固定保留：name、path、【全部字段】。\n"
            "下面只配置额外的自定义项；支持逗号 / 空格 / 换行分隔。"
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        fixed_label = QLabel("固定项：name, path, 【全部字段】")
        fixed_label.setStyleSheet("color: #aaa;")
        layout.addWidget(fixed_label)

        self.fields_edit = QTextEdit()
        self.fields_edit.setPlaceholderText("例如: id,parent_id,name_multil")
        self.fields_edit.setPlainText("\n".join(custom_fields))
        layout.addWidget(self.fields_edit, 1)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def build_fields(self) -> list[str]:
        return _normalize_search_field_list(self.fields_edit.toPlainText())


def _pick_name_from_multil(m: Any) -> str | None:
    if not isinstance(m, dict):
        return None
    for k in ("zh_cn", "zh-CN", "zh", "zh_CN", "cn", "en", "en_us", "en-US"):
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for v in m.values():
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _tree_node_inner_mapping(value: Any) -> dict[str, Any] | None:
    """提取目录节点的真实字段映射，兼容 __dict__ / _state / 普通对象。"""
    try:
        child_map = {str(k): v for k, v in _iter_children(value)}
    except Exception:
        child_map = {}

    raw_dict = child_map.get("__dict__")
    if isinstance(raw_dict, dict):
        return raw_dict

    raw_state = child_map.get("_state")
    if isinstance(raw_state, dict):
        return raw_state

    if isinstance(value, dict):
        return value

    st = getattr(value, "_state", None)
    if isinstance(st, dict):
        return st

    if child_map:
        return child_map
    return None


def _tree_node_name(value: Any) -> str | None:
    try:
        for k, v in _iter_children(value):
            if k == "name" and isinstance(v, str) and v.strip():
                return v.strip()
            if k == "name_multil":
                nm = _pick_name_from_multil(v)
                if nm:
                    return nm
    except Exception:
        pass

    inner = _tree_node_inner_mapping(value)
    if isinstance(inner, dict):
        nm = inner.get("name")
        if isinstance(nm, str) and nm.strip():
            return nm.strip()
        return _pick_name_from_multil(inner.get("name_multil"))

    nm = getattr(value, "name", None)
    if isinstance(nm, str) and nm.strip():
        return nm.strip()
    return _pick_name_from_multil(getattr(value, "name_multil", None))


def _tree_node_children(value: Any) -> list[Any] | None:
    try:
        child_v = None
        cats_v = None
        for k, v in _iter_children(value):
            if k == "children":
                child_v = v
            elif k == "categories":
                cats_v = v
        if isinstance(child_v, list):
            return child_v
        if isinstance(cats_v, list):
            return cats_v
    except Exception:
        pass

    inner = _tree_node_inner_mapping(value)
    if isinstance(inner, dict):
        ch = inner.get("children")
        if isinstance(ch, list):
            return ch
        ch2 = inner.get("categories")
        return ch2 if isinstance(ch2, list) else None

    ch = getattr(value, "children", None)
    if isinstance(ch, list):
        return ch
    ch2 = getattr(value, "categories", None)
    return ch2 if isinstance(ch2, list) else None


def _tree_node_id(value: Any) -> str | None:
    try:
        for k, v in _iter_children(value):
            if k == "id" and v is not None:
                s = str(v).strip()
                if s:
                    return s
    except Exception:
        pass

    inner = _tree_node_inner_mapping(value)
    if isinstance(inner, dict):
        v = inner.get("id")
        if v is not None:
            s = str(v).strip()
            return s or None

    v = getattr(value, "id", None)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _object_field_value(value: Any, field_name: str) -> Any:
    if field_name == "name":
        return _tree_node_name(value)
    if field_name == "id":
        return _tree_node_id(value)
    if field_name in ("children", "categories"):
        return _tree_node_children(value)

    try:
        for k, v in _iter_children(value):
            if str(k) == field_name:
                return v
    except Exception:
        pass

    inner = _tree_node_inner_mapping(value)
    if isinstance(inner, dict) and field_name in inner:
        return inner.get(field_name)

    return getattr(value, field_name, None)


def _is_tree_node_like(value: Any) -> bool:
    """用于详情面板：识别类似 MuseCategoryTreeNodeModel 的目录节点（name + children）。"""
    try:
        return _tree_node_name(value) is not None and isinstance(_tree_node_children(value), list)
    except Exception:
        return False


def _render_name_subtree(
    root: Any,
    *,
    max_depth: int = _DETAIL_SUBTREE_MAX_DEPTH,
    max_nodes: int = _DETAIL_SUBTREE_MAX_NODES,
) -> str:
    """将 name/children 结构渲染为缩进文本树。"""
    lines: list[str] = []
    seen: set[int] = set()
    node_count = 0
    truncated = False

    def _pick_name_from_multil(m: Any) -> str | None:
        if not isinstance(m, dict):
            return None
        for k in ("zh_cn", "zh-CN", "zh", "zh_CN", "cn", "en", "en_us", "en-US"):
            v = m.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in m.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    def _get_name(node: Any) -> str:
        # 通用兜底：通过 _iter_children 抽取（适配 pydantic/降级对象/自定义容器）
        try:
            name_v = None
            multil_v = None
            for k, v in _iter_children(node):
                if k == "name":
                    name_v = v
                elif k == "name_multil":
                    multil_v = v
            if isinstance(name_v, str) and name_v.strip():
                return name_v.strip()
            nm2 = _pick_name_from_multil(multil_v)
            if nm2 is not None:
                return nm2
        except Exception:
            pass

        if isinstance(node, dict):
            nm = node.get("name")
            if isinstance(nm, str) and nm.strip():
                return nm.strip()
            nm2 = _pick_name_from_multil(node.get("name_multil"))
            return nm2 if nm2 is not None else "<no-name>"
        st = getattr(node, "_state", None)
        if isinstance(st, dict) and "name" in st:
            nm = st.get("name")
            if isinstance(nm, str) and nm.strip():
                return nm.strip()
            nm2 = _pick_name_from_multil(st.get("name_multil"))
            return nm2 if nm2 is not None else "<no-name>"
        nm = getattr(node, "name", None)
        if isinstance(nm, str) and nm.strip():
            return nm.strip()
        nm2 = _pick_name_from_multil(getattr(node, "name_multil", None))
        return nm2 if nm2 is not None else "<no-name>"

    def _has_meaningful_name(node: Any) -> bool:
        return _get_name(node) != "<no-name>"

    def _get_children(node: Any) -> list[Any] | None:
        # 通用兜底：通过 _iter_children 抽取（适配 pydantic/降级对象/自定义容器）
        try:
            child_v = None
            cats_v = None
            for k, v in _iter_children(node):
                if k == "children":
                    child_v = v
                elif k == "categories":
                    cats_v = v
            if isinstance(child_v, list):
                return child_v
            if isinstance(cats_v, list):
                return cats_v
        except Exception:
            pass

        if isinstance(node, dict):
            ch = node.get("children")
            if isinstance(ch, list):
                return ch
            ch2 = node.get("categories")
            return ch2 if isinstance(ch2, list) else None
        st = getattr(node, "_state", None)
        if isinstance(st, dict):
            ch = st.get("children")
            if isinstance(ch, list):
                return ch
            ch2 = st.get("categories")
            return ch2 if isinstance(ch2, list) else None
        ch = getattr(node, "children", None)
        if isinstance(ch, list):
            return ch
        ch2 = getattr(node, "categories", None)
        return ch2 if isinstance(ch2, list) else None

    def walk(node: Any, depth: int) -> None:
        nonlocal node_count, truncated
        if truncated:
            return
        if depth > max_depth:
            lines.append(("  " * depth) + "… (max depth reached)")
            truncated = True
            return
        nid = id(node)
        if nid in seen:
            lines.append(("  " * depth) + "… (cycle)")
            return
        seen.add(nid)
        node_count += 1
        if node_count > max_nodes:
            lines.append(("  " * depth) + f"… (truncated, >{max_nodes} nodes)")
            truncated = True
            return

        lines.append(("  " * depth) + _get_name(node))

        children = _get_children(node)
        if not isinstance(children, list) or not children:
            return
        for ch in children:
            # 子节点只要能提取出 name，就继续递归（即使它没有 children，也会作为叶子显示）
            if _has_meaningful_name(ch):
                walk(ch, depth + 1)
            else:
                lines.append(("  " * (depth + 1)) + f"<{_type_label(ch)}>")

    walk(root, 0)
    header = f"name subtree: nodes={min(node_count, max_nodes)}"
    if truncated:
        header += " (truncated)"
    return header + "\n" + "\n".join(lines)


def _build_recursive_preview_data(
    value: Any,
    *,
    max_depth: int = _DETAIL_SUBTREE_MAX_DEPTH,
    max_nodes: int = _DETAIL_SUBTREE_MAX_NODES,
    allowed_fields: set[str] | None = None,
) -> Any:
    """将对象递归转换为可 JSON 序列化的预览结构。"""
    seen: set[int] = set()
    node_count = 0

    def walk(obj: Any, depth: int) -> Any:
        nonlocal node_count
        if not _is_container(obj):
            return obj
        if depth >= max_depth:
            return f"<max depth: {max_depth}>"

        oid = id(obj)
        if oid in seen:
            return "<cycle>"
        seen.add(oid)

        node_count += 1
        if node_count > max_nodes:
            return f"<truncated: >{max_nodes} nodes>"

        if isinstance(obj, (list, tuple)):
            out_list: list[Any] = []
            for _k, child in _iter_children(obj):
                out_list.append(walk(child, depth + 1))
            return out_list

        out_dict: dict[str, Any] = {}
        for k, child in _iter_children(obj):
            key_s = str(k)
            if allowed_fields is not None and key_s not in allowed_fields:
                continue
            out_dict[key_s] = walk(child, depth + 1)
        return out_dict

    return walk(value, 0)


def _populate_detail_subtree_widget(
    tree: QTreeWidget,
    value: Any,
    *,
    max_depth: int = _DETAIL_SUBTREE_MAX_DEPTH,
    max_nodes: int = _DETAIL_SUBTREE_MAX_NODES,
    display_fields: list[str] | None = None,
) -> None:
    """将目录节点填充到右侧独立树控件。"""
    tree.clear()
    leaf_fields = [f for f in (display_fields or ["name", "id"]) if f not in ("children", "categories")]
    if not leaf_fields:
        leaf_fields = ["name", "id"]
    if len(leaf_fields) == 1:
        leaf_fields.append("id" if leaf_fields[0] != "id" else "")
    tree.setHeaderLabels([leaf_fields[0], leaf_fields[1]])

    if not _is_tree_node_like(value):
        QTreeWidgetItem(tree, ["当前选择不是目录节点", ""])
        tree.expandAll()
        return

    seen: set[int] = set()
    node_count = 0
    truncated = False

    def walk(parent: QTreeWidget | QTreeWidgetItem, node: Any, depth: int) -> None:
        nonlocal node_count, truncated
        if truncated:
            return
        if depth > max_depth:
            QTreeWidgetItem(parent, ["… (max depth reached)", ""])
            truncated = True
            return
        nid = id(node)
        if nid in seen:
            QTreeWidgetItem(parent, ["… (cycle)", ""])
            return
        seen.add(nid)

        node_count += 1
        if node_count > max_nodes:
            QTreeWidgetItem(parent, [f"… (truncated, >{max_nodes} nodes)", ""])
            truncated = True
            return

        first = _object_field_value(node, leaf_fields[0])
        second = _object_field_value(node, leaf_fields[1]) if leaf_fields[1] else ""
        item = QTreeWidgetItem(
            parent,
            [
                str(first).strip() if first not in (None, "") else "<no-value>",
                str(second).strip() if second not in (None, "") else "",
            ],
        )
        children = _tree_node_children(node)
        if not isinstance(children, list) or not children:
            return
        for child in children:
            child_name = _tree_node_name(child)
            if child_name is not None:
                walk(item, child, depth + 1)
            else:
                QTreeWidgetItem(item, [f"<{_type_label(child)}>", ""])

    walk(tree, value, 0)
    tree.expandToDepth(min(max_depth, 2))


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

    @staticmethod
    def _ensure_missing_types_module() -> types.ModuleType:
        """
        让缺失类型占位类可被 pickle 保存。

        pickle 保存 class 时要求：class.__module__ 可 import，且模块内有同名符号。
        由于本工具常以脚本形式运行（无 package），这里在运行时注册一个模块到 sys.modules。
        """
        pkg_name = "view_pkl_tool"
        mod_name = "view_pkl_tool.missing_types"

        pkg = sys.modules.get(pkg_name)
        if pkg is None:
            pkg = types.ModuleType(pkg_name)
            pkg.__path__ = []  # 标记为 package-like
            sys.modules[pkg_name] = pkg

        mod = sys.modules.get(mod_name)
        if mod is None:
            mod = types.ModuleType(mod_name)
            sys.modules[mod_name] = mod
            setattr(pkg, "missing_types", mod)
        return mod

    def find_class(self, module, name):
        try:
            return super().find_class(module, name)
        except Exception:
            key = (module, name)
            cached = self._missing_class_cache.get(key)
            if cached is not None:
                return cached

            missing_mod = self._ensure_missing_types_module()
            missing_type = type(
                self._safe_type_name(module, name),
                (GenericPickleObject,),
                {
                    "__missing_module__": module,
                    "__missing_class__": name,
                    "__module__": "view_pkl_tool.missing_types",
                },
            )
            # 确保该类型在模块命名空间下可 import（pickle 保存需要）
            setattr(missing_mod, missing_type.__name__, missing_type)
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
    done: Signal = Signal(object, str, bool, str)  # obj, type_name, used_fallback, format_label
    error: Signal = Signal(str)

    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = path
        self._is_fory = Path(path).suffix.lower() == ".fory"

    def run(self) -> None:
        try:
            raw = Path(self._path).read_bytes()
            used_fallback = False
            if self._is_fory:
                if not HAS_FORY:
                    self.error.emit(
                        "缺少 pyfory 库，无法加载 .fory 文件。\n"
                        "请执行: pip install pyfory"
                    )
                    return
                fory = pyfory.Fory(xlang=False, ref=True, strict=False)
                obj = fory.loads(raw)
                fmt_label = "Fory"
            else:
                try:
                    obj = pickle.loads(raw)
                    fmt_label = "Pickle"
                except Exception:
                    obj = TolerantUnpickler(io.BytesIO(raw)).load()
                    used_fallback = True
                    fmt_label = "Pickle 兼容模式"
            self.done.emit(obj, type(obj).__name__, used_fallback, fmt_label)
        except Exception as e:
            traceback.print_exc()
            _log_exception(f"加载失败: {self._path}")
            self.error.emit(str(e))


_TYPE_LABEL_CACHE: dict[int, str] = {}
_NODE_LABEL_CACHE: dict[int, str] = {}


def _type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    # 基础类型没必要缓存
    if isinstance(value, (str, int, float, bool, bytes, bytearray)):
        return type(value).__name__
    vid = id(value)
    cached = _TYPE_LABEL_CACHE.get(vid)
    if cached is not None:
        return cached
    # For placeholder objects created during tolerant unpickling, only show
    # struct/class name to keep the type column compact.
    if isinstance(value, GenericPickleObject):
        class_name = getattr(value.__class__, "__missing_class__", type(value).__name__)
        _TYPE_LABEL_CACHE[vid] = class_name
        return class_name
    t = type(value).__name__
    _TYPE_LABEL_CACHE[vid] = t
    return t


def _node_label(value: Any) -> str:
    # 基础类型直接显示
    if value is None:
        return "None"
    if isinstance(value, (str, int, float, bool)):
        s = str(value)
        return s[:120] + "..." if len(s) > 120 else s
    if isinstance(value, (bytes, bytearray)):
        return f"bytes  ({len(value)})"

    vid = id(value)
    cached = _NODE_LABEL_CACHE.get(vid)
    if cached is not None:
        return cached

    if isinstance(value, dict):
        out = f"dict  ({len(value)})"
        _NODE_LABEL_CACHE[vid] = out
        return out
    if isinstance(value, (list, tuple)):
        t = "list" if isinstance(value, list) else "tuple"
        out = f"{t}  ({len(value)})"
        _NODE_LABEL_CACHE[vid] = out
        return out
    # 常见业务对象（如 MuseCategoryTreeNodeModel）优先展示 name，便于树浏览
    try:
        nm = getattr(value, "name", None)
        if isinstance(nm, str) and nm.strip():
            out = nm.strip()
            _NODE_LABEL_CACHE[vid] = out
            return out
    except Exception:
        pass
    # Missing-type fallback objects: keep value column compact and readable.
    if isinstance(value, GenericPickleObject):
        out = _type_label(value)
        _NODE_LABEL_CACHE[vid] = out
        return out
    if hasattr(value, "__fields__"):
        out = f"{type(value).__name__}  ({len(value.__fields__)} fields)"
        _NODE_LABEL_CACHE[vid] = out
        return out
    if hasattr(value, "__dict__") and not isinstance(value, type):
        out = f"{type(value).__name__}  ({len(vars(value))} attrs)"
        _NODE_LABEL_CACHE[vid] = out
        return out

    # 避免对未知对象调用 str()（可能非常慢/包含大量递归信息）
    out = f"<{type(value).__name__}>"
    _NODE_LABEL_CACHE[vid] = out
    return out


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


def _replace_strings_in_object(
    root: Any,
    *,
    fields: set[str] | None,
    needle: str,
    replacement: str,
) -> tuple[Any, int, int]:
    """递归替换对象图中的字符串值；支持限制到指定字段名。"""
    if not needle:
        return root, 0, 0

    values_changed = 0
    total_replacements = 0
    memo: dict[int, Any] = {}
    active: set[int] = set()

    def replace_text(text: str) -> str:
        nonlocal values_changed, total_replacements
        hit_count = text.count(needle)
        if hit_count <= 0:
            return text
        values_changed += 1
        total_replacements += hit_count
        return text.replace(needle, replacement)

    def visit_child(key: str, child: Any, *, force_replace: bool) -> Any:
        if isinstance(child, str):
            if not force_replace and fields is not None and key not in fields:
                return child
            return replace_text(child)
        child_force = force_replace or fields is None or (key in fields)
        return walk(child, force_replace=child_force)

    def rebuild_tuple(value: tuple[Any, ...], *, force_replace: bool) -> tuple[Any, ...]:
        items = [
            visit_child(f"[{idx}]", child, force_replace=force_replace)
            for idx, child in enumerate(value)
        ]
        if type(value) is tuple:
            return tuple(items)
        try:
            return type(value)(*items)
        except Exception:
            return tuple(items)

    def walk(value: Any, *, force_replace: bool = False) -> Any:
        if isinstance(value, str):
            if force_replace or fields is None:
                return replace_text(value)
            return value
        if value is None or isinstance(
            value, (int, float, bool, complex, bytes, bytearray)
        ):
            return value

        vid = id(value)
        if vid in memo:
            return memo[vid]
        if vid in active:
            return value

        active.add(vid)
        try:
            if isinstance(value, dict):
                memo[vid] = value
                for key, child in list(value.items()):
                    new_child = visit_child(
                        str(key), child, force_replace=force_replace
                    )
                    if new_child is not child:
                        value[key] = new_child
                return value

            if isinstance(value, list):
                memo[vid] = value
                for idx, child in enumerate(value):
                    new_child = visit_child(
                        f"[{idx}]", child, force_replace=force_replace
                    )
                    if new_child is not child:
                        value[idx] = new_child
                return value

            if isinstance(value, tuple):
                new_value = rebuild_tuple(value, force_replace=force_replace)
                memo[vid] = new_value
                return new_value

            if isinstance(value, set):
                memo[vid] = value
                new_items: list[Any] = []
                changed = False
                for child in value:
                    new_child = walk(child, force_replace=force_replace)
                    if new_child is not child:
                        changed = True
                    new_items.append(new_child)
                if changed:
                    value.clear()
                    value.update(new_items)
                return value

            if isinstance(value, frozenset):
                new_value = frozenset(
                    walk(child, force_replace=force_replace) for child in value
                )
                memo[vid] = new_value
                return new_value

            if hasattr(value, "__fields__"):
                memo[vid] = value
                for field_name in value.__fields__:
                    child = getattr(value, field_name, None)
                    new_child = visit_child(
                        str(field_name), child, force_replace=force_replace
                    )
                    if new_child is not child:
                        setattr(value, field_name, new_child)
                return value

            if hasattr(value, "__dict__") and not isinstance(value, type):
                memo[vid] = value
                for attr_name, child in list(vars(value).items()):
                    new_child = visit_child(
                        str(attr_name), child, force_replace=force_replace
                    )
                    if new_child is not child:
                        setattr(value, attr_name, new_child)
                return value

            return value
        finally:
            active.discard(vid)

    return walk(root, force_replace=(fields is None)), values_changed, total_replacements


def _make_portable_pickle_data(root: Any) -> Any:
    """
    将对象图转换成可移植的 pickle 数据（只包含内置基础类型），避免依赖业务类/本工具模块。

    - GenericPickleObject -> dict（保留缺失类信息与 _state）
    - 其它对象：尽量转 dict(vars)；失败则转 repr
    - 循环引用：打断为 "<cycle>"
    """

    seen: set[int] = set()

    def walk(v: Any) -> Any:
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, (bytes, bytearray)):
            return bytes(v)

        vid = id(v)
        if vid in seen:
            return "<cycle>"
        seen.add(vid)

        if isinstance(v, dict):
            return {str(k): walk(vv) for k, vv in v.items()}
        if isinstance(v, list):
            return [walk(x) for x in v]
        if isinstance(v, tuple):
            return [walk(x) for x in v]
        if isinstance(v, set):
            return [walk(x) for x in v]
        if isinstance(v, frozenset):
            return [walk(x) for x in v]

        if isinstance(v, GenericPickleObject):
            module_name = getattr(v.__class__, "__missing_module__", "unknown_module")
            class_name = getattr(v.__class__, "__missing_class__", "unknown_class")
            state = getattr(v, "_state", None)
            if state is None and hasattr(v, "__dict__"):
                state = dict(vars(v))
            return {
                "__missing__": {"module": str(module_name), "class": str(class_name)},
                "_state": walk(state) if state is not None else {},
            }

        if hasattr(v, "__fields__"):
            out: dict[str, Any] = {"__type__": type(v).__name__}
            for fn in v.__fields__:
                out[str(fn)] = walk(getattr(v, fn, None))
            return out

        if hasattr(v, "__dict__") and not isinstance(v, type):
            out2: dict[str, Any] = {"__type__": type(v).__name__}
            for k, vv in vars(v).items():
                out2[str(k)] = walk(vv)
            return out2

        return {"__type__": type(v).__name__, "__repr__": repr(v)}

    return walk(root)


def _child_count(value: Any) -> int:
    if hasattr(value, "__fields__"):
        return len(value.__fields__)
    if isinstance(value, (dict, list, tuple)):
        return len(value)
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return len(vars(value))
    return 0


_PENDING_ADD_CHILDREN_JOBS: dict[tuple[int, int], dict[str, Any]] = {}


def _start_add_children_job(
    tree: QTreeWidget,
    parent: QTreeWidgetItem,
    parent_value: Any,
    *,
    offset: int = 0,
    total: int | None = None,
) -> None:
    """分批将 parent_value 的子项插入到 parent 下。"""
    key = (id(tree), id(parent))
    if key in _PENDING_ADD_CHILDREN_JOBS:
        return
    if total is None:
        try:
            total = _child_count(parent_value)
        except Exception:
            total = None

    it = iter(_iter_children(parent_value))
    skipped = 0
    if offset > 0:
        try:
            while skipped < offset:
                next(it)
                skipped += 1
        except StopIteration:
            return

    _PENDING_ADD_CHILDREN_JOBS[key] = {
        "it": it,
        "added": 0,
        "offset": offset,
        "total": total,
        "parent_value": parent_value,
    }

    def step() -> None:
        st = _PENDING_ADD_CHILDREN_JOBS.get(key)
        if st is None:
            return
        # 如果 item 已经脱离树，直接停止
        try:
            if parent.treeWidget() is None or parent.treeWidget() is not tree:
                _PENDING_ADD_CHILDREN_JOBS.pop(key, None)
                return
        except Exception:
            _PENDING_ADD_CHILDREN_JOBS.pop(key, None)
            return

        tree.setUpdatesEnabled(False)
        try:
            batch = 0
            while batch < _BATCH_ADD_CHILDREN and st["added"] < _MAX_CHILDREN:
                try:
                    k, v = next(st["it"])
                except StopIteration:
                    _PENDING_ADD_CHILDREN_JOBS.pop(key, None)
                    break
                _make_node(parent, str(k), v)
                st["added"] += 1
                batch += 1
        finally:
            tree.setUpdatesEnabled(True)

        # 达到分页上限：插入 “more items” 并结束
        st2 = _PENDING_ADD_CHILDREN_JOBS.get(key)
        if st2 is None:
            # iterator exhausted or cancelled; no more node
            return
        if st2["added"] >= _MAX_CHILDREN:
            try:
                total2 = st2.get("total")
                next_offset = int(st2.get("offset") or 0) + int(st2.get("added") or 0)
                remaining = None
                if isinstance(total2, int) and total2 >= next_offset:
                    remaining = total2 - next_offset
                more = QTreeWidgetItem(parent, ["...", f"{remaining} more items" if remaining is not None else "more items", ""])
                more.setForeground(1, QColor("#888"))
                more.setData(0, Qt.ItemDataRole.UserRole, ("__more__", st2["parent_value"], next_offset))
            finally:
                _PENDING_ADD_CHILDREN_JOBS.pop(key, None)
            return

        # 还有剩余：下一帧继续
        QTimer.singleShot(0, step)

    QTimer.singleShot(0, step)


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


def _row_matches_search_fields(
    fields: set[str] | None, needle: str, key: str, value: Any
) -> bool:
    """多字段版匹配规则：fields=None 表示【全部字段】。"""
    k0 = key
    k1 = _node_label(value)
    k2 = _type_label(value)
    if fields is not None and k0 not in fields:
        return False
    n = needle.strip().lower()
    if not n:
        return fields is not None
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
        _start_add_children_job(tree, item, value, offset=0, total=_child_count(value))
    return True


def _tree_expand_one_more_chunk(tree: QTreeWidget, item: QTreeWidgetItem) -> bool:
    """展开 item 下第一个「… 还有更多」分页块。返回是否执行了展开。"""
    for i in range(item.childCount()):
        ch = item.child(i)
        meta = ch.data(0, Qt.ItemDataRole.UserRole)
        if isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__":
            _, parent_value, offset = meta
            item.removeChild(ch)
            _start_add_children_job(tree, item, parent_value, offset=int(offset), total=_child_count(parent_value))
            return True
    return False


def _tree_expand_more_item(tree: QTreeWidget, item: QTreeWidgetItem) -> bool:
    """若 item 本身是分页占位节点，则加载下一批兄弟节点。"""
    meta = item.data(0, Qt.ItemDataRole.UserRole)
    if not (isinstance(meta, tuple) and len(meta) == 3 and meta[0] == "__more__"):
        return False
    parent = item.parent()
    if parent is None:
        return False
    _, parent_value, offset = meta
    parent.removeChild(item)
    _start_add_children_job(tree, parent, parent_value, offset=int(offset), total=_child_count(parent_value))
    return True


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
        fields: set[str] | None,
        needle: str,
        job_gen: int,
        get_job_gen: Any,
    ) -> None:
        super().__init__()
        self._root = root
        self._fields = fields
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
                if _row_matches_search_fields(self._fields, self._needle, key, child):
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

        # 完整路径（9px 小字）
        self._path_row_label = QLabel(path)
        self._path_row_label.setStyleSheet(
            "color: #666; font-size: 9px; padding: 0px 2px 2px 2px; background: transparent;"
        )
        self._path_row_label.setWordWrap(True)
        self._path_row_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(self._path_row_label, 0)

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

    def enterEvent(self, event) -> None:  # type: ignore[override]
        super().enterEvent(event)
        self._viewer._status.showMessage(self._path, 0)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        super().leaveEvent(event)
        self._viewer._restore_status_path()

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
            self._path_row_label.setStyleSheet(
                "color: #444; font-size: 9px; padding: 0px 2px 2px 2px; background: transparent;"
            )
        elif selected:
            self._meta_label.setStyleSheet(
                "color: #aaa; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
            )
            self._path_row_label.setStyleSheet(
                "color: #888; font-size: 9px; padding: 0px 2px 2px 2px; background: transparent;"
            )
        else:
            self._meta_label.setStyleSheet(
                "color: #888; font-size: 10px; padding: 0px 2px 1px 2px; background: transparent;"
            )
            self._path_row_label.setStyleSheet(
                "color: #666; font-size: 9px; padding: 0px 2px 2px 2px; background: transparent;"
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
        try:
            src = Path(__file__).resolve()
            stamp = datetime.datetime.fromtimestamp(src.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            self.setWindowTitle(f"PKL Viewer  |  {src.name}  |  {stamp}")
        except Exception:
            self.setWindowTitle("PKL Viewer")
        self.resize(1300, 700)
        self._current_obj: Any = None
        self._current_used_fallback = False
        self._current_dirty = False
        self._force_writeback = True
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
        self._selected_search_fields: set[str] = {_SEARCH_DEFAULT_FIELD}
        self._search_field_btn: QToolButton | None = None
        self._search_field_menu: QMenu | None = None
        self._deep_search_running: bool = False
        self._search_first_nav_pending: bool = False
        self._detail_presets: list[dict[str, Any]] = self._load_detail_presets()
        self._custom_search_fields: list[str] = self._load_search_field_presets()
        self._replace_line: QLineEdit | None = None
        self._replace_btn: QPushButton | None = None
        self._save_btn: QPushButton | None = None
        self._save_as_btn: QPushButton | None = None
        self._force_writeback_check: QCheckBox | None = None
        self._rebuild_combo_timer = QTimer(self)
        self._rebuild_combo_timer.setSingleShot(True)
        self._rebuild_combo_timer.setInterval(200)
        self._rebuild_combo_timer.timeout.connect(self._on_rebuild_combo_timeout)
        self._load_ui()
        # 在状态栏也标识当前运行脚本，避免误启动到旧版本
        try:
            src = Path(__file__).resolve()
            stamp = datetime.datetime.fromtimestamp(src.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            if hasattr(self, "_status") and isinstance(self._status, QStatusBar):
                self._status.showMessage(f"脚本: {src}  |  mtime: {stamp}", 6000)
        except Exception:
            pass
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
        open_btn = _require_ui_child(central, QPushButton, "openButton")
        open_btn.clicked.connect(self._on_open)
        reload_btn = _require_ui_child(central, QPushButton, "reloadButton")
        reload_btn.clicked.connect(self._on_reload)
        _require_ui_child(central, QPushButton, "refreshMetaButton").clicked.connect(
            self._on_refresh_log_and_file_meta
        )
        _require_ui_child(central, QPushButton, "removeFromListButton").clicked.connect(
            self._on_history_remove
        )

        # 顶栏增加“重启程序”按钮（不修改 .ui，运行时插入到 toolbar 布局）
        try:
            toolbar_lay = central.findChild(QHBoxLayout, "horizontalLayout_toolbar")
            if isinstance(toolbar_lay, QHBoxLayout):
                save_btn = QPushButton("保存", central)
                save_btn.setToolTip("将当前替换结果保存回 PKL 文件")
                save_btn.clicked.connect(self._on_save)
                save_as_btn = QPushButton("另存为...", central)
                save_as_btn.setToolTip("将当前数据另存为 PKL 或 Fory 文件")
                save_as_btn.clicked.connect(self._on_save_as)
                force_check = QCheckBox("强制回写", central)
                force_check.setToolTip(
                    "允许在兼容模式下替换并保存（高风险：可能破坏原始数据结构，建议只用“另存为”）"
                )
                force_check.setChecked(True)
                force_check.stateChanged.connect(self._on_force_writeback_changed)
                restart_btn = QPushButton("重启程序", central)
                restart_btn.setToolTip("重新拉起当前程序并退出（用于热更新 UI/代码）")
                restart_btn.clicked.connect(self._on_restart_app)
                insert_idx = toolbar_lay.indexOf(reload_btn) + 1
                try:
                    toolbar_lay.insertWidget(insert_idx, save_btn)
                    toolbar_lay.insertWidget(insert_idx + 1, save_as_btn)
                    toolbar_lay.insertWidget(insert_idx + 2, force_check)
                    toolbar_lay.insertWidget(insert_idx + 3, restart_btn)
                except Exception:
                    toolbar_lay.addWidget(save_btn)
                    toolbar_lay.addWidget(save_as_btn)
                    toolbar_lay.addWidget(force_check)
                    toolbar_lay.addWidget(restart_btn)
                self._save_btn = save_btn
                self._save_as_btn = save_as_btn
                self._force_writeback_check = force_check
        except Exception:
            traceback.print_exc()
            _log_exception("插入重启按钮失败")

        self._main_tabs = _require_ui_child(central, QTabWidget, "mainTabs")
        self._tree = _require_ui_child(central, QTreeWidget, "treeWidget")
        self._detail = _require_ui_child(central, QTextEdit, "detailText")
        self._detail_splitter: QSplitter | None = None
        self._detail_preset_combo: QComboBox | None = None
        self._detail_tree: QTreeWidget | None = None
        self._subtree_level_combo: QComboBox | None = None
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
        # 旧版：单选 QComboBox。新版：隐藏 combo，使用可多选的 QToolButton + QMenu
        self._search_field_combo.setVisible(False)
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
        try:
            search_lay = central.findChild(QHBoxLayout, "horizontalLayout_search")
            if isinstance(search_lay, QHBoxLayout):
                # 用按钮替代字段名下拉（支持多选）
                field_btn = QToolButton(central)
                field_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
                field_btn.setToolTip("选择要匹配的字段名（可多选）；勾选【全部字段】表示不限制字段")
                field_btn.setStyleSheet(
                    "QToolButton { background: #2d2d2d; color: #d4d4d4; padding: 3px 10px; border: 1px solid #3c3c3c; } "
                    "QToolButton:hover { background: #353535; } "
                    "QToolButton:pressed { background: #3f3f3f; }"
                )
                menu = QMenu(field_btn)
                menu.setStyleSheet(
                    "QMenu { background: #2d2d2d; color: #d4d4d4; }"
                    " QMenu::item:selected { background: #264f78; }"
                )
                field_btn.setMenu(menu)
                self._search_field_btn = field_btn
                self._search_field_menu = menu

                # 放到原字段名下拉的位置（label_field 后面）
                try:
                    combo_idx = search_lay.indexOf(self._search_field_combo)
                    if combo_idx >= 0:
                        search_lay.insertWidget(combo_idx, field_btn)
                except Exception:
                    pass

                search_fields_btn = QPushButton("字段设置...", central)
                search_fields_btn.setToolTip("设置字段名下拉框中的自定义枚举值")
                search_fields_btn.clicked.connect(self._on_edit_search_fields)
                replace_label = QLabel("替换为", central)
                replace_label.setStyleSheet("color: #aaa; font-size: 12px;")
                replace_line = QLineEdit(central)
                replace_line.setPlaceholderText("支持留空，用于删除表情或其它文本")
                replace_line.setStyleSheet(
                    "QLineEdit { background: #2d2d2d; color: #d4d4d4; border: 1px solid #3c3c3c; padding: 4px 8px; }"
                )
                replace_line.returnPressed.connect(self._on_replace_all)
                replace_btn = QPushButton("替换全部", central)
                replace_btn.setStyleSheet(
                    "QPushButton { background: #8b5cf6; color: #fff; padding: 4px 8px; border: none; } "
                    "QPushButton:hover:enabled { background: #9d72ff; } "
                    "QPushButton:pressed:enabled { background: #7447d6; } "
                    "QPushButton:disabled { background: #4d3d70; color: #b9abd9; }"
                )
                replace_btn.clicked.connect(self._on_replace_all)
                insert_idx = search_lay.indexOf(self._search_next_btn) + 1
                search_lay.insertWidget(insert_idx, search_fields_btn)
                search_lay.insertWidget(insert_idx + 1, replace_label)
                search_lay.insertWidget(insert_idx + 2, replace_line, 1)
                search_lay.insertWidget(insert_idx + 3, replace_btn)
                self._replace_line = replace_line
                self._replace_btn = replace_btn
        except Exception:
            traceback.print_exc()
            _log_exception("插入替换控件失败")

        self._log_view = _require_ui_child(central, QTextEdit, "logView")
        log_hint = _require_ui_child(central, QLabel, "logHintLabel")
        log_hint.setText(f"日志文件：{_LOG_FILE}")

        hdr = self._tree.header()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._tree.itemExpanded.connect(self._on_item_expanded)
        self._tree.itemDoubleClicked.connect(self._on_item_double_clicked)
        self._tree.currentItemChanged.connect(self._on_item_selected)
        self._detail.setFont(QFont("Consolas", 12))
        self._log_view.setFont(QFont("Consolas", 10))

        # 右侧详情：detailText 在 .ui 中是 QSplitter 的直接子控件，包一层容器后放入“文本预览 + 独立目录树”
        try:
            splitter = self._detail.parentWidget()
            if isinstance(splitter, QSplitter):
                idx = splitter.indexOf(self._detail)
                self._detail.setParent(None)

                detail_wrap = QWidget(splitter)
                detail_wrap.setObjectName("detailWrap")
                wrap_lay = QVBoxLayout(detail_wrap)
                wrap_lay.setContentsMargins(0, 0, 0, 0)
                wrap_lay.setSpacing(4)

                bar = QWidget(detail_wrap)
                bar_lay = QHBoxLayout(bar)
                bar_lay.setContentsMargins(0, 0, 0, 0)
                bar_lay.setSpacing(8)
                bar_lay.addWidget(QLabel("完整预设：", bar))
                detail_preset_combo = QComboBox(bar)
                detail_preset_combo.currentIndexChanged.connect(
                    lambda _idx: self._refresh_detail_from_current_item()
                )
                bar_lay.addWidget(detail_preset_combo, 1)

                edit_preset_btn = QPushButton("编辑预设...", bar)
                edit_preset_btn.clicked.connect(self._on_edit_detail_preset)
                bar_lay.addWidget(edit_preset_btn)

                bar_lay.addWidget(QLabel("子树展开：", bar))
                level_combo = QComboBox(bar)
                level_combo.setEditable(True)
                level_combo.setToolTip("只控制展开到第几级，例如 3 / 5 / 12")
                for level in _DETAIL_SUBTREE_LEVEL_PRESETS:
                    level_combo.addItem(level)
                level_combo.setCurrentText("12")
                level_combo.currentIndexChanged.connect(
                    lambda _idx: self._refresh_detail_from_current_item()
                )
                level_combo.editTextChanged.connect(
                    lambda _text: self._refresh_detail_from_current_item()
                )
                bar_lay.addWidget(level_combo)
                bar_lay.addStretch(1)

                wrap_lay.addWidget(bar)
                detail_splitter = QSplitter(Qt.Orientation.Vertical, detail_wrap)
                detail_splitter.setChildrenCollapsible(False)
                detail_splitter.addWidget(self._detail)

                detail_tree = QTreeWidget(detail_splitter)
                detail_tree.setObjectName("detailTreeWidget")
                detail_tree.setAlternatingRowColors(True)
                detail_tree.setUniformRowHeights(True)
                detail_tree.setStyleSheet(
                    "QTreeWidget { background: #1e1e1e; color: #d4d4d4; border: none; font-size: 13px; } "
                    "QTreeWidget::item:selected { background: #264f78; } "
                    "QHeaderView::section { background: #2d2d2d; color: #ccc; padding: 4px; border: none; }"
                )
                detail_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                detail_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                detail_tree.setHeaderLabels(["name", "id"])
                detail_splitter.addWidget(detail_tree)
                detail_splitter.setSizes([340, 260])

                wrap_lay.addWidget(detail_splitter, 1)
                splitter.insertWidget(idx, detail_wrap)
                self._detail_splitter = detail_splitter
                self._detail_preset_combo = detail_preset_combo
                self._detail_tree = detail_tree
                self._subtree_level_combo = level_combo
                self._refresh_detail_preset_combo()
        except Exception:
            traceback.print_exc()
            _log_exception("插入子树展开下拉框失败")

        self._reload_app_log_view()

        self._status = QStatusBar()
        self.setStatusBar(self._status)
        self.setStyleSheet("QMainWindow { background: #252526; }")
        self._refresh_path_label()
        self._update_edit_action_state()
        self._rebuild_search_field_combo()

    def _refresh_path_label(self) -> None:
        path = self._path_label.toolTip().strip()
        if not path:
            self._path_label.setText("未加载文件")
            return
        name = Path(path).name
        tags: list[str] = []
        if self._current_used_fallback:
            tags.append("兼容模式")
            if self._force_writeback:
                tags.append("强制回写")
        if self._current_dirty:
            tags.append("已修改")
        if tags:
            name += "  [" + " | ".join(tags) + "]"
        self._path_label.setText(name)

    def _update_edit_action_state(self) -> None:
        has_obj = self._current_obj is not None
        can_mutate = has_obj and (self._force_writeback or not self._current_used_fallback)
        if isinstance(self._replace_line, QLineEdit):
            self._replace_line.setEnabled(has_obj)
            if self._current_used_fallback:
                self._replace_line.setToolTip(
                    "兼容模式下默认不支持安全回写；如确需修改，请勾选“强制回写”（高风险）"
                )
            else:
                self._replace_line.setToolTip("支持留空，用于删除表情或其它文本")
        if isinstance(self._replace_btn, QPushButton):
            self._replace_btn.setEnabled(has_obj)
            if self._current_used_fallback:
                self._replace_btn.setToolTip(
                    "兼容模式下默认不允许回写；如确需修改，请勾选“强制回写”（高风险）"
                )
            else:
                self._replace_btn.setToolTip("按当前字段 + 搜索词，替换对象中的所有字符串值")
        if isinstance(self._save_btn, QPushButton):
            self._save_btn.setEnabled(can_mutate and self._current_dirty)
        if isinstance(self._save_as_btn, QPushButton):
            self._save_as_btn.setEnabled(can_mutate)
        if isinstance(self._force_writeback_check, QCheckBox):
            self._force_writeback_check.setEnabled(has_obj and self._current_used_fallback)

    def _on_force_writeback_changed(self, state: int) -> None:
        self._force_writeback = bool(state)
        if self._force_writeback and self._current_used_fallback:
            QMessageBox.warning(
                self,
                "强制回写（高风险）",
                "你已开启“强制回写”。\n\n"
                "兼容模式表示原始类缺失，当前对象含降级占位类型；强行保存后，"
                "原始结构可能无法被其它程序正确读取。\n\n"
                "强烈建议：优先用“另存为...”保存到新文件，并保留原文件备份。",
            )
        self._refresh_path_label()
        self._update_edit_action_state()

    def _current_file_path(self) -> str:
        return self._path_label.toolTip().strip()

    @staticmethod
    def _tree_item_path(item: QTreeWidgetItem | None) -> tuple[str, ...] | None:
        if item is None:
            return None
        parts: list[str] = []
        cur: QTreeWidgetItem | None = item
        while cur is not None:
            parts.append(cur.text(0))
            cur = cur.parent()
        return tuple(reversed(parts))

    def _load_detail_presets(self) -> list[dict[str, Any]]:
        try:
            if _DETAIL_PRESET_FILE.is_file():
                data = json.loads(_DETAIL_PRESET_FILE.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    presets = [_normalize_detail_preset(item) for item in data]
                    if presets:
                        return presets
        except Exception:
            traceback.print_exc()
            _log_exception("读取完整预设失败")
        return _clone_default_detail_presets()

    def _save_detail_presets(self) -> None:
        try:
            _DETAIL_PRESET_FILE.parent.mkdir(parents=True, exist_ok=True)
            _DETAIL_PRESET_FILE.write_text(
                json.dumps(self._detail_presets, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            traceback.print_exc()
            _log_exception("保存完整预设失败")

    def _load_search_field_presets(self) -> list[str]:
        try:
            if _SEARCH_FIELD_PRESET_FILE.is_file():
                data = json.loads(_SEARCH_FIELD_PRESET_FILE.read_text(encoding="utf-8"))
                return _normalize_search_field_list(data)
        except Exception:
            traceback.print_exc()
            _log_exception("读取字段名设置失败")
        return []

    def _save_search_field_presets(self) -> None:
        try:
            _SEARCH_FIELD_PRESET_FILE.parent.mkdir(parents=True, exist_ok=True)
            _SEARCH_FIELD_PRESET_FILE.write_text(
                json.dumps(self._custom_search_fields, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            traceback.print_exc()
            _log_exception("保存字段名设置失败")

    def _refresh_detail_preset_combo(self, selected_name: str | None = None) -> None:
        combo = getattr(self, "_detail_preset_combo", None)
        if not isinstance(combo, QComboBox):
            return
        current_name = selected_name or combo.currentText()
        combo.blockSignals(True)
        combo.clear()
        target_idx = 0
        for idx, preset in enumerate(self._detail_presets):
            name = str(preset.get("name") or f"预设{idx+1}")
            combo.addItem(name)
            if name == current_name:
                target_idx = idx
        combo.setCurrentIndex(target_idx)
        combo.blockSignals(False)

    def _current_detail_preset(self) -> dict[str, Any]:
        combo = getattr(self, "_detail_preset_combo", None)
        idx = combo.currentIndex() if isinstance(combo, QComboBox) else -1
        if 0 <= idx < len(self._detail_presets):
            return _normalize_detail_preset(self._detail_presets[idx])
        if self._detail_presets:
            return _normalize_detail_preset(self._detail_presets[0])
        return _normalize_detail_preset(_DEFAULT_DETAIL_PRESETS[0])

    def _current_subtree_level(self) -> int:
        combo = getattr(self, "_subtree_level_combo", None)
        raw = combo.currentText().strip() if isinstance(combo, QComboBox) else ""
        try:
            return max(1, min(50, int(raw)))
        except ValueError:
            return 12

    def _on_edit_detail_preset(self) -> None:
        current = self._current_detail_preset()
        dlg = _DetailPresetEditorDialog(self, current)
        if dlg.exec() != int(QDialog.DialogCode.Accepted):
            return
        combo = getattr(self, "_detail_preset_combo", None)
        idx = combo.currentIndex() if isinstance(combo, QComboBox) else 0
        if dlg.delete_requested:
            if len(self._detail_presets) <= 1:
                self._status.showMessage("至少保留一个完整预设", 3000)
                return
            if 0 <= idx < len(self._detail_presets):
                self._detail_presets.pop(idx)
                self._save_detail_presets()
                self._refresh_detail_preset_combo()
                self._refresh_detail_from_current_item()
            return

        updated = dlg.build_preset()
        if 0 <= idx < len(self._detail_presets):
            self._detail_presets[idx] = updated
        else:
            self._detail_presets.append(updated)
        self._save_detail_presets()
        self._refresh_detail_preset_combo(str(updated.get("name") or ""))
        self._refresh_detail_from_current_item()

    def _refresh_detail_from_current_item(self) -> None:
        """下拉框变化时，重绘当前选中项的右侧详情。"""
        try:
            it = self._tree.currentItem()
            if it is None:
                return
            self._on_item_selected(it, None)
        except Exception:
            traceback.print_exc()
            _log_exception("刷新详情失败")

    def _on_edit_search_fields(self) -> None:
        dlg = _SearchFieldSettingsDialog(self, self._custom_search_fields)
        if dlg.exec() != int(QDialog.DialogCode.Accepted):
            return
        self._custom_search_fields = dlg.build_fields()
        self._save_search_field_presets()
        self._rebuild_search_field_combo()
        self._status.showMessage("字段名设置已更新", 3000)

    def _apply_detail_preview_visibility(self, *, show_text: bool, show_tree: bool) -> None:
        self._detail.setVisible(bool(show_text))
        tree = self._detail_tree
        if isinstance(tree, QTreeWidget):
            tree.setVisible(bool(show_tree))
        splitter = self._detail_splitter
        if isinstance(splitter, QSplitter):
            if show_text and show_tree:
                splitter.setSizes([340, 260])
            elif show_text:
                splitter.setSizes([1, 0])
            elif show_tree:
                splitter.setSizes([0, 1])

    def _set_detail_tree_message(self, text: str) -> None:
        tree = self._detail_tree
        if not isinstance(tree, QTreeWidget):
            return
        preset = self._current_detail_preset()
        fields = preset.get("tree_columns")
        leaf_fields = [f for f in (fields or ["name", "id"]) if f not in ("children", "categories")]
        if not leaf_fields:
            leaf_fields = ["name", "id"]
        if len(leaf_fields) == 1:
            leaf_fields.append("id" if leaf_fields[0] != "id" else "")
        tree.clear()
        tree.setHeaderLabels([leaf_fields[0], leaf_fields[1]])
        QTreeWidgetItem(tree, [text, ""])
        tree.expandAll()

    def _build_shallow_preview_text(self, value: Any, allowed_fields: set[str] | None = None) -> str:
        shallow: dict = {}
        for k, v in _iter_children(value):
            key_s = str(k)
            if allowed_fields is not None and key_s not in allowed_fields:
                continue
            shallow[key_s] = {"value": _node_label(v), "type": _type_label(v)}
        return json.dumps(shallow, ensure_ascii=False, indent=2, default=str)

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
        """字段名菜单只显示固定项和用户在设置中配置的自定义项（支持多选）。"""
        menu = self._search_field_menu
        btn = self._search_field_btn
        if not isinstance(menu, QMenu) or not isinstance(btn, QToolButton):
            return

        ordered_fields: list[str] = []
        seen: set[str] = set()
        for k in _SEARCH_FIXED_FIELDS:
            if k not in seen:
                ordered_fields.append(k)
                seen.add(k)
        for k in self._custom_search_fields:
            if k not in seen:
                ordered_fields.append(k)
                seen.add(k)

        # 清理已选，避免保留不存在的字段
        self._selected_search_fields = {
            f for f in self._selected_search_fields if (f in seen or f == _SEARCH_ALL_FIELDS)
        }
        if not self._selected_search_fields:
            self._selected_search_fields = {_SEARCH_DEFAULT_FIELD}

        menu.clear()
        for f in ordered_fields:
            act = menu.addAction(f)
            act.setCheckable(True)
            act.setChecked(f in self._selected_search_fields)
            act.triggered.connect(lambda _checked=False, name=f: self._on_toggle_search_field(name))

        self._update_search_field_button_text()

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
        fields = self._current_search_fields()
        needle = self._search_line.text()
        if fields is None and not needle.strip():
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
            fields,
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

    def _restore_status_path(self) -> None:
        """恢复状态栏显示当前打开文件的完整路径；未加载时清空。"""
        cur = self._path_label.toolTip() if hasattr(self, "_path_label") else ""
        if cur:
            self._status.showMessage(cur, 0)
        else:
            self._status.clearMessage()

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
        cur_path = self._path_label.toolTip() if hasattr(self, "_path_label") else ""
        for path in self._history.all():
            row = _HistoryRow(path, self)
            missing = not Path(path).exists()
            is_current = bool(cur_path) and Path(path) == Path(cur_path)
            row.apply_style(selected=is_current, missing=missing)
            if is_current:
                self._history_selected_row = row
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
            "数据文件 (*.pkl *.fory);;Pickle 文件 (*.pkl);;Fory 文件 (*.fory);;所有文件 (*)",
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
            self, "选择数据文件", "", "数据文件 (*.pkl *.fory);;Pickle 文件 (*.pkl);;Fory 文件 (*.fory);;所有文件 (*)"
        )
        if path:
            self._load_file(path)

    def _on_reload(self) -> None:
        path = self._path_label.toolTip()
        if path:
            self._load_file(path)

    def _on_restart_app(self) -> None:
        """重启当前程序进程。"""
        try:
            args = list(sys.argv)
            # 没带参数时，尽量把当前已打开的文件带上（方便“重启后继续看同一份 pkl”）
            if len(args) <= 1:
                tip = self._path_label.toolTip()
                if tip and Path(tip).exists():
                    args.append(tip)
            cmd = [sys.executable] + args
            subprocess.Popen(cmd, close_fds=True)
        except Exception as e:
            traceback.print_exc()
            _log_exception("重启失败")
            self._status.showMessage(f"重启失败: {e}", 4500)
            return
        QApplication.quit()

    def _on_replace_all(self) -> None:
        if self._current_obj is None:
            QMessageBox.information(self, "无法替换", "请先加载 PKL 文件。")
            self._status.showMessage("请先加载 PKL 文件", 3000)
            return
        if self._current_used_fallback:
            if not self._force_writeback:
                QMessageBox.warning(
                    self,
                    "无法替换并保存",
                    "当前 PKL 是以兼容模式打开的（原始类缺失），默认不允许回写。\n\n"
                    "如果你确定要强制替换并保存，请先勾选工具栏的“强制回写”（高风险，建议只用“另存为...”）。",
                )
                return
        replace_line = self._replace_line
        if not isinstance(replace_line, QLineEdit):
            QMessageBox.critical(self, "无法替换", "替换输入框初始化失败。")
            self._status.showMessage("替换输入框初始化失败", 3000)
            return
        fields = self._current_search_fields()
        needle = self._search_line.text()
        replacement = replace_line.text()
        if fields is None and not needle:
            QMessageBox.information(self, "无法替换", "请先输入要替换的搜索文本。")
            self._status.showMessage("请先输入要替换的搜索文本", 3000)
            return
        if not needle:
            QMessageBox.information(
                self,
                "无法替换",
                "指定字段替换时也需要提供搜索文本。",
            )
            self._status.showMessage("指定字段替换时也需要提供搜索文本", 3000)
            return

        progress = QProgressDialog("正在替换文本，请稍候...", None, 0, 0, self)
        progress.setWindowTitle("替换中")
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setMinimumDuration(0)
        progress.setCancelButton(None)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.show()

        old_btn_text = ""
        if isinstance(self._replace_btn, QPushButton):
            old_btn_text = self._replace_btn.text()
            self._replace_btn.setText("替换中...")
            self._replace_btn.setEnabled(False)
        self._status.showMessage("正在替换文本...", 0)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        QApplication.processEvents()

        try:
            selected_path = self._tree_item_path(self._tree.currentItem())
            new_root, values_changed, total_replacements = _replace_strings_in_object(
                self._current_obj,
                fields=fields,
                needle=needle,
                replacement=replacement,
            )
            if values_changed <= 0:
                QMessageBox.information(
                    self,
                    "未找到替换项",
                    "未找到可替换的字符串值。\n如果内容在嵌套字典里，建议先试试字段选择“【全部字段】”。",
                )
                self._status.showMessage("未找到可替换的字符串值", 4000)
                return

            self._current_obj = new_root
            self._current_dirty = True
            self._refresh_tree(self._current_obj)
            if selected_path:
                item = self._find_tree_item_for_path(selected_path)
                if item is not None:
                    self._ensure_tree_item_visible(item)
            self._refresh_path_label()
            self._update_edit_action_state()
            self._status.showMessage(
                f"替换完成：共修改 {values_changed} 个字符串值，命中 {total_replacements} 次；请记得保存",
                6000,
            )
            QMessageBox.information(
                self,
                "替换完成",
                f"共修改 {values_changed} 个字符串值，命中 {total_replacements} 次。\n如果结果符合预期，请继续点击“保存”或“另存为...”。",
            )
        finally:
            if isinstance(self._replace_btn, QPushButton):
                self._replace_btn.setText(old_btn_text or "替换全部")
            self._update_edit_action_state()
            progress.close()
            QApplication.restoreOverrideCursor()

    def _on_toggle_search_field(self, name: str) -> None:
        # 特殊项：全部字段互斥
        if name == _SEARCH_ALL_FIELDS:
            self._selected_search_fields = {_SEARCH_ALL_FIELDS}
        else:
            if _SEARCH_ALL_FIELDS in self._selected_search_fields:
                self._selected_search_fields.discard(_SEARCH_ALL_FIELDS)
            if name in self._selected_search_fields:
                self._selected_search_fields.discard(name)
            else:
                self._selected_search_fields.add(name)
        if not self._selected_search_fields:
            self._selected_search_fields = {_SEARCH_DEFAULT_FIELD}
        self._update_search_field_button_text()

    def _update_search_field_button_text(self) -> None:
        btn = self._search_field_btn
        if not isinstance(btn, QToolButton):
            return
        if _SEARCH_ALL_FIELDS in self._selected_search_fields:
            btn.setText(_SEARCH_ALL_FIELDS)
            return
        items = sorted(self._selected_search_fields, key=lambda s: (s.lower(), s))
        if not items:
            btn.setText(_SEARCH_DEFAULT_FIELD)
            return
        if len(items) <= 2:
            btn.setText(", ".join(items))
            return
        btn.setText(f"{items[0]}, {items[1]} +{len(items) - 2}")

    def _current_search_fields(self) -> set[str] | None:
        """None 表示【全部字段】。"""
        if _SEARCH_ALL_FIELDS in self._selected_search_fields:
            return None
        return set(self._selected_search_fields)

    def _save_to_file(self, path: str) -> bool:
        """根据扩展名选择序列化方式保存当前对象到文件。"""
        if self._current_obj is None:
            self._status.showMessage("没有可保存的对象", 3000)
            return False

        target = Path(path)
        ext = target.suffix.lower()
        is_fory = ext == ".fory"

        # 如果目标是 .fory 但 pyfory 不可用，提前拦截
        if is_fory and not HAS_FORY:
            QMessageBox.warning(
                self,
                "无法保存",
                "pyfory 未安装，无法保存为 Fory 格式。\n\n"
                "请执行: pip install pyfory",
            )
            return False

        # 兼容模式保护（仅 pickle 格式需要检查）
        if not is_fory and self._current_used_fallback:
            if not self._force_writeback:
                QMessageBox.warning(
                    self,
                    "无法保存",
                    "当前 PKL 是以兼容模式打开的（原始类缺失），默认不允许回写。\n\n"
                    "如需强制保存，请先勾选“强制回写”（高风险，建议只用“另存为...”）。",
                )
                return False
            ans = QMessageBox.question(
                self,
                "确认强制回写？",
                "你正在兼容模式下强制保存。\n\n"
                "这可能破坏原始数据结构，导致其它程序无法正常读取。\n\n"
                "是否继续？（建议优先另存为）",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return False

        try:
            obj_to_dump = self._current_obj

            if is_fory:
                # Fory 序列化 —— 先将动态占位类型转为纯内置类型，
                # 避免 pyfory 序列化自定义类的模块路径导致加载时 import 失败
                obj_to_dump = _make_portable_pickle_data(obj_to_dump)
                fory = pyfory.Fory(xlang=False, ref=True, strict=False)
                payload = fory.dumps(obj_to_dump)
                fmt_label = "Fory 格式"
            else:
                # Pickle 序列化（含兼容模式可移植选项）
                if self._current_used_fallback:
                    mode = QMessageBox.question(
                        self,
                        "保存方式",
                        "当前处于兼容模式。\n\n"
                        "Yes：保存为【可移植 PKL（无依赖）】——任何程序都能反序列化，但类型会变成普通 dict/list 数据。\n"
                        "No：保存为【占位类 PKL】——可能依赖本工具模块/缺失类型模块。\n\n"
                        "是否选择可移植保存？",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    )
                    if mode == QMessageBox.StandardButton.Yes:
                        obj_to_dump = _make_portable_pickle_data(self._current_obj)
                payload = pickle.dumps(obj_to_dump, protocol=pickle.HIGHEST_PROTOCOL)
                fmt_label = "Pickle 格式"

            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                backup_path = target.with_name(target.name + ".bak")
                backup_path.write_bytes(target.read_bytes())
            temp_path = target.with_name(target.name + ".tmp")
            temp_path.write_bytes(payload)
            temp_path.replace(target)
        except Exception as e:
            traceback.print_exc()
            _log_exception(f"保存文件失败: {path}")
            QMessageBox.critical(self, "保存失败", f"保存文件失败：\n{e}")
            return False

        resolved_path = str(target.resolve())
        self._history.push(resolved_path)
        self._refresh_history_list()
        self._path_label.setToolTip(resolved_path)
        self._current_dirty = False
        self._refresh_path_label()
        self._update_edit_action_state()
        self._status.showMessage(f"已保存: {resolved_path} ({fmt_label})", 5000)
        return True

    def _save_current_obj_to_path(self, path: str) -> bool:
        """向后兼容：旧调用点仍使用此方法名。"""
        return self._save_to_file(path)

    def _on_save(self) -> None:
        path = self._current_file_path()
        if not path:
            self._on_save_as()
            return
        self._save_to_file(path)

    def _on_save_as(self) -> None:
        current_path = self._current_file_path()
        initial_path = current_path or ""
        filters = "Pickle 文件 (*.pkl);;Fory 文件 (*.fory);;所有文件 (*)"
        path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "另存为",
            initial_path,
            filters,
        )
        if path:
            self._save_to_file(path)

    def _load_file(self, path: str) -> None:
        if self._load_thread and self._load_thread.isRunning():
            self._load_thread.quit()
            self._load_thread.wait(500)

        self._stop_deep_search_thread()
        self._stop_keys_thread()

        self._history.push(path)
        self._refresh_history_list()

        self._current_obj = None
        self._current_used_fallback = False
        self._current_dirty = False
        self._loading_path = path
        self._dot_count = 0
        self._dot_timer.start()
        self._status.showMessage(f"加载中: {Path(path).name} .", 0)
        self._tree.clear()
        self._detail.clear()
        self._set_detail_tree_message("等待选择节点")
        self._path_label.setToolTip(path)
        self._refresh_path_label()
        self._update_edit_action_state()

        thread = _LoadThread(path)
        thread.done.connect(lambda obj, t, fallback, fmt: self._on_loaded(obj, t, path, fallback, fmt))
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
        QMessageBox.critical(self, "加载失败", f"无法加载文件：\n{msg}")

    def _on_loaded(self, obj: Any, type_name: str, path: str, used_fallback: bool,
                    fmt_label: str = "Pickle") -> None:
        self._dot_timer.stop()
        self._current_obj = obj
        self._current_used_fallback = used_fallback
        self._current_dirty = False
        self._path_label.setToolTip(path)
        self._refresh_path_label()
        self._update_edit_action_state()
        self._status.showMessage("构建树中...", 0)
        QApplication.processEvents()
        self._refresh_tree(obj)
        suffix = " | 已使用兼容模式(缺失类已降级)" if used_fallback else ""
        self._status.showMessage(f"{path}  |  格式: {fmt_label}  |  类型: {type_name}{suffix}", 0)
        self._refresh_history_list()

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

    def _on_item_double_clicked(self, item: QTreeWidgetItem, _column: int) -> None:
        if _tree_expand_more_item(self._tree, item):
            self._status.showMessage("已加载更多条目", 2000)
            return

    def _on_item_selected(self, current: QTreeWidgetItem | None, _prev: QTreeWidgetItem | None) -> None:
        if current is None:
            return
        cfg = self._current_detail_preset()
        field_list = cfg.get("fields")
        allowed_fields = None if field_list is None else set(field_list)
        max_depth = self._current_subtree_level()
        max_nodes = 20_000
        self._apply_detail_preview_visibility(
            show_text=bool(cfg.get("show_text", True)),
            show_tree=bool(cfg.get("show_tree", True)),
        )
        value = current.data(0, Qt.ItemDataRole.UserRole)
        if value is None or value == _PLACEHOLDER:
            if bool(cfg.get("show_text", True)):
                self._detail.setPlainText(current.text(1))
            self._set_detail_tree_message("当前选择不是目录节点")
            return
        if isinstance(value, tuple) and len(value) == 3 and value[0] == "__more__":
            if bool(cfg.get("show_text", True)):
                self._detail.setPlainText("(more items placeholder)")
            self._set_detail_tree_message("占位节点不支持目录树预览")
            return
        try:
            # 目录节点使用真正的递归 JSON 预览，直接展开 children/categories
            if _is_tree_node_like(value):
                if bool(cfg.get("show_text", True)):
                    if cfg.get("text_mode") == "shallow":
                        text = self._build_shallow_preview_text(value, allowed_fields=allowed_fields)
                    else:
                        preview = _build_recursive_preview_data(
                            value,
                            max_depth=max_depth,
                            max_nodes=max_nodes,
                            allowed_fields=allowed_fields,
                        )
                        text = json.dumps(preview, ensure_ascii=False, indent=2, default=str)
                    if len(text) > _DETAIL_MAX_CHARS:
                        text = text[:_DETAIL_MAX_CHARS] + f"\n\n... (truncated, total {len(text)} chars)"
                    self._detail.setPlainText(text)
                tree = self._detail_tree
                if bool(cfg.get("show_tree", True)) and isinstance(tree, QTreeWidget):
                    _populate_detail_subtree_widget(
                        tree,
                        value,
                        max_depth=max_depth,
                        max_nodes=max_nodes,
                        display_fields=cfg.get("tree_columns"),
                    )
                return

            if bool(cfg.get("show_text", True)):
                text = self._build_shallow_preview_text(value, allowed_fields=allowed_fields)
                if len(text) > _DETAIL_MAX_CHARS:
                    text = text[:_DETAIL_MAX_CHARS] + f"\n\n... (truncated, total {len(text)} chars)"
                self._detail.setPlainText(text)
            self._set_detail_tree_message("当前选择不是目录节点")
        except Exception as e:
            traceback.print_exc()
            _log_exception("序列化详情视图失败")
            if bool(cfg.get("show_text", True)):
                self._detail.setPlainText(f"序列化失败: {e}\n\n{current.text(1)}")
            self._set_detail_tree_message("目录树预览失败")


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

