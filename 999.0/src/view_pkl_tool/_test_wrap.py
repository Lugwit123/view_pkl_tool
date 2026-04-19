"""验证：无空格长文件名折行要用 QTextEdit + WrapAnywhere（QLabel 只在词边界换行）。
用法: python _test_wrap.py
"""
import sys
from PySide6.QtCore import Qt
from PySide6.QtGui import QTextOption
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QTextEdit,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

app = QApplication(sys.argv)
app.setStyle("Fusion")

win = QWidget()
win.setWindowTitle("WrapAnywhere vs QLabel（应看到多行折行）")
win.resize(260, 420)

layout = QVBoxLayout(win)

long_name = (
    "this_is_a_very_long_filename_without_spaces_that QLabel_will_not_break.pkl"
)

lbl_note = QTextEdit()
lbl_note.setReadOnly(True)
lbl_note.setPlainText(
    "上面模拟「整串无空格」。下面 QTextEdit + WrapAnywhere 应折行；"
    "若仍单行，多半是 Qt/样式环境问题。"
)
lbl_note.setMaximumHeight(72)
lbl_note.setFrameShape(QFrame.Shape.NoFrame)

te = QTextEdit()
te.setReadOnly(True)
te.setFrameShape(QFrame.Shape.NoFrame)
te.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
te.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
te.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
opt = QTextOption()
opt.setWrapMode(QTextOption.WrapMode.WrapAnywhere)
te.document().setDefaultTextOption(opt)
te.setPlainText(long_name)
te.document().setDocumentMargin(4)
te.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
doc_w = float(win.width() - 40)
te.document().setTextWidth(doc_w)
h = int(te.document().size().height()) + 14
te.setFixedHeight(max(h, 60))

layout.addWidget(lbl_note)
layout.addWidget(te)
layout.addStretch(1)

win.setStyleSheet("background: #252526; color: #ccc;")
win.show()
sys.exit(app.exec())
