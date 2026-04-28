# -*- coding: utf-8 -*-

name = "view_pkl_tool"
version = "999.0"
description = "PKL viewer with tolerant unpickling fallback"
authors = ["Lugwit Team"]

requires = [
    "python-3.12+<3.13",
    "pyfory-999.0-py3.12"
]

build_command = False
cachable = True
relocatable = True


def commands():
    env.PYTHONPATH.prepend("{root}/src")
    alias("view_pkl_tool", "python {root}/src/view_pkl_tool/main.py")

