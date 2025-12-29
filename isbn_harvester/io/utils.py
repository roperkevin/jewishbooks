from __future__ import annotations

import os
import tempfile


def atomic_write_csv(write_fn, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    d = os.path.dirname(out_path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, newline="", encoding="utf-8") as tf:
        tmp_path = tf.name
    try:
        write_fn(tmp_path)
        os.replace(tmp_path, out_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def atomic_write_text(write_fn, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    d = os.path.dirname(out_path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, encoding="utf-8") as tf:
        tmp_path = tf.name
    try:
        write_fn(tmp_path)
        os.replace(tmp_path, out_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
