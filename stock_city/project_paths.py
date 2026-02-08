"""專案路徑集中管理。

目標：避免因為模組搬移導致 DB 路徑跟著變動。

規則：
- 專案根目錄 = stock_city/ 的上一層
- DB 固定放在 <root>/data/txf_ticks.db
"""

from __future__ import annotations

from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_data_dir() -> Path:
    return get_project_root() / "data"


def get_db_path() -> Path:
    return get_data_dir() / "txf_ticks.db"
