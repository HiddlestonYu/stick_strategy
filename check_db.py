#!/usr/bin/env python
import sqlite3
from stock_city.project_paths import get_db_path

db_path = get_db_path()
print(f"Database path: {db_path}")

conn = sqlite3.connect(str(db_path))
cursor = conn.cursor()

# 檢查表是否存在
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ticks'")
exists = cursor.fetchone()
print(f"ticks 表存在: {exists is not None}")

if exists:
    cursor.execute('SELECT COUNT(*) FROM ticks')
    total = cursor.fetchone()[0]
    print(f'總筆數: {total}')
    
    cursor.execute('SELECT COUNT(DISTINCT code) FROM ticks')
    codes = cursor.fetchone()[0]
    print(f'商品數: {codes}')
    
    if total > 0:
        cursor.execute('SELECT DISTINCT code FROM ticks LIMIT 5')
        code_list = [row[0] for row in cursor.fetchall()]
        print(f'商品代碼: {code_list}')
        
        cursor.execute('SELECT MIN(date(ts)), MAX(date(ts)) FROM ticks')
        dates = cursor.fetchone()
        print(f'日期範圍: {dates[0]} ~ {dates[1]}')
else:
    print("ticks 表不存在，數據庫可能為空")

conn.close()
