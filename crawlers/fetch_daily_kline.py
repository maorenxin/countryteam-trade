#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量拉取持仓股票近1年日K数据，存储为 CSV 供 Streamlit sparkline 使用

用法: python3 crawlers/fetch_daily_kline.py
输出: data/raw/daily_kline.csv
"""

import os, json, time, subprocess
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
OUTPUT_PATH = os.path.join(DATA_DIR, 'raw', 'daily_kline.csv')


def fetch_kline_curl(stock_code: str, start_date: str, end_date: str) -> list:
    """通过 curl 子进程拉取日K数据（绕过 Python requests 的网络兼容性问题）"""
    market = '1' if stock_code.startswith('6') else '0'
    secid = f"{market}.{stock_code}"
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
        f"fields1=f1,f2,f3,f4,f5,f6&"
        f"fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116&"
        f"ut=7eea3edcaed734bea9cbfc24409ed989&"
        f"klt=101&fqt=1&secid={secid}&beg={start_date}&end={end_date}"
    )
    try:
        result = subprocess.run(
            ['curl', '-s', '--max-time', '10',
             '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
             '-H', 'Referer: https://quote.eastmoney.com/',
             url],
            capture_output=True, text=True, timeout=15,
        )
        if not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data.get('data', {}).get('klines', [])
    except Exception as e:
        print(f"  Error fetching {stock_code}: {e}")
        return []


def main():
    # 读取所有持仓股票代码
    raw_path = os.path.join(DATA_DIR, 'raw', 'selenium_country_team_stock.csv')
    df = pd.read_csv(raw_path, dtype={'股票代码': str})
    codes = sorted(df['股票代码'].unique())
    print(f"共 {len(codes)} 只股票需要拉取日K数据")

    # 计算日期范围：近1年
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    print(f"日期范围: {start_date} ~ {end_date}")

    rows = []
    success = 0
    for i, code in enumerate(codes):
        klines = fetch_kline_curl(code, start_date, end_date)
        if klines:
            for kline in klines:
                parts = kline.split(',')
                if len(parts) >= 6:
                    rows.append({
                        '股票代码': code,
                        '日期': parts[0],
                        '收盘价': float(parts[2]),
                    })
            success += 1
        if (i + 1) % 50 == 0:
            print(f"  进度: {i+1}/{len(codes)}, 成功: {success}")
        time.sleep(0.3)  # 限速

    result_df = pd.DataFrame(rows)
    result_df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8')
    print(f"\n完成! 成功 {success}/{len(codes)} 只, 共 {len(rows)} 条记录")
    print(f"输出: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
