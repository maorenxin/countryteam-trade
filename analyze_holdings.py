#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分析国家队最新加仓情况"""

import os
import pandas as pd
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# 读取数据
df = pd.read_csv(
    os.path.join(DATA_DIR, 'raw', 'selenium_country_team_stock.csv'),
    dtype={'股票代码': str},
    parse_dates=['公告日', '报告期']
)
df = df.dropna(subset=['公告日', '报告期'])

# 对齐到季度
df['report_q'] = df['报告期'].dt.to_period('Q')

# 去重：同一股东+股票+季度，取最新报告期
df = (
    df.sort_values(['股东代码', '股票代码', '报告期'], ascending=[True, True, False])
      .drop_duplicates(subset=['股东代码', '股票代码', 'report_q'], keep='first')
)

# 找出数据完整的最近两个季度（排除记录数过少的季度）
q_counts = df.groupby('report_q').size()
valid_quarters = sorted(q_counts[q_counts >= 200].index)
latest_q = valid_quarters[-1]
prev_q = valid_quarters[-2] if len(valid_quarters) >= 2 else None

print(f"最新季度: {latest_q}")
print(f"上一季度: {prev_q}")
print(f"总记录数: {len(df)}")
print()

# 最新季度数据
latest = df[df['report_q'] == latest_q].copy()
prev = df[df['report_q'] == prev_q].copy() if prev_q else pd.DataFrame()

# === 透视表：每只股票在最新季度被多少国家队持有，总流通市值 ===
latest_summary = latest.groupby(['股票代码', '股票简称']).agg(
    持有机构数=('股东名称', 'nunique'),
    持有机构列表=('股东名称', lambda x: '、'.join(x.unique())),
    总持仓数量=('数量', 'sum'),
    总流通市值=('流通市值', 'sum'),
).reset_index()

# === 上一季度汇总 ===
if not prev.empty:
    prev_summary = prev.groupby(['股票代码', '股票简称']).agg(
        上季持有机构数=('股东名称', 'nunique'),
        上季总持仓数量=('数量', 'sum'),
        上季总流通市值=('流通市值', 'sum'),
    ).reset_index()

    # 合并
    merged = latest_summary.merge(prev_summary, on=['股票代码', '股票简称'], how='outer', indicator=True)

    # 计算变化
    for col in ['总持仓数量', '总流通市值', '持有机构数']:
        merged[col] = merged[col].fillna(0)
    for col in ['上季总持仓数量', '上季总流通市值', '上季持有机构数']:
        merged[col] = merged[col].fillna(0)

    merged['机构数变化'] = merged['持有机构数'] - merged['上季持有机构数']
    merged['持仓数量变化'] = merged['总持仓数量'] - merged['上季总持仓数量']
    merged['流通市值变化'] = merged['总流通市值'] - merged['上季总流通市值']
    merged['流通市值变化_亿'] = merged['流通市值变化'] / 1e8

    # 加仓 = 流通市值增加的
    added = merged[merged['流通市值变化'] > 0].sort_values('流通市值变化', ascending=False).copy()
    # 新进 = 上季度不存在的
    new_entries = merged[merged['_merge'] == 'left_only'].sort_values('总流通市值', ascending=False).copy()
    # 减仓
    reduced = merged[merged['流通市值变化'] < 0].sort_values('流通市值变化', ascending=True).copy()
    # 退出
    exited = merged[merged['_merge'] == 'right_only'].sort_values('上季总流通市值', ascending=False).copy()

    # 输出CSV
    output_dir = os.path.join(DATA_DIR, 'processed')
    merged.to_csv(os.path.join(output_dir, 'quarterly_change_analysis.csv'), index=False, encoding='utf-8-sig')

    # === 打印分析结果 ===
    print(f"{'='*80}")
    print(f"国家队持仓变化分析: {prev_q} → {latest_q}")
    print(f"{'='*80}")

    print(f"\n## 新进股票 ({len(new_entries)} 只)")
    print(f"{'股票代码':<10} {'股票简称':<10} {'持有机构数':>8} {'总流通市值(亿)':>14} {'持有机构'}")
    print('-' * 80)
    for _, row in new_entries.head(30).iterrows():
        print(f"{row['股票代码']:<10} {row['股票简称']:<10} {int(row['持有机构数']):>8} {row['总流通市值']/1e8:>14.2f} {row.get('持有机构列表','')}")

    print(f"\n## 加仓股票 TOP 30 (流通市值增加，共 {len(added)} 只)")
    print(f"{'股票代码':<10} {'股票简称':<10} {'机构数变化':>8} {'流通市值变化(亿)':>16} {'当前机构数':>8} {'持有机构'}")
    print('-' * 100)
    for _, row in added.head(30).iterrows():
        print(f"{row['股票代码']:<10} {row['股票简称']:<10} {int(row['机构数变化']):>+8} {row['流通市值变化']/1e8:>16.2f} {int(row['持有机构数']):>8} {row.get('持有机构列表','')}")

    print(f"\n## 减仓股票 TOP 20 (流通市值减少，共 {len(reduced)} 只)")
    print(f"{'股票代码':<10} {'股票简称':<10} {'机构数变化':>8} {'流通市值变化(亿)':>16}")
    print('-' * 60)
    for _, row in reduced.head(20).iterrows():
        print(f"{row['股票代码']:<10} {row['股票简称']:<10} {int(row['机构数变化']):>+8} {row['流通市值变化']/1e8:>16.2f}")

    print(f"\n## 退出股票 ({len(exited)} 只)")
    print(f"{'股票代码':<10} {'股票简称':<10} {'上季机构数':>8} {'上季流通市值(亿)':>16}")
    print('-' * 60)
    for _, row in exited.head(20).iterrows():
        print(f"{row['股票代码']:<10} {row['股票简称']:<10} {int(row['上季持有机构数']):>8} {row['上季总流通市值']/1e8:>16.2f}")

    # 汇总
    print(f"\n{'='*80}")
    print(f"汇总统计:")
    print(f"  最新季度持有股票数: {len(latest_summary)}")
    print(f"  新进: {len(new_entries)} 只")
    print(f"  加仓: {len(added)} 只, 合计加仓 {added['流通市值变化'].sum()/1e8:.2f} 亿")
    print(f"  减仓: {len(reduced)} 只, 合计减仓 {reduced['流通市值变化'].sum()/1e8:.2f} 亿")
    print(f"  退出: {len(exited)} 只")
else:
    print("只有一个季度的数据，无法比较变化")
