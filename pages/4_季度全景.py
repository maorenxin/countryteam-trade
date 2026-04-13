#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""季度全景页面"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.data_loader import (
    get_valid_quarters, get_quarter_pairs, compute_quarter_change,
    load_industry_map, is_quarter_complete,
)

st.title("季度全景")

# --- 季度选择器 ---
pairs = get_quarter_pairs()
if not pairs:
    st.error("没有足够的有效季度数据")
    st.stop()

pair_labels = [f"{p} → {l}" for p, l in pairs]
selected_idx = st.selectbox("选择季度对", range(len(pair_labels)), format_func=lambda i: pair_labels[i])
prev_q, latest_q = pairs[selected_idx]

# --- 计算变化并关联行业 ---
merged = compute_quarter_change(latest_q, prev_q)
latest_complete = is_quarter_complete(latest_q)
if not latest_complete:
    st.info(f"{latest_q} 季报尚未完整披露，仅展示已披露持仓的变化。")
    merged = merged[merged['_merge'] != 'right_only']
industry_map = load_industry_map()
merged = merged.merge(industry_map, on='股票代码', how='left')
merged['行业'] = merged['行业'].fillna('未知行业')
merged['流通市值变化_亿'] = merged['流通市值变化'] / 1e8
merged['总流通市值_亿'] = merged['总流通市值'] / 1e8

# --- 气泡图 ---
st.subheader("行业持仓变化气泡图")
# 按行业聚合
ind_agg = merged.groupby('行业').agg(
    机构数变化=('机构数变化', 'sum'),
    流通市值变化_亿=('流通市值变化_亿', 'sum'),
    总流通市值_亿=('总流通市值_亿', 'sum'),
    股票数=('股票代码', 'count'),
).reset_index()
ind_agg['流通市值变化_abs'] = ind_agg['流通市值变化_亿'].abs()
ind_agg['方向'] = ind_agg['流通市值变化_亿'].apply(lambda x: '加仓' if x >= 0 else '减仓')

fig_bubble = px.scatter(
    ind_agg,
    x='行业',
    y='机构数变化',
    size='流通市值变化_abs',
    color='方向',
    color_discrete_map={'加仓': '#00a854', '减仓': '#f5222d'},
    hover_data={'总流通市值_亿': ':.2f', '流通市值变化_亿': ':.2f', '股票数': True, '流通市值变化_abs': False},
    labels={'机构数变化': '机构数变化(合计)', '行业': '行业'},
    title=f'行业持仓变化 ({prev_q} → {latest_q})',
)
fig_bubble.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig_bubble, use_container_width=True)

# --- 行业加仓排行柱状图 ---
st.subheader("行业净加仓排行")
ind_rank = ind_agg.sort_values('流通市值变化_亿', ascending=True)
colors = ['#00a854' if v >= 0 else '#f5222d' for v in ind_rank['流通市值变化_亿']]

fig_rank = px.bar(
    ind_rank,
    x='流通市值变化_亿',
    y='行业',
    orientation='h',
    labels={'流通市值变化_亿': '净加仓金额(亿)', '行业': '行业'},
    title=f'行业净加仓排行 ({prev_q} → {latest_q})',
    color='方向',
    color_discrete_map={'加仓': '#00a854', '减仓': '#f5222d'},
)
fig_rank.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
st.plotly_chart(fig_rank, use_container_width=True)

# --- 数据表 ---
st.subheader("持仓数据表")
table_data = merged.copy()
table_data['持有机构数'] = table_data['持有机构数'].astype(int)
table_data['机构数变化'] = table_data['机构数变化'].astype(int)
table_data['流通市值变化(亿)'] = table_data['流通市值变化_亿'].map('{:,.2f}'.format)
table_data['总流通市值(亿)'] = table_data['总流通市值_亿'].map('{:,.2f}'.format)

display_cols = ['股票代码', '股票简称', '行业', '持有机构数', '总流通市值(亿)', '机构数变化', '流通市值变化(亿)']
st.dataframe(
    table_data[display_cols],
    use_container_width=True,
    hide_index=True,
)

# --- CSV 导出 ---
csv_export = table_data[display_cols].to_csv(index=False).encode('utf-8-sig')
st.download_button(
    label="导出 CSV",
    data=csv_export,
    file_name=f"持仓全景_{latest_q}.csv",
    mime="text/csv",
)
