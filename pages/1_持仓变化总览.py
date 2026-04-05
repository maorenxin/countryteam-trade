#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""持仓变化总览页面"""

import streamlit as st
import pandas as pd
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.data_loader import get_quarter_pairs, compute_quarter_change

st.title("持仓变化总览")

# --- 季度选择器 ---
pairs = get_quarter_pairs()
if not pairs:
    st.error("没有足够的有效季度数据")
    st.stop()

pair_labels = [f"{p} → {l}" for p, l in pairs]
selected_idx = st.selectbox("选择季度对", range(len(pair_labels)), format_func=lambda i: pair_labels[i])
prev_q, latest_q = pairs[selected_idx]

# --- 计算变化 ---
merged = compute_quarter_change(latest_q, prev_q)

new_entries = merged[merged['_merge'] == 'left_only']
exited = merged[merged['_merge'] == 'right_only']
added = merged[(merged['流通市值变化'] > 0) & (merged['_merge'] == 'both')].sort_values('流通市值变化', ascending=False)
# 减仓仅包含持续持有但市值下降的股票（排除退出股票）
reduced = merged[(merged['流通市值变化'] < 0) & (merged['_merge'] == 'both')].sort_values('流通市值变化', ascending=True)
net_amount = merged['流通市值变化'].sum() / 1e8
add_amount = added['流通市值变化'].sum() / 1e8
reduce_amount = reduced['流通市值变化'].sum() / 1e8
exit_value = exited['上季总流通市值'].sum() / 1e8
new_value = new_entries['总流通市值'].sum() / 1e8

# --- KPI 卡片 ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("新进股票数", f"{len(new_entries)}", delta=f"+{new_value:,.2f}亿")
c2.metric("加仓股票数", f"{len(added)}", delta=f"+{add_amount:,.2f}亿")
c3.metric("减仓股票数", f"{len(reduced)}", delta=f"{reduce_amount:,.2f}亿")
c4.metric("退出股票数", f"{len(exited)}", delta=f"-{exit_value:,.2f}亿")
c5.metric("净加仓金额(亿)", f"{net_amount:,.2f}", delta=f"{net_amount:,.2f}亿", delta_color="normal")

# --- 搜索框 ---
search = st.text_input("搜索股票代码或简称", placeholder="输入代码或简称筛选...")


def filter_df(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query:
        return df
    q = query.strip()
    mask = df['股票代码'].astype(str).str.contains(q, na=False) | df['股票简称'].astype(str).str.contains(q, na=False)
    return df[mask]



# --- 加仓 TOP 表格 ---
st.subheader("加仓 TOP", divider="green")
added_display = filter_df(added, search).copy()
added_display['持有机构列表'] = added_display['持有机构列表'].fillna('')
added_display['流通市值变化_num'] = added_display['流通市值变化'] / 1e8
added_display['机构数变化_fmt'] = added_display['机构数变化'].astype(int).map('{:+d}'.format)
added_display['当前持有机构数'] = added_display['持有机构数'].astype(int)

st.dataframe(
    added_display[['股票代码', '股票简称', '机构数变化_fmt', '流通市值变化_num', '当前持有机构数', '持有机构列表']].rename(
        columns={'机构数变化_fmt': '机构数变化', '流通市值变化_num': '流通市值变化(亿)'}
    ),
    use_container_width=True,
    hide_index=True,
    column_config={
        '流通市值变化(亿)': st.column_config.NumberColumn(
            '流通市值变化(亿)', format='%.2f',
        ),
    },
)

# --- 减仓 TOP 表格 ---
st.subheader("减仓 TOP", divider="red")
reduced_display = filter_df(reduced, search).copy()
reduced_display['持有机构列表'] = reduced_display['持有机构列表'].fillna('')
reduced_display['流通市值变化_num'] = reduced_display['流通市值变化'] / 1e8
reduced_display['机构数变化_fmt'] = reduced_display['机构数变化'].astype(int).map('{:+d}'.format)
reduced_display['当前持有机构数'] = reduced_display['持有机构数'].astype(int)

st.dataframe(
    reduced_display[['股票代码', '股票简称', '机构数变化_fmt', '流通市值变化_num', '当前持有机构数', '持有机构列表']].rename(
        columns={'机构数变化_fmt': '机构数变化', '流通市值变化_num': '流通市值变化(亿)'}
    ),
    use_container_width=True,
    hide_index=True,
    column_config={
        '流通市值变化(亿)': st.column_config.NumberColumn(
            '流通市值变化(亿)', format='%.2f',
        ),
    },
)

# --- 新进/退出 折叠表格 ---
with st.expander(f"新进股票 ({len(new_entries)} 只)", expanded=False):
    ne = filter_df(new_entries, search).copy()
    ne['持有机构列表'] = ne['持有机构列表'].fillna('')
    ne['总流通市值(亿)'] = (ne['总流通市值'] / 1e8).map('{:,.2f}'.format)
    ne['持有机构数'] = ne['持有机构数'].astype(int)
    st.dataframe(
        ne[['股票代码', '股票简称', '持有机构数', '总流通市值(亿)', '持有机构列表']],
        use_container_width=True,
        hide_index=True,
    )

with st.expander(f"退出股票 ({len(exited)} 只)", expanded=False):
    ex = filter_df(exited, search).copy()
    ex['上季总流通市值(亿)'] = (ex['上季总流通市值'] / 1e8).map('{:,.2f}'.format)
    ex['上季持有机构数'] = ex['上季持有机构数'].astype(int)
    st.dataframe(
        ex[['股票代码', '股票简称', '上季持有机构数', '上季总流通市值(亿)']],
        use_container_width=True,
        hide_index=True,
    )
