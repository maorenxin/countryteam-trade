#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""持仓变化总览页面"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.data_loader import (
    get_quarter_pairs, compute_quarter_change, get_quarterly_new_counts,
    get_quarter_stock_counts, get_data_update_time, get_sparkline_data,
    is_quarter_complete,
)

# --- 标题 + 更新时间 ---
col_title, col_time = st.columns([3, 1])
with col_title:
    st.title("持仓变化总览")
with col_time:
    st.markdown(f"<div style='text-align:right;padding-top:28px;color:gray;font-size:14px;'>数据更新: {get_data_update_time()}</div>", unsafe_allow_html=True)

# --- 季度选择器 ---
pairs = get_quarter_pairs()
if not pairs:
    st.error("没有足够的有效季度数据")
    st.stop()

stock_counts = get_quarter_stock_counts()
pair_labels = []
for p, l in pairs:
    l_count = stock_counts.get(l, '?')
    pair_labels.append(f"{p} → {l} ({l_count}只)")

selected_idx = st.selectbox("选择季度对", range(len(pair_labels)), format_func=lambda i: pair_labels[i])
prev_q, latest_q = pairs[selected_idx]

# --- 计算变化 ---
merged = compute_quarter_change(latest_q, prev_q)
sparklines = get_sparkline_data()
latest_complete = is_quarter_complete(latest_q)

if not latest_complete:
    st.info(f"{latest_q} 季报尚未完整披露，仅展示已披露持仓的变化，未披露股票不计入退出。")

new_entries = merged[merged['_merge'] == 'left_only']
right_only = merged[merged['_merge'] == 'right_only']
added = merged[(merged['流通市值变化'] > 0) & (merged['_merge'] == 'both')].sort_values('流通市值变化', ascending=False)
reduced = merged[(merged['流通市值变化'] < 0) & (merged['_merge'] == 'both')].sort_values('流通市值变化', ascending=True)
add_amount = added['流通市值变化'].sum() / 1e8
reduce_amount = reduced['流通市值变化'].sum() / 1e8
new_value = new_entries['总流通市值'].sum() / 1e8

if latest_complete:
    exited = right_only
    exit_value = exited['上季总流通市值'].sum() / 1e8
    net_amount = merged['流通市值变化'].sum() / 1e8
else:
    exited = pd.DataFrame(columns=merged.columns)
    exit_value = 0
    # 不完整季度：净加仓只算已披露部分（排除 right_only 的虚假减值）
    net_amount = merged[merged['_merge'] != 'right_only']['流通市值变化'].sum() / 1e8

# --- KPI 卡片（3+2 布局，手机端更友好）---
c1, c2, c3 = st.columns(3)
c1.metric("新进股票数", f"{len(new_entries)}", delta=f"+{new_value:,.2f}亿")
c2.metric("加仓股票数", f"{len(added)}", delta=f"+{add_amount:,.2f}亿")
c3.metric("减仓股票数", f"{len(reduced)}", delta=f"{reduce_amount:,.2f}亿")
c4, c5, _ = st.columns(3)
if latest_complete:
    c4.metric("退出股票数", f"{len(exited)}", delta=f"-{exit_value:,.2f}亿")
else:
    c4.metric("未披露股票数", f"{len(right_only)}", delta="季报更新中", delta_color="off")
c5.metric("净加仓金额(亿)", f"{net_amount:,.2f}", delta=f"{net_amount:,.2f}亿", delta_color="normal")


def add_sparkline(df):
    """给 DataFrame 添加走势列"""
    df['国家队持仓走势'] = df['股票代码'].map(lambda c: sparklines.get(c, []))
    return df


# --- 加仓 TOP 表格 ---
st.subheader("加仓 TOP", divider="green")
added_display = added.copy()
added_display['持有机构列表'] = added_display['持有机构列表'].fillna('')
added_display['流通市值变化(亿)'] = added_display['流通市值变化'] / 1e8
added_display['机构数变化_fmt'] = added_display['机构数变化'].astype(int).map('{:+d}'.format)
added_display['当前持有机构数'] = added_display['持有机构数'].astype(int)
added_display = added_display.sort_values('流通市值变化(亿)', ascending=False)
added_display = add_sparkline(added_display)
st.dataframe(
    added_display[['股票代码', '股票简称', '机构数变化_fmt', '流通市值变化(亿)', '当前持有机构数', '持有机构列表', '国家队持仓走势']].rename(
        columns={'机构数变化_fmt': '机构数变化'}
    ),
    use_container_width=True,
    hide_index=True,
    height=400,
    column_config={
        '流通市值变化(亿)': st.column_config.NumberColumn('流通市值变化(亿)', format='%.2f'),
        '国家队持仓走势': st.column_config.LineChartColumn('国家队持仓走势', width='small'),
    },
)

# --- 新进股票 ---
st.subheader(f"新进股票 ({len(new_entries)} 只)", divider="blue")
ne = new_entries.copy()
ne['持有机构列表'] = ne['持有机构列表'].fillna('')
ne['总流通市值(亿)'] = ne['总流通市值'] / 1e8
ne['持有机构数'] = ne['持有机构数'].astype(int)
ne = ne.sort_values('总流通市值(亿)', ascending=False)
ne = add_sparkline(ne)
st.dataframe(
    ne[['股票代码', '股票简称', '持有机构数', '总流通市值(亿)', '持有机构列表', '国家队持仓走势']],
    use_container_width=True,
    hide_index=True,
    height=400,
    column_config={
        '总流通市值(亿)': st.column_config.NumberColumn('总流通市值(亿)', format='%.2f'),
        '国家队持仓走势': st.column_config.LineChartColumn('国家队持仓走势', width='small'),
    },
)

# --- 减仓 TOP 表格 ---
st.subheader("减仓 TOP", divider="red")
reduced_display = reduced.copy()
reduced_display['持有机构列表'] = reduced_display['持有机构列表'].fillna('')
reduced_display['流通市值变化(亿)'] = reduced_display['流通市值变化'] / 1e8
reduced_display['机构数变化_fmt'] = reduced_display['机构数变化'].astype(int).map('{:+d}'.format)
reduced_display['当前持有机构数'] = reduced_display['持有机构数'].astype(int)
reduced_display = reduced_display.sort_values('流通市值变化(亿)', ascending=True)
reduced_display = add_sparkline(reduced_display)
st.dataframe(
    reduced_display[['股票代码', '股票简称', '机构数变化_fmt', '流通市值变化(亿)', '当前持有机构数', '持有机构列表', '国家队持仓走势']].rename(
        columns={'机构数变化_fmt': '机构数变化'}
    ),
    use_container_width=True,
    hide_index=True,
    height=400,
    column_config={
        '流通市值变化(亿)': st.column_config.NumberColumn('流通市值变化(亿)', format='%.2f'),
        '国家队持仓走势': st.column_config.LineChartColumn('国家队持仓走势', width='small'),
    },
)

# --- 退出/未披露股票 ---
if latest_complete:
    st.subheader(f"退出股票 ({len(exited)} 只)", divider="orange")
    ex = exited.copy()
    ex['上季总流通市值(亿)'] = ex['上季总流通市值'] / 1e8
    ex['上季持有机构数'] = ex['上季持有机构数'].astype(int)
    ex = ex.sort_values('上季总流通市值(亿)', ascending=False)
    ex = add_sparkline(ex)
    st.dataframe(
        ex[['股票代码', '股票简称', '上季持有机构数', '上季总流通市值(亿)', '国家队持仓走势']],
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            '上季总流通市值(亿)': st.column_config.NumberColumn('上季总流通市值(亿)', format='%.2f'),
            '国家队持仓走势': st.column_config.LineChartColumn('国家队持仓走势', width='small'),
        },
    )
else:
    st.subheader(f"未披露股票 ({len(right_only)} 只)", divider="gray")
    st.caption("以下股票上季度有持仓，但本季度尚未披露季报，待后续更新。")
    uo = right_only.copy()
    uo['上季总流通市值(亿)'] = uo['上季总流通市值'] / 1e8
    uo['上季持有机构数'] = uo['上季持有机构数'].astype(int)
    uo = uo.sort_values('上季总流通市值(亿)', ascending=False)
    uo = add_sparkline(uo)
    st.dataframe(
        uo[['股票代码', '股票简称', '上季持有机构数', '上季总流通市值(亿)', '国家队持仓走势']],
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            '上季总流通市值(亿)': st.column_config.NumberColumn('上季总流通市值(亿)', format='%.2f'),
            '国家队持仓走势': st.column_config.LineChartColumn('国家队持仓走势', width='small'),
        },
    )

# --- 季度趋势图（上下排列，手机端友好）---
st.divider()
st.subheader("季度持仓趋势")

report_counts, announce_counts = get_quarterly_new_counts()

fig_report = px.bar(
    report_counts, x='季度', y='持仓股票数',
    title='按财报季度（报告期）',
    labels={'持仓股票数': '持仓股票数', '季度': '财报季度'},
    text='持仓股票数',
)
fig_report.update_traces(textposition='outside', textfont_size=9)
fig_report.update_layout(xaxis_tickangle=-45, height=400, margin=dict(t=40, b=80))
st.plotly_chart(fig_report, use_container_width=True)

fig_announce = px.bar(
    announce_counts, x='季度', y='持仓股票数',
    title='按公告季度（公告日）',
    labels={'持仓股票数': '持仓股票数', '季度': '公告季度'},
    text='持仓股票数',
)
fig_announce.update_traces(textposition='outside', textfont_size=9)
fig_announce.update_layout(xaxis_tickangle=-45, height=400, margin=dict(t=40, b=80))
st.plotly_chart(fig_announce, use_container_width=True)

# --- 数据来源脚注 ---
st.divider()
st.caption(
    "数据来源：持仓数据来自[东方财富网股东明细](https://data.eastmoney.com/gdfx/)，"
    "行业分类来自[理杏仁](https://www.lixinger.com/)及[AkShare](https://akshare.akfamily.xyz/)（申万行业体系），"
    "日K线数据来自[东方财富行情接口](https://quote.eastmoney.com/)。"
)
