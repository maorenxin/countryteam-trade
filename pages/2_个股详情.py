#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""个股详情页面"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.data_loader import (
    get_all_stocks, get_stock_timeline, get_stock_institution_detail,
)

st.title("个股详情")

# --- 股票搜索 ---
all_stocks = get_all_stocks()
search_query = st.text_input("搜索股票（代码或简称）", placeholder="输入代码或简称...")

if search_query:
    q = search_query.strip()
    matches = all_stocks[
        all_stocks['股票代码'].str.contains(q, case=False, na=False)
        | all_stocks['股票简称'].str.contains(q, case=False, na=False)
    ]
else:
    matches = pd.DataFrame()

if search_query and matches.empty:
    st.warning("未找到匹配的股票")
    st.stop()

if not search_query:
    st.info("请输入股票代码或简称进行搜索")
    st.stop()

# 候选列表
options = [f"{row['股票代码']} {row['股票简称']}" for _, row in matches.head(20).iterrows()]
selected = st.selectbox("选择股票", options)
stock_code = selected.split(" ")[0]
stock_name = selected.split(" ", 1)[1]

st.subheader(f"{stock_code} {stock_name}")

# --- 持仓时间线 ---
timeline = get_stock_timeline(stock_code)
if timeline.empty:
    st.warning("该股票无持仓数据")
    st.stop()

timeline['总流通市值_亿'] = timeline['总流通市值'] / 1e8

fig_line = px.line(
    timeline,
    x='report_q_str',
    y='总流通市值_亿',
    markers=True,
    labels={'report_q_str': '季度', '总流通市值_亿': '总流通市值(亿)'},
    title='持仓流通市值时间线',
)
fig_line.update_yaxes(tickformat=',.2f')
fig_line.update_layout(hovermode='x unified')
st.plotly_chart(fig_line, use_container_width=True)

# --- 机构堆叠柱状图 ---
detail = get_stock_institution_detail(stock_code)
if not detail.empty:
    inst_q = detail.groupby(['report_q_str', '股东别称']).agg(
        流通市值=('流通市值', 'sum'),
    ).reset_index()
    inst_q['流通市值_亿'] = inst_q['流通市值'] / 1e8

    fig_bar = px.bar(
        inst_q.sort_values('report_q_str'),
        x='report_q_str',
        y='流通市值_亿',
        color='股东别称',
        labels={'report_q_str': '季度', '流通市值_亿': '流通市值(亿)', '股东别称': '机构'},
        title='持有机构变化',
    )
    fig_bar.update_yaxes(tickformat=',.2f')
    fig_bar.update_layout(barmode='stack', legend=dict(orientation='h', y=-0.3))
    st.plotly_chart(fig_bar, use_container_width=True)

# --- 持仓明细表 ---
st.subheader("持仓明细")
detail_table = detail[['report_q_str', '股东别称', '数量', '流通市值']].copy()
detail_table['流通市值(亿)'] = (detail_table['流通市值'] / 1e8).map('{:,.2f}'.format)
detail_table['数量(万股)'] = (detail_table['数量'] / 1e4).map('{:,.2f}'.format)
st.dataframe(
    detail_table[['report_q_str', '股东别称', '数量(万股)', '流通市值(亿)']].rename(
        columns={'report_q_str': '季度', '股东别称': '机构'}
    ),
    use_container_width=True,
    hide_index=True,
)
