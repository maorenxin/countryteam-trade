#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国家队一览页面"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.data_loader import (
    get_quarter_pairs,
    get_institution_list, get_institution_holdings,
    get_institution_change, load_industry_map,
)

st.title("国家队一览")

# --- 季度选择 ---
pairs = get_quarter_pairs()
if not pairs:
    st.error("没有足够的有效季度数据")
    st.stop()

pair_labels = [f"{p} → {l}" for p, l in pairs]
selected_idx = st.selectbox("选择季度对", range(len(pair_labels)), format_func=lambda i: pair_labels[i])
prev_q, latest_q = pairs[selected_idx]

# --- 机构列表 ---
inst_list = get_institution_list(latest_q)
if inst_list.empty:
    st.error("当前季度无机构数据")
    st.stop()

inst_options = []
for _, row in inst_list.iterrows():
    label = f"{row['股东别称']}（{row['总流通市值']/1e8:,.2f}亿，{int(row['持仓股票数'])}只）"
    inst_options.append((row['股东别称'], label))

selected_inst = st.selectbox(
    "选择机构",
    range(len(inst_options)),
    format_func=lambda i: inst_options[i][1],
)
institution_name = inst_options[selected_inst][0]

st.subheader(institution_name)

# --- 当前持仓列表 ---
holdings = get_institution_holdings(institution_name, latest_q)
st.markdown(f"**当前持仓（{latest_q}）**")
if not holdings.empty:
    h_display = holdings.copy()
    h_display['流通市值(亿)'] = (h_display['流通市值'] / 1e8).map('{:,.2f}'.format)
    h_display['数量(万股)'] = (h_display['数量'] / 1e4).map('{:,.2f}'.format)
    st.dataframe(
        h_display[['股票代码', '股票简称', '数量(万股)', '流通市值(亿)']],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("该机构在当前季度无持仓数据")

# --- 持仓变化表 ---
st.markdown(f"**持仓变化（{prev_q} → {latest_q}）**")
change = get_institution_change(institution_name, latest_q, prev_q)
if not change.empty:
    ch = change.copy()
    ch['本季市值(亿)'] = (ch['本季市值'] / 1e8).map('{:,.2f}'.format)
    ch['上季市值(亿)'] = (ch['上季市值'] / 1e8).map('{:,.2f}'.format)
    ch['市值变化(亿)'] = (ch['市值变化'] / 1e8).map('{:,.2f}'.format)
    ch['数量变化(万股)'] = (ch['数量变化'] / 1e4).map('{:,.2f}'.format)
    st.dataframe(
        ch[['股票代码', '股票简称', '本季市值(亿)', '上季市值(亿)', '市值变化(亿)', '数量变化(万股)']],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("无变化数据")

# --- 行业分布饼图 ---
st.markdown(f"**行业分布（{latest_q}）**")
if not holdings.empty:
    industry_map = load_industry_map()
    h_with_ind = holdings.merge(industry_map, on='股票代码', how='left')
    h_with_ind['行业'] = h_with_ind['行业'].fillna('未知行业')
    ind_agg = h_with_ind.groupby('行业')['流通市值'].sum().reset_index()
    ind_agg = ind_agg.sort_values('流通市值', ascending=False)

    fig_pie = px.pie(
        ind_agg,
        values='流通市值',
        names='行业',
        title=f'{institution_name} 行业分布',
    )
    fig_pie.update_traces(textposition='inside', textinfo='label+percent')
    fig_pie.update_layout(showlegend=True)
    st.plotly_chart(fig_pie, use_container_width=True)
