#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国家队持仓追踪看板 — Streamlit 主入口"""

import streamlit as st

st.set_page_config(
    page_title="国家队持仓追踪",
    page_icon="📊",
    layout="wide",
)

homepage = st.Page("pages/1_持仓变化总览.py", title="持仓变化总览", icon="📈", default=True)
stock_detail = st.Page("pages/2_个股详情.py", title="个股详情", icon="🔍")
institution = st.Page("pages/3_国家队一览.py", title="国家队一览", icon="🏛️")
quarterly = st.Page("pages/4_季度全景.py", title="季度全景", icon="🌐")

pg = st.navigation([homepage, stock_detail, institution, quarterly])
pg.run()
