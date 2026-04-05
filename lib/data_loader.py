#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据加载与处理模块"""

import os
import pandas as pd
import streamlit as st

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# 申万行业代码到名称映射（从 symbol_sector.py 提取）
INDUSTRY_MAPPING = {
    '11': '农林牧渔', '12': '采掘', '13': '化工', '14': '钢铁', '15': '有色金属',
    '16': '电子', '17': '家用电器', '18': '食品饮料', '19': '纺织服装', '20': '轻工制造',
    '21': '医药生物', '22': '公用事业', '23': '交通运输', '24': '房地产', '25': '商业贸易',
    '26': '休闲服务', '27': '综合', '28': '建筑材料', '29': '建筑装饰', '30': '电气设备',
    '31': '国防军工', '32': '计算机', '33': '传媒', '34': '通信', '35': '银行',
    '36': '非银金融', '37': '汽车', '38': '机械设备',
    '430101': '房地产开发', '430102': '园区开发',
    '440101': '银行', '440102': '非银金融',
    '450101': '汽车', '450102': '汽车零部件',
    '460101': '机械设备', '460102': '通用设备', '460103': '专用设备',
    '470101': '医药生物', '470102': '化学制药', '470103': '中药', '470104': '生物制品',
    '480101': '计算机', '480102': '软件开发', '480103': 'IT服务',
    '480301': '计算机',
    '490101': '传媒', '490102': '影视', '490103': '出版',
    '510101': '通信', '510102': '通信设备',
    '520101': '电子', '520102': '半导体', '520103': '电子制造', '520104': '光学光电子',
    '530101': '家用电器',
    '540101': '食品饮料', '540102': '饮料制造',
    '550101': '纺织服装', '550102': '服装家纺',
    '560101': '轻工制造', '560102': '造纸', '560103': '包装印刷',
    '570101': '化工', '570102': '石油化工', '570103': '化学原料', '570104': '化学制品',
    '580101': '钢铁', '580102': '钢铁制品',
    '590101': '有色金属', '590102': '金属制品',
    '600101': '建筑材料', '600102': '水泥', '600103': '玻璃',
    '610101': '建筑装饰', '610102': '装修装饰',
    '620101': '电气设备', '620102': '电源设备', '620103': '高低压设备',
    '630101': '国防军工', '630102': '航天装备', '630103': '航空装备',
    '640101': '公用事业', '640102': '电力', '640103': '燃气', '640104': '水务',
    '640201': '电力', '640202': '燃气', '640203': '水务', '640204': '环保', '640205': '新能源',
    '640206': '节能环保', '640207': '清洁能源', '640208': '可再生能源', '640209': '电力设备',
    '650101': '交通运输', '650102': '铁路运输', '650103': '公路运输', '650104': '航空运输',
    '660101': '房地产', '660102': '房地产开发', '660103': '园区开发',
    '670101': '商业贸易', '670102': '零售',
    '680101': '休闲服务', '680102': '旅游', '680103': '酒店',
    '690101': '综合',
    '700101': '农林牧渔', '700102': '农业', '700103': '林业', '700104': '畜牧业', '700105': '渔业',
    '710101': '采掘', '710102': '煤炭开采', '710103': '石油开采', '710104': '有色金属开采',
    '710301': '公用事业', '710402': '采掘',
    '720101': '综合',
    '270103': '综合', '280204': '建筑材料',
}


def _resolve_industry_name(code) -> str:
    """根据行业代码解析行业名称，优先匹配完整代码，逐级回退"""
    if pd.isna(code):
        return '未知行业'
    code_str = str(int(float(code))).zfill(6)
    if code_str in INDUSTRY_MAPPING:
        return INDUSTRY_MAPPING[code_str]
    if code_str[:4] in INDUSTRY_MAPPING:
        return INDUSTRY_MAPPING[code_str[:4]]
    if code_str[:2] in INDUSTRY_MAPPING:
        return INDUSTRY_MAPPING[code_str[:2]]
    return '未知行业'


@st.cache_data
def load_raw_data() -> pd.DataFrame:
    """读取原始持仓数据并执行去重"""
    df = pd.read_csv(
        os.path.join(DATA_DIR, 'raw', 'selenium_country_team_stock.csv'),
        dtype={'股票代码': str},
        parse_dates=['公告日', '报告期'],
    )
    df = df.dropna(subset=['公告日', '报告期'])
    df['report_q'] = df['报告期'].dt.to_period('Q')
    df = (
        df.sort_values(['股东代码', '股票代码', '报告期'], ascending=[True, True, False])
          .drop_duplicates(subset=['股东代码', '股票代码', 'report_q'], keep='first')
    )
    return df


@st.cache_data
def get_valid_quarters() -> list:
    """返回有效季度列表（记录数 >= 200），按时间升序"""
    df = load_raw_data()
    q_counts = df.groupby('report_q').size()
    return sorted(q_counts[q_counts >= 200].index)


@st.cache_data
def compute_quarter_change(latest_q_str: str, prev_q_str: str) -> pd.DataFrame:
    """计算两个季度之间的持仓变化，逻辑与 analyze_holdings.py 一致"""
    df = load_raw_data()
    latest_q = pd.Period(latest_q_str, freq='Q')
    prev_q = pd.Period(prev_q_str, freq='Q')

    latest = df[df['report_q'] == latest_q]
    prev = df[df['report_q'] == prev_q]

    latest_summary = latest.groupby(['股票代码', '股票简称']).agg(
        持有机构数=('股东名称', 'nunique'),
        持有机构列表=('股东名称', lambda x: '、'.join(x.unique())),
        总持仓数量=('数量', 'sum'),
        总流通市值=('流通市值', 'sum'),
    ).reset_index()

    prev_summary = prev.groupby(['股票代码', '股票简称']).agg(
        上季持有机构数=('股东名称', 'nunique'),
        上季总持仓数量=('数量', 'sum'),
        上季总流通市值=('流通市值', 'sum'),
    ).reset_index()

    merged = latest_summary.merge(
        prev_summary, on=['股票代码', '股票简称'], how='outer', indicator=True
    )

    for col in ['总持仓数量', '总流通市值', '持有机构数']:
        merged[col] = merged[col].fillna(0)
    for col in ['上季总持仓数量', '上季总流通市值', '上季持有机构数']:
        merged[col] = merged[col].fillna(0)

    merged['机构数变化'] = merged['持有机构数'] - merged['上季持有机构数']
    merged['持仓数量变化'] = merged['总持仓数量'] - merged['上季总持仓数量']
    merged['流通市值变化'] = merged['总流通市值'] - merged['上季总流通市值']
    merged['流通市值变化_亿'] = merged['流通市值变化'] / 1e8

    return merged


@st.cache_data
def load_industry_map() -> pd.DataFrame:
    """构建 股票代码 -> 行业名称 映射表"""
    # 优先使用 stock_sector.csv
    sector_path = os.path.join(DATA_DIR, 'raw', 'stock_sector.csv')
    sector_df = pd.read_csv(sector_path, dtype={'代码': str})
    sector_df = sector_df.rename(columns={'代码': '股票代码'})
    # stock_sector.csv 已有 行业名称 列，但部分为"未知行业"
    # 对于有行业代码的，用映射重新解析
    sector_df['行业'] = sector_df.apply(
        lambda r: r['行业名称'] if pd.notna(r.get('行业名称')) and r['行业名称'] != '未知行业'
        else _resolve_industry_name(r.get('行业代码')),
        axis=1,
    )
    industry = sector_df[['股票代码', '行业']].drop_duplicates(subset='股票代码', keep='first')

    # 回退：lixinger_industry_data.csv
    lixinger_path = os.path.join(DATA_DIR, 'raw', 'lixinger_industry_data.csv')
    lixinger_df = pd.read_csv(lixinger_path, dtype={'股票代码': str})
    lixinger_df['行业'] = lixinger_df['行业链接'].apply(_resolve_industry_name)
    lixinger_industry = lixinger_df[['股票代码', '行业']].drop_duplicates(subset='股票代码', keep='first')

    # 合并：stock_sector 优先，缺失的用 lixinger 补充
    combined = industry.set_index('股票代码')['行业'].to_dict()
    for _, row in lixinger_industry.iterrows():
        code = row['股票代码']
        if code not in combined or combined[code] == '未知行业':
            combined[code] = row['行业']

    result = pd.DataFrame(list(combined.items()), columns=['股票代码', '行业'])
    return result


def get_quarter_pairs() -> list[tuple[str, str]]:
    """返回所有有效季度对 [(prev_q, latest_q), ...]，最新的在前"""
    quarters = get_valid_quarters()
    pairs = []
    for i in range(len(quarters) - 1, 0, -1):
        pairs.append((str(quarters[i - 1]), str(quarters[i])))
    return pairs


@st.cache_data
def get_stock_timeline(stock_code: str) -> pd.DataFrame:
    """获取单只股票在所有季度的持仓汇总（用于时间线图）"""
    df = load_raw_data()
    stock_df = df[df['股票代码'] == stock_code]
    if stock_df.empty:
        return pd.DataFrame()
    summary = stock_df.groupby('report_q').agg(
        总流通市值=('流通市值', 'sum'),
        总持仓数量=('数量', 'sum'),
        持有机构数=('股东名称', 'nunique'),
    ).reset_index()
    summary['report_q_str'] = summary['report_q'].astype(str)
    summary = summary.sort_values('report_q')
    return summary


@st.cache_data
def get_stock_institution_detail(stock_code: str) -> pd.DataFrame:
    """获取单只股票各机构各季度的持仓明细（用于堆叠柱状图和明细表）"""
    df = load_raw_data()
    stock_df = df[df['股票代码'] == stock_code].copy()
    if stock_df.empty:
        return pd.DataFrame()
    stock_df['report_q_str'] = stock_df['report_q'].astype(str)
    return stock_df.sort_values('report_q', ascending=False)


@st.cache_data
def get_all_stocks() -> pd.DataFrame:
    """获取所有股票代码和简称的去重列表（用于搜索）"""
    df = load_raw_data()
    stocks = df[['股票代码', '股票简称']].drop_duplicates()
    return stocks.sort_values('股票代码')


@st.cache_data
def get_institution_list(quarter_str: str) -> pd.DataFrame:
    """获取指定季度所有机构的持仓汇总，按总流通市值降序"""
    df = load_raw_data()
    q = pd.Period(quarter_str, freq='Q')
    q_df = df[df['report_q'] == q]
    summary = q_df.groupby(['股东代码', '股东名称']).agg(
        持仓股票数=('股票代码', 'nunique'),
        总流通市值=('流通市值', 'sum'),
    ).reset_index()
    return summary.sort_values('总流通市值', ascending=False)


@st.cache_data
def get_institution_holdings(institution_name: str, quarter_str: str) -> pd.DataFrame:
    """获取指定机构在指定季度的持仓列表"""
    df = load_raw_data()
    q = pd.Period(quarter_str, freq='Q')
    mask = (df['report_q'] == q) & (df['股东名称'] == institution_name)
    holdings = df[mask][['股票代码', '股票简称', '数量', '流通市值', '报告期']].copy()
    return holdings.sort_values('流通市值', ascending=False)


@st.cache_data
def get_institution_change(institution_name: str, latest_q_str: str, prev_q_str: str) -> pd.DataFrame:
    """获取指定机构在两个季度之间的持仓变化"""
    df = load_raw_data()
    latest_q = pd.Period(latest_q_str, freq='Q')
    prev_q = pd.Period(prev_q_str, freq='Q')

    latest = df[(df['report_q'] == latest_q) & (df['股东名称'] == institution_name)]
    prev = df[(df['report_q'] == prev_q) & (df['股东名称'] == institution_name)]

    l_agg = latest.groupby(['股票代码', '股票简称']).agg(
        本季数量=('数量', 'sum'), 本季市值=('流通市值', 'sum'),
    ).reset_index()
    p_agg = prev.groupby(['股票代码', '股票简称']).agg(
        上季数量=('数量', 'sum'), 上季市值=('流通市值', 'sum'),
    ).reset_index()

    merged = l_agg.merge(p_agg, on=['股票代码', '股票简称'], how='outer')
    for c in ['本季数量', '本季市值']:
        merged[c] = merged[c].fillna(0)
    for c in ['上季数量', '上季市值']:
        merged[c] = merged[c].fillna(0)
    merged['数量变化'] = merged['本季数量'] - merged['上季数量']
    merged['市值变化'] = merged['本季市值'] - merged['上季市值']
    return merged.sort_values('市值变化', ascending=False)
