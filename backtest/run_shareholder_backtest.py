#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股东持股策略回测脚本

基于股东持股数据中的公告日进行调仓，按照流通市值分配权重
"""

import os
import sys
import pandas as pd
import numpy as np
import backtrader as bt
import quantstats as qs
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from strategy.CerebroUtils import ECerebro

# 添加项目路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
sys.path.append(PROJECT_ROOT)

from strategy.ShareholderStrat import ShareholderStrategy
from utils.data_loader import *


def load_available_stocks(stock_csv_path: str) -> pd.DataFrame:
    """从stock.csv加载可用的股票列表"""
    print(f"加载可用股票列表: {stock_csv_path}")
    
    try:
        stock_df = pd.read_csv(stock_csv_path)
        
        # 筛选市场为'sh'和'sz'的股票
        available_stocks = stock_df[(stock_df['market'] == 'sh') | (stock_df['market'] == 'sz')]
        
        print(f"成功加载 {len(available_stocks)} 只可用股票")
        return available_stocks
        
    except Exception as e:
        print(f"加载股票列表失败: {e}")
        raise

def setup_cerebro(initial_cash: float = 10000000000.0) -> ECerebro:
    """设置回测引擎"""
    print("设置回测引擎...")
    
    # 创建cerebro引擎
    cerebro = ECerebro()

    # 设置初始资金
    cerebro.broker.setcash(initial_cash)
    
    # 设置手续费
    cerebro.broker.setcommission(commission=0.0003)
    
    # 从stock.csv加载可用的股票列表
    stock_csv_path = os.path.join(DATA_DIR, 'config', 'stock.csv')
    available_stocks = load_available_stocks(stock_csv_path)
    
    # 创建SYMBOL列表, 并加载数据
    symbol_list = [SYMBOL(row['index'], row['code'], row['name'], row['market'], row['type'], row['interval']) for _, row in available_stocks.iterrows()]
    # bt_data = BTDataLoader(symbol_list).load()
    # bt_data = BTDataLoader(symbol_list).random_pick(100).load().align_datetime()
    bt_data = BTDataLoader(symbol_list).load().align_datetime()
    bt_data = bt_data.resample('1M').bt_data_dict

    for name, data in bt_data.items():
        cerebro.adddata(data, name=name)
    
    # 添加策略
    # cerebro.addstrategy(ShareholderStrategy, shareholder_data_path=shareholder_data_path)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
    
    print("回测引擎设置完成")
    return cerebro

def load_shareholder_data(shareholder_data_path: str) -> pd.DataFrame:
    df = pd.read_csv(shareholder_data_path, dtype={'股票代码': str}, parse_dates=['公告日', '报告期'])
        
    # 移除缺失值
    df = df.dropna(subset=['公告日', '报告期'])

    # 数据预处理：将“报告期”统一对齐到对应季度的最后一天
    # 如果报告期是2025-03-01，则映射为2025-03-31
    df['report_q'] = df['报告期'].dt.to_period('Q')  # 直接得到PeriodIndex，无需再.dt
    # 在每个 report_q 内，如果存在多个“股东代码”和“股票简称”的数据，取 max("报告期") 的那条
    df = (
        df.sort_values(['股东代码', '股票代码', '报告期'], ascending=[True, True, False])
            .drop_duplicates(subset=['股东代码', '股票代码', 'report_q'], keep='first')
    )

    return df

def pivot(df: pd.DataFrame, watch_value: str, method: str = 'sum') -> pd.DataFrame:
    """加载股东持股数据"""
    try:
        # 生成从2015-03-31开始的每个季度最后一天的序列（PeriodIndex）
        quarterly_periods = pd.period_range(
            start='2015Q1',
            end=pd.Timestamp.now().to_period('Q'),
            freq='Q'
        )

        # 用 pivot_table 一句话完成“按股票代码+report_q汇总数量”
        pivot_table = (
            df.pivot_table(
                index='股票代码',
                columns='report_q',
                values=watch_value,
                aggfunc=method,
                fill_value=0
            )
            .reindex(columns=quarterly_periods, fill_value=0)   # 列标签与 quarterly_periods 保持一致
        )
                
        return pivot_table
        
    except Exception as e:
        print(f"加载股东持股数据失败: {e}")
        raise

if __name__ == '__main__':
    cerebro = setup_cerebro()

    # 加载股东数据
    shareholder_df = load_shareholder_data(os.path.join(DATA_DIR, 'raw', 'selenium_country_team_stock.csv'))
    # 生成股东持股数据透视表    
    pivot(shareholder_df, watch_value='流通市值', method='sum').to_csv(os.path.join(DATA_DIR, 'processed', 'shareholder_pivot_流通市值_sum.csv'), encoding='utf-8-sig')
    pivot(shareholder_df, watch_value='数量', method='count').to_csv(os.path.join(DATA_DIR, 'processed', 'shareholder_pivot_数量_count.csv'), encoding='utf-8-sig')

    pivot_table = pivot(shareholder_df, watch_value='数量')
    cerebro.addstrategy(ShareholderStrategy, shareholder_pivot=pivot_table)

    # 运行回测
    results = cerebro.run()
    # strategy = results[0]
    cerebro.saveQuantStatsTo(file_path=os.path.join(PROJECT_ROOT, 'backtest', 'backtest_results', 'shareholder_strategy_report.html'))

    # for stock_code in shareholder_df['股东代码'].unique():
    #     df = shareholder_df[shareholder_df['股东代码'] == stock_code]
    #     pivot_table = pivot(df, watch_value='数量')
    #     cerebro.addstrategy(ShareholderStrategy, shareholder_pivot=pivot_table)
    
    #     # 运行回测
    #     results = cerebro.run()
    #     # strategy = results[0]
    #     cerebro.saveQuantStatsTo(file_path=f'backtest_results/shareholder_strategy_report_{stock_code}.html')
