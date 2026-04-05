# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

国家队持仓追踪与量化分析工具。通过爬取东方财富、理杏仁等数据源，获取国家队（社保基金、中央汇金、证金公司等）的股票持仓数据，结合行业分类进行分析，并用 Backtrader 回测持仓跟踪策略。

## 运行命令

```bash
pip install -r requirements.txt

# 爬取国家队持仓数据（东方财富，增量模式）
python crawlers/selenium_stock_crawler.py

# 爬取行业分类数据（理杏仁）
python crawlers/lixinger_industry_crawler.py

# 生成申万行业分类（通过 AkShare）
python crawlers/symbol_sector.py

# 运行回测（依赖 utils/ 和 strategy/ 模块，当前缺失）
python backtest/run_shareholder_backtest.py
```

## 目录结构

```
crawlers/                        # 爬虫脚本
  selenium_stock_crawler.py      # 东方财富持仓爬虫 + 季度统计生成
  lixinger_industry_crawler.py   # 理杏仁行业爬虫
  symbol_sector.py               # AkShare 申万行业分类
backtest/                        # 回测相关
  run_shareholder_backtest.py    # Backtrader 回测引擎
data/
  config/                        # 输入配置（纳入git）
    stock_holder.csv             # 112个股东实体及URL
  raw/                           # 原始爬取数据（git忽略）
    selenium_country_team_stock.csv
    国家队持股_清洗.csv
    lixinger_industry_data.csv
    stock_sector.csv
  processed/                     # 加工后的透视表（git忽略）
    quarterly_stock_statistics.csv
    quarterly_announce_statistics.csv
    shareholder_pivot_*.csv
    sector.csv
```

所有脚本通过 `PROJECT_ROOT` 变量定位项目根目录，CSV 路径使用 `os.path.join(PROJECT_ROOT, 'data', ...)` 构建。

## 数据流

```
data/config/stock_holder.csv (112个股东实体及URL)
    → crawlers/selenium_stock_crawler.py (Selenium爬虫)
    → data/raw/selenium_country_team_stock.csv (原始持仓，~6万行)
    → generate_quarterly_statistics() 数据清洗 + 季度透视表
    → data/processed/quarterly_*.csv / shareholder_pivot_*.csv
    → backtest/run_shareholder_backtest.py (回测)
    → backtest/backtest_results/shareholder_strategy_report.html
```

## 核心脚本

- **selenium_stock_crawler.py** — 主爬虫。`SeleniumStockHolderCrawler` 类，从东方财富爬取持仓数据。支持增量爬取（跳过已有数据）、分页、反检测（UA伪装/隐藏自动化标志）。包含 `generate_quarterly_statistics()` 生成季度透视表。
- **lixinger_industry_crawler.py** — 理杏仁行业爬虫。`LixingerIndustryCrawler` 类，爬取申万2021行业分类，支持断点续爬。
- **symbol_sector.py** — 行业分类管理。`SymbolSectorManager` 类，通过 AkShare 获取申万三级行业体系（38个一级行业），导出 sector.csv。
- **run_shareholder_backtest.py** — 回测引擎。使用 Backtrader 框架，月频重采样，输出 Sharpe/回撤/收益等指标，生成 QuantStats HTML 报告。

## 缺失模块

爬虫和回测脚本依赖以下模块，当前仓库中不存在：
- `utils.util.LoggableMixin` — 爬虫日志
- `utils.data_loader.BTDataLoader`, `SYMBOL` — 回测数据加载
- `strategy.CerebroUtils.ECerebro` — 自定义 Cerebro 封装
- `strategy.ShareholderStrat.ShareholderStrategy` — 持仓跟踪策略

## 数据格式

主持仓数据列：股东代码, 股东名称, 股票代码, 股票简称, 报告期, 公告日, 数量, 流通市值

爬虫内部会转换中文日期格式（"10月30日" → "2025/10/30"）和数量单位（万/亿 → 整数）。

## 注意事项

- 爬虫使用 Selenium headless Chrome，延迟 1-3 秒随机间隔
- 所有数据文件为 UTF-8 编码
