#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票板块数据获取模块

使用akshare获取A股板块数据，包括行业分类、概念板块等
支持多种数据源：申万行业分类、同花顺行业板块等
"""

import logging
import os
import sys
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Any

import akshare as ak
import pandas as pd
from tqdm import tqdm

# 添加项目根目录到Python路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.append(PROJECT_ROOT)

logger = logging.getLogger(__name__)


class SymbolSectorManager:
    """股票板块数据管理器"""
    
    def __init__(self, cache_dir: str = None):
        """
        初始化板块数据管理器
        
        Args:
            cache_dir: 缓存目录路径，如果为None则使用默认缓存目录
        """
        self.cache_dir = cache_dir or os.path.join(PROJECT_ROOT, 'data', 'sector_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 缓存数据
        self._sector_cache: Dict[str, pd.DataFrame] = {}
        
        logger.info(f"股票板块数据管理器初始化完成，缓存目录: {self.cache_dir}")
    
    def get_all_stock_sectors(self) -> pd.DataFrame:
        """
        获取所有A股的板块数据
        
        Returns:
            包含所有A股板块数据的DataFrame
        """
        cache_key = "all_stock_sectors"
        
        if cache_key in self._sector_cache:
            logger.info("从缓存获取所有A股板块数据")
            return self._sector_cache[cache_key]
        
        try:
            logger.info("开始获取所有A股板块数据...")
            
            # 获取A股基本信息
            stock_info = ak.stock_info_a_code_name()
            logger.info(f"获取到{len(stock_info)}只A股基本信息")
            
            # 获取申万行业分类数据
            sw_industry = self._get_sw_industry_data()
            
            # 获取同花顺行业板块数据
            ths_industry = self._get_ths_industry_data()
            
            # 合并所有板块数据
            all_sectors = self._merge_sector_data(stock_info, sw_industry, ths_industry)
            
            # 缓存数据
            self._sector_cache[cache_key] = all_sectors
            
            # 保存到文件
            cache_file = os.path.join(self.cache_dir, "all_stock_sectors.csv")
            all_sectors.to_csv(cache_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"成功获取所有A股板块数据，共{len(all_sectors)}条记录")
            return all_sectors
            
        except Exception as e:
            logger.error(f"获取所有A股板块数据失败: {e}")
            raise
    
    def _get_sw_industry_data(self) -> pd.DataFrame:
        """
        获取申万行业分类数据
        
        Returns:
            申万行业分类DataFrame
        """
        try:
            logger.info("获取申万行业分类数据...")
            
            # 获取申万行业历史分类
            sw_data = ak.stock_industry_clf_hist_sw()
            
            # 获取最新的行业分类（按update_time排序）
            sw_data = sw_data.sort_values('update_time', ascending=False).drop_duplicates('symbol')
            
            # 重命名列
            sw_data = sw_data.rename(columns={
                'symbol': '代码',
                'industry_code': '申万行业代码',
                'update_time': '更新时间'
            })
            
            # 添加行业名称
            sw_data = self._add_sw_industry_names(sw_data)
            
            logger.info(f"申万行业分类数据获取完成，共{len(sw_data)}条记录")
            return sw_data
            
        except Exception as e:
            logger.error(f"获取申万行业分类数据失败: {e}")
            # 返回空DataFrame
            return pd.DataFrame(columns=['代码', '申万行业代码', '申万行业名称', '更新时间'])
    
    def _get_ths_industry_data(self) -> pd.DataFrame:
        """
        获取同花顺行业板块数据
        
        Returns:
            同花顺行业板块DataFrame
        """
        try:
            logger.info("获取同花顺行业板块数据...")
            
            # 获取同花顺行业板块名称
            ths_industry_names = ak.stock_board_industry_name_ths()
            
            all_ths_data = []
            
            # 遍历所有行业板块，获取成分股
            for _, industry in tqdm(ths_industry_names.iterrows(), 
                                   total=len(ths_industry_names),
                                   desc="获取同花顺行业成分股"):
                industry_name = industry['name']
                industry_code = industry['code']
                
                try:
                    # 尝试获取行业板块信息
                    industry_info = ak.stock_board_industry_info_ths()
                    
                    # 获取行业指数数据
                    industry_index = ak.stock_board_industry_index_ths()
                    
                    # 创建行业数据记录
                    industry_data = {
                        '同花顺行业代码': industry_code,
                        '同花顺行业名称': industry_name,
                        '行业指数': industry_index.iloc[0]['最新价'] if len(industry_index) > 0 else None,
                        '更新时间': datetime.now().strftime('%Y-%m-%d')
                    }
                    
                    all_ths_data.append(industry_data)
                    
                except Exception as e:
                    logger.warning(f"获取行业'{industry_name}'数据失败: {e}")
                    continue
            
            ths_df = pd.DataFrame(all_ths_data)
            logger.info(f"同花顺行业板块数据获取完成，共{len(ths_df)}条记录")
            return ths_df
            
        except Exception as e:
            logger.error(f"获取同花顺行业板块数据失败: {e}")
            # 返回空DataFrame
            return pd.DataFrame(columns=['同花顺行业代码', '同花顺行业名称', '行业指数', '更新时间'])
    
    def _add_sw_industry_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        为申万行业数据添加行业名称和层级信息
        
        Args:
            df: 申万行业数据DataFrame
            
        Returns:
            添加了行业名称和层级信息的DataFrame
        """
        # 完整的申万行业代码到名称映射表（基于申万宏源研究官方分类）
        industry_mapping = {
            # 一级行业分类（2位代码）
            '11': '农林牧渔', '12': '采掘', '13': '化工', '14': '钢铁', '15': '有色金属',
            '16': '电子', '17': '家用电器', '18': '食品饮料', '19': '纺织服装', '20': '轻工制造',
            '21': '医药生物', '22': '公用事业', '23': '交通运输', '24': '房地产', '25': '商业贸易',
            '26': '休闲服务', '27': '综合', '28': '建筑材料', '29': '建筑装饰', '30': '电气设备',
            '31': '国防军工', '32': '计算机', '33': '传媒', '34': '通信', '35': '银行',
            '36': '非银金融', '37': '汽车', '38': '机械设备', 
            
            # 二级行业分类（4位代码）
            '1101': '农业', '1102': '林业', '1103': '畜牧业', '1104': '渔业',
            '1201': '煤炭开采', '1202': '石油开采', '1203': '有色金属开采',
            '1301': '石油化工', '1302': '化学原料', '1303': '化学制品', '1304': '化学纤维',
            '1401': '钢铁', '1402': '钢铁制品',
            '1501': '有色金属', '1502': '金属制品',
            '1601': '半导体', '1602': '电子制造', '1603': '光学光电子', '1604': '电子元件',
            '1701': '家用电器',
            '1801': '食品饮料', '1802': '饮料制造',
            '1901': '纺织服装', '1902': '服装家纺',
            '2001': '轻工制造', '2002': '造纸', '2003': '包装印刷',
            '2101': '化学制药', '2102': '中药', '2103': '生物制品', '2104': '医药商业',
            '2201': '电力', '2202': '燃气', '2203': '水务',
            '2301': '铁路运输', '2302': '公路运输', '2303': '航空运输', '2304': '港口',
            '2401': '房地产开发', '2402': '园区开发',
            '2501': '商业贸易', '2502': '零售',
            '2601': '休闲服务', '2602': '旅游', '2603': '酒店',
            '2701': '综合',
            '2801': '建筑材料', '2802': '水泥', '2803': '玻璃',
            '2901': '建筑装饰', '2902': '装修装饰',
            '3001': '电气设备', '3002': '电源设备', '3003': '高低压设备',
            '3101': '国防军工', '3102': '航天装备', '3103': '航空装备',
            '3201': '计算机', '3202': '软件开发', '3203': 'IT服务',
            '3301': '传媒', '3302': '影视', '3303': '出版',
            '3401': '通信', '3402': '通信设备',
            '3501': '银行',
            '3601': '非银金融', '3602': '证券', '3603': '保险',
            '3701': '汽车', '3702': '汽车零部件',
            '3801': '机械设备', '3802': '通用设备', '3803': '专用设备',
            
            # 三级行业分类（6位代码）- 基于实际数据中出现频率较高的代码
            '430101': '房地产开发', '430102': '园区开发',
            '440101': '银行', '440102': '非银金融',
            '450101': '汽车', '450102': '汽车零部件',
            '460101': '机械设备', '460102': '通用设备', '460103': '专用设备',
            '470101': '医药生物', '470102': '化学制药', '470103': '中药', '470104': '生物制品',
            '480101': '计算机', '480102': '软件开发', '480103': 'IT服务',
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
            '720101': '综合',
        }
        
        def get_industry_hierarchy(code: str) -> dict:
            """根据行业代码获取行业层级信息"""
            if pd.isna(code):
                return {
                    '申万一级行业代码': None,
                    '申万一级行业名称': '未知行业',
                    '申万二级行业代码': None,
                    '申万二级行业名称': '未知行业',
                    '申万三级行业代码': None,
                    '申万三级行业名称': '未知行业',
                    '申万行业名称': '未知行业'
                }
            
            # 处理代码格式
            code_str = str(int(float(code))) if not pd.isna(code) else ''
            
            # 如果代码长度不足6位，补零到6位
            if len(code_str) < 6:
                code_str = code_str.zfill(6)
            
            # 提取各级代码
            level1_code = code_str[:2] if len(code_str) >= 2 else None
            level2_code = code_str[:4] if len(code_str) >= 4 else None
            level3_code = code_str if len(code_str) >= 6 else None
            
            # 获取各级行业名称
            level1_name = industry_mapping.get(level1_code, f'未知行业({level1_code})') if level1_code else '未知行业'
            level2_name = industry_mapping.get(level2_code, f'未知行业({level2_code})') if level2_code else '未知行业'
            level3_name = industry_mapping.get(level3_code, f'未知行业({level3_code})') if level3_code else '未知行业'
            
            # 确定最终的行业名称（优先使用三级，其次二级，最后一级）
            if level3_code and level3_code in industry_mapping:
                final_name = level3_name
            elif level2_code and level2_code in industry_mapping:
                final_name = level2_name
            elif level1_code and level1_code in industry_mapping:
                final_name = level1_name
            else:
                final_name = f'未知行业({code_str})'
            
            return {
                '申万一级行业代码': level1_code,
                '申万一级行业名称': level1_name,
                '申万二级行业代码': level2_code,
                '申万二级行业名称': level2_name,
                '申万三级行业代码': level3_code,
                '申万三级行业名称': level3_name,
                '申万行业名称': final_name
            }
        
        # 添加行业层级信息
        hierarchy_info = df['申万行业代码'].apply(get_industry_hierarchy)
        hierarchy_df = pd.DataFrame(hierarchy_info.tolist())
        
        # 合并层级信息到原数据
        df = pd.concat([df, hierarchy_df], axis=1)
        
        return df
    
    def _merge_sector_data(self, stock_info: pd.DataFrame, 
                          sw_industry: pd.DataFrame, 
                          ths_industry: pd.DataFrame) -> pd.DataFrame:
        """
        合并所有板块数据
        
        Args:
            stock_info: A股基本信息
            sw_industry: 申万行业数据
            ths_industry: 同花顺行业数据
            
        Returns:
            合并后的板块数据DataFrame
        """
        # 重命名列
        stock_info = stock_info.rename(columns={'code': '代码', 'name': '股票名称'})
        
        # 合并申万行业数据
        if not sw_industry.empty:
            merged_data = pd.merge(stock_info, sw_industry, on='代码', how='left')
        else:
            merged_data = stock_info.copy()
            merged_data['申万行业代码'] = None
            merged_data['申万行业名称'] = None
            merged_data['更新时间'] = None
        
        # 合并同花顺行业数据（这里主要是行业信息，不是个股信息）
        # 同花顺数据主要是行业级别的，不是个股级别的
        
        logger.info(f"板块数据合并完成，共{len(merged_data)}条记录")
        return merged_data
    
    def export_to_csv(self, filename: str = os.path.join(PROJECT_ROOT, 'data', 'processed', 'sector.csv')) -> str:
        """
        导出板块数据到CSV文件
        
        Args:
            filename: 输出文件名
            
        Returns:
            导出的文件路径
        """
        try:
            # 获取板块数据
            sector_data = self.get_all_stock_sectors()
            
            # 只保留申万行业代码相关字段
            sw_industry_columns = [
                '代码',  # 股票代码
                '申万行业代码',
                '申万一级行业代码',
                '申万二级行业代码', 
                '申万三级行业代码'
            ]
            
            # 只保留存在的列
            available_columns = [col for col in sw_industry_columns if col in sector_data.columns]
            sector_data = sector_data[available_columns]
            
            sector_data.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"板块数据已导出到 {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"导出CSV文件失败: {e}")
            raise
    
    def get_sector_detail(self, sector: str = "hangye_ZL01") -> pd.DataFrame:
        """
        获取板块详情数据
        
        Args:
            sector: 板块代码，支持akshare的板块代码格式
                - "hangye_ZL01": 行业板块-出版业
                - 其他板块代码可参考akshare文档
                
        Returns:
            板块详情DataFrame，包含股票代码、名称、交易数据等信息
        """
        cache_key = f"sector_detail_{sector}"
        
        if cache_key in self._sector_cache:
            logger.info(f"从缓存获取板块详情数据: {cache_key}")
            return self._sector_cache[cache_key]
        
        try:
            logger.info(f"开始获取{sector}板块详情数据...")
            
            # 使用akshare获取板块详情数据
            df = ak.stock_sector_detail(sector=sector)
            
            # 数据清洗和标准化
            df = self._clean_sector_detail_data(df)
            
            # 缓存数据
            self._sector_cache[cache_key] = df
            
            # 保存到文件
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.csv")
            df.to_csv(cache_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"成功获取{sector}板块详情数据，共{len(df)}条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取{sector}板块详情数据失败: {e}")
            raise
    
    def _clean_sector_detail_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗板块详情数据
        
        Args:
            df: 原始板块详情数据
            
        Returns:
            清洗后的DataFrame
        """
        # 重命名列
        column_mapping = {
            'symbol': '代码',
            'code': '股票代码',
            'name': '股票名称',
            'trade': '最新价',
            'pricechange': '涨跌额',
            'changepercent': '涨跌幅',
            'buy': '买入价',
            'sell': '卖出价',
            'settlement': '昨收',
            'open': '今开',
            'high': '最高',
            'low': '最低',
            'volume': '成交量',
            'amount': '成交额',
            'ticktime': '更新时间',
            'per': '市盈率',
            'pb': '市净率',
            'mktcap': '总市值',
            'nmc': '流通市值',
            'turnoverratio': '换手率'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 选择需要的列
        required_columns = ['代码', '股票名称', '最新价', '涨跌幅', '成交量', '成交额', '总市值', '流通市值']
        available_columns = [col for col in required_columns if col in df.columns]
        
        return df[available_columns]


def main():
    """主函数"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 创建板块数据管理器
        manager = SymbolSectorManager()
        
        # 导出板块数据到CSV
        output_file = manager.export_to_csv()
        
        # 获取数据统计信息
        sector_data = manager.get_all_stock_sectors()
        
        print(f"\n=== 板块数据导出完成 ===")
        print(f"导出文件: {output_file}")
        print(f"数据记录数: {len(sector_data)}")
        print(f"包含的列: {sector_data.columns.tolist()}")
        
        # 显示数据统计
        if '申万行业名称' in sector_data.columns:
            industry_stats = sector_data['申万行业名称'].value_counts()
            print(f"\n行业分布统计:")
            print(industry_stats.head(10))
        
        print(f"\n前5条数据:")
        print(sector_data.head())
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()