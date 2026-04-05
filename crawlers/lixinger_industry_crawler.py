#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
理杏仁行业数据Selenium爬虫

使用Selenium模拟真实浏览器行为，爬取理杏仁网站的行业数据

功能：
1. 使用Selenium Chrome浏览器模拟真实用户行为
2. 爬取stock.csv中股票的行业数据
3. 爬取href以'/equity/industry/detail/sw_2021'开头的3个链接的数据
4. 智能等待页面加载和数据渲染
5. 防限流机制，设置合理的爬虫间隔
6. 数据格式转换和清洗
7. 断点续传和错误重试机制

作者：量化框架开发助手
创建时间：2025年
"""

import pandas as pd
import time
import random
import re
import os
import sys
import logging

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, 
    StaleElementReferenceException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

from utils.util import LoggableMixin

logger = LoggableMixin(name='LixingerIndustryCrawler', console=True).logger

class LixingerIndustryCrawler(LoggableMixin):
    """基于Selenium的理杏仁行业数据爬虫类"""
    
    def __init__(self, 
                 headless: bool = True,
                 delay_min: float = 2.0,  # 增加延迟时间，避免被限流
                 delay_max: float = 5.0,
                 timeout: int = 30):
        """
        初始化Selenium爬虫
        
        Args:
            headless: 是否无头模式
            delay_min: 最小延迟时间（秒）
            delay_max: 最大延迟时间（秒）
            timeout: 页面加载超时时间（秒）
        """
        self.headless = headless
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.timeout = timeout
        self.driver = None
        
        LoggableMixin.__init__(self, name=self.__class__.__name__, console=True)
        
        # 初始化浏览器驱动
        self._init_driver()
    
    def _init_driver(self) -> None:
        """初始化Chrome浏览器驱动"""
        try:
            # 配置Chrome选项
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument('--headless')
            
            # 添加常用选项
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 设置用户代理
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 使用webdriver_manager自动管理驱动
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 隐藏自动化特征
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']})")
            
            # 设置页面加载超时
            self.driver.set_page_load_timeout(self.timeout)
            
            self.logger.info("Chrome浏览器驱动初始化成功")
            
        except Exception as e:
            self.logger.error(f"浏览器驱动初始化失败: {e}")
            raise
    
    def random_delay(self) -> None:
        """随机延迟，模拟真实用户行为"""
        delay = random.uniform(self.delay_min, self.delay_max)
        time.sleep(delay)
    
    def wait_for_element(self, by: By, value: str, timeout: int = None) -> Optional[object]:
        """等待元素出现
        
        Args:
            by: 定位方式
            value: 定位值
            timeout: 超时时间
            
        Returns:
            找到的元素或None
        """
        if timeout is None:
            timeout = self.timeout
            
        try:
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, value)))
            return element
        except TimeoutException:
            self.logger.warning(f"等待元素超时: {by}={value}")
            return None
    
    def wait_for_page_load(self) -> bool:
        """等待页面加载完成
        
        Returns:
            是否成功加载页面
        """
        try:
            # 等待页面主要内容出现
            main_content = self.wait_for_element(By.CLASS_NAME, 'info-container')
            if not main_content:
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning(f"等待页面加载失败: {e}")
            return False
    
    def get_stock_codes_from_csv(self, csv_file: str = os.path.join(PROJECT_ROOT, 'data', 'config', 'stock.csv')) -> List[Tuple[str, str]]:
        """从stock.csv读取股票代码
        
        Args:
            csv_file: CSV文件路径
            
        Returns:
            股票代码和类型列表，格式为[(code, type)]
        """
        try:
            if not os.path.exists(csv_file):
                self.logger.error(f"股票数据文件不存在: {csv_file}")
                return []
            
            df = pd.read_csv(csv_file)
            
            # 过滤market为sh或sz的股票，剔除cn_前缀
            stock_data = df[df['market'].isin(['sh', 'sz'])].copy()
            
            # 处理index列，剔除cn_前缀
            stock_data['processed_code'] = stock_data['index'].str.replace('cn_', '')
            
            # 返回股票代码和市场的元组列表
            result = list(zip(stock_data['processed_code'], stock_data['market'], stock_data['type']))
            
            self.logger.info(f"从 {csv_file} 读取到 {len(result)} 个股票代码")
            return result
            
        except Exception as e:
            self.logger.error(f"读取股票数据文件失败: {e}")
            return []
    
    def build_url(self, stock_code: str, market: str, type: str) -> str:
        """构建理杏仁网站URL
        
        Args:
            stock_code: 股票代码
            market: 市场类型（sh/sz）
            
        Returns:
            完整的URL
        """
        code1 = stock_code
        code2 = str(int(stock_code))
        stock_type = 'index' if type == 'index' else 'company'
        return f"https://www.lixinger.com/equity/{stock_type}/detail/{market}/{code1}/{code2}/fundamental/valuation/pe-ttm"
    
    def find_industry_codes(self) -> List[str]:
        """查找href以'/equity/industry/detail/sw_2021'开头的链接
        
        Returns:
            行业链接列表
        """
        
        try:
            # 查找所有a标签
            company_info = self.driver.find_element(By.CLASS_NAME, 'industry')
            links = company_info.find_elements(By.TAG_NAME, 'a')

            sw_2021_codes = [link.get_attribute('href').split('/')[-1] for link in links if 'sw_2021' in link.get_attribute('href')]
            
            self.logger.info(f"共找到 {len(sw_2021_codes)} 个行业代码")
            return sw_2021_codes[-1]
            
        except Exception as e:
            self.logger.error(f"查找行业代码失败: {e}")
            return None
    
    def crawl_single_stock(self, stock_code: str, market: str, type: str) -> List[Dict]:
        """爬取单个股票的行业数据
        
        Args:
            stock_code: 股票代码
            market: 市场类型
            type: 股票类型（index/company）
            
        Returns:
            行业数据列表
        """
        
        try:
            self.logger.info(f"开始爬取股票 {stock_code}.{market} 的行业数据")
            
            # 构建URL
            url = self.build_url(stock_code, market, type)
            
            # 访问股票页面
            self.driver.get(url)
            self.random_delay()
            
            # 等待页面加载
            if not self.wait_for_page_load():
                self.logger.error(f"股票页面加载失败: {url}")
                return None
            
            # 查找行业代码
            industry_code = self.find_industry_codes()
            
            if not industry_code:
                self.logger.warning(f"股票 {stock_code}.{market} 未找到行业代码")
                return None
            
            self.logger.info(f"股票 {stock_code}.{market} 爬取完成，行业代码: {industry_code}")
            
        except Exception as e:
            self.logger.error(f"爬取股票 {stock_code}.{market} 失败: {e}")
        
        return industry_code
    
    def crawl_all_stocks(self, 
                        csv_file: str = os.path.join(PROJECT_ROOT, 'data', 'config', 'stock.csv'),
                        output_file: str = os.path.join(PROJECT_ROOT, 'data', 'raw', 'lixinger_industry_data.csv'),
                        max_stocks: int = None) -> None:
        """爬取所有股票的行业数据
        
        Args:
            csv_file: 输入股票数据文件
            output_file: 输出数据文件路径
            max_stocks: 最大爬取股票数量（用于测试）
        """
        try:
            # 读取股票代码
            stock_codes = self.get_stock_codes_from_csv(csv_file)
            
            if not stock_codes:
                self.logger.error("未找到有效的股票代码")
                return
            
            # 读取已爬取的股票代码，避免重复爬取
            crawled_codes = set()
            if os.path.exists(output_file):
                try:
                    df_existing = pd.read_csv(output_file,dtype={'股票代码': str})
                    crawled_codes = set(df_existing['股票代码'].astype(str).unique())
                    self.logger.info(f"检测到已爬取 {len(crawled_codes)} 个股票的行业数据")
                except Exception as e:
                    self.logger.warning(f"读取已存在输出文件失败，将重新爬取所有股票: {e}")

            # 过滤掉已爬取的股票
            stock_codes = [item for item in stock_codes if item[0] not in crawled_codes]
            if not stock_codes:
                self.logger.info("所有股票的行业数据均已爬取，无需重复爬取")
                return


            # 限制爬取数量（用于测试）
            if max_stocks:
                stock_codes = stock_codes[:max_stocks]
            
            self.logger.info(f"开始爬取 {len(stock_codes)} 个股票的行业数据")
            
            all_data = []
            
            # 创建或追加到输出文件
            header = not os.path.exists(output_file)
            
            for i, (stock_code, market, type) in enumerate(stock_codes):
                self.logger.info(f"处理第 {i + 1}/{len(stock_codes)} 个股票: {stock_code}.{market}")
                
                # 爬取单个股票数据
                industry_code = self.crawl_single_stock(stock_code, market, type)

                code = industry_code if industry_code is not None else '000000'
                
                # 添加股票代码、市场类型、行业链接、爬取时间
                industry_info= {
                    '股票代码': stock_code,
                    '市场类型': market,
                    '行业链接': code
                }
                
                # 转换为DataFrame并保存
                df_stock = pd.DataFrame([industry_info])
                
                # 追加到文件
                df_stock.to_csv(output_file, mode='a', header=header, index=False)
                
                # 第一次写入后，后续不再写入表头
                if header:
                    header = False
                
                all_data.extend(code)
                
                self.logger.info(f"股票 {stock_code}.{market} 数据已保存，当前累计 {len(all_data)} 条数据")
                
                # 随机延迟，避免被限流
                self.random_delay()
            
            self.logger.info(f"爬取完成！共获得 {len(all_data)} 条行业数据，已保存到 {output_file}")
            
        except Exception as e:
            self.logger.error(f"爬取所有股票数据失败: {e}")
        
    def close(self) -> None:
        """关闭浏览器驱动"""
        if self.driver:
            self.driver.quit()
            self.logger.info("浏览器驱动已关闭")


def main():
    """主函数"""
    crawler = None
    
    try:
        # 创建爬虫实例
        crawler = LixingerIndustryCrawler(
            headless=False,
            delay_min=1.0,
            delay_max=3.0,
            timeout=30
        )
        
        # 爬取所有股票数据（限制前10个用于测试）
        # crawler.crawl_all_stocks(max_stocks=10)
        crawler.crawl_all_stocks()
        # crawler.crawl_single_stock('688533', 'sh', 'stock')
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
    
    finally:
        # 确保关闭浏览器
        if crawler:
            crawler.close()


if __name__ == "__main__":
    main()