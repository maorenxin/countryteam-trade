#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富网股东明细数据Selenium爬虫

使用Selenium模拟真实浏览器行为，绕过东方财富网的反爬虫机制

功能：
1. 使用Selenium Chrome浏览器模拟真实用户行为
2. 爬取stock_holder.csv中112个企业的股东明细数据
3. 智能等待页面加载和数据渲染
4. 多页面翻页处理
5. 数据格式转换和清洗
6. 断点续传和错误重试机制

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
from typing import List, Dict, Optional
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

logger = LoggableMixin(name='SeleniumStockHolderCrawler', console=True).logger

# 数量转 int
def parse_wan_yi_num(s):
    if pd.isna(s) or s == '':
        return 0
    s = str(s).replace(',', '')
    if '万' in s:
        return int(float(s.replace('万', '')) * 10_000)
    elif '亿' in s:
        return int(float(s.replace('亿', '')) * 100_000_000)
    else:
        try:
            return int(float(s))
        except:
            return 0

class SeleniumStockHolderCrawler(LoggableMixin):
    """基于Selenium的股东明细数据爬虫类"""
    
    def __init__(self, 
                 headless: bool = True,
                 delay_min: float = 1.0, 
                 delay_max: float = 3.0,
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
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 设置用户代理
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 使用webdriver_manager自动管理驱动
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 隐藏自动化特征
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
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
    
    def wait_for_table_data(self) -> bool:
        """等待表格数据加载完成
        
        Returns:
            是否成功加载数据
        """
        try:
            # 等待数据容器出现
            data_container = self.wait_for_element(By.CLASS_NAME, 'dataview')
            if not data_container:
                return False
            
            # 等待表格出现
            table = self.wait_for_element(By.TAG_NAME, 'table')
            if not table:
                return False
            
            # 等待数据行加载（至少有一行数据）
            wait = WebDriverWait(self.driver, 10)
            wait.until(lambda driver: len(driver.find_elements(By.TAG_NAME, 'tr')) > 1)
            
            # 额外等待数据渲染
            time.sleep(2)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"等待表格数据加载失败: {e}")
            return False
    
    def extract_year_from_report_date(self, report_date: str) -> int:
        """从报告期提取年份
        
        Args:
            report_date: 报告期日期，格式如 "2025/9/30"
            
        Returns:
            年份
        """
        try:
            match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', report_date)
            if match:
                return int(match.group(1))
            else:
                date_obj = datetime.strptime(report_date, '%Y/%m/%d')
                return date_obj.year
        except Exception as e:
            self.logger.warning(f"无法从报告期 {report_date} 提取年份: {e}")
            return datetime.now().year
    
    def convert_announcement_date(self, announcement_date: str, report_date: str) -> str:
        """转换公告日格式
        
        Args:
            announcement_date: 公告日，格式如 "10月30日"
            report_date: 报告期，格式如 "2025/9/30"
            
        Returns:
            转换后的日期，格式如 "2025/10/30"
        """
        try:
            year = self.extract_year_from_report_date(report_date)
            
            match = re.match(r'(\d{1,2})月(\d{1,2})日', announcement_date)
            if match:
                month = int(match.group(1))
                day = int(match.group(2))
                return f"{year}/{month:02d}/{day:02d}"
            else:
                self.logger.warning(f"无法解析公告日格式: {announcement_date}")
                return announcement_date
        except Exception as e:
            self.logger.error(f"转换公告日格式失败: {announcement_date}, {report_date}, 错误: {e}")
            return announcement_date
    
    def get_total_pages(self) -> int:
        """获取总页数
        
        Returns:
            总页数
        """
        try:
            # 查找分页信息
            page_selectors = [
                '.page_info',
                '.paginate_button',
                '.pagination',
                '[data-page]'
            ]
            
            for selector in page_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        # 查找包含页码的元素
                        for element in elements:
                            text = element.text
                            if '共' in text and '页' in text:
                                match = re.search(r'共(\d+)页', text)
                                if match:
                                    return int(match.group(1))
                            
                            # 查找最后一个页码按钮
                            if element.get_attribute('data-page'):
                                pages = [el.get_attribute('data-page') for el in 
                                        self.driver.find_elements(By.CSS_SELECTOR, '[data-page]')]
                                if pages:
                                    return max([int(p) for p in pages if p.isdigit()])
                except:
                    continue
            
            # 默认只有1页
            return 1
            
        except Exception as e:
            self.logger.warning(f"获取总页数失败: {e}")
            return 1
    
    def parse_table_data(self, hd_code: str, pretify: bool = True) -> List[Dict]:
        """解析表格数据
        
        Args:
            hd_code: 股东代码
            pretify: 是否格式化数据，默认True
            
        Returns:
            解析后的数据列表
        """
        data_list = []
        
        try:
            # 查找数据表格 - 优先选择包含实际数据的表格
            table = None
            row_list = []
            
            # 查找表格
            try:
                tables = self.driver.find_elements(By.CSS_SELECTOR, 'table[data-type="sdltgd"]')
                title = tables[0]
                content = tables[1]
                if content:
                    rows = content.find_elements(By.CSS_SELECTOR, 'tbody tr')
            except:
                self.logger.warning(f"解析表格数据失败，hd_code: {hd_code}")

            # 解析rows，获取数据
            for row in rows:
                content = row.find_elements(By.TAG_NAME, 'td')
                text = [c.text for c in content]

                o = {
                        '股东代码': hd_code,
                        '股东名称': text[1],
                        '股票代码': text[4],
                        '股票简称': text[5],
                        '报告期': text[7],
                        '公告日': text[14],
                        '数量': text[8],
                        '流通市值': text[13],
                    }

                # 如果不需要格式化，直接添加
                if not pretify:
                    row_list.append(o)
                    continue

                # 下面是格式化的逻辑，最后也有row_list.append(o)
                # 报告期转 datetime，示例："2025-09-30" -> datetime.date(2025, 9, 30)
                o['报告期'] = pd.to_datetime(o['报告期'], format='%Y-%m-%d')

                # 公告日转 datetime，示例："09-30" -> datetime.date(2025, 9, 30)，需要补充报告期的年份
                # 但如果报告期是2024-10-31，但公告日是03-31，那么公告日需要补充2025年
                report_ts = o['报告期']
                report_year = report_ts.year if isinstance(report_ts, pd.Timestamp) else int(str(report_ts)[:4])
                announce_day = o['公告日']          # 形如 "09-30"
                # 如果公告月小于报告月，说明公告日跨年，年份+1
                announce_month = int(announce_day.split('-')[0])
                report_month = report_ts.month if isinstance(report_ts, pd.Timestamp) else int(str(report_ts)[5:7])
                announce_year = report_year + 1 if announce_month < report_month else report_year
                new_announce_day = f"{announce_year}-{announce_day}"
                o['公告日'] = pd.to_datetime(new_announce_day, format='%Y-%m-%d')

                # 把万和亿去掉变成int
                o['数量'] = parse_wan_yi_num(o['数量'])
                o['流通市值'] = parse_wan_yi_num(o['流通市值'])

                # 添加股东代码
                row_list.append(o)
        except Exception as e:
            self.logger.error(f"解析表格数据失败，hd_code: {hd_code}, 错误: {e}")
        
        return row_list
    
    def navigate_to_page(self, page: int) -> bool:
        """导航到指定页面
        
        Args:
            page: 页码
            
        Returns:
            是否成功导航
        """
        try:
            current_url = self.driver.current_url

            # 查找页码元素
            page_element = self.driver.find_element(By.CSS_SELECTOR, f'[data-page="{page}"]')
            
            # 使用JavaScript执行点击
            click_success = self.driver.execute_script("arguments[0].click();", page_element)

            if click_success is None or click_success:
                # 等待页面加载
                time.sleep(3)
                
                # 验证是否真的翻页了
                if self._verify_page_change(current_url, page):
                    self.logger.info(f"成功导航到第 {page} 页")
                    return True
                else:
                    self.logger.warning(f"页面导航后验证失败，可能未真正翻页")
        except NoSuchElementException:
                # 当前页面没有页码，直接返回True
                return True
        except Exception as e:
            self.logger.warning(f"导航到第 {page} 页失败: {e}")
            return False
    
    def _verify_page_change(self, original_url: str, target_page: int) -> bool:
        """验证页面是否真的发生了变化"""
        try:
            # 检查URL是否包含目标页码
            current_url = self.driver.current_url
            
            # 方法1: 检查URL中的page参数
            if f'page={target_page}' in current_url:
                return True
            
            # 方法2: 检查当前激活的页码按钮
            active_page_element = self.driver.find_element(By.CSS_SELECTOR, '[data-page].active')
            active_page = active_page_element.get_attribute('data-page')
            
            if active_page and str(target_page) == active_page:
                return True
            
            # 方法3: 检查表格数据是否更新
            if self.wait_for_table_data():
                return True
            
            return False
            
        except Exception as e:
            self.logger.warning(f"页面变化验证失败: {e}")
            return False
    
    def _use_pagination_input(self, page: int) -> bool:
        """使用分页输入框跳转页面"""
        try:
            # 查找页码输入框
            page_input = self.driver.find_element(By.CSS_SELECTOR, 'input.ipt')
            page_input.clear()
            page_input.send_keys(str(page))
            
            # 查找跳转按钮
            go_button = self.driver.find_element(By.CSS_SELECTOR, 'input.btn[type="submit"]')
            
            # 使用JavaScript点击，确保触发事件
            self.driver.execute_script("arguments[0].click();", go_button)
            
            return True
        except Exception as e:
            self.logger.warning(f"分页输入框方法失败: {e}")
            return False
    
    def crawl_single_company(self, hd_code: str, url: str, output_file: str = os.path.join(PROJECT_ROOT, 'data', 'raw', 'selenium_country_team_stock.csv')) -> List[Dict]:
        """爬取单个公司的增量数据
        
        Args:
            hd_code: 股东代码
            url: 基础URL
            output_file: 输出文件路径
            
        Returns:
            增量数据列表
        """
        all_data = []
        
        try:
            self.logger.info(f"开始爬取股东 {hd_code} 的增量数据")
            
            # (1) 从CSV文件中读取现有数据，获取最大报告期日期
            existing_data = pd.read_csv(output_file)
            existing_data['公告日'] = pd.to_datetime(existing_data['公告日'], format='%Y-%m-%d', errors='coerce')
            existing_data['报告期'] = pd.to_datetime(existing_data['报告期'], format='%Y-%m-%d', errors='coerce')
            existing_data['股东代码'] = existing_data['股东代码'].astype(str)
            
            company_existing_data = existing_data[existing_data['股东代码'] == hd_code]
            max_existing_date = None
            if not company_existing_data.empty:
                max_existing_date = company_existing_data['公告日'].max()
                self.logger.info(f"股东 {hd_code} 现有数据最大公告日: {max_existing_date}")

            # 访问页面
            self.driver.get(url)
            
            # 等待页面加载
            if not self.wait_for_table_data():
                self.logger.error(f"页面加载失败，hd_code: {hd_code}")
                return all_data
            
            # 获取总页数
            total_pages = self.get_total_pages()
            current_page = 1
            self.logger.info(f"股东 {hd_code} 共有 {total_pages} 页数据")
            
            # 从当前页开始，逐页爬取，直到最大报告期日期出现在数据中
            while max_existing_date not in [item.get('公告日', '') for item in all_data if item.get('公告日')]:
                self.random_delay()
                try:
                    if self.navigate_to_page(current_page):
                        page_data = self.parse_table_data(hd_code)
                        all_data.extend(page_data)
                        current_page += 1
                    
                    if current_page > total_pages:
                        self.logger.info(f"已到达最后一页，hd_code: {hd_code}")
                        break
                except Exception as e:
                    self.logger.error(f"爬取第 {current_page} 页失败，hd_code: {hd_code}, 错误: {e}")
                    break

            # 筛选增量数据即可
            if max_existing_date is not None:
                all_data = [item for item in all_data if item.get('公告日', '') > max_existing_date]
            self.logger.info(f"新增 {len(all_data)} 条数据，内容为: {all_data}")
            
        except Exception as e:
            self.logger.error(f"爬取公司增量数据失败，hd_code: {hd_code}, 错误: {e}")
        
        return all_data
    
    def crawl_all_companies(self, input_file: str = os.path.join(PROJECT_ROOT, 'data', 'config', 'stock_holder.csv'),
                           output_file: str = os.path.join(PROJECT_ROOT, 'data', 'raw', 'selenium_country_team_stock.csv')) -> None:
        """爬取所有公司的数据（增量写入）
        
        Args:
            input_file: 输入文件路径
            output_file: 输出数据文件路径
        """
        try:
            # 读取输入文件
            if not os.path.exists(input_file):
                self.logger.error(f"输入文件不存在: {input_file}")
                return
            
            df_input = pd.read_csv(input_file)
            self.logger.info(f"读取到 {len(df_input)} 个企业数据")
            
            # 获取已存在的数据
            existing_data = pd.read_csv(output_file)
            
            # 创建或追加到输出文件
            header = not os.path.exists(output_file)
            
            for index, row in df_input.iterrows():
                hd_code = str(row['code'])
                url = row['url']
                
                self.logger.info(f"处理第 {index + 1}/{len(df_input)} 个企业: {hd_code}")
                
                # 爬取单个公司增量数据
                company_data = self.crawl_single_company(hd_code, url, output_file)

                # 把 company_data 写入 output_file
                if company_data:
                    # 构造 DataFrame
                    df_all = pd.DataFrame(company_data)
                    # 若文件已存在则追加，否则写入表头
                    df_all.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False, encoding='utf-8-sig')
                    self.logger.info(f"股东 {hd_code} 的全部数据已写入 {output_file}，共 {len(df_all)} 条")
                else:
                    self.logger.info(f"股东 {hd_code} 无数据可写入")
                
                # 公司之间的延迟
                self.random_delay()
            
            self.logger.info(f"爬取完成，最终数据文件: {output_file}")
            
        except Exception as e:
            self.logger.error(f"爬取所有公司数据失败: {e}")
        finally:
            self.close()
    
    def close(self) -> None:
        """关闭浏览器驱动"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("浏览器驱动已关闭")
            except Exception as e:
                self.logger.error(f"关闭浏览器驱动失败: {e}")

def generate_quarterly_statistics(data_file: str = os.path.join(PROJECT_ROOT, 'data', 'raw', 'selenium_country_team_stock.csv'),
                                    share_output_file: str = os.path.join(PROJECT_ROOT, 'data', 'processed', 'quarterly_stock_statistics.csv'),
                                    announce_output_file: str = os.path.join(PROJECT_ROOT, 'data', 'processed', 'quarterly_announce_statistics.csv')) -> None:
    """生成季度统计CSV文件
    
    Args:
        data_file: 输入数据文件路径
        share_output_file: 输出统计文件路径
        announce_output_file: 输出公告统计文件路径
    """
    try:
        logger.info(f"开始生成季度统计文件，数据源: {data_file}")
        
        # 读取数据文件
        if not os.path.exists(data_file):
            logger.error(f"数据文件不存在: {data_file}")
            return
        
        df = pd.read_csv(data_file, dtype={'股票代码': str}, parse_dates=['公告日', '报告期'])
        
        # 数据预处理
        #df['公告日'] = pd.to_datetime(df['公告日'], format='%Y-%m-%d', errors='coerce')
        #df['报告期'] = pd.to_datetime(df['报告期'], format='%Y-%m-%d', errors='coerce')
        
        # 过滤掉日期无效的数据
        df = df.dropna(subset=['公告日', '报告期'])
        
        if df.empty:
            logger.warning("数据文件为空或所有日期数据无效")
            return
        
        # 数据预处理：将“报告期”统一对齐到对应季度的最后一天
        # 如果报告期是2025-03-01，则映射为2025-03-31
        df['report_q'] = df['报告期'].dt.to_period('Q')  # 直接得到PeriodIndex，无需再.dt
        # 在每个 report_q 内，如果存在多个“股东代码”和“股票简称”的数据，取 max("报告期") 的那条
        df = (
            df.sort_values(['股东代码', '股票代码', '报告期'], ascending=[True, True, False])
                .drop_duplicates(subset=['股东代码', '股票代码', 'report_q'], keep='first')
        )

        # 生成从2015-03-31开始的每个季度最后一天的序列（PeriodIndex）
        quarterly_periods = pd.period_range(
            start='2015Q1',
            end=pd.Timestamp.now().to_period('Q'),
            freq='Q'
        )

        df.to_csv(os.path.join(PROJECT_ROOT, 'data', 'raw', '国家队持股_清洗.csv'), index=False, encoding='utf-8-sig')

        # 用 pivot_table 一句话完成“按股票代码+report_q汇总数量”
        share_matrix = (
            df.pivot_table(
                index='股票代码',
                columns='report_q',
                values='数量',
                aggfunc='sum',
                fill_value=0
            )
            .reindex(columns=quarterly_periods, fill_value=0)   # 列标签与 quarterly_periods 保持一致
        )
        # 写入CSV文件（每次重写）
        share_matrix.to_csv(share_output_file, encoding='utf-8-sig')
        
        # 接下去用pivot_table做一个每个季度的公告日
        announcement_matrix = (
            df.pivot_table(
                index='股票代码',
                columns='report_q',
                values='公告日',
                aggfunc='max',
                fill_value=''
            )
            .reindex(columns=quarterly_periods, fill_value='')   # 列标签与 quarterly_periods 保持一致
        )
        # 写入CSV文件（每次重写）
        announcement_matrix.to_csv(announce_output_file, encoding='utf-8-sig')
        
        logger.info(f"季度统计文件已生成: {share_output_file}，形状: {share_matrix.shape}")
        logger.info(f"公告日统计文件已生成: {announce_output_file}，形状: {announcement_matrix.shape}")
        
    except Exception as e:
        logger.error(f"生成季度统计文件失败: {e}")

if __name__ == "__main__":
    """主函数"""    
    try:
        # # 创建爬虫实例
        # crawler = SeleniumStockHolderCrawler(
        #     headless=False,  # 设置为False可以查看浏览器操作
        #     delay_min=1.0,
        #     delay_max=3.0,
        #     timeout=30
        # )
        # #crawler.crawl_all_companies()
        # crawler.crawl_single_company(hd_code='70413155', url='https://data.eastmoney.com/gdfx/ShareHolderDetail.html?hdCode=70413155', output_file='selenium_country_team_stock.csv')

        # 产生统计数据
        # 爬取完成后生成季度统计文件
        generate_quarterly_statistics()
    except KeyboardInterrupt:
        logger.info("用户中断爬取过程")
    except Exception as e:
        logger.error(f"爬取过程发生错误: {e}")
    finally:
        #crawler.close()
        pass
