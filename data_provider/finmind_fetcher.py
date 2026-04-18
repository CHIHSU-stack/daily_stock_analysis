# -*- coding: utf-8 -*-
"""
===================================
FinMindFetcher - 台股專業數據源 (Priority 0)
===================================
數據來源：FinMind API
定位：提供台股最精準的 K 線、三大法人籌碼、融資融券等深度數據
"""

import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from FinMind.data import DataLoader
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource

logger = logging.getLogger(__name__)

class FinMindFetcher(BaseFetcher):
    """
    FinMind 數據源實現
    """
    name = "FinMindFetcher"
    # 預設優先級設為 0，讓它在台股分析中排在第一位
    priority = int(os.getenv("FINMIND_PRIORITY", "0"))

    def __init__(self):
        self.api_token = os.getenv("FINMIND_API_KEY") or os.getenv("FINMIND_TOKEN")
        self.api = DataLoader()
        if self.api_token:
            self.api.login(token=self.api_token)
            logger.info("FinMind API 登錄成功")
        else:
            logger.warning("未配置 FINMIND_API_KEY，將使用匿名限額模式")

    def _convert_stock_code(self, stock_code: str) -> str:
        """將 2330.TW 轉換為 FinMind 格式 (2330)"""
        return stock_code.replace('.TW', '').replace('.TWO', '').strip()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """獲取台股日線數據"""
        fm_code = self._convert_stock_code(stock_code)
        try:
            df = self.api.taiwan_stock_daily(
                stock_id=fm_code,
                start_date=start_date,
                end_date=end_date
            )
            if df.empty:
                raise DataFetchError(f"FinMind 未查詢到 {stock_code} 的數據")
            return df
        except Exception as e:
            raise DataFetchError(f"FinMind 獲取數據失敗: {e}")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """標準化 FinMind 數據格式"""
        df = df.copy()
        # FinMind 欄位對照: date, open, max, min, close, Trading_Volume, Trading_money
        column_mapping = {
            'date': 'date',
            'open': 'open',
            'max': 'high',
            'min': 'low',
            'close': 'close',
            'Trading_Volume': 'volume',
            'Trading_money': 'amount'
        }
        df = df.rename(columns=column_mapping)
        
        # 計算漲跌幅
        df['pct_chg'] = df['close'].pct_change() * 100
        df['pct_chg'] = df['pct_chg'].fillna(0).round(2)
        df['code'] = stock_code
        
        return df[['code'] + STANDARD_COLUMNS]

    # --- FinMind 獨有指標：籌碼面分析 ---

    def get_institutional_investors(self, stock_code: str, days: int = 10) -> pd.DataFrame:
        """獲取三大法人買賣超 (獨有指標)"""
        fm_code = self._convert_stock_code(stock_code)
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        try:
            df = self.api.taiwan_stock_institutional_investors(
                stock_id=fm_code,
                start_date=start_date
            )
            # 整理數據：將外資、投信、自營商買賣張數合計
            if not df.empty:
                df = df.groupby(['date', 'name']).sum().reset_index()
            return df
        except Exception as e:
            logger.warning(f"獲取三大法人數據失敗: {e}")
            return pd.DataFrame()

    def get_margin_purchase_short_sale(self, stock_code: str, days: int = 10) -> pd.DataFrame:
        """獲取融資融券數據 (獨有指標)"""
        fm_code = self._convert_stock_code(stock_code)
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        try:
            df = self.api.taiwan_stock_margin_purchase_short_sale(
                stock_id=fm_code,
                start_date=start_date
            )
            return df
        except Exception as e:
            logger.warning(f"獲取融資融券數據失敗: {e}")
            return pd.DataFrame()

def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """獲取 FinMind 即時行情並整合三大法人籌碼數據"""
        fm_code = self._convert_stock_code(stock_code)
        try:
            # 1. 獲取價格快照
            df = self.api.taiwan_stock_tick_snapshot(stock_ids=[fm_code])
            if df.empty: 
                return None
            
            row = df.iloc[0]
            
            # 2. 獲取籌碼面數據 (近 1 日，即今日買賣超)
            # 注意：盤中可能尚未更新，FinMind 籌碼通常在 15:30 後更新
            chip_info = ""
            try:
                # 調用我們之前定義的 get_institutional_investors 邏輯
                from datetime import datetime
                today_str = datetime.now().strftime('%Y-%m-%d')
                
                # 抓取最近 3 天的法人數據以判斷連續性
                inst_df = self.api.taiwan_stock_institutional_investors(
                    stock_id=fm_code, 
                    start_date=(datetime.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
                )
                
                if not inst_df.empty:
                    # 統計外資與投信近三日的累計買賣
                    foreign = inst_df[inst_df['name'] == 'Foreign_Investor']['buy'].sum() - inst_df[inst_df['name'] == 'Foreign_Investor']['sell'].sum()
                    itrust = inst_df[inst_df['name'] == 'Investment_Trust']['buy'].sum() - inst_df[inst_df['name'] == 'Investment_Trust']['sell'].sum()
                    chip_info = f"近3日外資累計:{int(foreign)}張, 投信累計:{int(itrust)}張"
            except Exception as chip_err:
                logger.debug(f"籌碼數據附加失敗: {chip_err}")

            # 3. 建立並回傳物件
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=row.get('name', stock_code),
                source=RealtimeSource.FINMIND,
                price=float(row['last_price']),
                change_pct=round(float(row['change_rate']), 2),
                change_amount=round(float(row['change_value']), 2),
                volume=int(row['trade_volume']),
                open_price=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                pre_close=float(row['last_close']),
                total_mv=None
            )
            
            # --- 關鍵點：將籌碼數據動態附加到物件中，讓 AI 序列化時能讀到 ---
            # 我們將數據塞進物件的 __dict__，這樣 runner.py 的 serialize_tool_result 就能抓到它
            setattr(quote, 'chip_analysis', chip_info)
            # 如果你的 UnifiedRealtimeQuote 有空閒欄位，也可以直接塞入
            # quote.turnover_rate = itrust # 假設用週轉率欄位代傳投信數據 (不推薦但可行)
            
            return quote

        except Exception as e:
            logger.error(f"FinMind 即時行情與籌碼獲取失敗: {e}")
            return None
