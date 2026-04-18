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
        # 直接在實例化時傳入 token
        if self.api_token:
            self.api = DataLoader()
            self.api.login_by_token(api_token=self.api_token)
            logger.info("FinMind API 使用 Token 登錄成功")
        else:
            self.api = DataLoader()
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
        actual_start = (pd.to_datetime(start_date) - pd.Timedelta(days=60)).strftime('%Y-%m-%d')
        try:
            df = self.api.taiwan_stock_daily(
                stock_id=fm_code,
                start_date=actual_start,
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
        """獲取 FinMind 即時行情並強制注入法人籌碼指標"""
        fm_code = self._convert_stock_code(stock_code)
        df = self.api.taiwan_stock_tick_snapshot(stock_ids=[fm_code])
        try:
            # 1. 獲取價格快照
            df = self.api.taiwan_stock_tick_snapshot(stock_ids=[fm_code])
            if df.empty:
                return None
            
            row = df.iloc[0]
            stock_name = row.get('name', stock_code)

            # 2. 獲取深度籌碼數據 (抓取近 5 日以確保數據連續性)
            chip_tag = ""
            inst_summary = "尚無數據"
            net_buy_volume = 0.0
            
            try:
                from datetime import datetime, timedelta
                # 往前抓 7 天確保能涵蓋到最近 3 個交易日
                start_dt = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                inst_df = self.api.taiwan_stock_institutional_investors(
                    stock_id=fm_code,
                    start_date=start_dt
                )
                
                if not inst_df.empty:
                    # 取最近一個交易日的數據
                    latest_date = inst_df['date'].max()
                    latest_inst = inst_df[inst_df['date'] == latest_date]
                    
                    # 計算外資與投信買賣超
                    foreign = latest_inst[latest_inst['name'] == 'Foreign_Investor']
                    itrust = latest_inst[latest_inst['name'] == 'Investment_Trust']
                    
                    f_net = int(foreign['buy'].sum() - foreign['sell'].sum())
                    i_net = int(itrust['buy'].sum() - itrust['sell'].sum())
                    net_buy_volume = float(f_net + i_net)
                    
                    # 建立標籤 (AI 看到名稱會直接被提示)
                    f_label = "外資買" if f_net > 0 else "外資賣"
                    i_label = "投信買" if i_net > 0 else "投信賣"
                    chip_tag = f"[{f_label}{abs(f_net)}|{i_label}{abs(i_net)}]"
                    inst_summary = f"日期:{latest_date}, 外資:{f_net}張, 投信:{i_net}張, 土洋買超比:{(net_buy_volume/row['trade_volume']*100):.2f}%"

            except Exception as ce:
                logger.debug(f"籌碼計算微調失敗: {ce}")

            # 3. 構造 UnifiedRealtimeQuote
            # 策略：將 chip_tag 塞進 name，將 net_buy_volume 塞進 turnover_rate
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=f"{stock_name} {chip_tag}", # 名稱增強：AI 輸出標題時就會帶入
                source=RealtimeSource.FINMIND,
                price=float(row['last_price']),
                change_pct=round(float(row['change_rate']), 2),
                change_amount=round(float(row['change_value']), 2),
                volume=int(row['trade_volume']),
                open_price=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                pre_close=float(row['last_close']),
                # 權宜之計：AI 非常看重 turnover_rate，我們把法人買超張數轉化後塞進去
                turnover_rate=round(net_buy_volume, 2), 
                total_mv=None
            )
            
            # 4. 強制注入額外屬性 (供 Runner 序列化)
            setattr(quote, 'chip_analysis', inst_summary)
            setattr(quote, 'foreign_net_buy', f_net if 'f_net' in locals() else 0)
            setattr(quote, 'trust_net_buy', i_net if 'i_net' in locals() else 0)

            logger.info(f"成功注入台股籌碼: {stock_code} -> {inst_summary}")
            return quote

        except Exception as e:
            logger.error(f"FinMind 即時行情重寫版獲取失敗: {e}")
            return None
