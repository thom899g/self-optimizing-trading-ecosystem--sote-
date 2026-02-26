"""
Real-time market data ingestion with robust error handling and edge case management.
Architectural Choice: Multi-source aggregation with weighted confidence scoring
to ensure data reliability during API failures or rate limits.
"""
import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger
import ccxt
import yfinance as yf
from pydantic import BaseModel, ValidationError

class MarketDataPoint(BaseModel):
    """Validated market data structure"""
    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    
class DataIngestionEngine:
    """Robust multi-source data ingestion with fallback strategies"""
    
    def __init__(self, exchange_id: str = "binance"):
        self.exchange = self._initialize_exchange(exchange_id)
        self.data_cache: Dict[str, List[MarketDataPoint]] = {}
        self.last_update: Dict[str, datetime] = {}
        self.source_weights = {
            'ccxt': 0.7,
            'yfinance': 0.3,
            'alpha_vantage': 0.2
        }
        
    def _initialize_exchange(self, exchange_id: str) -> Optional[ccxt.Exchange]:
        """Initialize CCXT exchange with error handling"""
        try:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'enableRateLimit': True,
                'timeout': 30000,
            })
            
            # Test connectivity
            exchange.load_markets()
            logger.info(f"Initialized {exchange_id} with {len(exchange.markets)} markets")
            return exchange
            
        except Exception as e:
            logger.error(f"Failed to initialize {exchange_id}: {e}")
            return None
            
    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1h', 
                          limit: int = 100) -> List[MarketDataPoint]:
        """Fetch OHLCV data with multi-source fallback"""
        data_points = []
        
        # Primary source: CCXT
        if self.exchange and self.exchange.has['fetchOHLCV']:
            try:
                ohlcv = await asyncio.to_thread(
                    self.exchange.fetch_ohlcv, 
                    symbol, 
                    timeframe, 
                    limit=limit
                )
                
                for entry in ohlcv:
                    data_points.append(MarketDataPoint(
                        timestamp=datetime.fromtimestamp(entry[0] / 1000),
                        symbol=symbol,
                        open=entry[1],
                        high=entry[2],
                        low=entry[3],
                        close=entry[4],
                        volume=entry[5],
                        source='ccxt',
                        confidence=self.source_weights['ccxt']
                    ))
                    
            except (ccxt.NetworkError, ccxt.ExchangeError