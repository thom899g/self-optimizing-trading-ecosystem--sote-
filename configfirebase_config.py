"""
Firebase configuration for SOTE state management.
CRITICAL: Firebase is used for real-time state synchronization, 
portfolio tracking, and distributed system coordination.
"""
import os
import json
from typing import Optional
from loguru import logger
from pydantic import BaseSettings, Field

class FirebaseConfig(BaseSettings):
    """Firebase configuration with validation"""
    GOOGLE_APPLICATION_CREDENTIALS: str = Field(
        default="config/firebase_credentials.json",
        description="Path to Firebase service account JSON"
    )
    FIRESTORE_COLLECTION: str = "sote_trading_state"
    REALTIME_DB_URL: str = Field(
        default="https://sote-trading-default-rtdb.firebaseio.com/",
        description="Firebase Realtime Database URL"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        
    def initialize(self) -> bool:
        """Initialize Firebase with robust error handling"""
        try:
            import firebase_admin
            from firebase_admin import credentials, firestore, db
            
            # Check if Firebase app already exists
            if not firebase_admin._apps:
                # Verify credentials file exists
                if not os.path.exists(self.GOOGLE_APPLICATION_CREDENTIALS):
                    logger.error(f"Firebase credentials not found at {self.GOOGLE_APPLICATION_CREDENTIALS}")
                    return False
                    
                cred = credentials.Certificate(self.GOOGLE_APPLICATION_CREDENTIALS)
                firebase_admin.initialize_app(cred, {
                    'databaseURL': self.REALTIME_DB_URL
                })
                logger.success("Firebase initialized successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Firebase initialization failed: {e}")
            return False