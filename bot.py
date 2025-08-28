import logging
import re
import os
import tempfile
import asyncio
import concurrent.futures
import hashlib
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import yt_dlp

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "your_bot_token")
BOT_USERNAME = "eyysavebot"
STORAGE_CHAT_ID = os.getenv("STORAGE_CHAT_ID", "-1001234567890")  # Private channel for video storage
MAX_FILE_SIZE = 50 * 1024 * 1024
DATABASE_PATH = "bot_analytics.db"

class AnalyticsManager:
    """Handles all analytics tracking and database operations"""
    
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with all necessary tables"""
        conn = sqlite3.connect(self.db_path)
        
        # Users table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_requests INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        # Video requests table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS video_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                url TEXT NOT NULL,
                url_hash TEXT NOT NULL,
                platform TEXT NOT NULL,
                title TEXT,
                file_size INTEGER,
                processing_time REAL,
                status TEXT NOT NULL,  -- 'success', 'failed', 'cached'
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Video cache table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS video_cache (
                url_hash TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                title TEXT,
                platform TEXT,
                file_size INTEGER,
                download_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Daily stats table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                new_users INTEGER DEFAULT 0,
                total_requests INTEGER DEFAULT 0,
                successful_downloads INTEGER DEFAULT 0,
                failed_downloads INTEGER DEFAULT 0,
                cached_requests INTEGER DEFAULT 0,
                unique_active_users INTEGER DEFAULT 0,
                tiktok_requests INTEGER DEFAULT 0,
                instagram_requests INTEGER DEFAULT 0,
                avg_processing_time REAL DEFAULT 0
            )
        ''')
        
        # Monthly stats table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS monthly_stats (
                month TEXT PRIMARY KEY,  -- Format: 'YYYY-MM'
                new_users INTEGER DEFAULT 0,
                total_requests INTEGER DEFAULT 0,
                successful_downloads INTEGER DEFAULT 0,
                failed_downloads INTEGER DEFAULT 0,
                cached_requests INTEGER DEFAULT 0,
                unique_active_users INTEGER DEFAULT 0,
                tiktok_requests INTEGER DEFAULT 0,
                instagram_requests INTEGER DEFAULT 0,
                avg_processing_time REAL DEFAULT 0,
                total_data_processed INTEGER DEFAULT 0  -- in bytes
            )
        ''')
        
        # Create indexes for better performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_user_requests ON video_requests(user_id, timestamp)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_request_date ON video_requests(DATE(timestamp))')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cache_access ON video_cache(last_accessed)')
        
        conn.commit()
        conn.close()
        
        logger.info("Analytics database initialized successfully")
    
    def track_user(self, user_data: Dict) -> bool:
        """Track user information and update last active time"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            user_id = user_data['id']
            now = datetime.now()
            
            # Check if user exists
            cursor = conn.execute(
                "SELECT total_requests, first_seen FROM users WHERE user_id = ?",
                (user_id,)
            )
            result = cursor.fetchone()
            
            is_new_user = result is None
            
            if is_new_user:
                # New user
                conn.execute('''
                    INSERT INTO users (user_id, username, first_name, last_name, language_code, first_seen, last_active, total_requests)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                ''', (
                    user_id,
                    user_data.get('username'),
                    user_data.get('first_name'),
                    user_data.get('last_name'),
                    user_data.get('language_code'),
                    now,
                    now
                ))
                
                # Update daily stats for new user
                today = now.strftime('%Y-%m-%d')
                conn.execute('''
                    INSERT OR IGNORE INTO daily_stats (date) VALUES (?)
                ''', (today,))
                conn.execute('''
                    UPDATE daily_stats SET new_users = new_users + 1 WHERE date = ?
                ''', (today,))
                
                # Update monthly stats for new user
                month = now.strftime('%Y-%m')
                conn.execute('''
                    INSERT OR IGNORE INTO monthly_stats (month) VALUES (?)
                ''', (month,))
                conn.execute('''
                    UPDATE monthly_stats SET new_users = new_users + 1 WHERE month = ?
                ''', (month,))
                
            else:
                # Existing user - update last active and info
                conn.execute('''
                    UPDATE users SET 
                        last_active = ?,
                        username = ?,
                        first_name = ?,
                        last_name = ?,
                        language_code = ?
                    WHERE user_id = ?
                ''', (now, user_data.get('username'), user_data.get('first_name'), 
                     user_data.get('last_name'), user_data.get('language_code'), user_id))
            
            conn.commit()
            return is_new_user
            
        except Exception as e:
            logger.error(f"Error tracking user: {e}")
            return False
        finally:
            conn.close()
    
    def track_video_request(self, user_id: int, url: str, platform: str, 
                           status: str, title: str = None, file_size: int = 0,
                           processing_time: float = 0, error_message: str = None):
        """Track video download request"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            now = datetime.now()
            url_hash = hashlib.md5(url.encode()).hexdigest()
            
            # Insert request record
            conn.execute('''
                INSERT INTO video_requests 
                (user_id, url, url_hash, platform, title, file_size, processing_time, status, error_message, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, url, url_hash, platform, title, file_size, processing_time, status, error_message, now))
            
            # Update user total requests
            conn.execute('''
                UPDATE users SET total_requests = total_requests + 1 WHERE user_id = ?
            ''', (user_id,))
            
            # Update daily stats
            today = now.strftime('%Y-%m-%d')
            conn.execute('INSERT OR IGNORE INTO daily_stats (date) VALUES (?)', (today,))
            
            stats_update = {
                'total_requests': 1,
                f'{platform}_requests': 1
            }
            
            if status == 'success':
                stats_update['successful_downloads'] = 1
            elif status == 'failed':
                stats_update['failed_downloads'] = 1
            elif status == 'cached':
                stats_update['cached_requests'] = 1
            
            for field, increment in stats_update.items():
                conn.execute(f'''
                    UPDATE daily_stats SET {field} = {field} + ? WHERE date = ?
                ''', (increment, today))
            
            # Update monthly stats
            month = now.strftime('%Y-%m')
            conn.execute('INSERT OR IGNORE INTO monthly_stats (month) VALUES (?)', (month,))
            
            for field, increment in stats_update.items():
                if field != 'avg_processing_time':
                    conn.execute(f'''
                        UPDATE monthly_stats SET {field} = {field} + ? WHERE month = ?
                    ''', (increment, month))
            
            if file_size > 0:
                conn.execute('''
                    UPDATE monthly_stats SET total_data_processed = total_data_processed + ? WHERE month = ?
                ''', (file_size, month))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error tracking video request: {e}")
        finally:
            conn.close()
    
    def update_daily_active_users(self):
        """Update unique active users count for today"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Count unique users who made requests today
            cursor = conn.execute('''
                SELECT COUNT(DISTINCT user_id) 
                FROM video_requests 
                WHERE DATE(timestamp) = ?
            ''', (today,))
            active_users = cursor.fetchone()[0]
            
            conn.execute('''
                UPDATE daily_stats SET unique_active_users = ? WHERE date = ?
            ''', (active_users, today))
            
            # Update monthly active users
            month = datetime.now().strftime('%Y-%m')
            cursor = conn.execute('''
                SELECT COUNT(DISTINCT user_id) 
                FROM video_requests 
                WHERE strftime('%Y-%m', timestamp) = ?
            ''', (month,))
            monthly_active = cursor.fetchone()[0]
            
            conn.execute('''
                UPDATE monthly_stats SET unique_active_users = ? WHERE month = ?
            ''', (monthly_active, month))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating active users: {e}")
        finally:
            conn.close()
    
    def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Get statistics for a specific user"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # User info
            cursor = conn.execute('''
                SELECT username, first_name, total_requests, first_seen, last_active
                FROM users WHERE user_id = ?
            ''', (user_id,))
            user_info = cursor.fetchone()
            
            if not user_info:
                return {}
            
            # Request breakdown
            cursor = conn.execute('''
                SELECT status, COUNT(*) as count
                FROM video_requests 
                WHERE user_id = ?
                GROUP BY status
            ''', (user_id,))
            status_counts = dict(cursor.fetchall())
            
            # Platform breakdown
            cursor = conn.execute('''
                SELECT platform, COUNT(*) as count
                FROM video_requests 
                WHERE user_id = ?
                GROUP BY platform
            ''', (user_id,))
            platform_counts = dict(cursor.fetchall())
            
            # Recent activity (last 7 days)
            seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor = conn.execute('''
                SELECT COUNT(*) FROM video_requests 
                WHERE user_id = ? AND DATE(timestamp) >= ?
            ''', (user_id, seven_days_ago))
            recent_activity = cursor.fetchone()[0]
            
            return {
                'username': user_info[0],
                'first_name': user_info[1],
                'total_requests': user_info[2],
                'first_seen': user_info[3],
                'last_active': user_info[4],
                'status_breakdown': status_counts,
                'platform_breakdown': platform_counts,
                'requests_last_7_days': recent_activity
            }
            
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return {}
        finally:
            conn.close()
    
    def get_analytics_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive analytics summary"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # Total users
            cursor = conn.execute('SELECT COUNT(*) FROM users')
            total_users = cursor.fetchone()[0]
            
            # New users in period
            cursor = conn.execute('SELECT COUNT(*) FROM users WHERE DATE(first_seen) >= ?', (cutoff_date,))
            new_users = cursor.fetchone()[0]
            
            # Total requests in period
            cursor = conn.execute('SELECT COUNT(*) FROM video_requests WHERE DATE(timestamp) >= ?', (cutoff_date,))
            total_requests = cursor.fetchone()[0]
            
            # Success rate
            cursor = conn.execute('''
                SELECT 
                    SUM(CASE WHEN status = 'success' OR status = 'cached' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM video_requests WHERE DATE(timestamp) >= ?
            ''', (cutoff_date,))
            success_rate = cursor.fetchone()[0] or 0
            
            # Platform breakdown
            cursor = conn.execute('''
                SELECT platform, COUNT(*) as count
                FROM video_requests 
                WHERE DATE(timestamp) >= ?
                GROUP BY platform
            ''', (cutoff_date,))
            platform_stats = dict(cursor.fetchall())
            
            # Cache hit rate
            cursor = conn.execute('''
                SELECT 
                    SUM(CASE WHEN status = 'cached' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as cache_rate
                FROM video_requests WHERE DATE(timestamp) >= ?
            ''', (cutoff_date,))
            cache_rate = cursor.fetchone()[0] or 0
            
            # Active users in period
            cursor = conn.execute('''
                SELECT COUNT(DISTINCT user_id) 
                FROM video_requests 
                WHERE DATE(timestamp) >= ?
            ''', (cutoff_date,))
            active_users = cursor.fetchone()[0]
            
            # Top users
            cursor = conn.execute('''
                SELECT u.username, u.first_name, COUNT(vr.id) as requests
                FROM users u
                LEFT JOIN video_requests vr ON u.user_id = vr.user_id
                WHERE DATE(vr.timestamp) >= ?
                GROUP BY u.user_id
                ORDER BY requests DESC
                LIMIT 10
            ''', (cutoff_date,))
            top_users = cursor.fetchall()
            
            return {
                'period_days': days,
                'total_users': total_users,
                'new_users': new_users,
                'active_users': active_users,
                'total_requests': total_requests,
                'success_rate': round(success_rate, 2),
                'cache_hit_rate': round(cache_rate, 2),
                'platform_stats': platform_stats,
                'top_users': top_users
            }
            
        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {}
        finally:
            conn.close()

class TelegramVideoStorage:
    """Handles video storage using Telegram as backend"""
    
    def __init__(self, bot_token: str, storage_chat_id: str, analytics: AnalyticsManager):
        self.bot = Bot(token=bot_token)
        self.storage_chat_id = storage_chat_id
        self.analytics = analytics
    
    def _get_url_hash(self, url: str) -> str:
        """Create hash from URL for caching"""
        return hashlib.md5(url.encode()).hexdigest()
    
    async def get_cached_video(self, url: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
        """Check if video is already cached"""
        url_hash = self._get_url_hash(url)
        
        conn = sqlite3.connect(DATABASE_PATH)
        try:
            cursor = conn.execute('''
                SELECT file_id, title, file_size FROM video_cache WHERE url_hash = ?
            ''', (url_hash,))
            result = cursor.fetchone()
            
            if result:
                # Update last accessed time and download count
                conn.execute('''
                    UPDATE video_cache 
                    SET last_accessed = CURRENT_TIMESTAMP, download_count = download_count + 1
                    WHERE url_hash = ?
                ''', (url_hash,))
                conn.commit()
                
                return result[0], result[1], result[2]  # file_id, title, file_size
            
        except Exception as e:
            logger.error(f"Error checking cache: {e}")
        finally:
            conn.close()
            
        return None, None, None
    
    async def store_video(self, url: str, video_path: str, title: str, platform: str) -> tuple[str, int]:
        """Store video in Telegram and cache the file_id"""
        url_hash = self._get_url_hash(url)
        file_size = os.path.getsize(video_path)
        
        try:
            # Upload to storage channel
            with open(video_path, 'rb') as video_file:
                message = await self.bot.send_video(
                    chat_id=self.storage_chat_id,
                    video=video_file,
                    caption=f"🔗 {url[:100]}...\n📱 {platform}\n📹 {title[:50]}...\n📊 {file_size / 1024 / 1024:.1f}MB",
                    supports_streaming=True
                )
            
            file_id = message.video.file_id
            
            # Cache in database
            conn = sqlite3.connect(DATABASE_PATH)
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO video_cache 
                    (url_hash, file_id, title, platform, file_size, download_count, created_at, last_accessed) 
                    VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', (url_hash, file_id, title, platform, file_size))
                conn.commit()
            finally:
                conn.close()
            
            return file_id, file_size
            
        except Exception as e:
            logger.error(f"Error storing video: {e}")
            raise e
    
    async def send_cached_video(self, chat_id: int, file_id: str, title: str, platform: str):
        """Send cached video to user"""
        await self.bot.send_video(
            chat_id=chat_id,
            video=file_id,
            caption=f"✅ {title}\n📱 From {platform.title()}\n⚡ Cached\n🤖 @{BOT_USERNAME}",
            supports_streaming=True
        )

# Video download functions (keeping your existing logic)
def is_tiktok_url(url: str) -> bool:
    return 'tiktok.com' in url.lower() or 'vm.tiktok.com' in url.lower()

def is_instagram_url(url: str) -> bool:
    patterns = ['instagram.com', 'instagr.am', 'ig.me']
    return any(pattern in url.lower() for pattern in patterns)

def download_video(url: str, platform: str) -> tuple[Optional[str], str]:
    """Your existing download function - keeping it unchanged"""
    temp_dir = tempfile.mkdtemp()
    output_path = os.path.join(temp_dir, "video.mp4")
    
    base_opts = {
        'outtmpl': output_path,
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'format': 'best[ext=mp4][vcodec^=avc1]/best[ext=mp4]/bestvideo[ext=mp4]+bestaudio[ext=m4a]/best',
        'merge_output_format': 'mp4',
        'postprocessor_args': [
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-movflags', '+faststart'
        ],
        'socket_timeout': 30,
        'retries': 3,
    }
    
    if platform == "tiktok":
        methods = [
            {**base_opts, 'format': 'best[ext=mp4][vcodec^=avc1][height<=1080]/best[ext=mp4][height<=1080]/best[ext=mp4]/best'},
            {**base_opts, 'format': 'best[ext=mp4]/best'},
        ]
    else:
        methods = [
            {**base_opts, 'format': 'best[ext=mp4][vcodec^=avc1][height<=1080]/best[ext=mp4][height<=1080]/best[ext=mp4]/best'},
            {**base_opts, 'format': 'best[height<=720]/best'},
        ]
    
    for method_opts in methods:
        try:
            with yt_dlp.YoutubeDL(method_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                
                for file in os.listdir(temp_dir):
                    if file.startswith('video') and (file.endswith('.mp4') or file.endswith('.webm')):
                        actual_path = os.path.join(temp_dir, file)
                        return actual_path, info.get('title', 'Video')
        except Exception as e:
            logger.error(f"Download method failed: {e}")
            continue
    
    return None, "Failed to download video"

# Initialize components
analytics = AnalyticsManager()
video_storage = TelegramVideoStorage(BOT_TOKEN, STORAGE_CHAT_ID, analytics)

# Bot handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user_data = update.effective_user.to_dict()
    is_new_user = analytics.track_user(user_data)
    
    welcome_text = (
        f"👋 Welcome to @{BOT_USERNAME}!\n\n"
        "📱 Send me a TikTok or Instagram video link and I'll download it without watermarks.\n\n"
        "🔗 Supported platforms:\n"
        "• TikTok\n"
        "• Instagram (public videos)\n\n"
        "💡 Just paste the link and I'll handle the rest!\n"
        f"{'🎉 Thanks for joining!' if is_new_user else '👋 Welcome back!'}"
    )
    await update.message.reply_text(welcome_text)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler"""
    user_data = update.effective_user.to_dict()
    analytics.track_user(user_data)
    
    help_text = (
        "📖 How to use:\n\n"
        "1️⃣ Copy a TikTok or Instagram video link\n"
        "2️⃣ Send it to me\n"
        "3️⃣ I'll download and send the video back\n\n"
        "📊 Commands:\n"
        "• /start - Start the bot\n"
        "• /help - Show this help\n"
        "• /stats - Your personal stats\n"
        "• /mystats - Detailed statistics\n\n"
        "⚠️ Notes:\n"
        "• Videos must be under 50MB\n"
        "• Instagram videos must be public\n"
        "• Quality: Up to 1080p\n\n"
        "❓ Need help? Contact the developer"
    )
    await update.message.reply_text(help_text)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics"""
    user_data = update.effective_user.to_dict()
    analytics.track_user(user_data)
    
    user_stats = analytics.get_user_stats(update.effective_user.id)
    
    if not user_stats:
        await update.message.reply_text("❌ No statistics found.")
        return
    
    stats_text = (
        f"📊 **Your Statistics**\n\n"
        f"👤 User: {user_stats.get('first_name', 'Unknown')}\n"
        f"🔢 Total Requests: {user_stats['total_requests']}\n"
        f"📅 Member Since: {user_stats['first_seen'][:10]}\n"
        f"⏰ Last Active: {user_stats['last_active'][:10]}\n"
        f"📱 Last 7 Days: {user_stats['requests_last_7_days']} requests\n\n"
    )
    
    if user_stats['platform_breakdown']:
        stats_text += "📱 **Platform Usage:**\n"
        for platform, count in user_stats['platform_breakdown'].items():
            stats_text += f"• {platform.title()}: {count} videos\n"
        stats_text += "\n"
    
    if user_stats['status_breakdown']:
        stats_text += "✅ **Success Rate:**\n"
        total = sum(user_stats['status_breakdown'].values())
        for status, count in user_stats['status_breakdown'].items():
            percentage = (count / total * 100) if total > 0 else 0
            emoji = {'success': '✅', 'cached': '⚡', 'failed': '❌'}.get(status, '📊')
            stats_text += f"• {emoji} {status.title()}: {count} ({percentage:.1f}%)\n"
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show bot analytics (you can add admin check here)"""
    # TODO: Add admin user ID check here
    # if update.effective_user.id not in ADMIN_IDS:
    #     return
    
    analytics_data = analytics.get_analytics_summary(30)
    
    if not analytics_data:
        await update.message.reply_text("❌ No analytics data found.")
        return
    
    stats_text = (
        f"📊 **Bot Analytics (Last 30 Days)**\n\n"
        f"👥 Total Users: {analytics_data['total_users']}\n"
        f"🆕 New Users: {analytics_data['new_users']}\n"
        f"🔥 Active Users: {analytics_data['active_users']}\n"
        f"📱 Total Requests: {analytics_data['total_requests']}\n"
        f"✅ Success Rate: {analytics_data['success_rate']}%\n"
        f"⚡ Cache Hit Rate: {analytics_data['cache_hit_rate']}%\n\n"
    )
    
    if analytics_data['platform_stats']:
        stats_text += "📱 **Platform Breakdown:**\n"
        for platform, count in analytics_data['platform_stats'].items():
            stats_text += f"• {platform.title()}: {count}\n"
        stats_text += "\n"
    
    if analytics_data['top_users']:
        stats_text += "🏆 **Top Users:**\n"
        for i, (username, first_name, requests) in enumerate(analytics_data['top_users'][:5], 1):
            name = username or first_name or "Unknown"
            stats_text += f"{i}. {name}: {requests} requests\n"
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def handle_video_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main video processing handler with full analytics tracking"""
    start_time = datetime.now()
    url = update.message.text.strip()
    user_data = update.effective_user.to_dict()
    user_id = update.effective_user.id
    
    # Track user
    analytics.track_user(user_data)
    
    if not (is_tiktok_url(url) or is_instagram_url(url)):
        await update.message.reply_text("❌ Please send a valid TikTok or Instagram video link.")
        return
    
    platform = "tiktok" if is_tiktok_url(url) else "instagram"
    
    # Check cache first
    processing_msg = await update.message.reply_text("🔍 Checking cache...")
    
    cached_file_id, cached_title, cached_size = await video_storage.get_cached_video(url)
    
    if cached_file_id:
        try:
            await video_storage.send_cached_video(
                update.effective_chat.id, cached_file_id, cached_title, platform
            )
            await processing_msg.delete()
            
            # Track cached request
            processing_time = (datetime.now() - start_time).total_seconds()
            analytics.track_video_request(
                user_id, url, platform, 'cached', 
                cached_title, cached_size, processing_time
            )
            analytics.update_daily_active_users()
            return
            
        except Exception as e:
            logger.error(f"Error sending cached video: {e}")
            await processing_msg.edit_text("⏳ Cache unavailable, downloading fresh...")
    
    # Download fresh video
    await processing_msg.edit_text("⏳ Downloading video... This may take a moment.")
    
         try:
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                video_path, title = await loop.run_in_executor(
                    executor, download_video, url, platform
                )
        
            # CORRECTED INDENTATION - this line was misaligned
            if not video_path or not os.path.exists(video_path):
                await processing_msg.edit_text("❌ Failed to download video. Please try again later.")
                
                # Track failed request
                processing_time = (datetime.now() - start_time).total_seconds()
                analytics.track_video_request(
                    user_id, url, platform, 'failed', 
                    None, 0, processing_time, "Download failed"
                )
                analytics.update_daily_active_users()
                return
            
            # Check file size
            file_size = os.path.getsize(video_path)
            if file_size > MAX_FILE_SIZE:
                await processing_msg.edit_text(
                    f"❌ Video is too large ({file_size / 1024 / 1024:.1f}MB). "
                    f"Maximum size is {MAX_FILE_SIZE / 1024 / 1024}MB."
                )
                
                # Track failed request
                processing_time = (datetime.now() - start_time).total_seconds()
                analytics.track_video_request(
                    user_id, url, platform, 'failed', 
                    title, file_size, processing_time, "File too large"
                )
                analytics.update_daily_active_users()
                
                # Cleanup
                if os.path.exists(video_path):
                    os.remove(video_path)
                return
            
            # Store in cache and get file_id
            await processing_msg.edit_text("📤 Uploading video...")
            
            file_id, stored_size = await video_storage.store_video(url, video_path, title, platform)
            
            # Send to user
            await update.message.reply_video(
                video=file_id,
                caption=f"✅ {title}\n📱 From {platform.title()}\n🤖 @{BOT_USERNAME}",
                supports_streaming=True
            )
            
            await processing_msg.delete()
            
            # Track successful request
            processing_time = (datetime.now() - start_time).total_seconds()
            analytics.track_video_request(
                user_id, url, platform, 'success', 
                title, stored_size, processing_time
            )
            analytics.update_daily_active_users()
            
            # Cleanup temp files
            if os.path.exists(video_path):
                os.remove(video_path)
                temp_dir = os.path.dirname(video_path)
                if os.path.exists(temp_dir):
                    try:
                        os.rmdir(temp_dir)
                    except:
                        pass
            
        except Exception as e:
            logger.error(f"Error processing video: {e}")
            
            try:
                await processing_msg.edit_text(
                    "❌ An error occurred while processing your request. Please try again."
                )
            except:
                pass
            
            # Track failed request
            processing_time = (datetime.now() - start_time).total_seconds()
            analytics.track_video_request(
                user_id, url, platform, 'failed', 
                None, 0, processing_time, str(e)
            )
            analytics.update_daily_active_users()
            
            # Cleanup on error
            if 'video_path' in locals() and video_path and os.path.exists(video_path):
                os.remove(video_path)
                temp_dir = os.path.dirname(video_path)
                if os.path.exists(temp_dir):
                    try:
                        os.rmdir(temp_dir)
                    except:
                        pass

        
    except Exception as e:
        logger.error(f"Error processing video: {e}")
        
        try:
            await processing_msg.edit_text(
                "❌ An error occurred while processing your request. Please try again."
            )
        except:
            pass
        
        # Track failed request
        processing_time = (datetime.now() - start_time).total_seconds()
        analytics.track_video_request(
            user_id, url, platform, 'failed', 
            None, 0, processing_time, str(e)
        )
        analytics.update_daily_active_users()
        
        # Cleanup on error
        if 'video_path' in locals() and video_path and os.path.exists(video_path):
            os.remove(video_path)
            temp_dir = os.path.dirname(video_path)
            if os.path.exists(temp_dir):
                try:
                    os.rmdir(temp_dir)
                except:
                    pass

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log errors caused by Updates"""
    logger.error(f"Update {update} caused error {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ An unexpected error occurred. Please try again later."
            )
        except:
            pass

def main():
    """Start the bot"""
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("mystats", stats_command))  # Alias for stats
    application.add_handler(CommandHandler("admin", admin_stats_command))
    
    # Add message handler for video links
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_video_link
    ))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Start bot
    logger.info(f"Starting @{BOT_USERNAME}...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    # Verify environment variables
    if BOT_TOKEN == "your_bot_token":
        logger.error("Please set the BOT_TOKEN environment variable!")
        exit(1)
    
    if STORAGE_CHAT_ID == "-1001234567890":
        logger.error("Please set the STORAGE_CHAT_ID environment variable!")
        exit(1)
    
    # Run the bot
    main()