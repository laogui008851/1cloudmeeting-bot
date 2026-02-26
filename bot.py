# -*- coding: utf-8 -*-
"""
云际会议 · 自用型克隆机器人

逻辑极简：
  1. 从主机器人购买 / 主机器人赠送 → 主机器人发来含 #YUNJICODE:XXXX 的消息
     → 管理员将该消息转发给本机器人 → 自动识别并入库，无需任何手动录入
  2. 用户点「领取授权码」→ 从本地数据库取一个可用码发给用户
  3. 用户点「查询授权码」→ 看已领取的码 + 实时状态 + 剩余时间 + 可释放

克隆机器人不能自己生成授权码！码只来自主机器人下发。
"""
import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

load_dotenv()

# ============================================================
#  配置
# ============================================================
BOT_TOKEN    = os.getenv('BOT_TOKEN', '')
OWNER_ID     = int(os.getenv('OWNER_TELEGRAM_ID', '0'))
ADMIN_IDS    = {int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()}
ADMIN_IDS.add(OWNER_ID)
# 自己独立的数据库
LOCAL_DB = Path(os.getenv(
    'LOCAL_DB_PATH',
    str(Path(__file__).parent / 'data' / 'bot.db')
))
# 主机器人数据库（用于注册自身为代理）
MASTER_DB = Path(os.getenv(
    'MASTER_DB_PATH',
    str(Path(__file__).parent.parent / 'cloudmeeting-bot' / 'data' / 'master_bot.db')
))

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def register_to_master():
    """启动时将自身 BOT_TOKEN + OWNER_ID + 本地DB路径 注册进主机器人 agents 表"""
    if not MASTER_DB.exists():
        logger.warning(f'主机器人数据库不存在，跳过注册: {MASTER_DB}')
        return
    if not BOT_TOKEN or not OWNER_ID:
        logger.warning('BOT_TOKEN 或 OWNER_ID 未设置，跳过注册')
        return
    try:
        conn = sqlite3.connect(str(MASTER_DB))
        # 确保列存在
        cols = {r[1] for r in conn.execute('PRAGMA table_info(agents)').fetchall()}
        if 'local_db_path' not in cols:
            conn.execute('ALTER TABLE agents ADD COLUMN local_db_path TEXT')
        now = datetime.now().isoformat()
        conn.execute('''
            INSERT INTO agents(telegram_id, username, first_name, joined_at, join_code, bot_token, local_db_path)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                bot_token = excluded.bot_token,
                local_db_path = excluded.local_db_path,
                joined_at = excluded.joined_at
        ''', (OWNER_ID, '', '自用克隆机器人', now, '', BOT_TOKEN, str(LOCAL_DB)))
        conn.commit()
        conn.close()
        logger.info(f'已向主机器人注册: owner={OWNER_ID}, db={LOCAL_DB}')
    except Exception as e:
        logger.warning(f'注册主机器人失败（不影响运行）: {e}')



# ============================================================
#  本地数据库
#  auth_code_pool = 管理员从主机器人购买/接收后存进来的授权码
# ============================================================
class DB:
    def __init__(self):
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # 用户表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                first_seen  TEXT NOT NULL,
                role        TEXT DEFAULT NULL
            )
        ''')
        # 授权码本地库存（管理员 addcode 进来的）
        cur.execute('''
            CREATE TABLE IF NOT EXISTS auth_code_pool (
                pool_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT UNIQUE NOT NULL,
                status      TEXT NOT NULL DEFAULT 'available',
                assigned_to INTEGER,
                assigned_at TEXT,
                note        TEXT DEFAULT '',
                added_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        ''')
        # 迁移：为旧数据库添加 role 列
        cols = {r[1] for r in cur.execute('PRAGMA table_info(users)').fetchall()}
        if 'role' not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT NULL")
        # 确保 OWNER 始终是 root
        if OWNER_ID:
            cur.execute(
                "INSERT INTO users (telegram_id, username, first_name, first_seen, role) "
                "VALUES (?, '', 'ROOT', ?, 'root') "
                "ON CONFLICT(telegram_id) DO UPDATE SET role='root'",
                (OWNER_ID, datetime.now().isoformat())
            )
        conn.commit()
        conn.close()

    def _conn(self):
        conn = sqlite3.connect(str(LOCAL_DB))
        conn.row_factory = sqlite3.Row
        return conn

    # ---- 用户 ----
    def track_user(self, tid: int, username: str = None, first_name: str = None):
        with self._conn() as conn:
            conn.execute(
                'INSERT INTO users (telegram_id, username, first_name, first_seen) '
                'VALUES (?, ?, ?, ?) '
                'ON CONFLICT(telegram_id) DO UPDATE SET username=?, first_name=?',
                (tid, username, first_name, datetime.now().isoformat(), username, first_name)
            )
            conn.commit()

    def get_all_users(self):
        with self._conn() as conn:
            return conn.execute('SELECT * FROM users ORDER BY first_seen DESC').fetchall()

    # ---- 授权码库存 ----
    def add_code(self, code: str, note: str = '') -> bool:
        """管理员把从主机器人拿到的授权码存入本地库"""
        try:
            with self._conn() as conn:
                conn.execute(
                    'INSERT INTO auth_code_pool (code, note) VALUES (?, ?)',
                    (code.strip().upper(), note)
                )
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # 重复

    def assign_code(self, telegram_id: int) -> str | None:
        """从库存取一个可用码分配给用户"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT pool_id, code FROM auth_code_pool WHERE status='available' ORDER BY pool_id LIMIT 1"
            ).fetchone()
            if not row:
                return None
            conn.execute(
                "UPDATE auth_code_pool SET status='assigned', assigned_to=?, assigned_at=? WHERE pool_id=?",
                (telegram_id, datetime.now().isoformat(), row['pool_id'])
            )
            conn.commit()
            return row['code']

    def get_user_codes(self, telegram_id: int):
        """获取用户已领取的所有码"""
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM auth_code_pool WHERE assigned_to=? ORDER BY assigned_at DESC",
                (telegram_id,)
            ).fetchall()

    def assign_code_to(self, telegram_id: int, code: str) -> bool:
        """将指定的码分配给用户（用于 Vercel 拉取的码记录到本地）"""
        try:
            with self._conn() as conn:
                conn.execute(
                    "UPDATE auth_code_pool SET status='assigned', assigned_to=?, assigned_at=? WHERE code=? AND status='available'",
                    (telegram_id, datetime.now().isoformat(), code.upper())
                )
                conn.commit()
            return True
        except Exception:
            return False

    def stock_stats(self) -> dict:
        with self._conn() as conn:
            total     = conn.execute("SELECT COUNT(*) FROM auth_code_pool").fetchone()[0]
            available = conn.execute("SELECT COUNT(*) FROM auth_code_pool WHERE status='available'").fetchone()[0]
            assigned  = conn.execute("SELECT COUNT(*) FROM auth_code_pool WHERE status='assigned'").fetchone()[0]
        return {'total': total, 'available': available, 'assigned': assigned}

    def delete_code(self, code: str) -> bool:
        """只允许删除还未分配的码"""
        with self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM auth_code_pool WHERE code=? AND status='available'",
                (code.upper(),)
            )
            conn.commit()
            return cur.rowcount > 0

    def list_codes(self, limit: int = 30):
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM auth_code_pool ORDER BY pool_id DESC LIMIT ?", (limit,)
            ).fetchall()

    # ---- 绑定 / 角色 ----
    def get_user_role(self, tid: int) -> str | None:
        """获取用户角色：'root' / 'admin' / None"""
        if tid == OWNER_ID:
            return 'root'
        with self._conn() as conn:
            row = conn.execute("SELECT role FROM users WHERE telegram_id=?", (tid,)).fetchone()
            return row['role'] if row else None

    def is_authorized(self, tid: int) -> bool:
        """判断用户是否有权使用机器人"""
        return self.get_user_role(tid) in ('root', 'admin')

    def bind_admin(self, tid: int, username: str = None, first_name: str = None) -> str:
        """ROOT 绑定 Admin。返回 'ok'/'max'/'already'/'is_root'"""
        with self._conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
            if count >= 2:
                return 'max'
            existing = conn.execute("SELECT role FROM users WHERE telegram_id=?", (tid,)).fetchone()
            if existing and existing['role'] == 'root':
                return 'is_root'
            if existing and existing['role'] == 'admin':
                return 'already'
            conn.execute(
                "INSERT INTO users (telegram_id, username, first_name, first_seen, role) "
                "VALUES (?, ?, ?, ?, 'admin') "
                "ON CONFLICT(telegram_id) DO UPDATE SET role='admin', "
                "username=COALESCE(?, username), first_name=COALESCE(?, first_name)",
                (tid, username or '', first_name or '', datetime.now().isoformat(), username, first_name)
            )
            conn.commit()
            return 'ok'

    def unbind_user(self, tid: int) -> bool:
        """解除 admin 绑定"""
        with self._conn() as conn:
            cur = conn.execute("UPDATE users SET role=NULL WHERE telegram_id=? AND role='admin'", (tid,))
            conn.commit()
            return cur.rowcount > 0

    def get_bound_admins(self) -> list:
        """获取所有已绑定 Admin（不含 ROOT）"""
        with self._conn() as conn:
            return conn.execute("SELECT * FROM users WHERE role='admin' ORDER BY first_seen").fetchall()

    def get_admin_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]

    def get_user_info(self, tid):
        """按 telegram_id 获取用户信息"""
        if not tid:
            return None
        with self._conn() as conn:
            return conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()


db = DB()





# ============================================================
#  键盘
# ============================================================
def main_kb(role=None):
    if role in ('root', 'admin'):
        return ReplyKeyboardMarkup(
            [
                ['🎫 领取授权码', '🔍 查询授权码'],
            ],
            resize_keyboard=True,
            is_persistent=True,
        )
    else:
        # 未绑定用户看到绑定按钮
        return ReplyKeyboardMarkup(
            [
                ['🔐1️⃣ 使用者绑定1', '🔐2️⃣ 使用者绑定2'],
            ],
            resize_keyboard=True,
            is_persistent=True,
        )


# ============================================================
#  处理器
# ============================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)
    context.user_data['action'] = None

    role = db.get_user_role(user.id)
    if not role:
        count = db.get_admin_count()
        if count >= 2:
            await update.message.reply_text(
                '☁️ <b>云际会议</b>\n'
                '━━━━━━━━━━━━━━━\n\n'
                f'👋 你好，{user.first_name}！\n\n'
                '⛔ <b>绑定名额已满（2/2）</b>\n\n'
                '请联系管理员处理。',
                parse_mode='HTML',
            )
            return
        await update.message.reply_text(
            '☁️ <b>云际会议</b>\n'
            '━━━━━━━━━━━━━━━\n\n'
            f'👋 你好，{user.first_name}！\n\n'
            '您尚未绑定，点击下方按钮即可绑定使用。\n'
            f'📍 绑定名额：<b>{count}/2</b>',
            parse_mode='HTML',
            reply_markup=main_kb(),
        )
        return

    welcome = (
        '☁️ <b>云际会议</b>\n'
        '━━━━━━━━━━━━━━━\n\n'
        f'👋 欢迎，{user.first_name}！\n\n'
        '🎫 <b>领取授权码</b> — 获取一个会议授权码\n'
        '🔍 <b>查询授权码</b> — 查看已领取的授权码\n\n'
        '📌 <b>使用说明：</b>\n'
        '━━━━━━━━━━━━━━━\n'
        '🟢 <b>创建会议</b>\n'
        '  👉 输入：<code>授权码 + 房间号</code>\n\n'
        '🔵 <b>加入会议</b>\n'
        '  👉 输入：<code>创建者的授权码 + 创建时的房间号</code>\n\n'
        '⏰ 领取后，第一次开设房间才开始计时（时长由主机器人设定）\n'
        '🔑 授权码 <b>一码一房间</b>，会议结束后可再次开设房间'
    )
    if role == 'root':
        welcome += (
            '\n\n👑 <b>ROOT 命令：</b>\n'
            '/bind &lt;Telegram ID&gt; — 绑定 Admin\n'
            '/kick &lt;Telegram ID&gt; — 踢出 Admin\n'
            '/admin — 管理面板'
        )
    elif role == 'admin':
        welcome += '\n\n🔓 /unbind — 解除自己的绑定'

    await update.message.reply_text(welcome, parse_mode='HTML', reply_markup=main_kb(role))


async def claim_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """从本地库存分配一个授权码"""
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)

    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n'
            f'<code>{user.id}</code>',
            parse_mode='HTML',
        )
        return

    # 检查是否已领取过
    existing = db.get_user_codes(user.id)
    if existing:
        code = existing[0]['code']
        await update.message.reply_text(
            '⚠️ <b>您已领取过授权码</b>\n'
            '━━━━━━━━━━━━━━━\n\n'
            f'🔑 您的授权码：<code>{code}</code>\n\n'
            '📌 点击「🔍 查询授权码」查看详情',
            parse_mode='HTML',
            reply_markup=main_kb('admin'),
        )
        return

    code = db.assign_code(user.id)
    if not code:
        await update.message.reply_text(
            '❌ <b>暂无可用授权码</b>\n\n'
            '请联系管理员补充库存。',
            parse_mode='HTML',
            reply_markup=main_kb('admin'),
        )
        return

    stats = db.stock_stats()
    await update.message.reply_text(
        '✅ <b>领取成功！</b>\n'
        '━━━━━━━━━━━━━━━\n\n'
        f'🔑 授权码：<code>{code}</code>\n\n'
        '📌 <b>使用方法：</b>\n'
        '🟢 创建会议：<code>授权码 + 房间号</code>\n'
        '🔵 加入会议：<code>创建者授权码 + 房间号</code>\n\n'
        '⏰ 第一次开设房间后开始计时\n'
        '⚠️ 请勿将授权码分享给他人',
        parse_mode='HTML',
        reply_markup=main_kb('admin'),
    )


async def query_codes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查询用户已领取的码（纯本地DB）"""
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)

    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n'
            f'<code>{user.id}</code>',
            parse_mode='HTML',
        )
        return

    rows = db.get_user_codes(user.id)
    stats = db.stock_stats()

    if not rows:
        await update.message.reply_text(
            f'📋 <b>我的授权码</b>\n\n'
            f'您还未领取授权码。\n'
            f'📦 当前库存：<b>{stats["available"]}</b> 个可用\n\n'
            f'请点击「🎫 领取授权码」获取。',
            parse_mode='HTML',
            reply_markup=main_kb('admin'),
        )
        return

    msg = '📋 <b>我的授权码</b>\n━━━━━━━━━━━━━━━\n\n'
    msg += f'📦 库存剩余可用：<b>{stats["available"]}</b>\n\n'

    for i, row in enumerate(rows, 1):
        code_val = row['code']
        assigned_at = (row['assigned_at'] or '')[:16].replace('T', ' ')
        msg += f'{i}. <code>{code_val}</code>\n   🟢 可用\n   📅 领取时间：{assigned_at}\n\n'

    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=main_kb('admin'))


# ============================================================
#  绑定 / 解绑 / 踢出 命令
# ============================================================
async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ROOT 绑定 Admin：/bind <telegram_id>"""
    user = update.effective_user
    if db.get_user_role(user.id) != 'root':
        await update.message.reply_text('⛔ 仅 ROOT 可执行此命令')
        return

    args = context.args or []
    if not args:
        admins = db.get_bound_admins()
        msg = '👥 <b>已绑定 Admin</b>（{}/2）\n━━━━━━━━━━━━━━━\n\n'.format(len(admins))
        if admins:
            for i, a in enumerate(admins, 1):
                uname = f"@{a['username']}" if a['username'] else '无用户名'
                msg += f'{i}. {a["first_name"] or ""} {uname}\n   ID: <code>{a["telegram_id"]}</code>\n\n'
        else:
            msg += '暂无绑定用户\n\n'
        msg += '📌 用法：/bind &lt;Telegram ID&gt;'
        await update.message.reply_text(msg, parse_mode='HTML')
        return

    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text('❌ 请输入有效的 Telegram ID（数字）')
        return

    target_info = db.get_user_info(target_id)
    target_name = target_info['first_name'] if target_info else str(target_id)
    target_uname = f"@{target_info['username']}" if target_info and target_info['username'] else ''

    result = db.bind_admin(target_id)
    if result == 'ok':
        admins = db.get_bound_admins()
        display = f'{target_name} {target_uname}'.strip() or str(target_id)
        await update.message.reply_text(
            f'✅ 已绑定 <b>{display}</b> 为 Admin\n'
            f'👥 当前已绑定：{len(admins)}/2',
            parse_mode='HTML',
        )
    elif result == 'max':
        await update.message.reply_text('❌ 已达到最大绑定数量（2个），请先踢出再绑定。')
    elif result == 'already':
        await update.message.reply_text('⚠️ 该用户已经是 Admin')
    elif result == 'is_root':
        await update.message.reply_text('⚠️ 不能绑定 ROOT')


async def unbind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin 自行解绑：/unbind"""
    user = update.effective_user
    role = db.get_user_role(user.id)
    if role == 'root':
        await update.message.reply_text('⚠️ ROOT 无法解绑自己')
        return
    if role != 'admin':
        await update.message.reply_text('⛔ 您未被绑定')
        return

    ok = db.unbind_user(user.id)
    if ok:
        await update.message.reply_text(
            '✅ 已解除绑定，您将无法继续使用本机器人功能。\n'
            '如需重新绑定，请联系管理员。',
        )
    else:
        await update.message.reply_text('❌ 解绑失败')


async def kick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ROOT 踢出 Admin：/kick <telegram_id>"""
    user = update.effective_user
    if db.get_user_role(user.id) != 'root':
        await update.message.reply_text('⛔ 仅 ROOT 可执行此命令')
        return

    args = context.args or []
    if not args:
        admins = db.get_bound_admins()
        if not admins:
            await update.message.reply_text('当前无已绑定的 Admin')
            return
        msg = '👥 <b>可踢出的 Admin</b>\n━━━━━━━━━━━━━━━\n\n'
        for i, a in enumerate(admins, 1):
            uname = f"@{a['username']}" if a['username'] else '无用户名'
            msg += f'{i}. {a["first_name"] or ""} {uname}\n   ID: <code>{a["telegram_id"]}</code>\n\n'
        msg += '📌 用法：/kick &lt;Telegram ID&gt;'
        await update.message.reply_text(msg, parse_mode='HTML')
        return

    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text('❌ 请输入有效的 Telegram ID（数字）')
        return

    if target_id == user.id:
        await update.message.reply_text('⚠️ 不能踢出自己')
        return

    target_info = db.get_user_info(target_id)
    target_name = target_info['first_name'] if target_info else str(target_id)
    target_uname = f"@{target_info['username']}" if target_info and target_info['username'] else ''
    display = f'{target_name} {target_uname}'.strip() or str(target_id)

    ok = db.unbind_user(target_id)
    if ok:
        await update.message.reply_text(
            f'✅ 已踢出 <b>{display}</b>（<code>{target_id}</code>）',
            parse_mode='HTML',
        )
    else:
        await update.message.reply_text('❌ 该用户不是已绑定的 Admin')


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = (update.message.text or '').strip()

    # 管理员将主机器人下发的入库消息转发/粘贴过来，自动识别 #YUNJICODE:XXXX 并入库
    if uid in ADMIN_IDS and '#YUNJICODE:' in text:
        import re
        found = re.findall(r'#YUNJICODE:([A-Za-z0-9_\-]+)', text)
        if found:
            ok_list, dup_list = [], []
            for code in found:
                if db.add_code(code.upper(), note='主机器人下发'):
                    ok_list.append(code.upper())
                else:
                    dup_list.append(code.upper())
            stats = db.stock_stats()
            lines = []
            if ok_list:
                lines.append(f'✅ 入库 {len(ok_list)} 个：' + ', '.join(f'<code>{c}</code>' for c in ok_list))
            if dup_list:
                lines.append(f'⚠️ 重复跳过 {len(dup_list)} 个：' + ', '.join(f'<code>{c}</code>' for c in dup_list))
            lines.append(f'📦 当前可分发：<b>{stats["available"]}</b> 个')
            await update.message.reply_text('\n'.join(lines), parse_mode='HTML')
            return

    if text in ('🔐1️⃣ 使用者绑定1', '🔐2️⃣ 使用者绑定2'):
        user = update.effective_user
        role = db.get_user_role(uid)
        if role:
            await update.message.reply_text('✅ 您已绑定，无需重复操作。', reply_markup=main_kb(role))
            return
        result = db.bind_admin(uid, user.username, user.first_name)
        if result == 'ok':
            admins = db.get_bound_admins()
            slot = text[-1]  # '1' 或 '2'
            await update.message.reply_text(
                f'✅ <b>绑定成功！（使用者{slot}）</b>\n'
                '━━━━━━━━━━━━━━━\n\n'
                f'👤 用户：{user.first_name} {("@" + user.username) if user.username else ""}\n'
                f'👥 已绑定：{len(admins)}/2\n\n'
                '📌 <b>使用说明：</b>\n'
                '━━━━━━━━━━━━━━━\n'
                '🎫 点击「领取授权码」获取会议授权码\n'
                '🔍 点击「查询授权码」查看已领取的码\n\n'
                '🟢 <b>创建会议：</b><code>授权码 + 房间号</code>\n'
                '🔵 <b>加入会议：</b><code>创建者授权码 + 房间号</code>\n\n'
                '🔓 如需解除绑定，发送 /unbind 即可',
                parse_mode='HTML',
                reply_markup=main_kb('admin'),
            )
        elif result == 'max':
            await update.message.reply_text(
                '❌ 绑定名额已满（2/2），请联系管理员。',
                reply_markup=main_kb(),
            )
        elif result == 'already':
            await update.message.reply_text('✅ 您已绑定。', reply_markup=main_kb('admin'))
        return

    if text == '🎫 领取授权码':
        await claim_code(update, context)
    elif text == '🔍 查询授权码':
        await query_codes(update, context)
    else:
        role = db.get_user_role(uid)
        if role:
            await update.message.reply_text('请使用下方按钮操作 👇', reply_markup=main_kb(role))
        else:
            await update.message.reply_text(
                '您尚未绑定，点击下方按钮绑定 👇',
                reply_markup=main_kb(),
            )


# ============================================================
#  管理员命令
# ============================================================
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text('⛔ 权限不足')
        return

    args = context.args or []

    if not args:
        stats = db.stock_stats()
        users = db.get_all_users()
        admins = db.get_bound_admins()
        admin_lines = ''
        for a in admins:
            uname = f"@{a['username']}" if a['username'] else '无用户名'
            admin_lines += f'  • {a["first_name"] or ""} {uname} (<code>{a["telegram_id"]}</code>)\n'
        if not admin_lines:
            admin_lines = '  暂无\n'
        msg = (
            '👑 <b>管理面板</b>\n'
            '━━━━━━━━━━━━━━━\n\n'
            f'👥 已绑定 Admin（{len(admins)}/2）：\n{admin_lines}\n'
            f'👥 用户总数：{len(users)}\n'
            f'📦 库存总量：{stats["total"]}\n'
            f'🟢 可分发：{stats["available"]}\n'
            f'📤 已分发：{stats["assigned"]}\n\n'
            '📌 <b>命令：</b>\n'
            '/bind &lt;ID&gt; — 绑定 Admin\n'
            '/kick &lt;ID&gt; — 踢出 Admin\n'
            '/admin codes — 查看库存列表\n'
            '/admin delcode &lt;码&gt; — 删除未分发的码\n'
            '/admin users — 查看用户列表\n'
            '/admin addcode &lt;码&gt; [备注] — 手动录入\n\n'
            '💡 <b>自动入库：</b>将主机器人发来的购买成功消息直接转发给本机器人即可自动入库\n'
        )
        await update.message.reply_text(msg, parse_mode='HTML')
        return

    sub = args[0].lower()

    # /admin addcode <码> [备注]
    if sub == 'addcode':
        if len(args) < 2:
            await update.message.reply_text('用法：/admin addcode <授权码> [备注]')
            return
        code = args[1].strip().upper()
        note = ' '.join(args[2:]) if len(args) > 2 else ''
        ok = db.add_code(code, note)
        if ok:
            stats = db.stock_stats()
            await update.message.reply_text(
                f'✅ 授权码 <code>{code}</code> 已存入库存\n'
                f'📦 当前可分发：<b>{stats["available"]}</b> 个',
                parse_mode='HTML',
            )
        else:
            await update.message.reply_text(f'⚠️ 授权码 <code>{code}</code> 已存在，未重复添加', parse_mode='HTML')
        return

    # /admin codes
    if sub == 'codes':
        rows = db.list_codes(30)
        if not rows:
            await update.message.reply_text('📦 库存为空')
            return
        msg = '📦 <b>授权码库存（最近30条）</b>\n━━━━━━━━━━━━━━━\n\n'
        for r in rows:
            if r['status'] == 'available':
                st = '🟢 可用'
            else:
                assigned_user = db.get_user_info(r['assigned_to']) if r['assigned_to'] else None
                if assigned_user:
                    uname = f"@{assigned_user['username']}" if assigned_user['username'] else (assigned_user['first_name'] or '')
                    st = f'📤 {uname}'
                else:
                    st = f'📤 已分发→{r["assigned_to"]}'
            note = f' <i>{r["note"]}</i>' if r['note'] else ''
            msg += f'<code>{r["code"]}</code> {st}{note}\n'
        await update.message.reply_text(msg, parse_mode='HTML')
        return

    # /admin delcode <码>
    if sub == 'delcode':
        if len(args) < 2:
            await update.message.reply_text('用法：/admin delcode <授权码>')
            return
        code = args[1].strip().upper()
        ok = db.delete_code(code)
        if ok:
            await update.message.reply_text(f'✅ 已删除 <code>{code}</code>', parse_mode='HTML')
        else:
            await update.message.reply_text(f'❌ 未找到可删除的码（已分发的码不可删除）', parse_mode='HTML')
        return

    # /admin users
    if sub == 'users':
        users = db.get_all_users()
        if not users:
            await update.message.reply_text('暂无用户')
            return
        msg = '👥 <b>用户列表</b>\n━━━━━━━━━━━━━━━\n\n'
        for u in users[:50]:
            if u['telegram_id'] == OWNER_ID:
                continue  # ROOT 不显示
            uname = f"@{u['username']}" if u['username'] else '无用户名'
            role_tag = ' 🔑Admin' if u['role'] == 'admin' else ''
            msg += f'• <code>{u["telegram_id"]}</code>  {u["first_name"] or ""}  {uname}{role_tag}\n'
        await update.message.reply_text(msg, parse_mode='HTML')
        return

    await update.message.reply_text('❓ 未知命令，发送 /admin 查看帮助')


async def on_error(update, context):
    logger.exception('Unhandled exception', exc_info=context.error)


# ============================================================
#  主函数
# ============================================================
def main():
    if not BOT_TOKEN:
        raise RuntimeError('BOT_TOKEN 未设置')

    asyncio.set_event_loop(asyncio.new_event_loop())

    # 向主机器人注册自身
    register_to_master()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('admin', admin_cmd))
    app.add_handler(CommandHandler('bind', bind_cmd))
    app.add_handler(CommandHandler('unbind', unbind_cmd))
    app.add_handler(CommandHandler('kick', kick_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)

    logger.info('☁️ 自用型机器人启动中...')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
