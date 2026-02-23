import asyncio
import logging
import sys
import os
import time
import aiofiles
import csv
import json
import re
import gc
import aiohttp
import traceback
from functools import wraps
from datetime import datetime, timedelta, date
from datetime import time as dt_time
from typing import Dict, Optional, List
from contextlib import suppress
from datetime import timedelta
from aiogram.types import BotCommand, BotCommandScopeAllChatAdministrators


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("GroupCheckInBot")

# 禁用过于详细的日志
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# 导入配置和模块
from config import Config, beijing_tz
from database import db
from performance import (
    performance_monitor,
    task_manager,
    retry_manager,
    global_cache,
    track_performance,
    with_retry,
    message_deduplicate,
    handle_database_errors,
    handle_telegram_errors,
)
from utils import (
    MessageFormatter,
    user_lock_manager,
    timer_manager,
    performance_optimizer,
    heartbeat_manager,
    notification_service,
    shift_state_manager,
    NotificationService,
    get_beijing_time,
    calculate_cross_day_time_diff,
    rate_limit,
    send_reset_notification,
)

from bot_manager import bot_manager

from aiogram import Bot, Dispatcher, types, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web


from io import StringIO


# # 初始化bot
# bot = Bot(token=Config.TOKEN)
# dp = Dispatcher(storage=MemoryStorage())

# 使用新的管理器
# bot = bot_manager.bot
# dp = bot_manager.dispatcher

bot = None
dp = None

# 记录程序启动时间
start_time = time.time()

# 防重入全局表
active_back_processing: Dict[str, bool] = {}


# ========== 日志中间件 ==========
class LoggingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data):
        if event.text:
            logger.info(
                f"📨 收到消息: chat_id={event.chat.id}, uid={event.from_user.id}, text='{event.text}'"
            )
        return await handler(event, data)


# ========== 上下班打卡辅助函数 ==========
def get_user_lock(chat_id: int, uid: int):
    """获取用户锁的便捷函数"""
    return user_lock_manager.get_lock(chat_id, uid)


async def auto_end_current_activity(
    chat_id: int, uid: int, user_data: dict, now: datetime, message: types.Message
):
    """自动结束当前活动 - 统一使用班次判定确定日期归属"""
    try:
        act = user_data["current_activity"]
        start_time_dt = datetime.fromisoformat(user_data["activity_start_time"])
        shift = user_data.get("shift", "day")
        elapsed = int((now - start_time_dt).total_seconds())

        # ✅ 修复：通过班次判定获取正确的记录日期
        shift_info = await db.determine_shift_for_time(
            chat_id=chat_id,
            current_time=now,
            checkin_type="work_end",  # 自动结束相当于下班
        )

        forced_date = None
        if shift_info:
            # 使用班次判定返回的记录日期
            forced_date = shift_info.get("record_date")
            is_dual = shift_info.get("is_dual", False)
            logger.info(
                f"📅 自动结束活动 - 班次判定: {shift_info.get('shift_detail')}, "
                f"记录日期: {forced_date}, 双班模式: {is_dual}"
            )
        else:
            # 无法判定时使用当前日期
            forced_date = now.date()
            logger.info(f"📅 自动结束活动 - 无法判定班次，使用当前日期: {forced_date}")

        # ✅ 完成活动 - 传入正确的强制日期
        await db.complete_user_activity(
            chat_id=chat_id,
            user_id=uid,
            activity=act,
            elapsed_time=elapsed,
            fine_amount=0,
            is_overtime=False,
            shift=shift,
            forced_date=forced_date,  # 使用班次判定的日期
        )

        # 取消定时器
        await timer_manager.cancel_timer(f"{chat_id}-{uid}", preserve_message=False)

        logger.info(
            f"✅ 自动结束活动: {chat_id}-{uid} - {act} "
            f"(班次: {shift}, 日期: {forced_date})"
        )

    except Exception as e:
        logger.error(f"❌ 自动结束活动失败 {chat_id}-{uid}: {e}")
        logger.exception(e)


# ========== 特殊按钮定义 ==========
SPECIAL_BUTTONS = {
    "👑 管理员面板": "admin_panel",
    "🔙 返回主菜单": "back_to_main",
    "📤 导出数据": "export_data",
    "📊 我的记录": "my_record",
    "🏆 排行榜": "rank",
    "✅ 回座": "back",
    "🟢 上班": "work_start",
    "🔴 下班": "work_end",
}

# 🆕 官方指令映射配置 (英文指令: 中文活动名)
# Telegram 指令只支持小写字母、数字和下划线
ACTIVITY_MAP = {
    "wc_small": "小厕",
    "wc_large": "大厕",
    "smoke": "抽烟",
    "eat": "吃饭",
}


class AdminStates(StatesGroup):
    """管理员状态"""

    waiting_for_channel_id = State()
    waiting_for_group_id = State()


# ========== 工具函数 ==========
async def is_admin(uid: int) -> bool:
    """检查用户是否为管理员"""
    return uid in Config.ADMINS


async def calculate_work_fine(checkin_type: str, late_minutes: float) -> int:
    """根据分钟阈值动态计算上下班罚款金额"""
    work_fine_rates = await db.get_work_fine_rates_for_type(checkin_type)
    if not work_fine_rates:
        return 0

    # 转换键为整数并排序
    thresholds = sorted([int(k) for k in work_fine_rates.keys() if str(k).isdigit()])
    late_minutes_abs = abs(late_minutes)

    applicable_fine = 0
    for threshold in thresholds:
        if late_minutes_abs >= threshold:
            applicable_fine = work_fine_rates[str(threshold)]
        else:
            break

    return applicable_fine


# ========== 通知函数 ==========
async def send_startup_notification():
    """发送启动通知给管理员"""
    try:
        startup_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        message = (
            f"🤖 <b>打卡机器人已启动</b>\n"
            f"⏰ 启动时间: <code>{startup_time}</code>\n"
            f"🟢 系统状态: 正常运行\n"
            f"💾 数据库: {'已连接' if await db.health_check() else '连接异常'}\n"
            f"🔧 模式: 自动重连模式"
        )

        for admin_id in Config.ADMINS:
            try:
                success = await bot_manager.send_message_with_retry(
                    admin_id, message, parse_mode="HTML"
                )
                if success:
                    logger.info(f"✅ 启动通知已发送给管理员 {admin_id}")
                else:
                    logger.error(f"❌ 发送启动通知给管理员 {admin_id} 失败")
            except Exception as e:
                logger.error(f"发送启动通知给管理员 {admin_id} 失败: {e}")

    except Exception as e:
        logger.error(f"发送启动通知失败: {e}")


async def send_shutdown_notification():
    """发送关闭通知给管理员"""
    try:
        shutdown_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        uptime = time.time() - start_time
        uptime_str = MessageFormatter.format_time(int(uptime))

        message = (
            f"🛑 <b>打卡机器人已关闭</b>\n"
            f"⏰ 关闭时间: <code>{shutdown_time}</code>\n"
            f"⏱️ 运行时长: <code>{uptime_str}</code>\n"
            f"🔴 系统状态: 已停止"
        )

        for admin_id in Config.ADMINS:
            try:
                # 使用带重试的发送
                success = await bot_manager.send_message_with_retry(
                    admin_id, message, parse_mode="HTML"
                )
                if success:
                    logger.info(f"✅ 关闭通知已发送给管理员 {admin_id}")
                else:
                    logger.debug(f"发送关闭通知给管理员 {admin_id} 失败")
            except Exception as e:
                logger.debug(f"发送关闭通知失败: {e}")

    except Exception as e:
        logger.debug(f"准备关闭通知失败: {e}")


# ========== 生成月度报告函数 =========
async def generate_monthly_report(chat_id: int, year: int = None, month: int = None):
    """生成月度报告 - 基于新的月度统计表"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    # 🆕 使用新的月度统计方法（基于 monthly_statistics 表）
    monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
    work_stats = await db.get_monthly_work_statistics(chat_id, year, month)
    activity_ranking = await db.get_monthly_activity_ranking(chat_id, year, month)

    if not monthly_stats and not work_stats:
        return None

    chat_title = str(chat_id)
    try:
        chat_info = await bot.get_chat(chat_id)
        chat_title = chat_info.title or chat_title
    except:
        pass

    # 生成报告文本
    report = (
        f"📊 <b>{year}年{month}月打卡统计报告</b>\n"
        f"🏢 群组：<code>{chat_title}</code>\n"
        f"📅 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{MessageFormatter.create_dashed_line()}\n"
    )

    # 总体统计
    total_users = len(monthly_stats)
    total_activity_time = sum(stat.get("total_time", 0) for stat in monthly_stats)
    total_activity_count = sum(stat.get("total_count", 0) for stat in monthly_stats)
    total_fines = sum(stat.get("total_fines", 0) for stat in monthly_stats)

    # 🆕 新增：工作天数和工作时长统计
    total_work_days = sum(stat.get("work_days", 0) for stat in monthly_stats)
    total_work_hours = sum(stat.get("work_hours", 0) for stat in monthly_stats)

    report += (
        f"👥 <b>总体统计</b>\n"
        f"• 活跃用户：<code>{total_users}</code> 人\n"
        f"• 总活动时长：<code>{MessageFormatter.format_time(int(total_activity_time))}</code>\n"
        f"• 总活动次数：<code>{total_activity_count}</code> 次\n"
        f"• 总工作天数：<code>{total_work_days}</code> 天\n"
        f"• 总工作时长：<code>{MessageFormatter.format_time(int(total_work_hours))}</code>\n"
        f"• 总扣除绩效：<code>{total_fines}</code> 分\n\n"
    )

    # 上下班统计
    total_work_start = sum(stat.get("work_start_count", 0) for stat in work_stats)
    total_work_end = sum(stat.get("work_end_count", 0) for stat in work_stats)
    total_work_fines = sum(
        stat.get("work_start_fines", 0) + stat.get("work_end_fines", 0)
        for stat in work_stats
    )

    if total_work_start > 0 or total_work_end > 0:
        report += (
            f"🕒 <b>上下班统计</b>\n"
            f"• 上班打卡：<code>{total_work_start}</code> 次\n"
            f"• 下班打卡：<code>{total_work_end}</code> 次\n"
            f"• 上下班罚款：<code>{total_work_fines}</code> 分\n\n"
        )

    # 🆕 新增：个人工作统计排行
    if monthly_stats:
        report += f"👤 <b>个人工作统计</b>\n"

        # 按工作时长排行
        work_hours_ranking = sorted(
            [stat for stat in monthly_stats if stat.get("work_hours", 0) > 0],
            key=lambda x: x.get("work_hours", 0),
            reverse=True,
        )[:5]

        for i, stat in enumerate(work_hours_ranking, 1):
            work_hours_str = MessageFormatter.format_time(
                int(stat.get("work_hours", 0))
            )
            work_days = stat.get("work_days", 0)
            nickname = stat.get("nickname", f"用户{stat.get('user_id')}")
            report += (
                f"  <code>{i}.</code> {nickname} - {work_hours_str} ({work_days}天)\n"
            )
        report += "\n"

    # 活动排行榜
    report += f"🏆 <b>月度活动排行榜</b>\n"
    has_activity_data = False

    for activity, ranking in activity_ranking.items():
        if ranking:
            has_activity_data = True
            report += f"📈 <code>{activity}</code>：\n"
            for i, user in enumerate(ranking[:3], 1):
                time_str = MessageFormatter.format_time(int(user.get("total_time", 0)))
                count = user.get("total_count", 0)
                nickname = user.get("nickname", "未知用户")
                report += f"  <code>{i}.</code> {nickname} - {time_str} ({count}次)\n"
            report += "\n"

    if not has_activity_data:
        report += "暂无活动数据\n\n"

    # 🆕 新增：月度总结
    report += f"📈 <b>月度总结</b>\n"

    if total_activity_count > 0:
        avg_activity_time = (
            total_activity_time / total_activity_count
            if total_activity_count > 0
            else 0
        )
        report += f"• 平均每次活动时长：<code>{MessageFormatter.format_time(int(avg_activity_time))}</code>\n"

    if total_work_days > 0:
        avg_work_hours_per_day = (
            total_work_hours / total_work_days if total_work_days > 0 else 0
        )
        report += f"• 平均每日工作时长：<code>{MessageFormatter.format_time(int(avg_work_hours_per_day))}</code>\n"

    if total_users > 0:
        avg_activity_per_user = (
            total_activity_count / total_users if total_users > 0 else 0
        )
        report += f"• 人均活动次数：<code>{avg_activity_per_user:.1f}</code> 次\n"

        avg_work_days_per_user = total_work_days / total_users if total_users > 0 else 0
        report += f"• 人均工作天数：<code>{avg_work_days_per_user:.1f}</code> 天\n"

    # 🆕 新增：数据来源说明
    report += f"\n{MessageFormatter.create_dashed_line()}\n"
    report += f"💡 <i>注：本报告基于月度统计表生成，不受日常重置操作影响</i>"

    return report


# ========== 导出月度数据函数 =========
async def export_monthly_csv(
    chat_id: int,
    year: int = None,
    month: int = None,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
):
    """导出月度数据为 CSV 并推送 - 优化版本"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    if not file_name:
        file_name = f"group_{chat_id}_monthly_{year:04d}{month:02d}.csv"

    # 使用优化版导出
    csv_content = await optimized_monthly_export(chat_id, year, month)

    if not csv_content:
        await bot.send_message(chat_id, f"⚠️ {year}年{month}月没有数据需要导出")
        return

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 月度数据导出\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计月份：<code>{year}年{month}月</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"💾 包含每个用户的月度活动统计"
        )

        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ 发送到当前聊天失败: {e}")

        # ✅ 使用全局实例（推荐）
        await notification_service.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption=caption
        )

        logger.info(f"✅ 月度数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 月度导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 月度导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


# ========== 活动恢复函数 ==========
async def handle_expired_activity(
    chat_id: int, user_id: int, activity: str, start_time: datetime
):
    """智能恢复活动 - 按活动开始时间归档，保留班次信息，不误归前一天"""
    try:
        now = get_beijing_time()
        elapsed = int((now - start_time).total_seconds())
        nickname = "用户"

        # 获取用户信息
        user_data = await db.get_user_cached(chat_id, user_id)
        if user_data:
            nickname = user_data.get("nickname", str(user_id))

        # ✅ 使用活动开始时间作为归档日期
        forced_date = start_time.date()

        # 获取班次信息（从用户数据或活动时间判定）
        shift = user_data.get("shift", None)
        if not shift:
            shift_info = await db.determine_shift_for_time(
                chat_id=chat_id,
                current_time=start_time,
                checkin_type="work_start",
            )
            if shift_info:
                shift = shift_info.get("shift", "day")
        shift = shift or "day"  # 默认白班

        logger.info(
            f"🔄 恢复过期活动 - 活动开始时间: {start_time.strftime('%m/%d %H:%M:%S')}, "
            f"归档日期: {forced_date}, 班次: {shift}"
        )

        # 计算罚款
        time_limit = await db.get_activity_time_limit(activity)
        time_limit_seconds = time_limit * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, elapsed - time_limit_seconds)
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(activity, overtime_minutes)

        # 完成活动
        await db.complete_user_activity(
            chat_id=chat_id,
            user_id=user_id,
            activity=activity,
            elapsed_time=elapsed,
            fine_amount=fine_amount,
            is_overtime=is_overtime,
            shift=shift,
            forced_date=forced_date,
        )

        # 发送恢复通知
        date_desc = f"（归到{forced_date}）"
        timeout_msg = (
            f"🔄 <b>系统恢复通知</b>{date_desc}\n"
            f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
            f"📝 检测到未结束的活动：<code>{activity}</code>\n"
            f"⏰ 活动开始时间：<code>{start_time.strftime('%m/%d %H:%M:%S')}</code>\n"
            f"⏱️ 活动总时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>\n"
            f"⚠️ 由于服务重启，您的活动已自动结束"
        )

        if fine_amount > 0:
            timeout_msg += f"\n💰 超时扣除绩效：<code>{fine_amount}</code> 分"

        await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")

        logger.info(
            f"已处理过期活动: {chat_id}-{user_id} - {activity} "
            f"(开始时间: {start_time.strftime('%m/%d %H:%M:%S')}, "
            f"归到: {forced_date}, 班次: {shift})"
        )

    except Exception as e:
        logger.error(f"处理过期活动失败 {chat_id}-{user_id}: {e}")


async def recover_expired_activities():
    """恢复服务重启前的过期活动 - 添加返回值"""
    try:
        logger.info("🔄 检查并恢复过期活动...")
        all_groups = await db.get_all_groups()
        recovered_count = 0

        for chat_id in all_groups:
            try:
                group_members = await db.get_group_members(chat_id)
                for user_data in group_members:
                    if user_data.get("current_activity") and user_data.get(
                        "activity_start_time"
                    ):
                        activity = user_data["current_activity"]
                        start_time = datetime.fromisoformat(
                            user_data["activity_start_time"]
                        )
                        user_id = user_data["user_id"]

                        # 处理过期活动
                        await handle_expired_activity(
                            chat_id, user_id, activity, start_time
                        )
                        recovered_count += 1

            except Exception as e:
                logger.error(f"恢复群组 {chat_id} 活动失败: {e}")

        if recovered_count > 0:
            logger.info(f"✅ 已恢复 {recovered_count} 个过期活动")
        else:
            logger.info("✅ 没有需要恢复的过期活动")

        return recovered_count  # 添加返回值

    except Exception as e:
        logger.error(f"恢复过期活动失败: {e}")
        return 0


# ========== 每日重置逻辑 =========
async def reset_daily_data_if_needed(chat_id: int, uid: int):
    """业务日期统一版每日重置（完全对齐业务日期体系）"""
    try:
        now = get_beijing_time()

        # 🧠 获取业务日期（系统唯一的“今天”）
        business_date = await db.get_business_date(chat_id, now)

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        if not user_data:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, business_date)
            return

        last_updated_raw = user_data.get("last_updated")

        # 解析 last_updated
        if isinstance(last_updated_raw, datetime):
            last_updated = last_updated_raw.date()
        elif isinstance(last_updated_raw, str):
            try:
                last_updated = datetime.fromisoformat(
                    last_updated_raw.replace("Z", "+00:00")
                ).date()
            except Exception:
                try:
                    last_updated = datetime.strptime(
                        last_updated_raw, "%Y-%m-%d"
                    ).date()
                except Exception:
                    last_updated = business_date
        else:
            last_updated = business_date

        # 🎯 唯一重置规则：是否跨了业务日期
        if last_updated < business_date:
            logger.info(f"🔄 重置用户数据: {chat_id}-{uid} | 业务日期 {business_date}")
            await db.reset_user_daily_data(chat_id, uid, business_date)
            await db.update_user_last_updated(chat_id, uid, business_date)

    except Exception as e:
        logger.error(f"重置检查失败 {chat_id}-{uid}: {e}")
        try:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, datetime.now().date())
        except Exception as init_error:
            logger.error(f"用户初始化也失败: {init_error}")


async def check_activity_limit_by_shift(
    chat_id: int,
    user_id: int,
    activity: str,
    shift: str | None = None,
) -> tuple[bool, int, int]:
    """
    检查活动次数是否达到上限
    - 单班模式：不区分班次 (shift=None)
    - 双班模式：按班次统计 (shift="day" 或 "night")
    """
    await db.init_group(chat_id)
    await db.init_user(chat_id, user_id)

    shift_config = await db.get_shift_config(chat_id)

    # 单班模式：强制不按班次统计
    if not shift_config or not shift_config.get("dual_mode", False):
        shift = None

    # ✅ 统一调用，shift 可以是 None
    current_count = await db.get_user_activity_count_by_shift(
        chat_id, user_id, activity, shift
    )

    max_times = await db.get_activity_max_times(activity)
    return current_count < max_times, current_count, max_times


async def has_active_activity(chat_id: int, uid: int) -> tuple[bool, Optional[str]]:
    """检查用户是否有活动正在进行"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)
    return user_data["current_activity"] is not None, user_data["current_activity"]


async def can_perform_activities(
    chat_id: int,
    uid: int,
    current_shift: str = None,
    record_date: Optional[date] = None,
) -> tuple[bool, str]:
    """
    检查用户是否可以执行活动
    基于 user_shift_state 表
    """

    logger.info(f"🔍 [活动检查] 用户={uid}, 班次={current_shift}")

    # 如果没有启用上下班功能，直接允许
    if not await db.has_work_hours_enabled(chat_id):
        return True, ""

    now = get_beijing_time()

    # ===== 🎯 新增：先尝试获取用户当前活跃班次 =====
    user_current_shift = await db.get_user_current_shift(chat_id, uid)

    # ===== 1. 确定要检查的班次 =====
    check_shift = current_shift

    if not check_shift:
        # 优先使用用户当前活跃班次
        if user_current_shift:
            check_shift = user_current_shift["shift"]
            logger.info(f"📌 使用用户当前活跃班次: {check_shift}")
        else:
            # 从用户数据获取当前班次
            user_data = await db.get_user_cached(chat_id, uid)
            if user_data and user_data.get("shift"):
                check_shift = user_data["shift"]
                logger.info(f"📌 使用用户数据班次: {check_shift}")
            else:
                # 默认白班
                check_shift = "day"
                logger.info(f"📌 使用默认班次: {check_shift}")

    # ===== 2. 检查用户是否有该班次的活跃状态 =====
    shift_state = await db.get_user_shift_state(chat_id, uid, check_shift)

    if not shift_state:
        shift_text = "白班" if check_shift == "day" else "夜班"
        return (
            False,
            f"❌ 您当前没有进行中的{shift_text}班次，请先打{shift_text}上班卡！",
        )

    # ===== 3. 检查是否已过期（16小时）=====
    shift_start_time = shift_state["shift_start_time"]
    if isinstance(shift_start_time, str):
        try:
            shift_start_time = datetime.fromisoformat(
                shift_start_time.replace("Z", "+00:00")
            )
        except:
            shift_start_time = datetime.strptime(
                shift_start_time, "%Y-%m-%d %H:%M:%S.%f%z"
            )

    if now - shift_start_time > timedelta(hours=16):
        # 自动清理过期状态
        await db.clear_user_shift_state(chat_id, uid, check_shift)
        shift_text = "白班" if check_shift == "day" else "夜班"
        return False, f"❌ 您的{shift_text}班次已过期（超过16小时），请重新上班打卡！"

    # ===== 4. 检查时间窗口 =====
    shift_info = await db.determine_shift_for_time(
        chat_id=chat_id,
        current_time=now,
        checkin_type="activity",
        active_shift=check_shift,
    )

    if shift_info is None or shift_info.get("shift_detail") is None:
        shift_config = await db.get_shift_config(chat_id)
        day_start = shift_config.get("day_start", "09:00")
        day_end = shift_config.get("day_end", "21:00")

        shift_text = "白班" if check_shift == "day" else "夜班"
        window_text = (
            f"<code>{day_start} ~ {day_end}</code>"
            if check_shift == "day"
            else f"<code>{day_end} ~ 次日 {day_start}</code>"
        )

        return False, (
            f"❌ 当前时间不在{shift_text}活动允许的时间窗口内\n"
            f"📊 {shift_text}活动时间：{window_text}\n\n"
            f"⏰ 当前时间：<code>{now.strftime('%H:%M')}</code>\n"
            f"💡 请等待{shift_text}的正常活动时间"
        )

    # ===== 5. 检查是否已下班 =====
    async with db.pool.acquire() as conn:
        has_work_end = await conn.fetchval(
            """
            SELECT 1 FROM work_records 
            WHERE chat_id = $1 
              AND user_id = $2 
              AND checkin_type = 'work_end'
              AND shift = $3
              AND record_date = $4
            LIMIT 1
            """,
            chat_id,
            uid,
            check_shift,
            shift_state["record_date"],
        )

        if has_work_end:
            shift_text = "白班" if check_shift == "day" else "夜班"
            return False, f"❌ 您本{shift_text}已下班，无法进行活动！"

    # ===== 🆕 可选：更新用户最后活动时间 =====
    # 如果数据库有这个方法，可以取消注释
    # await db.update_user_activity_time(chat_id, uid, check_shift)

    shift_text = "白班" if check_shift == "day" else "夜班"
    logger.info(f"✅ [活动检查] 用户={uid} 允许执行活动（班次：{shift_text}）")
    return True, ""


async def calculate_fine(activity: str, overtime_minutes: float) -> int:
    """计算罚款金额"""
    fine_rates = await db.get_fine_rates_for_activity(activity)
    if not fine_rates:
        return 0

    # 处理罚款时间段
    segments = []
    for time_key in fine_rates.keys():
        try:
            if isinstance(time_key, str) and "min" in time_key.lower():
                time_value = int(time_key.lower().replace("min", "").strip())
            else:
                time_value = int(time_key)
            segments.append(time_value)
        except (ValueError, TypeError):
            continue

    if not segments:
        return 0

    segments.sort()

    applicable_fine = 0
    for segment in segments:
        if overtime_minutes <= segment:
            original_key = str(segment)
            if original_key not in fine_rates:
                original_key = f"{segment}min"
            applicable_fine = fine_rates.get(original_key, 0)
            break

    if applicable_fine == 0 and segments:
        max_segment = segments[-1]
        original_key = str(max_segment)
        if original_key not in fine_rates:
            original_key = f"{max_segment}min"
        applicable_fine = fine_rates.get(original_key, 0)

    return applicable_fine


# ========== 键盘生成 ==========
async def get_main_keyboard(
    chat_id: int = None, show_admin: bool = False
) -> ReplyKeyboardMarkup:
    """获取主回复键盘"""
    try:
        activity_limits = await db.get_activity_limits_cached()
    except Exception as e:
        logger.error(f"获取活动配置失败: {e}")
        activity_limits = await db.get_activity_limits_cached()

    dynamic_buttons = []
    current_row = []

    for act in activity_limits.keys():
        current_row.append(KeyboardButton(text=act))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    # 添加上下班按钮（如果启用）
    if chat_id and await db.has_work_hours_enabled(chat_id):
        current_row.append(KeyboardButton(text="🟢 上班"))
        current_row.append(KeyboardButton(text="🔴 下班"))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    if current_row:
        dynamic_buttons.append(current_row)

    fixed_buttons = []
    fixed_buttons.append([KeyboardButton(text="✅ 回座")])

    bottom_buttons = []
    if show_admin:
        bottom_buttons.append(
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📊 我的记录"),
                KeyboardButton(text="🏆 排行榜"),
            ]
        )
    else:
        bottom_buttons.append(
            [KeyboardButton(text="📊 我的记录"), KeyboardButton(text="🏆 排行榜")]
        )

    keyboard = dynamic_buttons + fixed_buttons + bottom_buttons

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="请选择操作或输入活动名称...",
    )


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """管理员专用键盘"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📤 导出数据"),
            ],
            [KeyboardButton(text="🔙 返回主菜单")],
        ],
        resize_keyboard=True,
    )
    logger.debug("生成管理员键盘")
    return keyboard


# ========== 活动定时提醒 ==========
async def activity_timer(
    chat_id: int,
    uid: int,
    act: str,
    limit: int,
    shift: str = "day",
    preserve_message: bool = False,  # 🆕 1. 添加 preserve_message 参数
):
    try:
        max_wait = 30
        wait_interval = 1
        waited = 0

        while not bot_manager or not bot_manager.bot and waited < max_wait:
            if waited == 0:
                logger.info(f"⏳ 等待 bot 初始化... (chat={chat_id}, uid={uid})")
            await asyncio.sleep(wait_interval)
            waited += wait_interval

        if not bot_manager or not bot_manager.bot:
            logger.error(f"❌ bot 未能在 {max_wait} 秒内初始化，定时器终止")
            return

        if waited > 0:
            logger.info(f"✅ bot 已就绪，继续执行定时器 (等待 {waited}s)")

        # 添加班次文本
        shift_text = "白班" if shift == "day" else "夜班"
        logger.info(f"⏰ 定时器启动: {chat_id}-{uid} - {act}（{shift_text}）")

        # ===== 状态标记 =====
        one_minute_warning_sent = False
        timeout_immediate_sent = False
        timeout_5min_sent = False
        last_reminder_minute = 0
        force_back_sent = False  # 防止重复强制回座

        # ===== 群消息发送封装（引用 + 自动降级 + 自动重试） =====
        _message_sent_cache = {}  # key -> last sent timestamp
        _cache_lock = asyncio.Lock()  # 并发安全锁

        async def send_group_message(text: str, kb=None):
            msg_key = f"{chat_id}:{uid}:{text}"
            now = time.time()

            async with _cache_lock:
                expired_keys = [
                    k for k, t in _message_sent_cache.items() if now - t > 5
                ]
                for k in expired_keys:
                    _message_sent_cache.pop(k, None)

                if msg_key in _message_sent_cache:
                    logger.debug(f"⏱️ 相同消息5秒内已发送，跳过: {text[:30]}...")
                    return None

                _message_sent_cache[msg_key] = now

            current_bot = bot_manager.bot
            if not current_bot:
                logger.error("❌ bot_manager.bot 为 None，无法发送消息")
                return None

            checkin_message_id = await db.get_user_checkin_message_id(chat_id, uid)
            if checkin_message_id:
                try:
                    return await current_bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=kb,
                        reply_to_message_id=checkin_message_id,
                    )
                except Exception as e:
                    logger.warning(f"⚠️ 引用发送失败，重试一次: {e}")
                    await asyncio.sleep(1)
                    try:
                        return await current_bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="HTML",
                            reply_markup=kb,
                            reply_to_message_id=checkin_message_id,
                        )
                    except Exception as e2:
                        logger.warning(f"⚠️ 引用发送重试失败，降级普通发送: {e2}")

            try:
                return await current_bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            except Exception as e:
                logger.error(f"❌ 普通发送也失败: {e}")
                return None

        # ===== 快速回座按钮 =====
        def build_quick_back_kb():
            return InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 点击✅立即回座 👈",
                            callback_data=f"quick_back:{chat_id}:{uid}:{shift}",
                        )
                    ]
                ]
            )

        # ===== 强制回座通知封装 =====
        async def push_force_back_notification(nickname, elapsed, fine_amount):
            try:
                current_bot = bot_manager.bot
                if not current_bot:
                    logger.error(f"❌ bot_manager.bot 为 None，无法获取聊天信息")
                    return False

                chat_title = str(chat_id)
                try:
                    info = await current_bot.get_chat(chat_id)
                    chat_title = info.title or chat_title
                except Exception as e:
                    logger.debug(f"获取聊天信息失败: {e}")

                notification_text = (
                    f"🚨 <b>超时强制回座通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"📝 活动：<code>{act}</code>\n"
                    f"📊 班次：<code>{shift_text}</code>\n"
                    f"⏰ 自动回座时间：<code>{get_beijing_time().strftime('%m/%d %H:%M:%S')}</code>\n"
                    f"⏱️ 总活动时长：<code>{MessageFormatter.format_time(elapsed)}</code>\n"
                    f"⚠️ 系统自动回座原因：超时超过2小时\n"
                    f"💰 本次扣除绩效：<code>{fine_amount}</code> 分"
                )

                if not notification_service.bot and bot_manager.bot:
                    notification_service.bot = bot_manager.bot
                if not notification_service.bot_manager and bot_manager:
                    notification_service.bot_manager = bot_manager

                await notification_service.send_notification(
                    chat_id,
                    notification_text,
                    notification_type="channel",
                )
                logger.info(
                    f"✅ 强制回座通知推送成功: chat={chat_id}, uid={uid}（班次: {shift}）"
                )
                return True
            except Exception as e:
                logger.error(f"❌ 强制回座通知推送失败: {e}")
                return False

        # ===== 主循环 =====
        while True:
            user_lock = user_lock_manager.get_lock(chat_id, uid)
            async with user_lock:
                user_data = await db.get_user_cached(chat_id, uid)
                if not user_data or user_data["current_activity"] != act:
                    break

                start_time = datetime.fromisoformat(user_data["activity_start_time"])
                now = get_beijing_time()
                elapsed = int((now - start_time).total_seconds())

                try:
                    limit_int = int(limit)
                except (ValueError, TypeError):
                    logger.error(f"时间限制格式错误: {limit}，使用默认值30分钟")
                    limit_int = 30

                remaining = limit_int * 60 - elapsed
                nickname = user_data.get("nickname", str(uid))

                # ===== 强制回座 2 小时 =====
                time_limit_seconds = limit_int * 60  # 活动限制时间（秒）
                overtime_seconds = max(0, elapsed - time_limit_seconds)  # 超时时长

                if overtime_seconds >= 120 * 60 and not force_back_sent:
                    force_back_sent = True
                    fine_amount = await calculate_fine(act, 120)
                    logger.info(
                        f"⏰ [强制回座] 用户{uid} 活动{act} "
                        f"超时 {MessageFormatter.format_time(overtime_seconds)} "
                        f"(总时长: {MessageFormatter.format_time(elapsed)}, "
                        f"限制: {limit_int}分钟)"
                    )

                    await db.complete_user_activity(
                        chat_id=chat_id,
                        user_id=uid,
                        activity=act,
                        elapsed_time=elapsed,
                        fine_amount=fine_amount,
                        is_overtime=True,
                        shift=shift,
                    )

                    break_data = {
                        "should_break": True,
                        "fine_amount": fine_amount,
                        "elapsed": elapsed,
                        "nickname": nickname,
                    }
                else:
                    break_data = {"should_break": False}

                # ===== 即将超时 1 分钟提醒 =====
                if 0 < remaining <= 60 and not one_minute_warning_sent:
                    msg = (
                        f"⏳ <b>即将超时警告</b>\n"
                        f"👤 {MessageFormatter.format_user_link(uid, nickname)} \n"
                        f"📊 班次： <code>{shift_text}</code> \n"
                        f"🕓 本次 {MessageFormatter.format_copyable_text(act)} 还有 <code>1</code> 分钟！\n"
                        f"💡 请及时回座，避免超时罚款"
                    )
                    await send_group_message(msg, build_quick_back_kb())
                    one_minute_warning_sent = True

                # ===== 超时提醒 =====
                if remaining <= 0:
                    overtime_minutes = int(-remaining // 60)
                    msg = None

                    if overtime_minutes == 0 and not timeout_immediate_sent:
                        timeout_immediate_sent = True
                        msg = (
                            f"⚠️ <b>超时警告</b>\n"
                            f"👤 {MessageFormatter.format_user_link(uid, nickname)} \n"
                            f"📊 班次： <code>{shift_text}</code> \n"
                            f"🕓 本次 {MessageFormatter.format_copyable_text(act)} 已超时\n"
                            f"🏃‍♂️ 请立即回座，避免产生更多罚款！"
                        )
                        last_reminder_minute = 0

                    elif overtime_minutes == 5 and not timeout_5min_sent:
                        timeout_5min_sent = True
                        msg = (
                            f"🔔 <b>超时警告</b> \n"
                            f"👤 {MessageFormatter.format_user_link(uid, nickname)} \n"
                            f"📊 班次： <code>{shift_text}</code> \n"
                            f"🕓 本次 {MessageFormatter.format_copyable_text(act)} 已超时 <code>{overtime_minutes}</code> 分钟！\n"
                            f"😤 罚款正在累积，请立即回座！"
                        )
                        last_reminder_minute = 5

                    elif (
                        overtime_minutes >= 10
                        and overtime_minutes % 10 == 0
                        and overtime_minutes != last_reminder_minute
                    ):
                        last_reminder_minute = overtime_minutes
                        msg = (
                            f"🚨 <b>超时警告</b>\n"
                            f"👤 {MessageFormatter.format_user_link(uid, nickname)} \n"
                            f"📊 班次： <code>{shift_text}</code> \n"
                            f"🕓 本次 {MessageFormatter.format_copyable_text(act)} 已超时 <code>{overtime_minutes}</code> 分钟！\n"
                            f"💢 请立刻回座，避免产生更多罚款！"
                        )

                    if msg:
                        await send_group_message(msg, build_quick_back_kb())

            # ===== 锁外处理强制回座 =====
            if break_data.get("should_break", False):
                msg = (
                    f"🛑 <b>自动安全回座</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, break_data['nickname'])}\n"
                    f"📝 活动：<code>{act}</code>\n"
                    f"📊 班次：<code>{shift_text}</code>\n"
                    f"⚠️ 超时超过2小时，系统已自动回座\n"
                    f"💰 本次扣除绩效：<code>{break_data['fine_amount']}</code> 分数"
                )
                await send_group_message(msg)

                for attempt in range(3):
                    if await push_force_back_notification(
                        break_data["nickname"],
                        break_data["elapsed"],
                        break_data["fine_amount"],
                    ):
                        break
                    logger.warning(f"⚠️ 强制回座通知发送失败，重试 {attempt + 1}/3")
                    await asyncio.sleep(2)

                await db.clear_user_checkin_message(chat_id, uid)
                await timer_manager.cancel_timer(
                    f"{chat_id}-{uid}", preserve_message=False
                )
                break

            await asyncio.sleep(30)

    except asyncio.CancelledError:
        logger.info(f"定时器 {chat_id}-{uid} 被取消（班次: {shift}）")
        # 🆕 2. 如果是被取消的且 preserve_message=True，不清理消息
        if preserve_message:
            logger.debug(f"⏭️ 被取消的定时器保留消息ID")
            return
        raise

    except Exception as e:
        logger.error(f"定时器错误（班次: {shift}）: {e}")

    finally:
        try:
            # 🆕 3. 如果 preserve_message 为 True，跳过清理
            if preserve_message:
                logger.debug(f"⏭️ 定时器跳过清理消息ID (preserve_message=True)")
                return

            # 否则执行正常的清理检查
            current_user_data = await db.get_user_cached(chat_id, uid)

            # 情况1：用户没有活动了，可以清理消息ID
            if not current_user_data or not current_user_data.get("current_activity"):
                current_message_id = await db.get_user_checkin_message_id(chat_id, uid)
                if current_message_id:
                    await db.clear_user_checkin_message(chat_id, uid)
                    logger.debug(
                        f"🧹 定时器清理消息ID: {current_message_id} (用户无活动)"
                    )

            # 情况2：用户有活动，但活动不同（新活动开始了）
            elif current_user_data.get("current_activity") != act:
                logger.debug(
                    f"⏭️ 定时器跳过清理: 用户已有新活动 {current_user_data['current_activity']}"
                )
                # 不清理消息ID，保留新活动的

            # 情况3：还是同一个活动（异常情况，理论上不应该发生）
            else:
                logger.warning(f"⚠️ 定时器异常退出但活动仍存在: {act}")
                await db.clear_user_checkin_message(chat_id, uid)

        except Exception as e:
            logger.error(f"❌ 定时器清理异常: {e}")


# ========== 核心打卡功能 ==========
async def start_activity(message: types.Message, act: str):
    """开始活动 - 优化版（使用班次判定）"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await reset_daily_data_if_needed(chat_id, uid)

        # -----------------------------
        # 基础检查
        # -----------------------------
        if not await db.activity_exists(act):
            await message.answer(
                f"❌ 活动 '{act}' 不存在", 
                reply_to_message_id=message.message_id
            )
            return

        has_active, current_act = await has_active_activity(chat_id, uid)
        if has_active:
            await message.answer(
                Config.MESSAGES["has_activity"].format(current_act),
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # -----------------------------
        # 获取用户信息和当前时间
        # -----------------------------
        name = message.from_user.full_name
        now = get_beijing_time()

        # ===== ⭐ 获取用户班次状态 =====
        user_shift_state = await db.get_user_active_shift(chat_id, uid)
        if not user_shift_state:
            await message.answer(
                "❌ 您当前没有进行中的班次，请先打上班卡！",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # 解析班次开始时间（用于过期检查）
        shift_start_time = user_shift_state["shift_start_time"]
        if isinstance(shift_start_time, str):
            try:
                shift_start_time = datetime.fromisoformat(
                    shift_start_time.replace("Z", "+00:00")
                )
            except:
                shift_start_time = datetime.strptime(
                    shift_start_time, "%Y-%m-%d %H:%M:%S.%f%z"
                )

        # 检查班次是否过期（16小时）
        if now - shift_start_time > timedelta(hours=16):
            await db.clear_user_shift_state(chat_id, uid, user_shift_state["shift"])
            await message.answer(
                "❌ 您的班次已过期（超过16小时），请重新上班打卡！",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # ===== 🎯 使用状态模型获取准确的班次信息 =====
        shift_info = await db.determine_shift_for_time(
            chat_id=chat_id,
            current_time=now,
            checkin_type="activity",
            active_shift=user_shift_state["shift"],
            active_record_date=user_shift_state["record_date"],
        )

        current_shift = shift_info["shift"]
        record_date = shift_info["record_date"]
        shift_detail = shift_info["shift_detail"]
        shift_text = "白班" if current_shift == "day" else "夜班"

        logger.info(
            f"🔄 [开始活动] 使用状态模型: {shift_text}, "
            f"详情={shift_detail}, 记录日期={record_date}"
        )

        # -----------------------------
        # 活动限制检查
        # -----------------------------
        can_perform, reason = await can_perform_activities(
            chat_id, uid, current_shift, record_date
        )
        if not can_perform:
            await message.answer(
                reason,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # 活动人数限制
        user_limit = await db.get_activity_user_limit(act)
        if user_limit > 0:
            current_users = await db.get_current_activity_users(chat_id, act)
            if current_users >= user_limit:
                await message.answer(
                    f"❌ 活动 '<code>{act}</code>' 人数已满！\n\n"
                    f"📊 限制人数：<code>{user_limit}</code> 人\n"
                    f"• 当前进行：<code>{current_users}</code> 人\n"
                    f"• 剩余名额：<code>0</code> 人",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    reply_to_message_id=message.message_id,
                    parse_mode="HTML",
                )
                return

        # 活动次数限制
        can_start, current_count, max_times = await check_activity_limit_by_shift(
            chat_id, uid, act, current_shift
        )
        if not can_start:
            await message.answer(
                f"❌ {shift_text}的 '<code>{act}</code>' 次数已达上限\n\n"
                f"📊 当前次数：<code>{current_count}</code> / <code>{max_times}</code>",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        # -----------------------------
        # 更新用户活动状态
        # -----------------------------
        await db.update_user_activity(chat_id, uid, act, str(now), name, current_shift)

        # 启动计时器
        time_limit = await db.get_activity_time_limit(act)
        await timer_manager.start_timer(
            chat_id, uid, act, time_limit, shift=current_shift
        )

        # -----------------------------
        # 发送打卡消息
        # -----------------------------
        sent_message = await message.answer(
            MessageFormatter.format_activity_message(
                uid,
                name,
                act,
                now.strftime("%m/%d %H:%M:%S"),
                current_count + 1,
                max_times,
                time_limit,
                current_shift,
            ),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        # 保存打卡消息ID
        await db.update_user_checkin_message(chat_id, uid, sent_message.message_id)

        # -----------------------------
        # 日志和推送
        # -----------------------------
        logger.info(
            f"📝 用户 {uid} 开始活动 {act}（{shift_text}），消息ID: {sent_message.message_id}, "
            f"记录日期: {record_date}, 班次详情: {shift_detail}"
        )

        # 吃饭推送
        if act == "吃饭":
            try:
                notification_text = (
                    f"🍽️ <b>吃饭通知</b> <code>{shift_text}</code>\n"
                    f" {MessageFormatter.format_user_link(uid, name)} 去吃饭了\n"
                    f"⏰ 时间：<code>{now.strftime('%H:%M:%S')}</code>\n"
                )
                asyncio.create_task(
                    notification_service.send_notification(chat_id, notification_text)
                )
                logger.info(f"📣 已触发用户 {uid}（{shift_text}）的 {act} 推送")
            except Exception as e:
                logger.error(f"❌ {act} 推送失败: {e}")


# ========== 回座功能 ==========
async def process_back(message: types.Message):
    """回座打卡"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)


async def _process_back_locked(
    message: types.Message,
    chat_id: int,
    uid: int,
    shift: str = None,
):
    """线程安全的回座逻辑 - 优先使用用户班次状态"""
    start_time = time.time()
    key = f"{chat_id}:{uid}"

    # ========== 🎯 优化1：带超时的防重入检测 ==========
    if key in active_back_processing:
        lock_time = active_back_processing.get(key)
        # 如果是时间戳且超过30秒，强制释放（防止死锁）
        if isinstance(lock_time, (int, float)) and time.time() - lock_time > 30:
            logger.warning(
                f"⚠️ [回座] 强制释放过期锁: {key} (持有时间: {time.time()-lock_time:.1f}秒)"
            )
            active_back_processing.pop(key, None)
        else:
            await message.answer(
                "⚠️ 您的回座请求正在处理中，请稍候。",
                reply_to_message_id=message.message_id,
            )
            return

    # 存储时间戳而不是简单的True，便于超时判断
    active_back_processing[key] = time.time()

    try:
        now = get_beijing_time()

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        logger.debug(f"🔍 用户数据: {user_data}")

        if not user_data or not user_data.get("current_activity"):
            await message.answer(
                Config.MESSAGES["no_activity"],
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                reply_to_message_id=message.message_id,
            )
            return

        act = user_data["current_activity"]
        activity_start_time_str = user_data["activity_start_time"]
        nickname = user_data.get("nickname", "未知用户")

        # ========== 🎯 获取原始班次信息 ==========
        original_shift = user_data.get("shift", "day")

        # 获取打卡消息ID
        checkin_message_id = await db.get_user_checkin_message_id(chat_id, uid)
        logger.info(f"📝 回座: 用户 {uid}，原打卡消息ID: {checkin_message_id}")

        # fallback 从缓存/数据库字段获取
        if not checkin_message_id and user_data.get("checkin_message_id"):
            checkin_message_id = user_data.get("checkin_message_id")
            logger.debug(f"📝 从user_data获取消息ID: {checkin_message_id}")

        if not checkin_message_id:
            logger.warning(f"⚠️ 用户 {uid} 没有找到打卡消息ID")

        # 解析活动开始时间
        start_time_dt = None
        try:
            if activity_start_time_str:
                clean_str = str(activity_start_time_str).strip()
                if clean_str.endswith("Z"):
                    clean_str = clean_str.replace("Z", "+00:00")
                try:
                    start_time_dt = datetime.fromisoformat(clean_str)
                    if start_time_dt.tzinfo is None:
                        start_time_dt = beijing_tz.localize(start_time_dt)
                except ValueError:
                    formats = [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M",
                        "%m/%d %H:%M:%S",
                        "%m/%d %H:%M",
                    ]
                    for fmt in formats:
                        try:
                            start_time_dt = datetime.strptime(clean_str, fmt)
                            if fmt.startswith("%m/%d"):
                                start_time_dt = start_time_dt.replace(year=now.year)
                            break
                        except ValueError:
                            continue
                    if start_time_dt and start_time_dt.tzinfo is None:
                        start_time_dt = beijing_tz.localize(start_time_dt)
        except Exception as e:
            logger.error(f"解析开始时间失败: {activity_start_time_str}, 错误: {e}")

        if not start_time_dt:
            logger.warning("时间解析失败，使用当前时间作为备用")
            start_time_dt = now

        # ========== ⭐ 修复：优先使用用户班次状态 ==========
        # 先尝试获取用户的活跃班次状态
        user_shift_state = await db.get_user_active_shift(chat_id, uid)

        if user_shift_state:
            # 使用用户当前班次状态
            final_shift = user_shift_state["shift"]
            record_date = user_shift_state["record_date"]  # 班次记录日期
            shift_start_time = user_shift_state["shift_start_time"]  # 班次开始时间

            # 解析班次开始时间
            if isinstance(shift_start_time, str):
                try:
                    shift_start_time = datetime.fromisoformat(
                        shift_start_time.replace("Z", "+00:00")
                    )
                except:
                    shift_start_time = datetime.strptime(
                        shift_start_time, "%Y-%m-%d %H:%M:%S.%f%z"
                    )

            logger.info(
                f"📝 回座使用班次状态: {final_shift}, "
                f"记录日期={record_date}, 班次开始时间={shift_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # 获取班次配置
            shift_config = await db.get_shift_config(chat_id)
            day_end_str = shift_config.get("day_end", "21:00")
            day_end_hour, day_end_min = map(int, day_end_str.split(":"))

            # ===== ⭐ 修复：根据班次开始时间判断归属 =====
            if final_shift == "day":
                forced_date = record_date  # 白班直接用班次记录日期
                shift_detail = "day"
            else:  # night
                # 构建当天的白班结束时间
                day_end_dt = shift_start_time.replace(
                    hour=day_end_hour, minute=day_end_min, second=0, microsecond=0
                )

                # 判断班次开始时间是否在白班结束后
                if shift_start_time >= day_end_dt:
                    # 今晚夜班：班次开始时间 >= 当天白班结束时间
                    forced_date = record_date
                    shift_detail = "night_tonight"
                else:
                    # 昨晚夜班：班次开始时间 < 当天白班结束时间
                    forced_date = record_date  # record_date 已经是昨天
                    shift_detail = "night_last"

            logger.info(f"📝 班次详情: {shift_detail}, 强制日期={forced_date}")

        else:
            # 没有班次状态，使用传入或原始班次
            logger.warning(f"⚠️ 用户 {uid} 没有活跃班次状态，使用原始逻辑")

            # 如果有传入班次参数（从快速回座按钮），优先使用
            if shift:
                final_shift = shift
                logger.info(f"📝 使用传入班次: {final_shift}")
            else:
                final_shift = original_shift
                logger.info(f"📝 使用用户原始班次: {final_shift}")

            # ========== 原有的 fallback 逻辑 ==========
            shift_info = await db.determine_shift_for_time(
                chat_id=chat_id,
                current_time=start_time_dt,
                checkin_type="activity",
                active_shift=final_shift,
            )

            if shift_info:
                determined_shift = shift_info["shift"]
                shift_detail = shift_info["shift_detail"]
                forced_date = shift_info["record_date"]

                if determined_shift != final_shift:
                    logger.info(
                        f"📝 班次修正: 原班次={final_shift}, 判定班次={determined_shift}"
                    )
                    final_shift = determined_shift
            else:
                # 最终保底逻辑
                logger.warning("⚠️ determine_shift_for_time 返回 None，使用保底逻辑")
                is_dual = await db.is_dual_mode_enabled(chat_id)
                shift_config = await db.get_shift_config(chat_id)

                if is_dual and final_shift == "night":
                    day_start_str = shift_config.get("day_start", "09:00")
                    day_start_hour, day_start_min = map(int, day_start_str.split(":"))
                    day_start_dt = start_time_dt.replace(
                        hour=day_start_hour,
                        minute=day_start_min,
                        second=0,
                        microsecond=0,
                    )

                    if start_time_dt >= day_start_dt:
                        forced_date = start_time_dt.date()
                        shift_detail = "night_tonight"
                    else:
                        forced_date = start_time_dt.date() - timedelta(days=1)
                        shift_detail = "night_last"
                else:
                    forced_date = start_time_dt.date()
                    shift_detail = "day"

        # 记录最终的班次信息
        shift_text_map = {
            "day": "白班",
            "night": "夜班",
            "night_last": "昨晚夜班",
            "night_tonight": "今晚夜班",
        }
        shift_text = shift_text_map.get(shift_detail, "白班")

        logger.info(
            f"📅 最终判定: 班次={final_shift}, 归属={shift_detail}, "
            f"强制日期={forced_date}"
        )

        # 计算经过时间
        elapsed = int((now - start_time_dt).total_seconds())

        # 并行获取时间限制
        time_limit_task = asyncio.create_task(db.get_activity_time_limit(act))
        time_limit_minutes = await time_limit_task
        time_limit_seconds = time_limit_minutes * 60

        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        # 准备消息数据
        elapsed_time_str = MessageFormatter.format_time(int(elapsed))
        time_str = now.strftime("%m/%d %H:%M:%S")
        activity_start_time_for_notification = activity_start_time_str

        # ========== 🎯 完成活动时传入完整信息 ==========
        logger.info(f"📝 完成活动 - 班次: {final_shift}, 强制日期: {forced_date}")
        await db.complete_user_activity(
            chat_id,
            uid,
            act,
            int(elapsed),
            fine_amount,
            is_overtime,
            final_shift,
            forced_date=forced_date,
        )

        # 取消计时器
        await timer_manager.cancel_timer(f"{chat_id}-{uid}", preserve_message=True)

        # 获取最新数据
        user_data_task = asyncio.create_task(db.get_user_cached(chat_id, uid))
        user_activities_task = asyncio.create_task(
            db.get_user_all_activities(chat_id, uid)
        )
        user_data = await user_data_task
        user_activities = await user_activities_task

        activity_counts = {
            a: info.get("count", 0) for a, info in user_activities.items()
        }

        # 构建回座消息
        back_message = MessageFormatter.format_back_message(
            user_id=uid,
            user_name=user_data.get("nickname", nickname),
            activity=act,
            time_str=time_str,
            elapsed_time=elapsed_time_str,
            total_activity_time=MessageFormatter.format_time(
                int(user_activities.get(act, {}).get("time", 0))
            ),
            total_time=MessageFormatter.format_time(
                int(user_data.get("total_accumulated_time", 0))
            ),
            activity_counts=activity_counts,
            total_count=user_data.get("total_activity_count", 0),
            is_overtime=is_overtime,
            overtime_seconds=overtime_seconds,
            fine_amount=fine_amount,
        )

        # 优先尝试引用回复
        send_success = False
        if checkin_message_id:
            try:
                await message.answer(
                    back_message,
                    reply_to_message_id=checkin_message_id,
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
                send_success = True
                logger.info(f"✅ 成功引用回复到消息 {checkin_message_id}")
            except Exception as e:
                error_msg = str(e).lower()
                if any(
                    k in error_msg
                    for k in [
                        "message to reply not found",
                        "message can't be replied",
                        "message not found",
                        "bad request: replied message not found",
                    ]
                ):
                    logger.warning(
                        f"⚠️ 打卡消息 {checkin_message_id} 不可用，降级普通回复"
                    )
                else:
                    logger.error(f"❌ 引用回复未知错误: {e}")
                    raise

        if not send_success:
            await message.answer(
                back_message,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"ℹ️ 降级发送回座消息，没有引用打卡消息")

        # 异步发送超时通知
        if is_overtime and fine_amount > 0:
            group_data = await db.get_group_cached(chat_id)
            if group_data.get("channel_id"):
                notification_user_data = user_data.copy() if user_data else {}
                notification_user_data["activity_start_time"] = (
                    activity_start_time_for_notification
                )
                notification_user_data["nickname"] = nickname
                asyncio.create_task(
                    send_overtime_notification_async(
                        chat_id=chat_id,
                        uid=uid,
                        user_data=notification_user_data,
                        act=act,
                        fine_amount=fine_amount,
                        now=now,
                        elapsed_time=int(elapsed),
                        time_limit_minutes=time_limit_minutes,
                    )
                )

        # 吃饭回座推送
        if act == "吃饭":
            try:
                chat_title = str(chat_id)
                try:
                    chat_info = await message.bot.get_chat(chat_id)
                    chat_title = chat_info.title or chat_title
                except Exception:
                    pass

                eat_end_notification_text = (
                    f"🍽️ <b>吃饭结束通知</b>\n"
                    f"{MessageFormatter.format_user_link(uid, user_data.get('nickname', '用户'))} 吃饭回来了\n"
                    f"⏱️ 吃饭耗时：<code>{elapsed_time_str}</code>\n"
                )

                asyncio.create_task(
                    notification_service.send_notification(
                        chat_id, eat_end_notification_text
                    )
                )
                logger.info(f"🍽️ 已触发用户 {uid} 的吃饭回座推送")

            except Exception as e:
                logger.error(f"❌ 吃饭回座推送失败: {e}")

        # 添加调试日志，确认日期归属
        logger.info(
            f"📊 [回座完成] 用户{uid} | 活动:{act} | "
            f"班次:{final_shift} | 归属:{shift_detail} | "
            f"强制日期:{forced_date} | 超时:{is_overtime} | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"回座处理异常: {e}")
        logger.error(traceback.format_exc())
        await message.answer(
            "❌ 回座失败，请稍后重试。", reply_to_message_id=message.message_id
        )

    finally:
        # ========== 🎯 必须释放防重入锁 ==========
        if key in active_back_processing:
            active_back_processing.pop(key, None)
            logger.info(f"✅ [回座锁释放] key={key}")
        else:
            logger.warning(f"⚠️ [回座锁释放] key={key} 已不存在")

        # finally 清理打卡消息ID
        try:
            # 检查是否已经清理过
            current_message_id = await db.get_user_checkin_message_id(chat_id, uid)
            if current_message_id:
                await db.clear_user_checkin_message(chat_id, uid)
                logger.info(f"🧹 finally 清理用户 {uid} 的打卡消息ID")
            else:
                logger.debug(f"用户 {uid} 的打卡消息ID已不存在，无需清理")
        except Exception as e:
            logger.warning(f"⚠️ finally 清理失败 chat_id={chat_id}, uid={uid}: {e}")

        # 总耗时日志
        duration = round(time.time() - start_time, 2)
        logger.info(f"✅ [回座结束] key={key}，总耗时 {duration}s")


async def send_overtime_notification_async(
    chat_id: int,
    uid: int,
    user_data: dict,
    act: str,
    fine_amount: int,
    now: datetime,
    elapsed_time: int = None,  # ✅ 可选参数
    time_limit_minutes: int = None,  # ✅ 可选参数
):
    """异步发送超时通知到频道"""
    try:
        # 检查是否绑定了频道
        group_data = await db.get_group_cached(chat_id)
        channel_id = group_data.get("channel_id")
        if not channel_id:
            logger.debug(f"⏱️ 群组 {chat_id} 未绑定频道，跳过推送")
            return

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except Exception:
            pass

        nickname = user_data.get("nickname", "未知用户")

        # ✅ 如果传入了计算好的值，直接使用
        if elapsed_time is not None and time_limit_minutes is not None:
            time_limit_seconds = time_limit_minutes * 60
            if elapsed_time > time_limit_seconds:
                overtime_seconds = elapsed_time - time_limit_seconds
                overtime_str = MessageFormatter.format_time(overtime_seconds)
            else:
                overtime_str = "未超时"
        else:
            activity_start_time = user_data.get("activity_start_time")
            if activity_start_time:
                try:
                    start_time = datetime.fromisoformat(activity_start_time)
                    time_limit = await db.get_activity_time_limit(act)
                    time_limit_seconds = time_limit * 60
                    total_elapsed = int((now - start_time).total_seconds())

                    if total_elapsed > time_limit_seconds:
                        overtime_seconds = total_elapsed - time_limit_seconds
                        overtime_str = MessageFormatter.format_time(overtime_seconds)
                except Exception as e:
                    logger.error(f"时间计算失败: {e}")

        notif_text = (
            f"🚨 <b>超时回座通知</b>\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
            f"📝 活动：<code>{act}</code>\n"
            f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
            f"⏱️ 超时时长：<code>{overtime_str}</code>\n"
            f"💰 扣除绩效：<code>{fine_amount}</code> 分"
        )

        # 发送到频道
        await notification_service.send_notification(chat_id, notif_text)
        logger.info(f"✅ 超时通知已推送到频道 {channel_id}: 用户{uid} - {act}")

    except Exception as e:
        logger.error(f"❌ 超时通知推送异常: {e}")


# ========== 上下班打卡功能 ==========
async def process_work_checkin(message: types.Message, checkin_type: str):
    """智能化上下班打卡系统 - 统一版（完整修复版）"""

    chat_id = message.chat.id
    uid = message.from_user.id
    name = message.from_user.full_name

    # 🛡️ 功能启用检查
    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 本群组尚未启用上下班打卡功能\n\n"
            "👑 请联系管理员使用命令：\n"
            "<code>/setworktime 09:00 18:00</code>\n"
            "设置上下班时间后即可使用",
            reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        logger.info(f"❌ 群组 {chat_id} 未启用上下班功能，用户 {uid} 尝试打卡")
        return

    now = get_beijing_time()
    current_time = now.strftime("%H:%M")
    trace_id = f"{chat_id}-{uid}-{int(time.time())}"

    # 动态action_text
    action_text = "上班" if checkin_type == "work_start" else "下班"
    status_type = "迟到" if checkin_type == "work_start" else "早退"

    logger.info(f"🟢[{trace_id}] 开始处理{action_text}打卡请求：{name}({uid})")

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        # ========== 并行获取必要配置 ==========
        work_hours_task = asyncio.create_task(db.get_group_work_time(chat_id))
        shift_config_task = asyncio.create_task(db.get_shift_config(chat_id))
        is_admin_task = asyncio.create_task(is_admin(uid))

        # ========== 初始化用户数据 ==========
        try:
            await db.init_group(chat_id)
            await db.init_user(chat_id, uid)
            await reset_daily_data_if_needed(chat_id, uid)
            user_data = await db.get_user_cached(chat_id, uid)
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 初始化用户/群组失败: {e}")
            logger.error(traceback.format_exc())
            await message.answer(
                "⚠️ 数据初始化失败，请稍后再试。",
                reply_to_message_id=message.message_id,
                reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
            )
            return

        # ========== 等待并行任务结果 ==========
        work_hours = await work_hours_task
        shift_config = await shift_config_task
        is_admin_user = await is_admin_task

        # ========== 模式判断 ==========
        is_dual_mode = shift_config.get("dual_mode", False) if shift_config else False

        # ========== ⭐ 统一使用 determine_shift_for_time 进行基础判定 ==========
        shift_info = await db.determine_shift_for_time(
            chat_id=chat_id,
            current_time=now,
            checkin_type=checkin_type,
            active_shift=None,
        )

        # 验证是否在窗口内
        if shift_info is None or shift_info.get("shift_detail") is None:
            # 获取友好的时间窗口描述
            if checkin_type == "work_start":
                day_start = shift_config.get("day_start", "09:00")
                day_end = shift_config.get("day_end", "21:00")
                grace_before = shift_config.get("grace_before", 120)
                grace_after = shift_config.get("grace_after", 360)

                # 白班上班窗口
                day_start_h, day_start_m = map(int, day_start.split(":"))
                day_start_dt = now.replace(
                    hour=day_start_h, minute=day_start_m, second=0
                )
                day_work_start_start = (
                    day_start_dt - timedelta(minutes=grace_before)
                ).strftime("%H:%M")
                day_work_start_end = (
                    day_start_dt + timedelta(minutes=grace_after)
                ).strftime("%H:%M")

                # 夜班上班窗口（对应昨晚下班）
                day_end_h, day_end_m = map(int, day_end.split(":"))
                day_end_dt = now.replace(hour=day_end_h, minute=day_end_m, second=0)
                night_work_start_start = (
                    day_end_dt - timedelta(days=1) - timedelta(minutes=grace_before)
                ).strftime("%H:%M")
                night_work_start_end = (
                    day_end_dt - timedelta(days=1) + timedelta(minutes=grace_after)
                ).strftime("%H:%M")

                await message.answer(
                    f"❌ 当前时间不在{action_text}打卡窗口内\n\n"
                    f"📊 <b>允许的上班时间：</b>\n"
                    f"• 白班上班：<code>{day_work_start_start} ~ {day_work_start_end}</code>\n"
                    f"• 夜班上班：<code>{night_work_start_start} ~ {night_work_start_end}</code>（次日凌晨）\n\n"
                    f"⏰ 当前时间：<code>{current_time}</code>\n"
                    f"💡 请等待对班时间窗口或联系管理员调整时间设置",
                    reply_to_message_id=message.message_id,
                    reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                    parse_mode="HTML",
                )
                return
            else:  # work_end
                day_start = shift_config.get("day_start", "09:00")
                day_end = shift_config.get("day_end", "21:00")
                workend_grace_before = shift_config.get("workend_grace_before", 120)
                workend_grace_after = shift_config.get("workend_grace_after", 360)

                # 白班下班窗口
                day_end_h, day_end_m = map(int, day_end.split(":"))
                day_end_dt = now.replace(hour=day_end_h, minute=day_end_m, second=0)
                day_work_end_start = (
                    day_end_dt - timedelta(minutes=workend_grace_before)
                ).strftime("%H:%M")
                day_work_end_end = (
                    day_end_dt + timedelta(minutes=workend_grace_after)
                ).strftime("%H:%M")

                # 夜班下班窗口（对应第二天早上）
                day_start_h, day_start_m = map(int, day_start.split(":"))
                day_start_dt = now.replace(
                    hour=day_start_h, minute=day_start_m, second=0
                )
                night_work_end_start = (
                    day_start_dt
                    + timedelta(days=1)
                    - timedelta(minutes=workend_grace_before)
                ).strftime("%H:%M")
                night_work_end_end = (
                    day_start_dt
                    + timedelta(days=1)
                    + timedelta(minutes=workend_grace_after)
                ).strftime("%H:%M")

                await message.answer(
                    f"❌ 当前时间不在{action_text}打卡窗口内\n\n"
                    f"📊 <b>允许的下班时间：</b>\n"
                    f"• 白班下班：<code>{day_work_end_start} ~ {day_work_end_end}</code>\n"
                    f"• 夜班下班：<code>{night_work_end_start} ~ {night_work_end_end}</code>（次日早上）\n\n"
                    f"⏰ 当前时间：<code>{current_time}</code>\n"
                    f"💡 请等待对班时间窗口或联系管理员调整时间设置",
                    reply_to_message_id=message.message_id,
                    reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                    parse_mode="HTML",
                )
                return

        # 从 shift_info 获取基础值
        shift = shift_info["shift"]  # "day" 或 "night"
        shift_detail = shift_info[
            "shift_detail"
        ]  # "day", "night_last", "night_tonight"
        record_date = shift_info["record_date"]
        business_date = shift_info.get("business_date", record_date)

        # ========== 班次文本映射 ==========
        shift_text_map = {
            "day": "白班",
            "night": "夜班",
            "night_last": "昨晚夜班",
            "night_tonight": "今晚夜班",
        }
        shift_text = shift_text_map.get(shift_detail, "白班")

        logger.info(
            f"[{trace_id}] ✅ 班次判定: {shift_text} | "
            f"shift={shift}, detail={shift_detail}, record_date={record_date}"
        )

        # ========== 1. 上班打卡 ==========
        if checkin_type == "work_start":
            # 班次有效性检查
            if is_dual_mode and shift_detail is None:
                await message.answer(
                    f"❌ 当前时间不在任何班次的{action_text}窗口内",
                    reply_to_message_id=message.message_id,
                    reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                )
                return

            # 🎯 修复1：检查班次切换，自动结束旧班次的活动
            user_data = await db.get_user_cached(chat_id, uid)
            if user_data and user_data.get("current_activity"):
                current_shift = user_data.get("shift", "day")
                current_activity = user_data["current_activity"]

                # 获取用户当前的班次状态
                current_state = await db.get_user_shift_state(
                    chat_id, uid, current_shift
                )

                # 如果用户有旧班次的活动，且新班次不同，自动结束旧活动
                if current_state and current_shift != shift:
                    logger.info(
                        f"[{trace_id}] 🔄 班次切换检测: "
                        f"旧班次={current_shift}(活动:{current_activity}), "
                        f"新班次={shift}，自动结束旧活动"
                    )

                    # 发送通知告知用户
                    await message.answer(
                        f"🔄 <b>系统自动处理</b>\n"
                        f"检测到您有未结束的<code>{current_shift}</code>班次活动：<code>{current_activity}</code>\n"
                        f"由于您正在打<code>{shift}</code>班次上班卡，该活动已自动结束。",
                        parse_mode="HTML",
                    )

                    # 自动结束旧活动
                    await auto_end_current_activity(
                        chat_id=chat_id,
                        uid=uid,
                        user_data=user_data,
                        now=now,
                        message=message,
                    )

                    # 重新获取用户数据
                    user_data = await db.get_user_cached(chat_id, uid)

            # 🎯 检查重复上班
            has_record = await _check_shift_work_record(
                chat_id,
                uid,
                "work_start",
                shift,
                record_date,
            )
            if has_record:
                existing_record = await _get_existing_work_record(
                    chat_id,
                    uid,
                    "work_start",
                    shift,
                    record_date,
                )
                if existing_record:
                    existing_time = existing_record.get("checkin_time", "未知时间")
                    existing_status = existing_record.get("status", "未知状态")
                    existing_created = existing_record.get("created_at")
                    created_str = (
                        existing_created.strftime("%m/%d %H:%M")
                        if existing_created
                        else "未知"
                    )

                    await message.answer(
                        f"🚫 您本班次已经打过{action_text}卡了！\n\n"
                        f"📊 <b>已有记录详情：</b>\n"
                        f"   • 打卡时间：<code>{existing_time}</code>\n"
                        f"   • 打卡状态：{existing_status}\n"
                        f"   • 班次类型：<code>{shift_text}</code>\n"
                        f"   • 记录时间：<code>{created_str}</code>\n\n"
                        f"💡 如需重新打卡，请联系管理员",
                        parse_mode="HTML",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                else:
                    await message.answer(
                        f"❌ 您本班次已经打过{action_text}卡！",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                logger.info(f"[{trace_id}] ⚠️ 用户本班次重复{action_text}")
                return

            # 🎯 检查下班后打上班卡异常
            has_work_end = await _check_shift_work_record(
                chat_id,
                uid,
                "work_end",
                shift,
                record_date,
            )
            if has_work_end:
                existing_record = await _get_existing_work_record(
                    chat_id,
                    uid,
                    "work_end",
                    shift,
                    record_date,
                )
                if existing_record:
                    existing_time = existing_record.get("checkin_time", "未知时间")
                    existing_created = existing_record.get("created_at")
                    created_str = (
                        existing_created.strftime("%m/%d %H:%M")
                        if existing_created
                        else "未知"
                    )

                    await message.answer(
                        f"🚫 您本班次已经在 <code>{existing_time}</code> 打过下班卡，无法再打{action_text}卡！\n\n"
                        f"💡 如需重新打卡，请联系管理员或等待下一班次",
                        parse_mode="HTML",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                else:
                    await message.answer(
                        f"🚫 您本班次已经打过下班卡，无法再打{action_text}卡！\n"
                        "💡 如需重新打卡，请联系管理员或等待下一班次",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                logger.info(f"[{trace_id}] 🔁 {action_text}后再次{action_text}打卡异常")
                return

            # ========== 🎯 计算迟到/罚款 ==========
            if shift_detail in ["night_last", "night_tonight"]:
                # 夜班：上班期望时间是前一天的白班结束时间
                expected_time = shift_config.get("day_end", "21:00")
                expected_date = record_date
                logger.info(
                    f"[{trace_id}] 🌙 夜班上班: 期望时间={expected_time}, 期望日期={expected_date}"
                )
            else:  # day
                expected_time = work_hours["work_start"]
                expected_date = record_date

            expected_hour, expected_minute = map(int, expected_time.split(":"))
            expected_dt = datetime.combine(
                expected_date, dt_time(expected_hour, expected_minute)
            ).replace(tzinfo=now.tzinfo)

            time_diff_seconds = int((now - expected_dt).total_seconds())
            time_diff_minutes = time_diff_seconds / 60

            fine_amount = 0
            status = "✅ 准时"
            is_late_early = False
            emoji_status = "👍"

            if time_diff_seconds > 0:
                fine_amount = await calculate_work_fine("work_start", time_diff_minutes)
                duration = MessageFormatter.format_duration(time_diff_seconds)
                status = f"🚨 迟到 {duration}"
                if fine_amount:
                    status += f"\n💰扣除绩效: {fine_amount} 分"
                is_late_early = True
                emoji_status = "😅"

            # ========== 🎯 写入数据库（带重试）==========
            db_write_success = False
            for attempt in range(3):
                try:
                    async with db.pool.acquire() as conn:
                        async with conn.transaction():
                            # 1. 插入上班记录
                            await conn.execute(
                                """
                                INSERT INTO work_records
                                (chat_id, user_id, record_date, checkin_type,
                                 checkin_time, status, time_diff_minutes,
                                 fine_amount, shift, shift_detail)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                                """,
                                chat_id,
                                uid,
                                record_date,
                                "work_start",
                                current_time,
                                status,
                                time_diff_minutes,
                                fine_amount,
                                shift,
                                shift_detail,
                            )

                            # 2. ✅ 设置用户班次状态（上班打卡）
                            if is_dual_mode:
                                success = await db.set_user_shift_state(
                                    chat_id=chat_id,
                                    user_id=uid,
                                    shift=shift,
                                    record_date=record_date,
                                )

                                if success:
                                    shift_text = "白班" if shift == "day" else "夜班"
                                    logger.info(
                                        f"🏁 [{trace_id}] 用户班次状态设置成功: {shift_text}, 用户={uid}"
                                    )

                    db_write_success = True
                    break

                except Exception as e:
                    logger.error(
                        f"[{trace_id}] ❌ 数据库写入失败 (尝试 {attempt + 1}/3): {e}"
                    )
                    if attempt == 2:
                        await message.answer("❌ 系统繁忙，请稍后重试")
                        return
                    await asyncio.sleep(1 * (2**attempt))

            if not db_write_success:
                return

            # ========== 发送成功消息 ==========
            result_msg = (
                f"{emoji_status} <b>{shift_text}{action_text}完成</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                f"⏰ 打卡时间：<code>{current_time}</code>\n"
                f"📅 {action_text}时间：<code>{expected_dt.strftime('%m/%d %H:%M')}</code>\n"
                f"📊 状态：{status}"
            )

            await message.answer(
                result_msg,
                reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )

            # ✅ 发送通知（传入 shift 参数）
            await send_work_notification(
                chat_id=chat_id,
                user_id=uid,
                user_name=name,
                checkin_time=current_time,
                expected_dt=expected_dt,
                action_text=action_text,
                status_type=status_type if is_late_early else "准时",
                fine_amount=fine_amount,
                trace_id=trace_id,
                shift=shift,
            )

            logger.info(f"✅[{trace_id}] {shift_text}{action_text}打卡流程完成")
            return

        # ========== 2. 下班打卡 ==========
        elif checkin_type == "work_end":
            # 班次有效性检查
            if is_dual_mode and shift_detail is None:
                await message.answer(
                    f"❌ 当前时间不在任何班次的{action_text}窗口内",
                    reply_to_message_id=message.message_id,
                    reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                )
                return

            # 🎯 检查重复下班
            has_record = await _check_shift_work_record(
                chat_id,
                uid,
                "work_end",
                shift,
                record_date,
            )
            if has_record:
                existing_record = await _get_existing_work_record(
                    chat_id,
                    uid,
                    "work_end",
                    shift,
                    record_date,
                )
                if existing_record:
                    existing_time = existing_record.get("checkin_time", "未知时间")
                    existing_status = existing_record.get("status", "未知状态")
                    existing_created = existing_record.get("created_at")
                    created_str = (
                        existing_created.strftime("%m/%d %H:%M")
                        if existing_created
                        else "未知"
                    )

                    await message.answer(
                        f"🚫 您本班次已经打过{action_text}卡了！\n\n"
                        f"📊 <b>已有记录详情：</b>\n"
                        f"    • 打卡时间：<code>{existing_time}</code>\n"
                        f"    • 打卡状态：{existing_status}\n"
                        f"    • 班次类型：<code>{shift_text}</code>\n"
                        f"    • 记录时间：<code>{created_str}</code>",
                        parse_mode="HTML",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                else:
                    await message.answer(
                        f"❌ 您本班次已经打过{action_text}卡！",
                        reply_to_message_id=message.message_id,
                        reply_markup=await get_main_keyboard(
                            chat_id, await is_admin_task
                        ),
                    )
                logger.info(f"[{trace_id}] ⚠️ 用户本班次重复{action_text}")
                return

            # ===== ⭐ 检查是否有对应的上班记录（支持跨天夜班）=====
            has_work_start = await _check_shift_work_record(
                chat_id,
                uid,
                "work_start",
                shift,
                record_date,
            )

            # 夜班特殊处理 - 检查昨天是否有上班记录
            if not has_work_start and shift == "night":
                yesterday = record_date - timedelta(days=1)
                has_work_start_yesterday = await _check_shift_work_record(
                    chat_id,
                    uid,
                    "work_start",
                    shift,
                    yesterday,
                )
                if has_work_start_yesterday:
                    record_date = yesterday
                    has_work_start = True
                    logger.info(
                        f"[{trace_id}] 🌙 检测到昨晚夜班上班记录，使用昨天日期: {yesterday}"
                    )

            if not has_work_start:
                shift_text_display = "白班" if shift == "day" else "夜班"
                await message.answer(
                    f"❌ 未找到 {record_date} 的上班记录，无法打{action_text}卡！\n"
                    f"💡 请先打{shift_text_display}上班卡",
                    reply_to_message_id=message.message_id,
                    reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                )
                logger.warning(
                    f"[{trace_id}] ⚠️ 用户试图{action_text}打卡但未找到上班记录"
                )
                return

            # ===== ⭐ 修复2：夜班下班时间正确计算 =====
            if shift_detail in ["night_last", "night_tonight"] or shift == "night":
                # 夜班：下班时间是第二天早上（白班开始时间）
                expected_time = work_hours["work_start"]

                # 🎯 关键修复：夜班下班日期 = 夜班上班日期 + 1天
                night_work_date = record_date

                if shift_detail == "night_tonight":
                    # 今晚夜班：上班是今天，下班是明天
                    expected_date = night_work_date + timedelta(days=1)
                    logger.info(
                        f"[{trace_id}] 🌙 今晚夜班下班: "
                        f"上班日期={night_work_date}, 下班日期={expected_date}"
                    )
                else:  # night_last
                    # 昨晚夜班：上班是昨天，下班是今天
                    expected_date = night_work_date + timedelta(days=1)  # 昨天+1天=今天
                    logger.info(
                        f"[{trace_id}] 🌙 昨晚夜班下班: "
                        f"上班日期={night_work_date}, 下班日期={expected_date}"
                    )

                logger.info(
                    f"[{trace_id}] 🌙 夜班下班最终: "
                    f"期望时间={expected_time}, 期望日期={expected_date}"
                )
                final_record_date = record_date  # 保持原始记录日期用于数据库
            else:  # day
                expected_time = work_hours["work_end"]
                expected_date = record_date
                final_record_date = record_date

            expected_hour, expected_minute = map(int, expected_time.split(":"))
            expected_dt = datetime.combine(
                expected_date, dt_time(expected_hour, expected_minute)
            ).replace(tzinfo=now.tzinfo)

            # 计算时间差
            time_diff_seconds = int((now - expected_dt).total_seconds())
            time_diff_minutes = time_diff_seconds / 60

            logger.debug(
                f"📊 [{trace_id}] 时间差计算: now={now}, expected={expected_dt}, 差值={time_diff_seconds}秒"
            )

            fine_amount = 0
            status = "✅ 准时"
            is_late_early = False
            emoji_status = "👍"

            if time_diff_seconds < 0:
                fine_amount = await calculate_work_fine(
                    "work_end", abs(time_diff_minutes)
                )
                duration = MessageFormatter.format_duration(abs(time_diff_seconds))
                status = f"🚨 早退 {duration}"
                if fine_amount:
                    status += f"（💰扣除绩效 {fine_amount} 分）"
                is_late_early = True
                emoji_status = "🏃"
            elif time_diff_seconds > 0:
                duration = MessageFormatter.format_duration(time_diff_seconds)
                status = f"✅ 加班 {duration}"
                emoji_status = "⏰"

            # 自动结束活动
            activity_auto_ended = False
            current_activity = user_data.get("current_activity") if user_data else None
            if current_activity:
                with suppress(Exception):
                    await auto_end_current_activity(
                        chat_id, uid, user_data, now, message
                    )
                    activity_auto_ended = True
                    logger.info(f"[{trace_id}] 🔄 已自动结束活动：{current_activity}")

            # ========== 🎯 写入数据库（带重试）==========
            db_write_success = False
            for attempt in range(3):
                try:
                    async with db.pool.acquire() as conn:
                        async with conn.transaction():
                            # 1. 插入下班记录
                            await conn.execute(
                                """
                                INSERT INTO work_records
                                (chat_id, user_id, record_date, checkin_type,
                                 checkin_time, status, time_diff_minutes,
                                 fine_amount, shift, shift_detail)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                                """,
                                chat_id,
                                uid,
                                final_record_date,
                                "work_end",
                                current_time,
                                status,
                                time_diff_minutes,
                                fine_amount,
                                shift,
                                shift_detail,
                            )

                            # 2. ✅ 清除用户班次状态（下班打卡）
                            if is_dual_mode:
                                success = await db.clear_user_shift_state(
                                    chat_id=chat_id,
                                    user_id=uid,
                                    shift=shift,
                                )

                                shift_text = "白班" if shift == "day" else "夜班"

                                if success:
                                    logger.info(
                                        f"🏁 [{trace_id}] 用户班次状态清除成功: {shift_text}, 用户={uid}"
                                    )

                                    # 3. ✅ 检查该班次是否还有其他人在工作中
                                    other_users = await conn.fetchval(
                                        """
                                        SELECT COUNT(*) FROM group_shift_state
                                        WHERE chat_id = $1 AND shift = $2
                                        """,
                                        chat_id,
                                        shift,
                                    )

                                    if other_users == 0:
                                        # 没有其他用户在这个班次了，发送班次结束通知
                                        async def send_end_notification():
                                            try:
                                                await message.answer(
                                                    f"📢 <b>{shift_text}班次结束</b>\n"
                                                    f"所有用户已完成下班打卡",
                                                    parse_mode="HTML",
                                                )
                                            except Exception as e:
                                                logger.error(
                                                    f"发送班次结束通知失败: {e}"
                                                )

                                        asyncio.create_task(send_end_notification())
                                        logger.info(
                                            f"🏁 [{trace_id}] {shift_text}班次所有用户已下班"
                                        )
                                    else:
                                        logger.info(
                                            f"ℹ️ [{trace_id}] 仍有 {other_users} 人在{shift_text}班次工作中"
                                        )
                                else:
                                    logger.warning(
                                        f"⚠️ [{trace_id}] 用户班次状态清除失败: {shift_text}, 用户={uid}"
                                    )

                    db_write_success = True
                    break

                except Exception as e:
                    logger.error(
                        f"[{trace_id}] ❌ 数据库写入失败 (尝试 {attempt + 1}/3): {e}"
                    )
                    if attempt == 2:
                        await message.answer("❌ 系统繁忙，请稍后重试")
                        return
                    await asyncio.sleep(1 * (2**attempt))

            if not db_write_success:
                return

            # ========== 发送成功消息 ==========
            result_msg = (
                f"{emoji_status} <b>{shift_text}{action_text}完成</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                f"⏰ 打卡时间：<code>{current_time}</code>\n"
                f"📅 {action_text}时间：<code>{expected_dt.strftime('%m/%d %H:%M')}</code>\n"
                f"📊 状态：{status}"
            )

            if activity_auto_ended and current_activity:
                result_msg += f"\n\n🔄 检测到未结束活动 <code>{current_activity}</code>，已自动结束"

            await message.answer(
                result_msg,
                reply_markup=await get_main_keyboard(chat_id, await is_admin_task),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )

            # ✅ 发送通知（传入 shift 参数）
            status_display = status_type if is_late_early else "准时"
            if time_diff_seconds > 0 and action_text == "下班":
                status_display = "加班"

            await send_work_notification(
                chat_id=chat_id,
                user_id=uid,
                user_name=name,
                checkin_time=current_time,
                expected_dt=expected_dt,
                action_text=action_text,
                status_type=status_display,
                fine_amount=fine_amount,
                trace_id=trace_id,
                shift=shift,
            )

            logger.info(f"✅[{trace_id}] {shift_text}{action_text}打卡流程完成")
            return


async def _check_shift_work_record(
    chat_id: int, user_id: int, checkin_type: str, shift: str, business_date: date
) -> bool:
    """
    检查指定班次的打卡记录 - 完整保留所有功能

    Args:
        chat_id: 群组ID
        user_id: 用户ID
        checkin_type: 打卡类型 (work_start/work_end)
        shift: 班次 (day/night)
        business_date: 业务日期（由上层 determine_shift_for_time 提供）

    Returns:
        bool: 是否存在记录
    """
    try:
        now = get_beijing_time()
        trace_id = f"{chat_id}-{user_id}-{int(time.time())}"

        # ========== 1. 参数验证 ==========
        if not all([chat_id, user_id, checkin_type, shift, business_date]):
            logger.error(f"❌ [{trace_id}] _check_shift_work_record 缺少必要参数")
            return False

        # 获取班次配置
        shift_config = await db.get_shift_config(chat_id)

        # ========== 2. 获取业务日期（用于夜班判断） ==========
        from database import db as database_db

        # 🎯 仍然通过 get_business_date 获取实际业务日期，确保与班次判定一致
        actual_business_date = await database_db.get_business_date(
            chat_id=chat_id, current_dt=now, shift=shift, checkin_type=checkin_type
        )

        async with db.pool.acquire() as conn:
            # ========== 3. 方法1：使用业务日期查询（主要方法） ==========
            row = await conn.fetchrow(
                """
                SELECT 1 FROM work_records 
                WHERE chat_id = $1 
                  AND user_id = $2 
                  AND checkin_type = $3 
                  AND shift = $4
                  AND record_date = $5
                LIMIT 1
                """,
                chat_id,
                user_id,
                checkin_type,
                shift,
                actual_business_date,  # 使用动态计算的业务日期
            )

            if row:
                logger.debug(
                    f"✅ [{trace_id}] 找到打卡记录(业务日期): "
                    f"type={checkin_type}, shift={shift}, date={actual_business_date}"
                )
                return True

            # ========== 4. 方法2：使用传入的 business_date 查询（备用） ==========
            row = await conn.fetchrow(
                """
                SELECT 1 FROM work_records 
                WHERE chat_id = $1 
                  AND user_id = $2 
                  AND checkin_type = $3 
                  AND shift = $4
                  AND record_date = $5
                LIMIT 1
                """,
                chat_id,
                user_id,
                checkin_type,
                shift,
                business_date,  # 使用传入的业务日期
            )

            if row:
                logger.debug(
                    f"✅ [{trace_id}] 找到打卡记录(传入日期): "
                    f"type={checkin_type}, shift={shift}, date={business_date}"
                )
                return True

            # ========== 5. 夜班特殊处理：检查前一天（保留跨天逻辑） ==========
            if shift == "night":
                previous_date = business_date - timedelta(days=1)
                row = await conn.fetchrow(
                    """
                    SELECT 1 FROM work_records 
                    WHERE chat_id = $1 
                      AND user_id = $2 
                      AND checkin_type = $3 
                      AND shift = $4
                      AND record_date = $5
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                    checkin_type,
                    shift,
                    previous_date,
                )

                if row:
                    logger.debug(
                        f"🌙 [{trace_id}] 找到夜班记录(前一天): "
                        f"type={checkin_type}, date={previous_date}"
                    )
                    return True

                # 再检查前一天的实际业务日期
                actual_previous = actual_business_date - timedelta(days=1)
                row = await conn.fetchrow(
                    """
                    SELECT 1 FROM work_records 
                    WHERE chat_id = $1 
                      AND user_id = $2 
                      AND checkin_type = $3 
                      AND shift = $4
                      AND record_date = $5
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                    checkin_type,
                    shift,
                    actual_previous,
                )

                if row:
                    logger.debug(
                        f"🌙 [{trace_id}] 找到夜班记录(前一天实际日期): "
                        f"type={checkin_type}, date={actual_previous}"
                    )
                    return True

            # ========== 6. 方法3：时间窗口查询（最终保底） ==========
            # 获取窗口信息
            window_info = db.calculate_shift_window(
                shift_config=shift_config, checkin_type=checkin_type, now=now
            )

            night_window = window_info.get("night_window", {})

            # 根据当前时间判断用哪个窗口
            day_start = shift_config.get("day_start", "09:00")
            day_start_hour, day_start_min = map(int, day_start.split(":"))
            today_start = datetime.combine(
                now.date(), dt_time(day_start_hour, day_start_min)
            ).replace(tzinfo=now.tzinfo)

            if now < today_start:
                last_night = night_window.get("last_night", {})
                target_start = last_night.get(checkin_type, {}).get("start")
                target_end = last_night.get(checkin_type, {}).get("end")
            else:
                tonight = night_window.get("tonight", {})
                target_start = tonight.get(checkin_type, {}).get("start")
                target_end = tonight.get(checkin_type, {}).get("end")

            if target_start and target_end:
                # 时区处理
                if target_start.tzinfo is None:
                    target_start = target_start.replace(tzinfo=now.tzinfo)
                if target_end.tzinfo is None:
                    target_end = target_end.replace(tzinfo=now.tzinfo)

                # 统一使用无时区比较
                row = await conn.fetchrow(
                    """
                    SELECT 1 FROM work_records 
                    WHERE chat_id = $1 
                      AND user_id = $2 
                      AND checkin_type = $3 
                      AND shift = $4
                      AND created_at AT TIME ZONE 'Asia/Shanghai' >= $5::timestamp
                      AND created_at AT TIME ZONE 'Asia/Shanghai' <= $6::timestamp
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                    checkin_type,
                    shift,
                    target_start.replace(tzinfo=None),
                    target_end.replace(tzinfo=None),
                )
                if row:
                    logger.debug(
                        f"✅ [{trace_id}] 找到打卡记录(窗口查询): "
                        f"type={checkin_type}, shift={shift}"
                    )
                    return True

            logger.debug(
                f"❌ [{trace_id}] 未找到打卡记录: "
                f"type={checkin_type}, shift={shift}, date={business_date}"
            )
            return False

    except Exception as e:
        logger.error(f"❌ [{trace_id}] 检查班次打卡记录失败: {e}")
        logger.error(traceback.format_exc())
        return False


# ✅ 修正后的函数定义（添加 business_date 参数）
async def _get_existing_work_record(
    chat_id: int, user_id: int, checkin_type: str, shift: str, business_date: date
) -> Optional[Dict]:
    """
    获取已存在的打卡记录详情 - 简化版
    直接使用 business_date 查询，不再依赖复杂的时间窗口
    """
    try:
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT checkin_time, status, created_at, record_date 
                FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 
                AND checkin_type = $3 AND shift = $4
                AND record_date = $5
                ORDER BY created_at DESC
                LIMIT 1
                """,
                chat_id,
                user_id,
                checkin_type,
                shift,
                business_date,  # ✅ 直接使用传入的 business_date
            )

            if row:
                return {
                    "checkin_time": row["checkin_time"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "record_date": row["record_date"],
                }
            return None

    except Exception as e:
        logger.error(f"获取现有记录失败: {e}")
        return None


async def send_work_notification(
    chat_id: int,
    user_id: int,
    user_name: str,
    checkin_time: str,
    expected_dt: datetime,
    action_text: str,
    status_type: str,
    fine_amount: int,
    trace_id: str,
    shift: str = None,  # 🆕 新增 shift 参数
):

    try:
        # 获取群配置
        group_data = await db.get_group_cached(chat_id)
        channel_id = group_data.get("channel_id") if group_data else None
        extra_work_group_id = await db.get_extra_work_group(chat_id)

        # 获取推送设置
        push_settings = await db.get_push_settings()
        enable_group_push = push_settings.get(
            "enable_group_push", False
        )  # 默认发送到当前群组
        enable_channel_push = push_settings.get(
            "enable_channel_push", True
        )  # 默认发送到频道

        # 获取群信息
        chat_info = await bot.get_chat(chat_id)
        chat_title = getattr(chat_info, "title", str(chat_id))

        # ========= 修复：直接使用班次判定的 expected_dt，不要手动调整 =========
        checkin_hour, checkin_min = map(int, checkin_time.split(":"))
        checkin_dt = datetime.combine(
            expected_dt.date(),  # 使用班次判定确定的日期
            dt_time(checkin_hour, checkin_min),
        ).replace(tzinfo=expected_dt.tzinfo)

        # 直接计算时间差，不进行任何跨天调整
        diff_seconds = int((checkin_dt - expected_dt).total_seconds())

        logger.debug(
            f"[{trace_id}] 📊 时间差计算:\n"
            f"   • 期望时间: {expected_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   • 打卡时间: {checkin_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   • 时间差: {diff_seconds}秒"
        )

        # ========= 迟到/早退判定 ==========
        if action_text == "上班":
            if diff_seconds > 0:
                actual_status = "迟到"
                title = "⚠️ <b>上班迟到通知</b>"
                status_line = f"⏱️ 迟到 {MessageFormatter.format_duration(diff_seconds)}"
            elif diff_seconds < 0:
                actual_status = "早到"
                title = "✅ <b>上班通知</b>"
                status_line = (
                    f"⏱️ 早到 {MessageFormatter.format_duration(abs(diff_seconds))}"
                )
            else:
                actual_status = "准时"
                title = "✅ <b>上班通知</b>"
                status_line = "⏱️ 准时到达"
        else:  # 下班
            if diff_seconds < 0:
                actual_status = "早退"
                title = "⚠️ <b>下班早退通知</b>"
                status_line = (
                    f"⏱️ 早退 {MessageFormatter.format_duration(abs(diff_seconds))}"
                )
            elif diff_seconds > 0:
                actual_status = "加班"
                title = "✅ <b>下班通知</b>"
                status_line = f"⏱️ 加班 {MessageFormatter.format_duration(diff_seconds)}"
            else:
                actual_status = "准时"
                title = "✅ <b>下班准时通知</b>"
                status_line = "⏱️ 准时下班"

        # ========= 班次信息（修改点1）==========
        # 🆕 直接从参数获取班次信息
        if shift is None:
            # 如果没有传入 shift 参数，尝试从 expected_dt 推断
            hour = expected_dt.hour
            shift = "day" if 6 <= hour < 18 else "night"  # 简单的推断逻辑

        shift_text = "白班" if shift == "day" else "夜班"

        # ========= 频道文案（修改点2）==========
        channel_notif_text = (
            f"{title} <code>{shift_text}</code>\n"
            f"🏢 群组/班次：<code>{chat_title}</code> \n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}\n"
            f"⏰ 打卡时间：<code>{checkin_time}</code>\n"
            f"📅 {action_text}时间：<code>{expected_dt.strftime('%m/%d %H:%M')}</code>\n"
        )

        # 如果是下班通知，添加上班时间和工作时长
        if action_text == "下班":
            try:
                # 获取今天的上班记录
                work_records = await db.get_work_records_by_shift(
                    chat_id, user_id, shift_text
                )
                if work_records and work_records.get("work_start"):
                    # 获取上班时间
                    work_start_time = work_records["work_start"][0]["checkin_time"]

                    # 计算工作时长
                    start_dt = datetime.strptime(work_start_time, "%H:%M")
                    end_dt = datetime.strptime(checkin_time, "%H:%M")

                    # 处理跨天（如果下班时间小于上班时间，说明跨天了）
                    if end_dt < start_dt:
                        end_dt += timedelta(days=1)

                    work_duration = int((end_dt - start_dt).total_seconds())
                    work_duration_str = MessageFormatter.format_duration(work_duration)

                    # 获取今日活动总时长
                    business_date = await db.get_business_date(chat_id)
                    async with db.pool.acquire() as conn:
                        activity_total = (
                            await conn.fetchval(
                                """
                            SELECT SUM(accumulated_time) 
                            FROM user_activities 
                            WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                            """,
                                chat_id,
                                user_id,
                                business_date,
                            )
                            or 0
                        )

                    # 计算实际工作时间（总工时 - 活动时间）
                    actual_work_duration = max(0, work_duration - activity_total)
                    actual_work_str = MessageFormatter.format_duration(
                        actual_work_duration
                    )
                    activity_total_str = MessageFormatter.format_duration(
                        activity_total
                    )

                    channel_notif_text += (
                        f"🕒 上班时间：<code>{work_start_time}</code>\n"
                    )
                    channel_notif_text += (
                        f"⏱️ 总工作时长：<code>{work_duration_str}</code>\n"
                    )
                    channel_notif_text += (
                        f"📊 活动总时长：<code>{activity_total_str}</code>\n"
                    )
                    channel_notif_text += (
                        f"💪 实际工作时间：<code>{actual_work_str}</code>\n"
                    )
            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 计算工作时长失败: {e}")

        channel_notif_text += f"{status_line}"

        # ========= 额外群组文案 ==========
        extra_notif_text = f"<code>{shift_text}</code> {MessageFormatter.format_user_link(user_id, user_name)} {action_text} 了！\n"

        if fine_amount > 0:
            extra_notif_text += f"\n💰 扣除绩效：<code>{fine_amount}</code> 分"

        # ========= 调试日志 ==========
        logger.info(
            f"[{trace_id}] 📊 通知详情:\n"
            f"   • 用户: {user_name}({user_id})\n"
            f"   • 动作: {action_text}\n"
            f"   • 状态: {actual_status}\n"
            f"   • 打卡时间: {checkin_time}\n"
            f"   • 期望时间: {expected_dt.strftime('%m/%d %H:%M')}\n"
            f"   • 时间差: {diff_seconds}秒 ({MessageFormatter.format_duration(abs(diff_seconds))})\n"
            f"   • 罚款: {fine_amount}\n"
            f"   • 班次: {shift_text}"
        )

        # ========= 发送逻辑 ==========
        async def safe_send(target_id: int, text: str, target_desc: str = ""):
            """安全发送消息"""
            try:
                logger.info(f"[{trace_id}] 📤 尝试发送到 {target_desc} ID: {target_id}")

                try:
                    target_info = await bot.get_chat(target_id)
                    logger.info(
                        f"[{trace_id}] ℹ️ 目标群组信息: 标题='{target_info.title}', 类型={target_info.type}"
                    )
                except Exception as e:
                    logger.error(
                        f"[{trace_id}] ❌ 无法获取目标群组信息，机器人可能不在群组中: {e}"
                    )
                    return

                await bot.send_message(target_id, text, parse_mode="HTML")

                if target_desc:
                    logger.info(f"[{trace_id}] ✅ {target_desc}发送成功({target_id})")
                else:
                    logger.info(f"[{trace_id}] ✅ 发送成功({target_id})")

            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 发送到 {target_desc} 失败: {e}")

                try:
                    logger.info(f"[{trace_id}] 🔄 尝试使用 bot_manager 重试...")
                    if bot_manager and hasattr(bot_manager, "send_message_with_retry"):
                        success = await bot_manager.send_message_with_retry(
                            target_id, text, parse_mode="HTML"
                        )
                        if success:
                            logger.info(
                                f"[{trace_id}] ✅ bot_manager {target_desc}发送成功({target_id})"
                            )
                            return
                except Exception as e2:
                    logger.error(f"[{trace_id}] ❌ bot_manager 重试也失败: {e2}")

        # 发送到频道
        if channel_id and enable_channel_push:
            await safe_send(channel_id, channel_notif_text, "频道")
        elif channel_id:
            logger.info(f"[{trace_id}] ℹ️ 推送设置已禁用频道通知")

        # 发送到额外群组
        if extra_work_group_id:
            logger.info(f"[{trace_id}] 📤 发送到额外群组: {extra_work_group_id}")
            await safe_send(extra_work_group_id, extra_notif_text, "额外上下班群组")
        else:
            logger.info(f"[{trace_id}] ℹ️ 没有配置额外群组")

    except Exception as e:
        logger.error(
            f"[{trace_id}] ❌ send_work_notification总异常: {e}", exc_info=True
        )


# ========== 管理员装饰器 ==========
def admin_required(func):
    """管理员权限检查装饰器"""

    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if not await is_admin(message.from_user.id):
            await message.answer(
                Config.MESSAGES["no_permission"],
                reply_markup=await get_main_keyboard(
                    message.chat.id, await is_admin(message.from_user.id)
                ),
            )
            return
        return await func(message, *args, **kwargs)

    return wrapper


# ========== 消息处理器 ==========
@rate_limit(rate=5, per=60)
@message_deduplicate
async def cmd_start(message: types.Message):
    """开始命令"""
    uid = message.from_user.id
    is_admin_user = await is_admin(uid)

    await message.answer(
        Config.MESSAGES["welcome"],
        reply_markup=await get_main_keyboard(message.chat.id, is_admin_user),
        reply_to_message_id=message.message_id,
    )


@rate_limit(rate=5, per=60)
async def cmd_menu(message: types.Message):
    """显示主菜单"""
    uid = message.from_user.id
    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
    )


@rate_limit(rate=5, per=60)
async def cmd_help(message: types.Message):
    """帮助命令"""
    uid = message.from_user.id

    help_text = (
        "📋 打卡机器人使用帮助\n\n"
        "🟢 开始活动打卡：\n"
        "• 直接输入活动名称\n"
        "• 或使用命令：/ci 活动名\n"
        "• 或点击下方活动按钮\n\n"
        "🔴 结束活动回座：\n"
        "• 直接输入：回座\n"
        "• 或使用命令：/at\n"
        "• 或点击下方 ✅ 回座 按钮\n\n"
        "🕒 上下班打卡：\n"
        "• /workstart - 上班打卡\n"
        "• /workend - 下班打卡\n"
        "• 或点击 🟢 上班 和 🔴 下班 按钮\n\n"
        "📊 查看记录：\n"
        "• 点击 📊 我的记录 查看个人统计\n"
        "• 点击 🏆 排行榜 查看群内排名\n\n"
        "🔧 其他命令：\n"
        "• /start - 开始使用机器人\n"
        "• /menu - 显示主菜单\n"
        "• /help - 显示此帮助信息"
    )

    await message.answer(
        help_text,
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


# 🆕 ========== 新增：我的记录和排行榜命令 ==========
@rate_limit(rate=10, per=60)
@track_performance("cmd_myinfo")
async def handle_myinfo_command(message: types.Message):
    """处理 /myinfo 命令 - 显示我的记录"""
    chat_id = message.chat.id
    uid = message.from_user.id

    # 检查是否有班次参数
    args = message.text.split()
    if len(args) == 2:
        # 有班次参数，交给专门的函数处理
        await handle_myinfo_shift_command(message)
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)  # 不传shift参数，显示全部


@rate_limit(rate=10, per=60)
@track_performance("cmd_myinfo_shift")
async def handle_myinfo_shift_command(message: types.Message):
    """处理 /myinfo <shift> 命令 - 按班次查看我的记录"""
    args = message.text.split()
    chat_id = message.chat.id
    uid = message.from_user.id

    if len(args) != 2:
        await message.answer(
            "❌ 用法：/myinfo <shift>\n" "💡 参数：day (白班) 或 night (夜班)",
            reply_to_message_id=message.message_id,
        )
        return

    shift = args[1].lower()
    if shift not in ["day", "night"]:
        await message.answer(
            "❌ 班次参数错误\n" "💡 请使用：day (白班) 或 night (夜班)",
            reply_to_message_id=message.message_id,
        )
        return

    # 检查双班模式是否启用
    shift_config = await db.get_shift_config(chat_id)
    if not shift_config.get("dual_mode", False):
        await message.answer(
            "❌ 当前群组未启用双班模式\n"
            "💡 请联系管理员使用 /setdualmode 命令开启双班模式",
            reply_to_message_id=message.message_id,
        )
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message, shift)  # 传递班次参数


@rate_limit(rate=10, per=60)
@track_performance("cmd_myinfo_day")
async def handle_myinfo_day_command(message: types.Message):
    """处理 /myinfoday 命令 - 查看白班记录"""
    chat_id = message.chat.id
    uid = message.from_user.id

    # 检查双班模式是否启用
    shift_config = await db.get_shift_config(chat_id)
    if not shift_config.get("dual_mode", False):
        await message.answer(
            "❌ 当前群组未启用双班模式\n"
            "💡 请联系管理员使用 /setdualmode 命令开启双班模式",
            reply_to_message_id=message.message_id,
        )
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message, "day")  # 直接传入 "day"


@rate_limit(rate=10, per=60)
@track_performance("cmd_myinfo_night")
async def handle_myinfo_night_command(message: types.Message):
    """处理 /myinfonight 命令 - 查看夜班记录"""
    chat_id = message.chat.id
    uid = message.from_user.id

    # 检查双班模式是否启用
    shift_config = await db.get_shift_config(chat_id)
    if not shift_config.get("dual_mode", False):
        await message.answer(
            "❌ 当前群组未启用双班模式\n"
            "💡 请联系管理员使用 /setdualmode 命令开启双班模式",
            reply_to_message_id=message.message_id,
        )
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message, "night")  # 直接传入 "night"


@rate_limit(rate=10, per=60)
@track_performance("cmd_ranking")
async def handle_ranking_command(message: types.Message):
    """处理 /ranking 命令 - 显示排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id

    # 检查是否有班次参数
    args = message.text.split()
    if len(args) == 2:
        # 有班次参数，交给专门的函数处理
        await handle_ranking_shift_command(message)
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)  # 不传shift参数，显示全部


@rate_limit(rate=10, per=60)
@track_performance("cmd_ranking_shift")
async def handle_ranking_shift_command(message: types.Message):
    """处理 /ranking <shift> 命令 - 按班次查看排行榜"""
    args = message.text.split()
    chat_id = message.chat.id
    uid = message.from_user.id

    if len(args) != 2:
        await message.answer(
            "❌ 用法：/ranking <shift>\n" "💡 参数：day (白班) 或 night (夜班)",
            reply_to_message_id=message.message_id,
        )
        return

    shift = args[1].lower()
    if shift not in ["day", "night"]:
        await message.answer(
            "❌ 班次参数错误\n" "💡 请使用：day (白班) 或 night (夜班)",
            reply_to_message_id=message.message_id,
        )
        return

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message, shift)


@rate_limit(rate=10, per=60)
@track_performance("cmd_ranking_day")
async def handle_ranking_day_command(message: types.Message):
    """处理 /rankingday 命令 - 查看白班排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message, "day")  # 直接传入 "day"


@rate_limit(rate=10, per=60)
@track_performance("cmd_ranking_night")
async def handle_ranking_night_command(message: types.Message):
    """处理 /rankingnight 命令 - 查看夜班排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message, "night")  # 直接传入 "night"


@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_ci", max_retries=2)
@track_performance("cmd_ci")
async def cmd_ci(message: types.Message):
    """指令打卡"""
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/ci <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    act = args[1].strip()

    activity_aliases = {
        "抽烟": "抽烟或休息",
        "休息": "抽烟或休息",
        "smoke": "抽烟或休息",
        "吸烟": "抽烟或休息",
    }
    if act in activity_aliases:
        act = activity_aliases[act]

    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        return

    await start_activity(message, act)


@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_at", max_retries=2)
@track_performance("cmd_at")
async def cmd_at(message: types.Message):
    """指令回座"""
    await process_back(message)


@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_start", max_retries=2)
@track_performance("work_start")
async def cmd_workstart(message: types.Message):
    """上班打卡"""
    await process_work_checkin(message, "work_start")


@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_end", max_retries=2)
@track_performance("work_end")
async def cmd_workend(message: types.Message):
    """下班打卡"""
    await process_work_checkin(message, "work_end")


# ========== 管理员命令 ==========
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_admin(message: types.Message):
    """管理员命令"""
    await message.answer(
        "👑 管理员面板",
        reply_markup=get_admin_keyboard(),
        reply_to_message_id=message.message_id,
    )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setdualmode(message: types.Message):
    """设置双班模式 - 保留今天状态"""

    args = message.text.split()
    chat_id = message.chat.id

    if len(args) < 2:
        await message.answer(
            "❌ 用法：\n"
            "• 开启双班: /setdualmode on <白班开始时间> <白班结束时间>\n"
            "• 关闭双班: /setdualmode off\n\n"
            "💡 示例:\n"
            "/setdualmode on 09:00 21:00\n"
            "/setdualmode off",
            reply_to_message_id=message.message_id,
        )
        return

    mode = args[1].lower()

    try:
        # ✅ 获取业务日期
        business_date = await db.get_business_date(chat_id)

        # ============================
        # 开启双班模式
        # ============================
        if mode == "on":
            if len(args) != 4:
                await message.answer(
                    "❌ 开启双班模式需要指定白班时间\n"
                    "📝 示例: /setdualmode on 09:00 21:00",
                    reply_to_message_id=message.message_id,
                )
                return

            day_start = args[2]
            day_end = args[3]

            import re

            time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")

            if not time_pattern.match(day_start) or not time_pattern.match(day_end):
                await message.answer(
                    "❌ 时间格式错误，请使用 HH:MM 格式",
                    reply_to_message_id=message.message_id,
                )
                return

            async with db.pool.acquire() as conn:
                async with conn.transaction():
                    # ✅ 只删除历史状态（< 今天）
                    delete_result = await conn.execute(
                        """
                        DELETE FROM group_shift_state
                        WHERE chat_id = $1
                        AND record_date < $2
                        """,
                        chat_id,
                        business_date,
                    )
                    deleted_count = _parse_delete_count(delete_result)

                    # 获取今天活跃的班次状态
                    active_count = (
                        await conn.fetchval(
                            """
                        SELECT COUNT(*)
                        FROM group_shift_state
                        WHERE chat_id = $1
                        AND record_date = $2
                        """,
                            chat_id,
                            business_date,
                        )
                        or 0
                    )

                    # 更新群组配置
                    await db.update_group_dual_mode(chat_id, True, day_start, day_end)

            # ✅ 清理历史缓存
            if deleted_count > 0:
                business_date_str = str(business_date)
                keys_to_remove = []

                for key in list(db._cache.keys()):
                    if not key.startswith(f"shift_state:{chat_id}:"):
                        continue

                    # key结构：shift_state:chat_id:user_id:shift
                    # 需要从对应的值获取 record_date
                    cache_key = key
                    if cache_key in db._cache_ttl:
                        keys_to_remove.append(cache_key)

                for key in keys_to_remove:
                    db._cache.pop(key, None)
                    db._cache_ttl.pop(key, None)

                logger.info(f"✅ 已清理 {len(keys_to_remove)} 个历史缓存")

            await message.answer(
                f"✅ 双班模式已开启\n\n"
                f"📊 配置信息:\n"
                f"• 白班时间: <code>{day_start} - {day_end}</code>\n"
                f"• 夜班时间: 自动推算\n\n"
                f"📈 状态清理:\n"
                f"• 清除历史状态: <code>{deleted_count}</code> 个\n"
                f"• 保留今天状态: <code>{active_count}</code> 个\n\n"
                f"💡 注意事项:\n"
                f"• 一个账号可支持两人轮班\n"
                f"• 上班行为创建班次状态\n"
                f"• 下班行为结束当前班次",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )

        # ============================
        # 关闭双班模式
        # ============================
        elif mode == "off":
            async with db.pool.acquire() as conn:
                async with conn.transaction():
                    # ✅ 只删除历史状态
                    delete_result = await conn.execute(
                        """
                        DELETE FROM group_shift_state
                        WHERE chat_id = $1
                        AND record_date < $2
                        """,
                        chat_id,
                        business_date,
                    )
                    deleted_count = _parse_delete_count(delete_result)

                    # 今天活跃的状态会被保留（因为要提醒用户）
                    active_count = (
                        await conn.fetchval(
                            """
                        SELECT COUNT(*)
                        FROM group_shift_state
                        WHERE chat_id = $1
                        AND record_date = $2
                        """,
                            chat_id,
                            business_date,
                        )
                        or 0
                    )

                    # 更新群组配置（关闭双班模式）
                    await db.update_group_dual_mode(chat_id, False, None, None)

            # ✅ 清理历史缓存
            if deleted_count > 0:
                business_date_str = str(business_date)
                keys_to_remove = []

                for key in list(db._cache.keys()):
                    if not key.startswith(f"shift_state:{chat_id}:"):
                        continue

                    cache_key = key
                    if cache_key in db._cache_ttl:
                        keys_to_remove.append(cache_key)

                for key in keys_to_remove:
                    db._cache.pop(key, None)
                    db._cache_ttl.pop(key, None)

                logger.info(f"✅ 已清理 {len(keys_to_remove)} 个历史缓存")

            # 如果有今天的活跃状态，需要提醒用户
            if active_count > 0:
                await message.answer(
                    f"✅ 双班模式已关闭\n\n"
                    f"📈 状态清理:\n"
                    f"• 清除历史状态: <code>{deleted_count}</code> 个\n"
                    f"• <b>⚠️ 发现 {active_count} 个今天的活跃班次</b>\n"
                    f"• 这些班次会被保留，但切换到单班模式后可能需要手动结束\n\n"
                    f"💡 建议用户手动结束今天的班次，或等待系统自动清理",
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )
            else:
                await message.answer(
                    f"✅ 双班模式已关闭\n\n"
                    f"📈 状态清理:\n"
                    f"• 清除历史状态: <code>{deleted_count}</code> 个\n"
                    f"• 没有今天的活跃状态",
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )

        else:
            await message.answer(
                "❌ 参数错误，请使用 'on' 或 'off'",
                reply_to_message_id=message.message_id,
            )

    except Exception as e:
        logger.exception(f"设置双班模式失败: {e}")
        await message.answer(
            f"❌ 设置失败: {str(e)[:200]}",
            reply_to_message_id=message.message_id,
        )


def _parse_delete_count(result: str) -> int:
    """解析 DELETE 语句返回的行数"""
    if not result or not isinstance(result, str):
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0] == "DELETE":
            return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 0


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setshiftgrace(message: types.Message):
    """设置时间宽容窗口"""
    args = message.text.split()
    chat_id = message.chat.id

    if len(args) != 3:
        await message.answer(
            "❌ 用法: /setshiftgrace <上班前允许分钟> <下班后允许分钟>\n"
            "💡 示例: /setshiftgrace 120 360\n\n"
            "📊 默认值:\n"
            "• 上班前: 120 分钟 (2小时)\n"
            "• 下班后: 360 分钟 (6小时)",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        grace_before = int(args[1])
        grace_after = int(args[2])

        if grace_before < 0 or grace_after < 0:
            await message.answer(
                "❌ 时间窗口不能为负数", reply_to_message_id=message.message_id
            )
            return

        await db.update_shift_grace_window(chat_id, grace_before, grace_after)

        await message.answer(
            f"✅ 时间宽容窗口已更新\n\n"
            f"📊 新设置:\n"
            f"• 上班前允许: <code>{grace_before}</code> 分钟\n"
            f"• 下班后允许: <code>{grace_after}</code> 分钟\n\n"
            f"💡 此设置影响双班模式下的打卡时间判定",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字", reply_to_message_id=message.message_id
        )
    except Exception as e:
        logger.error(f"设置时间窗口失败: {e}")
        await message.answer(
            f"❌ 设置失败: {e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworkendgrace(message: types.Message):
    """设置下班专用时间窗口"""
    args = message.text.split()
    chat_id = message.chat.id

    if len(args) != 3:
        await message.answer(
            "❌ 用法: /setworkendgrace <下班前允许分钟> <下班后允许分钟>\n"
            "💡 示例: /setworkendgrace 120 360\n\n"
            "📊 默认值:\n"
            "• 下班前: 120 分钟 (2小时)\n"
            "• 下班后: 360 分钟 (6小时)",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        before = int(args[1])
        after = int(args[2])

        if before < 0 or after < 0:
            await message.answer(
                "❌ 时间窗口不能为负数", reply_to_message_id=message.message_id
            )
            return

        # 更新数据库
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups 
                SET workend_grace_before = $1, 
                    workend_grace_after = $2,
                    updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $3
                """,
                before,
                after,
                chat_id,
            )
            db._cache.pop(f"group:{chat_id}", None)

        await message.answer(
            f"✅ 下班时间窗口已更新\n\n"
            f"📊 新设置:\n"
            f"• 下班前允许: <code>{before}</code> 分钟\n"
            f"• 下班后允许: <code>{after}</code> 分钟",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字", reply_to_message_id=message.message_id
        )
    except Exception as e:
        logger.error(f"设置下班时间窗口失败: {e}")
        await message.answer(
            f"❌ 设置失败: {e}", reply_to_message_id=message.message_id
        )


# ========== 修复消息引用 ==========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_fix_message_refs(message: types.Message):
    """修复消息引用（清除所有消息ID）"""
    chat_id = message.chat.id

    try:
        await message.answer("⏳ 正在清除所有消息引用记录...")

        # 使用数据库的 execute_with_retry 方法
        result = await db.execute_with_retry(
            "修复消息引用",
            """
            UPDATE users 
            SET checkin_message_id = NULL, updated_at = CURRENT_TIMESTAMP 
            WHERE chat_id = $1 AND checkin_message_id IS NOT NULL
            """,
            chat_id,
        )

        # 解析受影响的行数
        updated_count = 0
        if result and result.startswith("UPDATE"):
            parts = result.split()
            if len(parts) >= 2:
                updated_count = int(parts[-1])

        await message.answer(
            f"✅ 已清除 {updated_count} 个消息引用记录\n"
            f"💡 下次打卡将重新建立正确的消息引用",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
        )
        logger.info(
            f"管理员 {message.from_user.id} 清除了群组 {chat_id} 的 {updated_count} 个消息引用"
        )

    except Exception as e:
        logger.error(f"修复消息引用失败: {e}")
        await message.answer(
            f"❌ 修复失败：{str(e)[:200]}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )


# ========== 月度数据清理命令 ==========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_cleanup_monthly(message: types.Message):
    """清理月度统计数据"""
    args = message.text.split()

    target_date = None
    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
            target_date = date(year, month, 1)
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return
    elif len(args) == 2 and args[1].lower() == "all":
        # 特殊命令：清理所有月度数据（谨慎使用）
        await message.answer(
            "⚠️ <b>危险操作确认</b>\n\n"
            "您即将删除<u>所有</u>月度统计数据！\n"
            "此操作不可恢复！\n\n"
            "请输入 <code>/cleanup_monthly confirm_all</code> 确认执行",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
        return
    elif len(args) == 2 and args[1].lower() == "confirm_all":
        # 确认清理所有数据
        try:
            async with db.pool.acquire() as conn:
                result = await conn.execute("DELETE FROM monthly_statistics")
                deleted_count = (
                    int(result.split()[-1])
                    if result and result.startswith("DELETE")
                    else 0
                )

            await message.answer(
                f"🗑️ <b>已清理所有月度统计数据</b>\n"
                f"删除记录: <code>{deleted_count}</code> 条\n\n"
                f"⚠️ 所有月度统计已被清空，月度报告将无法生成历史数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.warning(f"👑 管理员 {message.from_user.id} 清理了所有月度统计数据")
            return
        except Exception as e:
            await message.answer(
                f"❌ 清理所有数据失败: {e}", reply_to_message_id=message.message_id
            )
            return

    await message.answer(
        "⏳ 正在清理月度统计数据...", reply_to_message_id=message.message_id
    )

    try:
        if target_date:
            # 清理指定月份
            deleted_count = await db.cleanup_specific_month(
                target_date.year, target_date.month
            )
            date_str = target_date.strftime("%Y年%m月")
            await message.answer(
                f"✅ <b>月度统计清理完成</b>\n"
                f"📅 清理月份: <code>{date_str}</code>\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
        else:
            # 默认清理3个月前的数据
            deleted_count = await db.cleanup_monthly_data()
            today = get_beijing_time()
            cutoff_date = (today - timedelta(days=90)).date().replace(day=1)
            cutoff_str = cutoff_date.strftime("%Y年%m月")

            await message.answer(
                f"✅ <b>月度统计自动清理完成</b>\n"
                f"📅 清理截止: <code>{cutoff_str}</code> 之前\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条\n\n"
                f"💡 保留了最近3个月的月度统计数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )

    except Exception as e:
        logger.error(f"❌ 清理月度数据失败: {e}")
        await message.answer(
            f"❌ 清理月度数据失败: {e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_monthly_stats_status(message: types.Message):
    """查看月度统计数据状态 - 高精度版"""
    chat_id = message.chat.id

    try:
        async with db.pool.acquire() as conn:
            # 查询每个月的总记录数、活跃用户数和活动类型数
            monthly_rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', statistic_date) AS month,
                    COUNT(*) AS total_records,
                    COUNT(DISTINCT user_id) AS monthly_users,
                    COUNT(DISTINCT activity_name) AS monthly_activities
                FROM monthly_statistics
                WHERE chat_id = $1
                GROUP BY month
                ORDER BY month DESC
                """,
                chat_id,
            )

            # 总计信息
            total_records = await conn.fetchval(
                "SELECT COUNT(*) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )
            total_users = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )
            total_activities = await conn.fetchval(
                "SELECT COUNT(DISTINCT activity_name) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

        if not monthly_rows:
            await message.answer(
                "📊 <b>月度统计数据状态</b>\n\n暂无月度统计数据",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return

        earliest = min(row["month"] for row in monthly_rows)
        latest = max(row["month"] for row in monthly_rows)

        status_text = (
            f"📊 <b>月度统计数据状态</b>\n\n"
            f"📅 数据范围: <code>{earliest.strftime('%Y年%m月')}</code> - <code>{latest.strftime('%Y年%m月')}</code>\n"
            f"👥 总用户数: <code>{total_users}</code> 人\n"
            f"📝 活动类型总数: <code>{total_activities}</code> 种\n"
            f"💾 总记录数: <code>{total_records}</code> 条\n\n"
            f"<b>最近12个月数据量:</b>\n"
        )

        for row in monthly_rows[:12]:
            month_str = row["month"].strftime("%Y年%m月")
            total = row["total_records"]
            users = row["monthly_users"]
            acts = row["monthly_activities"]
            status_text += f"• {month_str}: <code>{total}</code> 条, 用户 <code>{users}</code> 人, 活动类型 <code>{acts}</code> 种\n"

        if len(monthly_rows) > 12:
            status_text += f"• ... 还有 {len(monthly_rows) - 12} 个月份\n"

        status_text += (
            "\n💡 <b>可用命令:</b>\n"
            "• <code>/cleanup_monthly</code> - 自动清理（保留最近3个月）\n"
            "• <code>/cleanup_monthly 年 月</code> - 清理指定月份\n"
            "• <code>/cleanup_monthly all</code> - 清理所有数据（危险）"
        )

        await message.answer(
            status_text,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

    except Exception as e:
        logger.error(f"❌ 查看月度统计状态失败: {e}")
        await message.answer(
            "❌ 查看月度统计状态失败，请稍后重试",
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=1, per=60)
async def cmd_cleanup_inactive(message: types.Message):
    """清理长期未活动的用户数据"""
    args = message.text.split()
    days = 30  # 默认30天未活动

    # 用户指定天数
    if len(args) > 1:
        try:
            days = int(args[1])
            if days < 7:
                await message.answer(
                    "❌ 天数不能少于7天，避免误删活跃用户",
                    reply_to_message_id=message.message_id,
                )
                return
        except ValueError:
            await message.answer(
                "❌ 天数必须是数字，例如：/cleanup_inactive 60",
                reply_to_message_id=message.message_id,
            )
            return

    await message.answer(
        f"⏳ 正在清理 {days} 天未活动的用户，请稍候...",
        reply_to_message_id=message.message_id,
    )

    cutoff_date = (get_beijing_time() - timedelta(days=days)).date()

    try:
        async with db.pool.acquire() as conn:
            # 删除用户
            result_users = await conn.execute(
                "DELETE FROM users WHERE last_updated < $1", cutoff_date
            )
            deleted_users = (
                int(result_users.split()[-1])
                if result_users.startswith("DELETE")
                else 0
            )

            # 删除活动记录
            result_activities = await conn.execute(
                "DELETE FROM user_activities WHERE activity_date < $1", cutoff_date
            )
            deleted_activities = (
                int(result_activities.split()[-1])
                if result_activities.startswith("DELETE")
                else 0
            )

            # 删除工作记录
            result_work = await conn.execute(
                "DELETE FROM work_records WHERE record_date < $1", cutoff_date
            )
            deleted_work_records = (
                int(result_work.split()[-1]) if result_work.startswith("DELETE") else 0
            )

        total_deleted = deleted_users + deleted_activities + deleted_work_records

        await message.answer(
            f"🧹 <b>长期未活动用户清理完成</b>\n\n"
            f"📅 清理截止: <code>{cutoff_date}</code> 之前\n"
            f"🗑️ 删除用户: <code>{deleted_users}</code> 个\n"
            f"🗑️ 删除活动记录: <code>{deleted_activities}</code> 条\n"
            f"🗑️ 删除工作记录: <code>{deleted_work_records}</code> 条\n\n"
            f"📊 总计删除: <code>{total_deleted}</code> 条记录\n"
            f"⚠️ 此操作不可撤销",
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(
            f"👑 管理员 {message.from_user.id} 清理 {days} 天未活动用户: "
            f"{deleted_users} 用户, {deleted_activities} 活动, {deleted_work_records} 工作记录"
        )

    except Exception as e:
        logger.exception("❌ 清理未活动用户失败")
        await message.answer(
            f"❌ 清理未活动用户失败: {e}", reply_to_message_id=message.message_id
        )


# ========== 重置用户命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_reset_user(message: types.Message):
    """重置指定用户的今日数据"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "❌ 用法：/resetuser <用户ID> [confirm]\n"
            "💡 示例：/resetuser 123456789 confirm",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        chat_id = message.chat.id
        target_user_id = int(args[1])
        confirm = len(args) == 3 and args[2].lower() == "confirm"

        if not confirm:
            await message.answer(
                f"⚠️ 确认重置用户 <code>{target_user_id}</code> 的今日数据？\n"
                f"请输入 <code>/resetuser {target_user_id} confirm</code> 执行",
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            return

        await message.answer(
            f"⏳ 正在重置用户 {target_user_id} 的今日数据...",
            reply_to_message_id=message.message_id,
        )

        # 执行重置
        success = await db.reset_user_daily_data(chat_id, target_user_id)

        if success:
            await message.answer(
                f"✅ 已重置用户 <code>{target_user_id}</code> 的今日数据\n\n"
                f"🗑️ 已清除：今日活动记录 | 今日统计计数 | 当前活动状态 | 罚款计数（保留总罚款）",
                parse_mode="HTML",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            logger.info(
                f"👑 管理员 {message.from_user.id} 在群 {chat_id} 重置了用户 {target_user_id} 的今日数据"
            )
        else:
            await message.answer(
                f"❌ 重置用户 {target_user_id} 数据失败",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )

    except ValueError:
        await message.answer(
            "❌ 用户ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.exception(f"重置用户数据失败")
        await message.answer(
            f"❌ 重置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


# ========== 设置软重置时间命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setsoftresettime(message: types.Message):
    """设置软重置时间 - 二次重置"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/setsoftresettime <小时> <分钟>\n"
            "📝 示例：/setsoftresettime 12 0 (中午12点)\n"
            "💡 软重置特点：\n"
            "• 只重置打卡次数和'我的记录'显示\n"
            "• 不影响每日数据导出和月度统计\n"
            "• 设为 0 0 可禁用软重置",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])
        keyboard = await get_main_keyboard(chat_id=message.chat.id, show_admin=True)

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！\n"
                "💡 示例：/setsoftresettime 12 0 (中午12点软重置)\n"
                "      /setsoftresettime 0 0 (禁用软重置)",
                reply_markup=keyboard,
                reply_to_message_id=message.message_id,
            )
            return

        chat_id = message.chat.id
        await db.init_group(chat_id)
        await db.update_group_soft_reset_time(chat_id, hour, minute)

        if hour == 0 and minute == 0:
            await message.answer(
                "✅ 软重置功能已禁用\n\n" "💡 软重置功能已关闭，不会再执行二次重置",
                reply_markup=keyboard,
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"软重置功能已禁用: 群组 {chat_id}")
        else:
            await message.answer(
                f"✅ 软重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>\n\n"
                f"💡 软重置特点：\n"
                f"• 每天此时会重置打卡次数和'我的记录'显示\n"
                f"• 不影响每日数据导出和月度统计\n"
                f"• 用户可以重新开始打卡，但历史数据已保存",
                reply_markup=keyboard,
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(
                f"软重置时间设置成功: 群组 {chat_id} -> {hour:02d}:{minute:02d}"
            )

    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！\n" "💡 示例：/setsoftresettime 12 0 (中午12点)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置软重置时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_softresettime(message: types.Message):
    """查看当前软重置时间"""
    chat_id = message.chat.id
    try:
        keyboard = await get_main_keyboard(chat_id=chat_id, show_admin=True)
        soft_reset_hour, soft_reset_minute = await db.get_group_soft_reset_time(chat_id)

        if soft_reset_hour == 0 and soft_reset_minute == 0:
            status_text = "🔴 未启用"
        else:
            status_text = (
                f"🟢 <code>{soft_reset_hour:02d}:{soft_reset_minute:02d}</code>"
            )

        await message.answer(
            f"⏰ 当前重置时间设置\n\n"
            f"🔄 <b>硬重置（日常重置）</b>\n"
            f"• 重置所有数据（活动、上下班、记录、排行榜）\n"
            f"• 时间：根据 /setresettime 设置\n\n"
            f"🔄 <b>软重置（二次重置）</b>\n"
            f"• 只重置打卡次数和'我的记录'显示\n"
            f"• 不影响数据导出和月度统计\n"
            f"• 状态：{status_text}\n\n"
            f"💡 管理命令：\n"
            f"• /setresettime <小时> <分钟> - 设置硬重置时间\n"
            f"• /setsoftresettime <小时> <分钟> - 设置软重置时间\n"
            f"• /setsoftresettime 0 0 - 禁用软重置",
            reply_markup=keyboard,
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"查看软重置时间失败: {e}")
        await message.answer(
            f"❌ 获取重置时间失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 导出每日数据命令 ==========
@admin_required
@rate_limit(rate=2, per=60)
@track_performance("cmd_export")
async def cmd_export(message: types.Message):
    """导出数据"""
    chat_id = message.chat.id
    await message.answer(
        "⏳ 正在导出数据，请稍候...", reply_to_message_id=message.message_id
    )
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送！", reply_to_message_id=message.message_id
        )
    except Exception as e:
        await message.answer(
            f"❌ 导出失败：{e}", reply_to_message_id=message.message_id
        )


# ========== 月度报告函数 ==========
@track_performance("optimized_monthly_export")
async def optimized_monthly_export(chat_id: int, year: int, month: int):
    """
    高性能月度数据导出 - 优化版
    - 批量处理减少循环操作
    - 预编译活动列表
    - 缓存常用数据
    - 自动补全缺失字段
    """
    try:
        # ===== 1. 预获取活动配置（缓存）=====
        activity_limits = await db.get_activity_limits_cached()
        activity_names = sorted(activity_limits.keys())

        # ===== 2. 批量获取月度统计 =====
        monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
        if not monthly_stats:
            logger.warning(f"月度统计表中没有找到 {year}年{month}月 的数据")
            return None

        # ===== 3. 预计算班次显示映射 =====
        shift_display_map = {
            "day": "白班",
            "night": "夜班",
            "night_last": "夜班",
            "night_tonight": "夜班",
        }

        # ===== 4. 预创建CSV写入器（带缓冲区优化）=====
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer, lineterminator="\n")

        # ===== 5. 构建表头（一次性）=====
        headers = ["用户ID", "用户昵称", "班次"]
        for act in activity_names:
            headers.extend([f"{act}次数", f"{act}总时长"])

        headers.extend(
            [
                "活动次数总计",
                "活动用时总计",
                "罚款总分",
                "超时次数",
                "总超时时间",
                "工作天数",
                "工作时长",
                "上班次数",
                "下班次数",
                "上班罚款",
                "下班罚款",
                "迟到次数",
                "早退次数",
            ]
        )
        writer.writerow(headers)

        # ===== 6. 批量处理数据 =====
        format_time = db.format_time_for_csv
        rows = []

        for user_stat in monthly_stats:
            if not isinstance(user_stat, dict):
                continue

            # 🆕 确保所有必要字段都存在
            required_fields = [
                "work_start_count",
                "work_end_count",
                "work_start_fines",
                "work_end_fines",
                "late_count",
                "early_count",
                "work_days",
                "work_hours",
            ]
            for field in required_fields:
                if field not in user_stat:
                    user_stat[field] = 0

            user_id = str(user_stat.get("user_id", ""))
            nickname = user_stat.get("nickname", f"用户{user_id}")
            shift = user_stat.get("shift", "day")
            shift_display = shift_display_map.get(shift, "白班")

            row = [user_id, nickname, shift_display]

            # 活动数据处理
            activities = user_stat.get("activities", {})
            if isinstance(activities, str):
                try:
                    activities = json.loads(activities)
                except:
                    activities = {}
            elif not isinstance(activities, dict):
                activities = {}

            # 批量添加活动数据
            for act in activity_names:
                act_info = activities.get(act, {})
                if isinstance(act_info, dict):
                    count = act_info.get("count", 0)
                    time_sec = act_info.get("time", 0)
                else:
                    count, time_sec = 0, 0
                row.extend([count, format_time(time_sec)])

            # 批量添加统计数据
            row.extend(
                [
                    user_stat.get("total_activity_count", 0),
                    format_time(user_stat.get("total_accumulated_time", 0)),
                    user_stat.get("total_fines", 0),
                    user_stat.get("overtime_count", 0),
                    format_time(user_stat.get("total_overtime_time", 0)),
                    user_stat.get("work_days", 0),
                    format_time(user_stat.get("work_hours", 0)),
                    user_stat.get("work_start_count", 0),
                    user_stat.get("work_end_count", 0),
                    user_stat.get("work_start_fines", 0),
                    user_stat.get("work_end_fines", 0),
                    user_stat.get("late_count", 0),
                    user_stat.get("early_count", 0),
                ]
            )

            rows.append(row)

        # ===== 7. 批量写入CSV =====
        writer.writerows(rows)

        # ===== 8. 获取CSV内容并清理缓冲区 =====
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        # 🆕 添加数据统计日志
        total_records = len(rows)
        total_users = len(set(row[0] for row in rows))

        logger.info(
            f"✅ 月度导出完成\n"
            f"   ├─ 统计月份: {year}年{month}月\n"
            f"   ├─ 用户数量: {total_users}\n"
            f"   ├─ 数据行数: {total_records}\n"
            f"   └─ 文件大小: {len(csv_content)} 字节"
        )
        return csv_content

    except Exception as e:
        logger.error(f"❌ 月度导出失败: {e}")
        logger.error(traceback.format_exc())
        return None


# ========= 导出月度报告命令 ========
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_monthlyreport(message: types.Message):
    """生成月度报告 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer(
                    "❌ 月份必须在1-12之间", reply_to_message_id=message.message_id
                )
                return
        except ValueError:
            await message.answer(
                "❌ 请输入有效的年份和月份", reply_to_message_id=message.message_id
            )
            return

    await message.answer(
        "⏳ 正在生成月度报告，请稍候...", reply_to_message_id=message.message_id
    )

    try:
        # 生成报告
        report = await generate_monthly_report(chat_id, year, month)
        if report:
            await message.answer(
                report, parse_mode="HTML", reply_to_message_id=message.message_id
            )

            # 导出CSV
            await export_monthly_csv(chat_id, year, month)
            await message.answer(
                "✅ 月度数据已导出并推送！", reply_to_message_id=message.message_id
            )
        else:
            time_desc = f"{year}年{month}月" if year and month else "最近一个月"
            await message.answer(
                f"⚠️ {time_desc}没有数据需要报告", reply_to_message_id=message.message_id
            )

    except Exception as e:
        await message.answer(
            f"❌ 生成月度报告失败：{e}", reply_to_message_id=message.message_id
        )


# ========== 字段映射和兼容性函数 ==========
async def ensure_monthly_data_completeness(stats: List[Dict]) -> List[Dict]:
    """
    确保月度统计数据的完整性，为所有必要字段提供默认值
    """
    if not stats:
        return []

    result = []
    for stat in stats:
        # 确保所有工作相关字段都存在
        stat.setdefault("work_start_count", 0)
        stat.setdefault("work_end_count", 0)
        stat.setdefault("work_start_fines", 0)
        stat.setdefault("work_end_fines", 0)
        stat.setdefault("late_count", 0)
        stat.setdefault("early_count", 0)
        stat.setdefault("work_days", 0)
        stat.setdefault("work_hours", 0)

        # 确保活动数据格式正确
        if "activities" not in stat or not isinstance(stat["activities"], dict):
            stat["activities"] = {}

        result.append(stat)

    return result


async def get_monthly_stats_compatible(chat_id: int, target_date: date) -> List[Dict]:
    """
    兼容函数：获取月度统计数据并确保完整性
    用于日常表无数据时的回退方案
    """
    try:
        month_start = target_date.replace(day=1)
        monthly_stats = await db.get_monthly_statistics(
            chat_id, month_start.year, month_start.month
        )

        if not monthly_stats:
            return []

        return await ensure_monthly_data_completeness(monthly_stats)

    except Exception as e:
        logger.error(f"获取兼容月度数据失败: {e}")
        return []


@admin_required
@rate_limit(rate=2, per=60)
async def cmd_exportmonthly(message: types.Message):
    """导出月度数据 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在导出月度数据，请稍候...")

    try:
        await export_monthly_csv(chat_id, year, month)
        await message.answer("✅ 月度数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出月度数据失败：{e}")


# ========== 添加活动命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_addactivity(message: types.Message):
    """添加新活动"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["addactivity_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        act, max_times, time_limit = args[1], int(args[2]), int(args[3])
        existed = await db.activity_exists(act)
        await db.update_activity_config(act, max_times, time_limit)
        await db.force_refresh_activity_cache()

        if existed:
            await message.answer(
                f"✅ 已修改活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
    except Exception as e:
        await message.answer(
            f"❌ 添加/修改活动失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delactivity(message: types.Message):
    """删除活动 - 优化版本"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/delactivity <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return
    act = args[1]
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 <code>{act}</code> 不存在",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
        return

    await db.delete_activity_config(act)
    await db.force_refresh_activity_cache()  # 确保缓存立即更新

    await message.answer(
        f"✅ 活动 <code>{act}</code> 已删除",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )
    logger.info(f"删除活动: {act}")


# ========= 上下班指令 ========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworktime(message: types.Message):
    """设置上下班时间"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/setworktime <上班时间> <下班时间>\n"
            "📝 示例：/setworktime 09:00 18:00\n"
            "💡 时间格式：HH:MM (24小时制)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        work_start = args[1]
        work_end = args[2]

        # 验证时间格式

        time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")

        if not time_pattern.match(work_start) or not time_pattern.match(work_end):
            await message.answer(
                "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n"
                "📝 示例：09:00、18:30",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        # 更新工作时间
        chat_id = message.chat.id
        await db.update_group_work_time(chat_id, work_start, work_end)

        await message.answer(
            f"✅ 上下班时间设置成功！\n\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 上下班打卡功能已启用",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"设置工作时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


# ============= 重置命令 ==============
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setresettime(message: types.Message):
    """设置每日重置时间 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            Config.MESSAGES["setresettime_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_reset_time(chat_id, hour, minute)

            # 🎯【唯一修改】在这里触发双班硬重置调度
            # 注意：这里不是执行重置，而是为今天的重置做准备
            from dual_shift_reset import handle_hard_reset

            await handle_hard_reset(chat_id, message.from_user.id)

            await message.answer(
                f"✅ 每日重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>\n\n"
                f"💡 每天此时将自动重置所有用户的打卡数据",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            logger.info(f"重置时间设置成功: 群组 {chat_id} -> {hour:02d}:{minute:02d}")
        else:
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！\n"
                "💡 示例：/setresettime 0 0 （午夜重置）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！\n" "💡 示例：/setresettime 4 0 （凌晨4点重置）",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置重置时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_resettime(message: types.Message):
    """查看当前重置时间"""
    chat_id = message.chat.id
    try:
        group_data = await db.get_group_cached(chat_id)
        reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

        await message.answer(
            f"⏰ 当前重置时间设置\n\n"
            f"🕒 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
            f"📅 每天此时自动重置用户数据\n\n"
            f"💡 使用 /setresettime <小时> <分钟> 修改",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"查看重置时间失败: {e}")
        await message.answer(
            f"❌ 获取重置时间失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delwork_clear(message: types.Message):
    """移除上下班功能并清除所有记录 - 优化版"""
    chat_id = message.chat.id

    # 检查功能状态
    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 当前群组没有设置上下班功能",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )
        return

    # 获取当前设置用于显示
    work_hours = await db.get_group_work_time(chat_id)
    old_start = work_hours.get("work_start")
    old_end = work_hours.get("work_end")

    await message.answer("⏳ 正在移除上下班功能并清除记录...")

    try:
        # 重置为默认时间（禁用功能）
        await db.update_group_work_time(
            chat_id,
            Config.DEFAULT_WORK_HOURS["work_start"],
            Config.DEFAULT_WORK_HOURS["work_end"],
        )

        # 🆕 改进：使用数据库的带重试方法
        records_cleared = 0
        try:
            result = await db.execute_with_retry(
                "清除工作记录", "DELETE FROM work_records WHERE chat_id = $1", chat_id
            )
            records_cleared = (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )
        except Exception as e:
            logger.warning(f"清除工作记录时出现异常: {e}")
            # 不阻止主要功能，继续执行

        # 🆕 改进：同时清理月度统计中的工作数据
        try:
            await db.execute_with_retry(
                "清理月度工作统计",
                "DELETE FROM monthly_statistics WHERE chat_id = $1 AND activity_name IN ('work_days', 'work_hours')",
                chat_id,
            )
        except Exception as e:
            logger.warning(f"清理月度工作统计时出现异常: {e}")

        # 清理用户缓存确保立即生效
        await db.force_refresh_activity_cache()  # 🆕 强制刷新活动缓存
        db._cache.pop(f"group:{chat_id}", None)  # 🆕 清理群组缓存

        success_msg = (
            f"✅ <b>上下班功能已移除</b>\n\n"
            f"🗑️ <b>删除的设置：</b>\n"
            f"   • 上班时间: <code>{old_start}</code>\n"
            f"   • 下班时间: <code>{old_end}</code>\n"
            f"   • 清除记录: <code>{records_cleared}</code> 条\n\n"
            f"🔧 <b>当前状态：</b>\n"
            f"   • 上下班按钮已隐藏\n"
            f"   • 工作相关统计已重置\n"
            f"   • 可正常进行其他活动打卡\n\n"
            f"💡 如需重新启用，请使用 /setworktime 命令"
        )

        await message.answer(
            success_msg,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(
            f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能，清除 {records_cleared} 条记录"
        )

    except Exception as e:
        logger.error(f"移除上下班功能失败: {e}")
        await message.answer(
            f"❌ 移除失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========= 绑定频道与群组命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setchannel(message: types.Message):
    """绑定提醒频道 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setchannel_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        channel_id = int(args[1].strip())

        # 基本格式验证（确保是有效的频道ID格式）
        if channel_id > 0:
            await message.answer(
                "❌ 频道ID应该是负数格式（如 -100xxx）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, channel_id)

        await message.answer(
            f"✅ 已绑定超时提醒推送频道：<code>{channel_id}</code>\n\n"
            f"💡 超时打卡和迟到/早退通知将推送到此频道\n"
            f"⚠️ 如果推送失败，请检查：\n"
            f"• 频道ID是否正确\n"
            f"• 机器人是否已加入频道\n"
            f"• 机器人是否有发送消息权限",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"频道绑定成功: 群组 {chat_id} -> 频道 {channel_id}")

    except ValueError:
        await message.answer(
            "❌ 频道ID必须是数字格式\n" "💡 示例：/setchannel -1001234567890",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置频道失败: {e}")
        await message.answer(
            f"❌ 绑定频道失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setgroup(message: types.Message):
    """绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setgroup_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        group_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, group_id)

        await message.answer(
            f"✅ 已绑定通知群组：<code>{group_id}</code>\n\n"
            f"💡 打卡通知将推送到此群组",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群组绑定成功: 主群组 {chat_id} -> 通知群组 {group_id}")

    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字格式",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置群组失败: {e}")
        await message.answer(
            f"❌ 绑定群组失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


# ========== 设置额外的上下班通知群组 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_addextraworkgroup(message: types.Message):
    """添加上下班通知额外推送群组（不改变现有推送）"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            "❌ 用法：/addextraworkgroup <群组ID>\n"
            "📝 示例：/addextraworkgroup -1001234567890\n\n"
            "💡 设置后，上下班打卡通知会额外推送到该群组\n"
            "   原有的所有推送（群组、频道）保持不变",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        extra_group_id = int(args[1].strip())

        # 基本格式验证
        if extra_group_id > 0:
            await message.answer(
                "❌ 群组ID应该是负数格式（如 -100xxx）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.init_group(chat_id)

        # 获取当前配置用于显示
        group_data = await db.get_group_cached(chat_id)
        channel_id = group_data.get("channel_id") if group_data else None
        notify_group_id = (
            group_data.get("notification_group_id") if group_data else None
        )

        # 设置额外的上下班通知群组
        await db.update_group_extra_work_group(chat_id, extra_group_id)

        # 构建配置说明
        channel_text = f"频道 <code>{channel_id}</code>" if channel_id else "未设置"
        notify_text = (
            f"群组 <code>{notify_group_id}</code>" if notify_group_id else "当前群组"
        )

        await message.answer(
            f"✅ 已添加上下班通知额外推送群组\n\n"
            f"📊 <b>当前推送配置：</b>\n"
            f"• 原有推送（保持不变）：\n"
            f"  └─ 超时通知 → {channel_text}\n"
            f"  └─ 吃饭通知 → {notify_text}\n"
            f"  └─ 上下班通知 → 当前群组 + {channel_text}\n\n"
            f"• <b>新增额外推送：</b>\n"
            f"  └─ 上下班通知 → 额外群组 <code>{extra_group_id}</code>\n\n"
            f"💡 现在每次上下班打卡，都会额外发送一份到该群组\n"
            f"   如需清除，使用 /clearextraworkgroup 命令",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(
            f"添加上下班额外通知群组成功: 主群组 {chat_id} -> 额外群组 {extra_group_id}"
        )

    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字格式",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"添加上下班额外通知群组失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_clearextraworkgroup(message: types.Message):
    """清除额外的上下班通知群组"""
    chat_id = message.chat.id

    try:
        extra_group_id = await db.get_extra_work_group(chat_id)

        if not extra_group_id:
            await message.answer(
                "⚠️ 当前没有设置额外的上下班通知群组",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.clear_extra_work_group(chat_id)

        await message.answer(
            f"✅ 已清除额外的上下班通知群组 <code>{extra_group_id}</code>\n\n"
            f"📊 现在上下班通知将恢复原有推送逻辑，不再额外推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"已清除群组 {chat_id} 的额外上下班通知群组 {extra_group_id}")

    except Exception as e:
        logger.error(f"清除额外上下班通知群组失败: {e}")
        await message.answer(
            f"❌ 清除失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showeverypush(message: types.Message):
    """显示所有推送配置"""
    chat_id = message.chat.id

    try:
        group_data = await db.get_group_cached(chat_id) or {}
        channel_id = group_data.get("channel_id")
        notify_group_id = group_data.get("notification_group_id")
        extra_work_group_id = await db.get_extra_work_group(chat_id)

        config_text = (
            f"📢 <b>当前推送配置总览</b>\n\n"
            f"<b>🔴 超时通知：</b>\n"
            f"• 推送目标：{f'频道 <code>{channel_id}</code>' if channel_id else '未设置'}\n\n"
            f"<b>🍽️ 吃饭通知：</b>\n"
            f"• 推送目标：{f'群组 <code>{notify_group_id}</code>' if notify_group_id else '当前群组'}\n\n"
            f"<b>🕒 上下班通知：</b>\n"
            f"• 原有推送：当前群组 + {f'频道 <code>{channel_id}</code>' if channel_id else '无'}\n"
            f"• 额外推送：{f'群组 <code>{extra_work_group_id}</code>' if extra_work_group_id else '未设置'}\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💡 <b>管理命令：</b>\n"
            f"• /addextraworkgroup - 添加上下班额外推送群组\n"
            f"• /clearextraworkgroup - 清除额外推送\n"
            f"• /setchannel - 设置超时通知频道\n"
            f"• /setgroup - 设置吃饭通知群组"
        )

        await message.answer(
            config_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"显示推送配置失败: {e}")
        await message.answer(
            f"❌ 获取配置失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 活动人数限制命令 =========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_actnum(message: types.Message):
    """设置活动人数限制"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/actnum <活动名> <人数限制>\n"
            "例如：/actnum 小厕 3\n"
            "💡 设置为0表示取消限制",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        activity = args[1]
        max_users = int(args[2])

        # 检查活动是否存在
        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        if max_users < 0:
            await message.answer(
                "❌ 人数限制不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        chat_id = message.chat.id

        if max_users == 0:
            # 取消限制
            await db.remove_activity_user_limit(activity)
            await message.answer(
                f"✅ 已取消活动 '<code>{activity}</code>' 的人数限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"取消活动人数限制: {activity}")
        else:
            # 设置限制
            await db.set_activity_user_limit(activity, max_users)

            # 获取当前活动人数
            current_users = await db.get_current_activity_users(chat_id, activity)

            await message.answer(
                f"✅ 已设置活动 '<code>{activity}</code>' 的人数限制为 <code>{max_users}</code> 人\n\n"
                f"📊 当前状态：\n"
                f"• 限制人数：<code>{max_users}</code> 人\n"
                f"• 当前进行：<code>{current_users}</code> 人\n"
                f"• 剩余名额：<code>{max_users - current_users}</code> 人",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
            )
            logger.info(f"设置活动人数限制: {activity} -> {max_users}人")

    except ValueError:
        await message.answer(
            "❌ 人数限制必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置活动人数限制失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_actstatus(message: types.Message):
    """查看活动人数状态"""
    chat_id = message.chat.id

    try:
        activity_limits = await db.get_all_activity_limits()

        if not activity_limits:
            await message.answer(
                "📊 当前没有设置任何活动人数限制\n"
                "💡 使用 /actnum <活动名> <人数> 来设置限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            return

        status_text = "📊 活动人数限制状态\n\n"

        for activity, max_users in activity_limits.items():
            current_users = await db.get_current_activity_users(chat_id, activity)
            remaining = max(0, max_users - current_users) if max_users > 0 else "无限制"

            status_icon = "🟢" if remaining == "无限制" or remaining > 0 else "🔴"
            limit_display = f"{max_users}" if max_users > 0 else "无限制"

            status_text += (
                f"{status_icon} <code>{activity}</code>\n"
                f"   • 限制：<code>{limit_display}</code>\n"
                f"   • 当前：<code>{current_users}</code> 人\n"
                f"   • 剩余：<code>{remaining}</code> 人\n\n"
            )

        status_text += "💡 绿色表示还有名额，红色表示已满员"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )

        logger.info(f"查看活动状态: {chat_id}")

    except Exception as e:
        logger.error(f"获取活动状态失败: {e}")
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 罚款管理命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfines_all(message: types.Message):
    """为所有活动统一设置分段罚款 - 优化版本"""
    args = message.text.split()
    if len(args) < 3 or (len(args) - 1) % 2 != 0:
        await message.answer(
            Config.MESSAGES["setfines_all_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        pairs = args[1:]
        segments = {}
        for i in range(0, len(pairs), 2):
            t = int(pairs[i])
            f = int(pairs[i + 1])
            if t <= 0 or f < 0:
                await message.answer(
                    "❌ 时间段必须为正整数，罚款金额不能为负数",
                    reply_markup=await get_main_keyboard(
                        chat_id=message.chat.id, show_admin=True
                    ),
                    reply_to_message_id=message.message_id,
                )
                return
            segments[t] = f

        activity_limits = await db.get_activity_limits_cached()
        if not activity_limits:
            await message.answer(
                "⚠️ 当前没有活动，无法设置罚款",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        for act in activity_limits.keys():
            for time_segment, amount in segments.items():
                await db.update_fine_config(act, str(time_segment), amount)

        segments_text = " ".join(
            [f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()]
        )
        await message.answer(
            f"✅ 已为所有活动设置分段罚款：{segments_text}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群 {message.chat.id} 已统一设置所有活动罚款: {segments_text}")

    except ValueError:
        await message.answer(
            "❌ 时间段和金额必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置所有活动罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfine(message: types.Message):
    """设置单个活动的罚款费率"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["setfine_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
        return

    try:
        activity = args[1]
        time_segment = int(args[2])
        amount = int(args[3])

        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
                parse_mode="HTML",
            )
            return

        if time_segment <= 0 or amount < 0:
            await message.answer(
                "❌ 时间段必须为正整数，罚款金额不能为负数",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                reply_to_message_id=message.message_id,
            )
            return

        await db.update_fine_config(activity, str(time_segment), amount)

        await message.answer(
            f"✅ 已设置活动 '<code>{activity}</code>' 的罚款：\n"
            f"⏱️ 时间段：<code>{time_segment}</code>\n"
            f"💰 金额：<code>{amount}</code> 分",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(
            f"群 {message.chat.id} 已设置活动罚款: {activity} {time_segment} -> {amount}分"
        )

    except ValueError:
        await message.answer(
            "❌ 时间段和金额必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置单个活动罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_finesstatus(message: types.Message):
    """查看所有活动的罚款设置状态"""
    chat_id = message.chat.id
    try:
        # 获取所有活动和罚款配置
        activity_limits = await db.get_activity_limits_cached()
        fine_rates = await db.get_fine_rates()

        if not activity_limits:
            await message.answer(
                "⚠️ 当前没有配置任何活动",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                reply_to_message_id=message.message_id,
            )
            return

        status_text = "💰 活动罚款设置状态\n\n"

        for activity in activity_limits.keys():
            activity_fines = fine_rates.get(activity, {})
            status_text += f"🔹 <code>{activity}</code>\n"

            if activity_fines:
                # 按时间段排序
                for time_seg, amount in sorted(
                    activity_fines.items(), key=lambda x: int(x[0])
                ):
                    status_text += f"   • 时间段 <code>{time_seg}</code> 分钟：<code>{amount}</code> 分\n"
            else:
                status_text += f"   • 未设置罚款\n"

            status_text += "\n"

        status_text += "💡 设置命令：\n"
        status_text += "• /setfine <活动> <时间> <金额> - 设置单个活动\n"
        status_text += "• /setfines_all <t1> <f1> [t2 f2...] - 统一设置所有活动"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )

        logger.info(f"群 {chat_id} 查看了活动罚款状态")

    except Exception as e:
        logger.error(f"查看罚款状态失败: {e}")
        await message.answer(
            f"❌ 获取罚款状态失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=2, per=60)
async def cmd_checkdualsetup(message: types.Message):
    """检查双班重置配置"""
    chat_id = message.chat.id

    try:
        group_data = await db.get_group_cached(chat_id)
        if not group_data:
            await message.answer("❌ 群组未初始化")
            return

        reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

        shift_config = await db.get_shift_config(chat_id)
        is_dual = shift_config.get("dual_mode", False)

        now = get_beijing_time()
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )
        execute_time = reset_time_today + timedelta(hours=2)

        text = (
            f"🔍 <b>双班重置配置检查</b>\n\n"
            f"• 群组ID: <code>{chat_id}</code>\n"
            f"• 双班模式: {'✅ 开启' if is_dual else '❌ 关闭'}\n"
            f"• 重置时间: <code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
            f"• 执行时间: <code>{execute_time.strftime('%H:%M')}</code>\n"
            f"• 当前时间: <code>{now.strftime('%H:%M:%S')}</code>\n\n"
        )

        if is_dual:
            if now < execute_time:
                time_left = execute_time - now
                minutes = int(time_left.total_seconds() / 60)
                seconds = int(time_left.total_seconds() % 60)
                text += f"⏳ 距离下次执行还有: <code>{minutes}分{seconds}秒</code>"
            else:
                text += f"✅ 当前在执行窗口内"
        else:
            text += f"💡 群组未开启双班模式，无需检查"

        await message.answer(
            text, parse_mode="HTML", reply_to_message_id=message.message_id
        )

    except Exception as e:
        await message.answer(
            f"❌ 检查失败: {e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=2, per=60)
async def cmd_testgroupaccess(message: types.Message):
    """测试机器人是否能访问指定群组"""
    chat_id = message.chat.id
    args = message.text.split()

    if len(args) < 2:
        await message.answer(
            "❌ 用法：/testgroupaccess <群组ID>\n"
            "📝 示例：/testgroupaccess -5187163421",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        target_id = int(args[1])

        # 获取额外群组配置
        extra_group_id = await db.get_extra_work_group(chat_id)

        result_text = f"🔍 <b>群组访问测试</b>\n\n"

        # 测试目标群组
        try:
            chat_info = await bot.get_chat(target_id)
            result_text += f"✅ 目标群组 <code>{target_id}</code> 可访问\n"
            result_text += f"   • 标题：{chat_info.title}\n"
            result_text += f"   • 类型：{chat_info.type}\n"

            # 尝试发送测试消息
            test_msg = await bot.send_message(
                target_id,
                f"🧪 这是一条测试消息\n发送时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode="HTML",
            )
            result_text += f"✅ 测试消息发送成功 (消息ID: {test_msg.message_id})\n"

        except Exception as e:
            result_text += f"❌ 目标群组 <code>{target_id}</code> 访问失败\n"
            result_text += f"   • 错误：{str(e)}\n"
            if "403" in str(e):
                result_text += "   • 原因：机器人不在群组中或没有权限\n"

        # 显示当前配置
        result_text += f"\n📊 当前额外群组配置：\n"
        result_text += f"• 配置的群组：<code>{extra_group_id or '未设置'}</code>\n"

        if extra_group_id and extra_group_id == target_id:
            result_text += f"✅ 测试的群组与配置一致\n"
        elif extra_group_id:
            result_text += f"⚠️ 测试的群组与配置不一致\n"

        await message.answer(
            result_text, parse_mode="HTML", reply_to_message_id=message.message_id
        )

    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字", reply_to_message_id=message.message_id
        )
    except Exception as e:
        await message.answer(
            f"❌ 测试失败：{e}", reply_to_message_id=message.message_id
        )


@admin_required
@rate_limit(rate=2, per=60)
async def cmd_checkbotpermissions(message: types.Message):
    """检查机器人在各个群组的权限"""
    chat_id = message.chat.id

    result_text = f"🔍 <b>机器人权限检查</b>\n\n"
    result_text += f"🤖 机器人ID: <code>{bot.id}</code>\n"
    result_text += f"🤖 机器人用户名: @{(await bot.me()).username}\n\n"

    # 检查当前群组
    try:
        bot_member = await bot.get_chat_member(chat_id, bot.id)
        result_text += f"📊 当前群组 <code>{chat_id}</code>:\n"
        result_text += f"   • 状态：{bot_member.status}\n"
        result_text += f"   • 是否为管理员：{'是' if bot_member.status in ['administrator', 'creator'] else '否'}\n"
    except Exception as e:
        result_text += f"❌ 无法获取当前群组权限: {e}\n"

    # 检查额外群组
    extra_group_id = await db.get_extra_work_group(chat_id)
    if extra_group_id:
        result_text += f"\n📊 额外群组 <code>{extra_group_id}</code>:\n"
        try:
            extra_member = await bot.get_chat_member(extra_group_id, bot.id)
            result_text += f"   • 状态：{extra_member.status}\n"
            result_text += f"   • 是否为管理员：{'是' if extra_member.status in ['administrator', 'creator'] else '否'}\n"
            result_text += f"   • 可发送消息：{'是' if extra_member.can_send_messages else '未知'}\n"
        except Exception as e:
            result_text += f"   ❌ 无法获取权限: {e}\n"
            if "403" in str(e):
                result_text += f"   • 原因：机器人不在该群组中\n"

    # 检查频道
    group_data = await db.get_group_cached(chat_id)
    channel_id = group_data.get("channel_id") if group_data else None
    if channel_id:
        result_text += f"\n📊 频道 <code>{channel_id}</code>:\n"
        try:
            channel_member = await bot.get_chat_member(channel_id, bot.id)
            result_text += f"   • 状态：{channel_member.status}\n"
        except Exception as e:
            result_text += f"   ❌ 无法获取权限: {e}\n"

    result_text += f"\n💡 <b>常见问题：</b>\n"
    result_text += f"• 如果机器人不在群组中，请手动添加\n"
    result_text += f"• 如果机器人不是管理员，可能受群组限制\n"
    result_text += f"• 群组设置了慢速模式可能延迟消息显示"

    await message.answer(
        result_text, parse_mode="HTML", reply_to_message_id=message.message_id
    )


# =========== 上下班罚款指令 ===========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworkfine(message: types.Message):
    """
    设置上下班罚款规则
    用法：
    /setworkfine work_start 1 100 10 200 30 500
    表示：
        迟到1分钟以上罚100，
        迟到10分钟以上罚200，
        迟到30分钟以上罚500
    """
    args = message.text.split()

    # 1. 检查参数长度和格式
    if len(args) < 4 or (len(args) - 2) % 2 != 0:
        await message.answer(
            "❌ 用法错误\n正确格式：/setworkfine <work_start|work_end> <分钟1> <罚款1> [分钟2 罚款2 ...]",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
        return

    checkin_type = args[1]
    if checkin_type not in ["work_start", "work_end"]:
        await message.answer(
            "❌ 类型必须是 work_start 或 work_end",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
        return

    # 2. 解析分钟阈值和罚款金额
    fine_segments = {}
    try:
        for i in range(2, len(args), 2):
            minute = int(args[i])
            amount = int(args[i + 1])
            if minute <= 0 or amount < 0:
                await message.answer(
                    "❌ 分钟必须大于0，罚款金额不能为负数",
                    reply_markup=get_admin_keyboard(),  # 已移除 await
                    reply_to_message_id=message.message_id,
                )
                return
            fine_segments[str(minute)] = amount

        # 3. 更新数据库配置（重写整个罚款配置）
        await db.clear_work_fine_rates(checkin_type)
        for minute_str, fine_amount in fine_segments.items():
            await db.update_work_fine_rate(checkin_type, minute_str, fine_amount)

        # 4. 生成反馈文本
        segments_text = "\n".join(
            [
                f"⏰ 超过 {m} 分钟 → 💰 {a} 分"
                for m, a in sorted(fine_segments.items(), key=lambda x: int(x[0]))
            ]
        )

        type_text = "上班迟到" if checkin_type == "work_start" else "下班早退"

        await message.answer(
            f"✅ 已设置{type_text}罚款规则：\n{segments_text}",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )

        logger.info(f"设置上下班罚款成功: {checkin_type} -> {fine_segments}")

    except ValueError:
        await message.answer(
            "❌ 分钟和罚款必须是数字",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        logger.error(f"设置上下班罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=get_admin_keyboard(),  # 已移除 await
            reply_to_message_id=message.message_id,
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showsettings(message: types.Message):
    """显示目前的设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id) or {}

    activity_limits = await db.get_activity_limits_cached()
    fine_rates = await db.get_fine_rates()
    work_fine_rates = await db.get_work_fine_rates()

    # 生成输出文本
    text = f"🔧 当前群设置（群ID {chat_id}）\n\n"

    # 基本设置
    text += "📋 基本设置：\n"
    text += f"• 绑定频道ID: <code>{group_data.get('channel_id', '未设置')}</code>\n"
    text += f"• 通知群组ID: <code>{group_data.get('notification_group_id', '未设置')}</code>\n\n"

    # 重置与上下班时间
    text += "⏰ 重置与工作时间：\n"
    text += f"• 每日重置时间: <code>{group_data.get('reset_hour', Config.DAILY_RESET_HOUR):02d}:{group_data.get('reset_minute', Config.DAILY_RESET_MINUTE):02d}</code>\n"
    text += f"• 上班时间: <code>{group_data.get('work_start_time', '09:00')}</code>\n"
    text += f"• 下班时间: <code>{group_data.get('work_end_time', '18:00')}</code>\n\n"

    # 活动设置
    text += "🎯 活动设置：\n"
    if activity_limits:
        for act, v in activity_limits.items():
            text += f"• <code>{act}</code>：次数上限 <code>{v['max_times']}</code>，时间限制 <code>{v['time_limit']}</code> 分钟\n"
    else:
        text += "• 暂无活动设置\n"

    # 活动罚款设置
    text += "\n💰 活动罚款分段：\n"
    if fine_rates:
        for act, fr in fine_rates.items():
            if fr:
                try:
                    sorted_fines = sorted(
                        fr.items(), key=lambda x: int(x[0].replace("min", ""))
                    )
                    fines_text = " | ".join([f"{k}:{v}分" for k, v in sorted_fines])
                    text += f"• <code>{act}</code>：{fines_text}\n"
                except Exception:
                    text += f"• <code>{act}</code>：配置异常\n"
            else:
                text += f"• <code>{act}</code>：未设置\n"
    else:
        text += "• 暂无活动罚款设置\n"

    # 上下班罚款
    text += "\n⏰ 上下班罚款设置：\n"
    for key, label in [("work_start", "上班迟到"), ("work_end", "下班早退")]:
        wf = work_fine_rates.get(key, {})
        if wf:
            try:
                sorted_wf = sorted(wf.items(), key=lambda x: int(x[0]))
                wf_text = " | ".join([f"{k}分:{v}分" for k, v in sorted_wf])
                text += f"• {label}：{wf_text}\n"
            except Exception:
                text += f"• {label}：配置异常\n"
        else:
            text += f"• {label}：未设置\n"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )


# ========== 查看工作时间命令 =========
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_worktime(message: types.Message):
    """查看当前工作时间设置"""
    chat_id = message.chat.id
    try:
        work_hours = await db.get_group_work_time(chat_id) or {}
        has_enabled = await db.has_work_hours_enabled(chat_id)

        work_start = work_hours.get("work_start", "09:00")
        work_end = work_hours.get("work_end", "18:00")
        status = "🟢 已启用" if has_enabled else "🔴 未启用（使用默认时间）"

        await message.answer(
            f"🕒 当前工作时间设置\n\n"
            f"📊 状态：{status}\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 使用 /setworktime 09:00 18:00 来修改",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"查看工作时间失败: {e}")
        await message.answer(
            "❌ 获取工作时间失败，请稍后重试",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            reply_to_message_id=message.message_id,
        )


# ========== 按钮处理 ==========
@rate_limit(rate=10, per=60)
async def handle_back_command(message: types.Message):
    """处理回座命令"""
    await process_back(message)


@rate_limit(rate=5, per=60)
async def handle_work_buttons(message: types.Message):
    """处理上下班按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id
    text = message.text.strip()

    # 🎯 新增检查：是否启用了上下班功能
    if not await db.has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 本群组尚未启用上下班打卡功能\n\n" "👑 请联系管理员设置上下班时间",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    if text == "🟢 上班":
        await process_work_checkin(message, "work_start")
    elif text == "🔴 下班":
        await process_work_checkin(message, "work_end")


@admin_required
@rate_limit(rate=2, per=60)
@track_performance("handle_export_button")
async def handle_export_button(message: types.Message):
    """处理导出数据按钮"""
    chat_id = message.chat.id
    await message.answer(
        "⏳ 正在导出数据，请稍候...", reply_to_message_id=message.message_id
    )
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送！", reply_to_message_id=message.message_id
        )
    except Exception as e:
        await message.answer(
            f"❌ 导出失败：{e}", reply_to_message_id=message.message_id
        )


@rate_limit(rate=10, per=60)
@track_performance("handle_my_record")
async def handle_my_record(message: types.Message):
    """处理我的记录按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)


@rate_limit(rate=10, per=60)
@track_performance("handle_rank")
async def handle_rank(message: types.Message):
    """处理排行榜按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)


@rate_limit(rate=5, per=60)
async def handle_admin_panel_button(message: types.Message):
    """处理管理员面板按钮 - 简洁手机版"""
    if not await is_admin(message.from_user.id):
        markup = await get_main_keyboard(chat_id=message.chat.id, show_admin=False)
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=markup,
            reply_to_message_id=message.message_id,
            parse_mode=None,
        )
        return

    admin_text = (
        "👑 <b>管理员面板</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        "📢 <b>频道与推送</b>\n"
        "├ <code>/setchannel [ID]</code>\n"
        "├ <code>/setgroup [ID]</code>\n"
        "├ <code>/addextraworkgroup [ID]</code> - 添加上下班额外推送群组\n"
        "├ <code>/setpush [目标] [开关]</code>\n"
        "├ <code>/showeverypush</code>\n"
        "│ 目标: ch|gr|ad\n"
        "│ 开关: on|off\n\n"
        "🎯 <b>活动管理</b>\n"
        "├ <code>/addactivity [名] [次] [分]</code>\n"
        "├ <code>/delactivity [名]</code>\n"
        "├ <code>/actnum [名] [人数]</code>\n"
        "└ <code>/actstatus</code>\n\n"
        "💰 <b>罚款管理</b>\n"
        "├ <code>/setfine [名] [段] [元]</code>\n"
        "├ <code>/setfines_all [段1] [元1] ...</code>\n"
        "├ <code>/setworkfine [类型] [分] [元]</code>\n"
        "└ <code>/finesstatus</code>\n"
        "  类型: start|end\n\n"
        "🔄 <b>重置设置</b>\n"
        "├ <code>/setresettime [时] [分]</code>\n"
        "├ <code>/setsoftresettime [时] [分]</code>\n"
        "├ <code>/resetuser [用户ID]</code>\n"
        "└ <code>/resettime</code>\n\n"
        "⏰ <b>上下班管理</b>\n"
        "├ <code>/setdualmode on 9:00 21:00</code>\n"
        "├ <code>/setworktime [上] [下]</code>\n"
        "├ <code>/setshiftgrace</code>\n"
        "├ <code>/setworkendgrace</code>\n"
        "├ <code>/worktime</code>\n"
        "├ <code>/checkdual</code>\n"
        "├ <code>/delwork</code>\n"
        "└ <code>/delwork_clear</code>\n\n"
        "📊 <b>数据管理</b>\n"
        "├ <code>/export</code>\n"
        "├ <code>/exportmonthly [年] [月]</code>\n"
        "├ <code>/monthlyreport [年] [月]</code>\n"
        "├ <code>/cleanup_monthly [年] [月]</code>\n"
        "├ <code>/monthly_stats_status</code>\n"
        "└ <code>/cleanup_inactive [天]</code>\n\n"
        "💾 <b>数据显示</b>\n"
        "└ <code>/showsettings</code>\n\n"
        "━━━━━━━━━━━━━━━━\n"
        "<i>💡 提示：发送 /help [命令] 查看详情</i>"
    )

    await message.answer(
        admin_text,
        reply_markup=get_admin_keyboard(),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",  # 👈 改为 HTML
    )


# ========== 返回主菜单按钮处理 ==========
@rate_limit(rate=5, per=60)
async def handle_back_to_main_menu(message: types.Message):
    """处理返回主菜单按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id

    logger.info(f"用户 {uid} 点击了返回主菜单按钮")

    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
    )
    logger.info(f"已为用户 {uid} 返回主菜单")


# ========== 动态活动按钮处理 ==========
@rate_limit(rate=10, per=60)
async def handle_all_text_messages(message: types.Message):
    """统一处理所有文本消息"""
    text = message.text.strip()
    chat_id = message.chat.id
    uid = message.from_user.id

    # 如果是特殊按钮，直接返回让专门的处理程序处理
    if text in SPECIAL_BUTTONS:
        logger.debug(f"特殊按钮被点击: {text} - 用户 {uid}")
        return

    # 检查是否是活动按钮
    try:
        activity_limits = await db.get_activity_limits_cached()
        if text in activity_limits.keys():
            logger.info(f"活动按钮点击: {text} - 用户 {uid}")
            await start_activity(message, text)
            return
    except Exception as e:
        logger.error(f"处理活动按钮时出错: {e}")

    # 如果不是活动按钮，显示帮助信息
    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 点击活动按钮开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


# ========== 固定活动命令处理器 ==========
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("fixed_activity", max_retries=2)
@track_performance("fixed_activity")
async def handle_fixed_activity(message: types.Message):
    """处理固定活动命令（支持带用户名格式）"""
    # 获取完整的命令文本
    command_text = message.text.strip()
    logger.info(f"🔍 收到命令: {command_text}")

    # 定义活动映射
    activity_map = {
        "/wc": "小厕",
        "/bigwc": "大厕",
        "/eat": "吃饭",
        "/smoke": "抽烟或休息",
        "/rest": "休息",
    }

    # 1. 检查纯命令（如 /wc）
    if command_text in activity_map:
        act = activity_map[command_text]
        logger.info(f"✅ 匹配到纯命令: {command_text} -> {act}")
        await start_activity(message, act)
        return

    # 2. 检查带用户名的命令（如 /wc@dh188_bot）
    for cmd, act in activity_map.items():
        if command_text.startswith(cmd + "@"):
            logger.info(f"✅ 匹配到带用户名命令: {command_text} -> {act}")
            await start_activity(message, act)
            return

    # 3. 都不是，让其他处理器处理
    logger.warning(f"❌ 未匹配的命令: {command_text}")


# ========== 用户功能 ==========
async def show_history(message: types.Message, shift: str = None):
    """显示用户历史记录 - 业务周期权威版 + 班次维度（融合终极版）"""

    chat_id = message.chat.id
    uid = message.from_user.id

    # ==================== 基础初始化 ====================
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)

    business_date = await db.get_business_date(chat_id)

    group_data = await db.get_group_cached(chat_id)
    reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
    reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

    user_data = await db.get_user_cached(chat_id, uid)
    if not user_data:
        await message.answer(
            "暂无记录，请先进行打卡活动",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            reply_to_message_id=message.message_id,
        )
        return

    shift_config = await db.get_shift_config(chat_id)
    is_dual_mode = shift_config.get("dual_mode", False)

    # ==================== 标题构建 ====================
    first_line = (
        f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}"
    )

    if shift:
        shift_text = "白班" if shift == "day" else "夜班"
        title = f"{first_line}\n📊 【{shift_text}】记录统计"
    elif is_dual_mode:
        title = f"{first_line}\n📊 当前周期记录（双班）"
    else:
        title = f"{first_line}\n📊 当前周期记录"

    text = (
        f"{title}\n"
        f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
        f"⏰ 重置时间：{reset_hour:02d}:{reset_minute:02d}\n\n"
    )

    has_records = False

    # ==================== 1️⃣ 上下班记录 ====================
    work_records = await db.get_work_records_by_shift(chat_id, uid, shift)

    if work_records:
        text += "🕒 <b>上下班记录</b>\n"

        shift_work = {
            "day": {"work_start": [], "work_end": []},
            "night": {"work_start": [], "work_end": []},
        }

        for check_type, records in work_records.items():
            for r in records:
                s = r.get("shift", "day")
                shift_work[s][check_type].append(r)

        if shift:
            stats = shift_work.get(shift, {})
            for ct in ("work_start", "work_end"):
                if stats.get(ct):
                    type_text = "上班" if ct == "work_start" else "下班"
                    latest = stats[ct][0]
                    text += (
                        f"• {type_text}：<code>{len(stats[ct])}</code> 次\n"
                        f"  最近：{latest['checkin_time']}（{latest['status']}）\n"
                    )
        else:
            total_start = sum(len(shift_work[s]["work_start"]) for s in shift_work)
            total_end = sum(len(shift_work[s]["work_end"]) for s in shift_work)
            if total_start or total_end:
                text += (
                    f"• 上班：<code>{total_start}</code> 次\n"
                    f"• 下班：<code>{total_end}</code> 次\n"
                )

        text += "\n"
        has_records = True

    # ==================== 2️⃣ 活动记录（班次 + 权威） ====================
    activity_limits = await db.get_activity_limits_cached()

    async with db.pool.acquire() as conn:
        # 如果指定了班次，只查询该班次的数据
        if shift:
            rows = await conn.fetch(
                """
                SELECT activity_name, activity_count, accumulated_time, shift
                FROM user_activities
                WHERE chat_id = $1 AND user_id = $2 
                  AND activity_date = $3 AND shift = $4
                """,
                chat_id,
                uid,
                business_date,
                shift,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT activity_name, activity_count, accumulated_time, shift
                FROM user_activities
                WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                """,
                chat_id,
                uid,
                business_date,
            )

    activities_by_shift = {"day": {}, "night": {}}

    for r in rows:
        s = r["shift"] or "day"
        act = r["activity_name"]

        if act not in activities_by_shift[s]:
            activities_by_shift[s][act] = {"count": 0, "time": 0}

        activities_by_shift[s][act]["count"] += r["activity_count"]
        activities_by_shift[s][act]["time"] += r["accumulated_time"]

    # 根据班次参数计算总统计
    if shift:
        shift_data = activities_by_shift.get(shift, {})
        total_time_all = sum(info["time"] for info in shift_data.values())
        total_count_all = sum(info["count"] for info in shift_data.values())
        display_activities = shift_data
    else:
        total_time_all = 0
        total_count_all = 0
        for s_data in activities_by_shift.values():
            for info in s_data.values():
                total_time_all += info["time"]
                total_count_all += info["count"]

        display_activities = {}
        for s, acts in activities_by_shift.items():
            for act, info in acts.items():
                if act not in display_activities:
                    display_activities[act] = {"count": 0, "time": 0}
                display_activities[act]["count"] += info["count"]
                display_activities[act]["time"] += info["time"]

    text += "🎯 <b>活动记录</b>\n"

    def render_activity_block(act_map):
        nonlocal has_records
        block = ""
        for act in activity_limits.keys():
            info = act_map.get(act)
            if not info or (info["count"] == 0 and info["time"] == 0):
                continue
            count = info["count"]
            total_time = info["time"]
            max_times = activity_limits[act]["max_times"]
            status = "✅" if max_times == 0 or count < max_times else "❌"
            block += (
                f"• <code>{act}</code>："
                f"<code>{MessageFormatter.format_time(int(total_time))}</code>，"
                f"次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
            )
            has_records = True
        return block

    if shift:
        shift_display = render_activity_block(activities_by_shift.get(shift, {}))
        if shift_display:
            text += shift_display
    elif is_dual_mode:
        for s in ("day", "night"):
            block = render_activity_block(activities_by_shift.get(s, {}))
            if block:
                text += f"\n【{'白班' if s == 'day' else '夜班'}】\n{block}"
    else:
        text += render_activity_block(display_activities)

    # ==================== 3️⃣ 当前周期总统计（按班次）====================
    if shift:
        shift_text = "白班" if shift == "day" else "夜班"
        text += (
            f"\n📈 当前周期【{shift_text}】统计：\n"
            f"• {shift_text}累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
            f"• {shift_text}活动次数：<code>{total_count_all}</code> 次\n"
        )
    else:
        text += (
            f"\n📈 当前周期总统计：\n"
            f"• 总累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
            f"• 总活动次数：<code>{total_count_all}</code> 次\n"
        )

    # ==================== 4️⃣ 罚款统计（按班次）====================
    async with db.pool.acquire() as conn:
        if shift:
            # 查询指定班次的罚款
            fine_total = (
                await conn.fetchval(
                    """
                SELECT COALESCE(SUM(accumulated_time), 0)
                FROM daily_statistics
                WHERE chat_id = $1 
                  AND user_id = $2 
                  AND record_date = $3 
                  AND shift = $4
                  AND activity_name IN ('total_fines', 'work_fines', 
                                       'work_start_fines', 'work_end_fines')
                """,
                    chat_id,
                    uid,
                    business_date,
                    shift,
                )
                or 0
            )
        else:
            # 查询所有班次的罚款
            fine_total = (
                await conn.fetchval(
                    """
                SELECT COALESCE(SUM(accumulated_time), 0)
                FROM daily_statistics
                WHERE chat_id = $1 
                  AND user_id = $2 
                  AND record_date = $3 
                  AND activity_name IN ('total_fines', 'work_fines', 
                                       'work_start_fines', 'work_end_fines')
                """,
                    chat_id,
                    uid,
                    business_date,
                )
                or 0
            )

    if fine_total > 0:
        if shift:
            shift_text = "白班" if shift == "day" else "夜班"
            text += f"💰 {shift_text}累计罚款：<code>{fine_total}</code> 分\n"
        else:
            text += f"💰 累计罚款：<code>{fine_total}</code> 分\n"

    # ==================== 5️⃣ 班次提示 ====================
    if is_dual_mode and not shift:
        text += (
            "\n📊 <b>按班次查看</b>\n"
            "• /myinfoday - 点击查看白班记录\n"
            "• /myinfonight - 点击查看夜班记录\n"
        )

    if not has_records:
        text += "\n暂无记录，请先进行打卡活动"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        reply_to_message_id=message.message_id,
        parse_mode="HTML",
    )


async def show_rank(message: types.Message, shift: str = None):

    chat_id = message.chat.id
    uid = message.from_user.id

    await db.init_group(chat_id)
    activity_limits = await db.get_activity_limits_cached()

    if not activity_limits:
        await message.answer(
            "⚠️ 当前没有配置任何活动，无法生成排行榜。",
            reply_to_message_id=message.message_id,
        )
        return

    # 🧠 获取业务日期
    business_date = await db.get_business_date(chat_id)

    # 读取重置配置（原代码功能）
    group_data = await db.get_group_cached(chat_id)
    reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
    reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

    # ===== 构建标题（融合版） =====
    if shift:
        shift_text = "白班" if shift == "day" else "夜班"
        title = f"🏆 【{shift_text}】活动排行榜"
    else:
        title = "🏆 当前周期活动排行榜"

    rank_text = (
        f"{title}\n"
        f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
        f"⏰ 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
    )

    if shift:
        rank_text += f"📊 班次：<code>{'白班' if shift == 'day' else '夜班'}</code>\n\n"
    else:
        rank_text += "📊 班次：全部\n\n"

    found_any_data = False

    # ===== 遍历所有活动 =====
    for act in activity_limits.keys():
        try:
            query = """
                SELECT 
                    ds.user_id,
                    u.nickname,
                    SUM(ds.accumulated_time) AS total_time,
                    SUM(ds.activity_count) AS total_count,
                    CASE 
                        WHEN u.current_activity = $1 
                        THEN TRUE 
                        ELSE FALSE 
                    END AS is_active
                FROM daily_statistics ds
                LEFT JOIN users u 
                    ON ds.chat_id = u.chat_id 
                   AND ds.user_id = u.user_id
                WHERE ds.chat_id = $2
                  AND ds.record_date = $3
                  AND ds.activity_name = $4
            """

            params = [act, chat_id, business_date, act]

            # 🔀 班次过滤
            if shift:
                query += " AND ds.shift = $5"
                params.append(shift)

            query += """
                GROUP BY ds.user_id, u.nickname, u.current_activity
                HAVING SUM(ds.accumulated_time) > 0 OR u.current_activity = $1
                ORDER BY total_time DESC
                LIMIT 10
            """

            rows = await db.execute_with_retry(
                "获取活动排行榜", query, *params, fetch=True
            )

            if not rows:
                continue

            found_any_data = True
            rank_text += f"📈 <code>{act}</code>：\n"

            for i, row in enumerate(rows, 1):
                user_id = row["user_id"]
                nickname = row["nickname"]
                total_time = row["total_time"] or 0
                total_count = row["total_count"] or 0
                is_active = row["is_active"]

                if is_active:
                    rank_text += (
                        f"  <code>{i}.</code> 🟡 "
                        f"{MessageFormatter.format_user_link(user_id, nickname)} - 进行中\n"
                    )
                elif total_time > 0:
                    time_str = MessageFormatter.format_time(int(total_time))
                    rank_text += (
                        f"  <code>{i}.</code> 🟢 "
                        f"{MessageFormatter.format_user_link(user_id, nickname)} "
                        f"- {time_str} ({total_count}次)\n"
                    )

            rank_text += "\n"

        except Exception as e:
            logger.error(f"查询活动 {act} 排行榜失败: {e}")
            continue

    # ===== 没有任何数据 =====
    if not found_any_data:
        if shift:
            rank_text = (
                f"🏆 【{'白班' if shift == 'day' else '夜班'}】活动排行榜\n"
                f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n\n"
                f"📊 当前班次还没有活动记录\n"
                f"💪 开始第一个活动吧！\n\n"
            )
        else:
            rank_text = (
                f"🏆 当前周期活动排行榜\n"
                f"📅 统计周期：<code>{business_date.strftime('%Y-%m-%d')}</code>\n"
                f"⏰ 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n\n"
                f"📊 当前周期还没有活动记录\n"
                f"💪 开始第一个活动吧！\n\n"
                f"💡 提示：开始活动后会立即显示在这里"
            )

    # ===== 班次切换提示 =====
    if not shift:
        shift_config = await db.get_shift_config(chat_id)
        if shift_config.get("dual_mode"):
            rank_text += (
                "💡 按班次查看：\n"
                "• /rankingday - 白班排行榜\n"
                "• /rankingnight - 夜班排行榜\n"
            )

    await message.answer(
        rank_text,
        reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
    )


# ========== 快速回座回调 ==========
async def handle_quick_back(callback_query: types.CallbackQuery):
    """处理快速回座按钮 - 支持班次传递"""
    try:
        data_parts = callback_query.data.split(":")

        # 检查参数数量
        if len(data_parts) < 4:
            logger.warning(f"⚠️ 快速回座数据格式错误: {callback_query.data}")
            await callback_query.answer("❌ 按钮数据格式错误", show_alert=True)
            return

        chat_id = int(data_parts[1])
        uid = int(data_parts[2])
        shift = data_parts[3] if len(data_parts) > 3 else "day"  # 解析班次

        # 检查消息是否过期（10分钟）
        msg_ts = callback_query.message.date.timestamp()
        if time.time() - msg_ts > 600:
            await callback_query.answer(
                "⚠️ 此按钮已过期，请重新输入回座", show_alert=True
            )
            return

        # 检查是否是用户本人点击
        if callback_query.from_user.id != uid:
            await callback_query.answer("❌ 这不是您的回座按钮！", show_alert=True)
            return

        logger.info(f"🔄 快速回座: 用户{uid}, 群组{chat_id}, 班次{shift}")

        # 执行回座逻辑
        user_lock = user_lock_manager.get_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)

            if not user_data or not user_data.get("current_activity"):
                await callback_query.answer("❌ 您当前没有活动在进行", show_alert=True)
                return

            # ✅ 调用修改后的 _process_back_locked，传入班次
            await _process_back_locked(callback_query.message, chat_id, uid, shift)

        # 更新按钮状态
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            logger.warning(f"无法更新按钮状态: {e}")

        await callback_query.answer("✅ 已成功回座")

    except ValueError as e:
        logger.error(f"❌ 快速回座参数解析失败: {e}")
        await callback_query.answer("❌ 数据格式错误", show_alert=True)
    except Exception as e:
        logger.error(f"❌ 快速回座失败: {e}")
        await callback_query.answer("❌ 回座失败，请手动输入回座", show_alert=True)


# ========== 日常数据导出处理函数 =========
async def get_group_stats_from_monthly(chat_id: int, target_date: date) -> List[Dict]:
    """从月度统计表获取群组统计数据（完整版）- 修复字段缺失问题"""
    try:
        # 获取目标日期对应的月份
        month_start = target_date.replace(day=1)

        logger.info(
            f"🔍 从月度表查询数据: 群组{chat_id}, 日期{target_date}, 月份{month_start}"
        )

        # 从月度表获取数据 - 直接使用完整的月度统计数据
        monthly_stats = await db.get_monthly_statistics(
            chat_id, month_start.year, month_start.month
        )

        if not monthly_stats:
            logger.warning(f"⚠️ 月度表中没有找到 {month_start} 的数据")
            return []

        # ✅ 修复：直接返回完整数据，不进行裁剪
        # 但确保所有必要字段都存在
        result = []
        for stat in monthly_stats:
            # 确保所有字段都有默认值（防止None）
            user_data = {
                "user_id": stat.get("user_id", 0),
                "nickname": stat.get("nickname", f"用户{stat.get('user_id', 0)}"),
                "total_accumulated_time": stat.get("total_accumulated_time", 0),
                "total_activity_count": stat.get("total_activity_count", 0),
                "total_fines": stat.get("total_fines", 0),
                "overtime_count": stat.get("overtime_count", 0),
                "total_overtime_time": stat.get("total_overtime_time", 0),
                "work_days": stat.get("work_days", 0),
                "work_hours": stat.get("work_hours", 0),
                "work_start_count": stat.get("work_start_count", 0),
                "work_end_count": stat.get("work_end_count", 0),
                "work_start_fines": stat.get("work_start_fines", 0),
                "work_end_fines": stat.get("work_end_fines", 0),
                "late_count": stat.get("late_count", 0),
                "early_count": stat.get("early_count", 0),
                "activities": stat.get("activities", {}),
            }

            # 🆕 调试日志：检查工作相关字段
            logger.debug(
                f"📊 从月度表加载用户 {user_data['user_id']} 数据: "
                f"工作天数={user_data['work_days']}, "
                f"工作时长={user_data['work_hours']}秒, "
                f"上班次数={user_data['work_start_count']}, "
                f"下班次数={user_data['work_end_count']}"
            )

            result.append(user_data)

        logger.info(
            f"✅ 从月度表成功获取 {target_date} 的完整数据，共 {len(result)} 个用户"
        )
        return result

    except Exception as e:
        logger.error(f"❌ 从月度表获取数据失败: {e}")
        logger.error(traceback.format_exc())
        return []


# ========== 数据导出功能 ==========
async def export_and_push_csv(
    chat_id: int,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
    target_date=None,
    is_daily_reset: bool = False,
    from_monthly_table: bool = False,
    push_file: bool = True,
) -> bool:
    """
    导出群组数据为 CSV 并推送 - 终极完整整合版
    返回: True/False 表示导出是否成功
    """
    # ========== 0. 前置检查 ==========
    try:
        if not bot_manager or not bot_manager.bot:
            logger.error(f"❌ Bot管理器未初始化，无法导出 {chat_id}")
            if is_daily_reset:
                return True
            return False

        if not await db._ensure_healthy_connection():
            logger.error(f"❌ 数据库连接不健康，无法导出 {chat_id}")
            if is_daily_reset:
                return True
            return False

    except Exception as e:
        logger.error(f"❌ 前置检查失败 {chat_id}: {e}")
        if is_daily_reset:
            return True
        return False

    # ========== 1. 性能监控开始 ==========
    start_time = time.time()
    operation_id = f"export_{chat_id}_{int(start_time)}"
    logger.info(f"🚀 [{operation_id}] 开始导出群组 {chat_id} 的数据...")

    # 初始化变量
    temp_file = None
    group_stats = []
    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()

    try:
        # 初始化群组
        await db.init_group(chat_id)

        # ========== 2. 安全转换函数 ==========
        def safe_int(value, default=0):
            """安全转换为整数"""
            if value is None:
                return default
            try:
                if isinstance(value, str):
                    if value.isdigit():
                        return int(value)
                    elif value.replace(".", "", 1).isdigit():
                        return int(float(value))
                    else:
                        return default
                return int(value)
            except (ValueError, TypeError):
                return default

        def safe_format_time(seconds):
            """安全格式化时间"""
            try:
                return MessageFormatter.format_time_for_csv(safe_int(seconds))
            except Exception:
                return "0分0秒"

        def format_shift_for_export(shift: str) -> str:
            """格式化班次显示"""
            if not shift:
                return "白班"

            shift_lower = str(shift).lower()
            if shift_lower == "day":
                return "白班"
            if shift_lower in ["night", "night_last", "night_tonight"]:
                return "夜班"
            return "白班"

        # ========== 3. 规范日期与文件名 ==========
        beijing_now = get_beijing_time()

        if target_date is not None:
            if hasattr(target_date, "date"):
                target_date = target_date.date()
            elif not isinstance(target_date, date):
                try:
                    if isinstance(target_date, str):
                        target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
                except Exception as e:
                    logger.warning(
                        f"⚠️ [{operation_id}] 无法解析target_date: {target_date}"
                    )
                    target_date = None

        if target_date is None:
            target_date = await db.get_business_date(chat_id)

        if not file_name:
            if is_daily_reset:
                file_name = (
                    f"daily_backup_{chat_id}_{target_date.strftime('%Y%m%d')}.csv"
                )
            else:
                file_name = f"manual_export_{chat_id}_{beijing_now.strftime('%Y%m%d_%H%M%S')}.csv"

        # ========== 4. 获取统计数据 ==========
        logger.info(
            f"🔍 [{operation_id}] 获取群组 {chat_id} 的统计数据，日期: {target_date}"
        )

        if from_monthly_table:
            logger.info(f"📊 [{operation_id}] 尝试从月度表获取数据")
            try:
                group_stats = await get_group_stats_from_monthly(chat_id, target_date)
                if group_stats:
                    logger.info(
                        f"✅ [{operation_id}] 从月度表获取到 {len(group_stats)} 条完整数据"
                    )
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                else:
                    logger.warning(f"⚠️ [{operation_id}] 月度表无数据，回退到常规表")
                    from_monthly_table = False
            except Exception as e:
                logger.error(f"❌ [{operation_id}] 从月度表获取数据失败: {e}")
                from_monthly_table = False

        if not from_monthly_table:
            try:
                activity_task = asyncio.create_task(db.get_activity_limits_cached())
                stats_task = asyncio.create_task(
                    db.get_group_statistics(chat_id, target_date)
                )

                results = await asyncio.gather(
                    activity_task, stats_task, return_exceptions=True
                )

                if isinstance(results[0], Exception):
                    logger.error(f"❌ [{operation_id}] 获取活动配置失败: {results[0]}")
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                elif results[0]:
                    activity_limits = results[0]
                else:
                    activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()

                if isinstance(results[1], Exception):
                    logger.error(f"❌ [{operation_id}] 获取统计数据失败: {results[1]}")
                    group_stats = []
                elif results[1]:
                    group_stats = results[1]
                else:
                    group_stats = []

            except Exception as e:
                logger.error(f"❌ [{operation_id}] 并发获取数据失败: {e}")
                activity_limits = Config.DEFAULT_ACTIVITY_LIMITS.copy()
                group_stats = []

        # ========== 🆕 5. 数据验证和补全 ==========
        if not group_stats:
            logger.warning(f"⚠️ [{operation_id}] 群组 {chat_id} 没有数据需要导出")
            if not is_daily_reset:
                await bot.send_message(chat_id, "⚠️ 当前没有数据需要导出")
            return True

        # 验证并补全每个用户的数据
        validated_stats = []
        for idx, user_data in enumerate(group_stats):
            if not isinstance(user_data, dict):
                continue

            # 确保所有必要字段都存在
            user_id = user_data.get("user_id", f"unknown_{idx}")

            # 工作相关字段补全
            user_data["work_start_count"] = safe_int(
                user_data.get("work_start_count", 0)
            )
            user_data["work_end_count"] = safe_int(user_data.get("work_end_count", 0))
            user_data["work_start_fines"] = safe_int(
                user_data.get("work_start_fines", 0)
            )
            user_data["work_end_fines"] = safe_int(user_data.get("work_end_fines", 0))
            user_data["late_count"] = safe_int(user_data.get("late_count", 0))
            user_data["early_count"] = safe_int(user_data.get("early_count", 0))
            user_data["work_days"] = safe_int(user_data.get("work_days", 0))
            user_data["work_hours"] = safe_int(user_data.get("work_hours", 0))

            # 确保 activities 字段存在且为字典
            if "activities" not in user_data or not isinstance(
                user_data["activities"], dict
            ):
                user_data["activities"] = {}

            validated_stats.append(user_data)

            logger.debug(
                f"📊 [{operation_id}] 用户 {user_id} 数据验证完成: "
                f"上班={user_data['work_start_count']}, "
                f"下班={user_data['work_end_count']}, "
                f"工作时长={user_data['work_hours']}秒"
            )

        group_stats = validated_stats
        logger.info(
            f"📊 [{operation_id}] 数据验证完成，有效数据: {len(group_stats)} 条"
        )

        # ========== 6. 构造CSV表头 ==========
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        # ✅ 完整的表头，包含所有工作相关字段
        headers = ["用户ID", "用户昵称", "班次"]

        activity_names = sorted(activity_limits.keys())
        for act in activity_names:
            headers.extend([f"{act}次数", f"{act}总时长"])

        headers.extend(
            [
                "活动次数总计",
                "活动用时总计",
                "罚款总金额",
                "超时次数",
                "总超时时间",
                "工作天数",
                "工作时长",
                "上班次数",
                "下班次数",
                "上班罚款",
                "下班罚款",
                "迟到次数",
                "早退次数",
            ]
        )

        writer.writerow(headers)

        # ========== 7. 填充数据 ==========
        unique_users = set()
        total_records = 0
        has_valid_data = False

        for user_data in group_stats:
            total_records += 1

            user_id = user_data.get("user_id")
            if user_id:
                unique_users.add(str(user_id))

            # 检查是否有有效数据
            if any(
                [
                    safe_int(user_data.get("total_activity_count")) > 0,
                    safe_int(user_data.get("total_accumulated_time")) > 0,
                    safe_int(user_data.get("total_fines")) > 0,
                    safe_int(user_data.get("work_start_count")) > 0,
                    safe_int(user_data.get("work_end_count")) > 0,
                ]
            ):
                has_valid_data = True

            # 构建行数据
            row = [
                user_data.get("user_id", "未知"),
                user_data.get("nickname", "未知用户"),
                format_shift_for_export(user_data.get("shift", "day")),
            ]

            # 按排序后的活动名填充数据
            activities = user_data.get("activities", {})
            for act in activity_names:
                activity_info = activities.get(act, {})
                count = safe_int(activity_info.get("count"))
                time_seconds = safe_int(activity_info.get("time"))
                row.append(count)
                row.append(safe_format_time(time_seconds))

            # 填充通用统计数据
            row.extend(
                [
                    safe_int(user_data.get("total_activity_count", 0)),
                    safe_format_time(
                        safe_int(user_data.get("total_accumulated_time", 0))
                    ),
                    safe_int(user_data.get("total_fines", 0)),
                    safe_int(user_data.get("overtime_count", 0)),
                    safe_format_time(safe_int(user_data.get("total_overtime_time", 0))),
                    safe_int(user_data.get("work_days", 0)),
                    safe_format_time(safe_int(user_data.get("work_hours", 0))),
                    safe_int(user_data.get("work_start_count", 0)),
                    safe_int(user_data.get("work_end_count", 0)),
                    safe_int(user_data.get("work_start_fines", 0)),
                    safe_int(user_data.get("work_end_fines", 0)),
                    safe_int(user_data.get("late_count", 0)),
                    safe_int(user_data.get("early_count", 0)),
                ]
            )

            writer.writerow(row)

        # ========== 8. 最终数据验证 ==========
        if not has_valid_data and total_records == 0:
            logger.warning(f"⚠️ [{operation_id}] 群组 {chat_id} 没有有效数据需要导出")
            if not is_daily_reset:
                await bot.send_message(chat_id, "⚠️ 当前没有数据需要导出")
            return True

        # ========== 9. 生成CSV文件 ==========
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        temp_file = f"temp_{operation_id}_{file_name}"

        # ========== 10. 并行执行文件操作 ==========
        async def write_file_async():
            try:
                async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                    await f.write(csv_content)
                return True
            except Exception as e:
                logger.error(f"❌ [{operation_id}] 异步写入文件失败: {e}")
                try:
                    with open(temp_file, "w", encoding="utf-8-sig") as f:
                        f.write(csv_content)
                    return True
                except Exception as sync_e:
                    logger.error(f"❌ [{operation_id}] 同步写入文件也失败: {sync_e}")
                    return False

        async def get_chat_title_async():
            try:
                chat_info = await bot.get_chat(chat_id)
                return chat_info.title or f"群组 {chat_id}"
            except Exception as e:
                return f"群组 {chat_id}"

        write_result, chat_title = await asyncio.gather(
            write_file_async(), get_chat_title_async()
        )

        if not write_result:
            await bot.send_message(chat_id, f"❌ 导出失败: 文件写入失败")
            return False

        # ========== 11. 构建富文本描述 ==========
        display_date = target_date.strftime("%Y年%m月%d日")
        dashed_line = getattr(
            MessageFormatter, "create_dashed_line", lambda: "─" * 30
        )()

        caption = (
            f"📊 <b>数据导出报告</b>\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计日期：<code>{display_date}</code>\n"
            f"⏰ 导出时间：<code>{beijing_now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{dashed_line}\n"
            f"💾 包含完整的工作记录统计（上班/下班次数、迟到/早退等）"
        )

        # ========== 12. 发送到当前群组 ==========
        input_file = FSInputFile(temp_file, filename=file_name)
        send_to_group_success = False

        try:
            await bot.send_document(
                chat_id=chat_id,
                document=input_file,
                caption=caption,
                parse_mode="HTML",
            )
            send_to_group_success = True
            logger.info(f"✅ [{operation_id}] CSV文件已发送到群组 {chat_id}")
        except Exception as e:
            logger.error(f"❌ [{operation_id}] 发送到群组失败: {e}")
            await bot.send_message(chat_id, f"❌ 数据导出失败: {str(e)[:100]}")

        # ========== 13. 推送到通知服务 ==========
        if to_admin_if_no_group and notification_service:
            try:
                await notification_service.send_document(
                    chat_id, input_file, caption=caption
                )
            except Exception as e:
                logger.warning(f"⚠️ [{operation_id}] 推送到通知服务失败: {e}")

        # ========== 14. 后台清理 ==========
        async def cleanup_background():
            await asyncio.sleep(2)
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)

        asyncio.create_task(cleanup_background())

        # ========== 15. 性能统计 ==========
        duration = time.time() - start_time
        logger.info(
            f"✅ [{operation_id}] 数据导出完成\n"
            f"   文件: {file_name}\n"
            f"   用户数: {len(unique_users)}, 数据行: {total_records}\n"
            f"   耗时: {duration:.2f}秒"
        )

        return send_to_group_success

    except Exception as e:
        logger.error(f"❌ [{operation_id}] 导出过程发生异常: {e}")
        logger.error(traceback.format_exc())

        try:
            await bot.send_message(chat_id, f"❌ 数据导出失败: {str(e)[:100]}")
        except:
            pass

        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

        return False


# ========== 定时任务 ==========
async def daily_reset_task():
    """每日重置监控任务 - 统一处理单班/双班"""
    logger.info("🚀 每日重置监控任务已启动")

    # 先确保日志表存在
    try:
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reset_logs (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    mode VARCHAR(10) NOT NULL,
                    reset_date DATE NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(chat_id, mode, reset_date)
                )
            """
            )
            logger.info("✅ 重置日志表已确认")
    except Exception as e:
        logger.error(f"创建重置日志表失败: {e}")

    sem = asyncio.Semaphore(10)
    TASK_TIMEOUT = 300  # 单个群组重置超时时间（5分钟）

    async def process_group_reset(chat_id: int, now: datetime):
        """处理单个群组的重置（带超时保护）"""
        start_time = time.time()

        async with sem:
            try:
                # 使用超时保护
                async with asyncio.timeout(TASK_TIMEOUT):
                    # 1. 获取群组配置
                    group_data = await db.get_group_cached(chat_id)
                    if not group_data:
                        return

                    reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
                    reset_minute = group_data.get(
                        "reset_minute", Config.DAILY_RESET_MINUTE
                    )

                    # 2. 判断模式
                    shift_config = await db.get_shift_config(chat_id)
                    is_dual_mode = shift_config.get("dual_mode", False)

                    # 3. 根据模式选择处理器
                    if is_dual_mode:
                        await process_dual_mode_reset(
                            chat_id, now, reset_hour, reset_minute
                        )
                    else:
                        await process_single_mode_reset(
                            chat_id, now, reset_hour, reset_minute
                        )

                    # 记录执行时间
                    elapsed = time.time() - start_time
                    if elapsed > 10:  # 只记录耗时较长的
                        logger.info(f"⏱️ 群组 {chat_id} 重置完成，耗时: {elapsed:.2f}秒")

            except asyncio.TimeoutError:
                logger.error(f"❌ 群组 {chat_id} 重置超时（>{TASK_TIMEOUT}秒）")
            except Exception as e:
                logger.error(f"❌ 处理群组 {chat_id} 重置失败: {e}")
                logger.error(traceback.format_exc())

    async def process_dual_mode_reset(
        chat_id: int, now: datetime, reset_hour: int, reset_minute: int
    ):
        """处理双班模式重置 - 完整修复版"""
        from dual_shift_reset import handle_hard_reset
        from database import db

        # ===== 1. 获取业务日期 =====
        business_today = await db.get_business_date(chat_id, now)
        business_yesterday = business_today - timedelta(days=1)

        natural_today = now.date()

        # ===== 2. 计算执行时间 =====
        reset_time_today = datetime.combine(
            natural_today, dt_time(reset_hour, reset_minute)  # ✅ 更简洁
        ).replace(tzinfo=now.tzinfo)

        execute_time_today = reset_time_today + timedelta(hours=2)

        reset_time_yesterday = datetime.combine(
            natural_today - timedelta(days=1), dt_time(reset_hour, reset_minute)
        ).replace(tzinfo=now.tzinfo)

        execute_time_yesterday = reset_time_yesterday + timedelta(hours=2)

        # ===== 3. 判断执行窗口 =====
        EXECUTION_WINDOW = 300

        time_to_today = abs((now - execute_time_today).total_seconds())
        time_to_yesterday = abs((now - execute_time_yesterday).total_seconds())

        if time_to_today <= EXECUTION_WINDOW:
            target_date = business_yesterday
            period_info = "正常执行"
        elif time_to_yesterday <= EXECUTION_WINDOW:
            target_date = business_yesterday - timedelta(days=1)
            period_info = "补执行"
        else:
            return

        # ===== 4. 幂等性检查 =====
        reset_flag_key = f"dual_reset:{chat_id}:{business_today.strftime('%Y%m%d')}"
        from performance import global_cache

        if global_cache.get(reset_flag_key):
            logger.info(f"⏭️ 群组 {chat_id} 今天已执行")
            return

        logger.info(
            f"🚀 [双班重置] 群组 {chat_id}\n"
            f"   ├─ 业务今天: {business_today}\n"
            f"   ├─ 目标日期: {target_date}\n"
            f"   ├─ 执行类型: {period_info}"
        )

        # ===== 5. 执行重置 =====
        result = await handle_hard_reset(chat_id, None, target_date=target_date)

        if result is True:
            global_cache.set(reset_flag_key, True, ttl=86400)
            logger.info(f"✅ 成功")

    async def process_single_mode_reset(
        chat_id: int, now: datetime, reset_hour: int, reset_minute: int
    ):
        """处理单班模式重置 - 精确到分钟执行"""
        # 检查是否到达重置时间（精确匹配）
        if now.hour != reset_hour or now.minute != reset_minute:
            return

        # 统一幂等性检查
        if not await check_reset_idempotency(chat_id, "single", now.date()):
            return

        logger.info(
            f"🎯 [单班重置] 群组 {chat_id}\n"
            f"   • 当前时间: {now.strftime('%H:%M:%S')}\n"
            f"   • 重置时间: {reset_hour:02d}:{reset_minute:02d}"
        )

        # 计算业务日期（单班模式特有的逻辑）
        if now.hour >= 12:
            business_date = now.date()
        else:
            business_date = now.date() - timedelta(days=1)

        # 1. 导出备份 - 内部导入避免循环
        try:
            from main import export_and_push_csv

            timestamp = now.strftime("%Y%m%d_%H%M%S")
            await export_and_push_csv(
                chat_id,
                target_date=business_date,
                file_name=f"backup_{chat_id}_{business_date.strftime('%Y%m%d')}_{timestamp}.csv",
            )
        except Exception as e:
            logger.error(f"群组 {chat_id} 备份失败: {e}")

        # 2. 完成未结束活动
        await db.complete_all_pending_activities_before_reset(chat_id, now)

        # 3. 重置用户数据
        await db.force_reset_all_users_in_group(chat_id, target_date=business_date)

        # 4. 清理定时器
        if hasattr(timer_manager, "cancel_all_timers_for_group"):
            await timer_manager.cancel_all_timers_for_group(
                chat_id, preserve_message=False
            )

        # 5. 发送通知 - 内部导入避免循环
        try:
            from utils import send_reset_notification

            await send_reset_notification(chat_id, {}, now)
        except Exception as e:
            logger.error(f"群组 {chat_id} 通知发送失败: {e}")

        # 6. 设置幂等标记
        await set_reset_idempotency(chat_id, "single", now.date())
        logger.info(f"✅ [单班重置] 群组 {chat_id} 执行成功")

    async def check_reset_idempotency(
        chat_id: int, mode: str, target_date: date
    ) -> bool:
        """统一幂等性检查"""
        reset_flag_key = f"{mode}_reset:{chat_id}:{target_date.strftime('%Y%m%d')}"

        # 1. 检查缓存
        if global_cache.get(reset_flag_key):
            logger.debug(f"⏭️ 群组 {chat_id} {mode}模式 今天已执行重置")
            return False

        # 2. 检查数据库（双重保障）
        try:
            async with db.pool.acquire() as conn:
                exists = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM reset_logs 
                        WHERE chat_id = $1 AND mode = $2 AND reset_date = $3
                    )
                    """,
                    chat_id,
                    mode,
                    target_date,
                )
                if exists:
                    logger.debug(f"📦 群组 {chat_id} 数据库记录已存在重置")
                    # 同步到缓存
                    global_cache.set(reset_flag_key, True, ttl=86400)
                    return False
        except Exception as e:
            logger.error(f"数据库幂等性检查失败: {e}")
            # 出错时允许执行（防卡死）

        return True

    async def set_reset_idempotency(chat_id: int, mode: str, target_date: date):
        """设置幂等性标记"""
        reset_flag_key = f"{mode}_reset:{chat_id}:{target_date.strftime('%Y%m%d')}"
        global_cache.set(reset_flag_key, True, ttl=86400)

        # 记录到数据库
        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO reset_logs (chat_id, mode, reset_date)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (chat_id, mode, reset_date) DO NOTHING
                    """,
                    chat_id,
                    mode,
                    target_date,
                )
        except Exception as e:
            logger.error(f"记录重置日志失败: {e}")

    # ==================== 主循环 ====================
    loop_count = 0
    while True:
        try:
            loop_start = time.time()
            loop_count += 1

            now = db.get_beijing_time()  # ✅ 使用 db 的方法
            all_groups = await db.get_all_groups()

            # 只在整点或半小时记录调试日志，减少日志量
            if now.minute in [0, 30]:
                logger.debug(
                    f"🔄 第 {loop_count} 次检查，当前时间: {now.strftime('%H:%M')}, 群组数: {len(all_groups)}"
                )

            # 批量处理所有群组
            tasks = [process_group_reset(cid, now) for cid in all_groups]
            await asyncio.gather(*tasks, return_exceptions=True)

            # 记录循环执行时间（如果超过阈值）
            loop_elapsed = time.time() - loop_start
            if loop_elapsed > 10:
                logger.info(f"⏱️ 重置检查循环耗时: {loop_elapsed:.2f}秒")

        except Exception as e:
            logger.error(f"❌ 重置任务主循环出错: {e}")
            logger.error(traceback.format_exc())

        await asyncio.sleep(30)  # 30秒检查一次


# ========== 软重置定时任务 ==========
async def soft_reset_task():
    """
    每日软重置任务 - 只重置 users 表累计字段，保留 user_activities
    ✅ 每群每天只执行一次
    """
    executed_cache: dict[int, date] = {}  # 记录每个群最后一次软重置业务日期

    while True:
        now = get_beijing_time()
        logger.debug(f"软重置任务检查，当前时间: {now}")

        try:
            all_groups = await db.get_all_groups()
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}")
            await asyncio.sleep(60)
            continue

        for chat_id in all_groups:
            try:
                # 获取软重置时间
                soft_hour, soft_minute = await db.get_group_soft_reset_time(chat_id)

                # 未设置软重置，跳过
                if soft_hour == 0 and soft_minute == 0:
                    continue

                # 业务日期
                today = await db.get_business_date(chat_id)

                # 每群每天只执行一次
                if executed_cache.get(chat_id) == today:
                    continue

                # 判断是否到达软重置时间
                if now.hour == soft_hour and now.minute == soft_minute:
                    logger.info(
                        f"群组 {chat_id} 到达软重置时间 {soft_hour:02d}:{soft_minute:02d}，开始处理..."
                    )

                    # 获取群组成员
                    group_members = await db.get_group_members(chat_id)
                    reset_count = 0

                    for user_data in group_members:
                        user_lock = user_lock_manager.get_lock(
                            chat_id, user_data["user_id"]
                        )
                        async with user_lock:
                            success = await db.reset_user_soft_daily_data(
                                chat_id, user_data["user_id"]
                            )
                            if success:
                                reset_count += 1

                    # 查询当日活动记录数量
                    async with db.pool.acquire() as conn:
                        activity_count = await conn.fetchval(
                            """
                            SELECT COUNT(*) FROM user_activities
                            WHERE chat_id = $1 AND activity_date = $2
                            """,
                            chat_id,
                            today,
                        )

                    # 取消定时器
                    cancelled_count = 0
                    try:
                        if hasattr(timer_manager, "cancel_all_timers_for_group"):
                            cancelled_count = (
                                await timer_manager.cancel_all_timers_for_group(chat_id)
                            )
                    except Exception as e:
                        logger.error(f"取消定时器失败 {chat_id}: {e}")

                    # 发送通知
                    notification_text = (
                        f"🔄 <b>软重置完成</b>\n"
                        f"🏢 群组: <code>{chat_id}</code>\n"
                        f"⏰ 重置时间: <code>{soft_hour:02d}:{soft_minute:02d}</code>\n"
                        f"👥 重置用户: <code>{reset_count}</code> 人\n"
                        f"📊 保留活动记录: <code>{activity_count}</code> 条\n"
                        f"⏱️ 取消定时器: <code>{cancelled_count}</code> 个\n\n"
                        f"💡 软重置特点：\n"
                        f"• 只清除了展示/累计字段（打卡次数、我的记录）\n"
                        f"• 保留 {activity_count} 条历史活动记录\n"
                        f"• 用户可以重新打卡，历史数据已安全保存"
                    )
                    try:
                        await notification_service.send_notification(
                            chat_id, notification_text
                        )
                    except Exception as e:
                        logger.error(f"发送软重置通知失败: {e}")

                    # 标记已执行
                    executed_cache[chat_id] = today
                    logger.info(f"✅ 群组 {chat_id} 软重置完成")

            except Exception as e:
                logger.error(f"处理群组 {chat_id} 软重置失败: {e}")

        # 每分钟检查一次
        await asyncio.sleep(60)


async def memory_cleanup_task():
    """定期内存清理任务"""
    while True:
        try:
            await asyncio.sleep(Config.CLEANUP_INTERVAL)
            await performance_optimizer.memory_cleanup()
            logger.debug("定期内存清理任务完成")
        except Exception as e:
            logger.error(f"内存清理任务失败: {e}")
            await asyncio.sleep(300)


async def health_monitoring_task():
    """健康监控任务"""
    while True:
        try:
            # 检查内存使用
            if not performance_optimizer.memory_usage_ok():
                logger.warning("内存使用过高，执行紧急清理")
                await performance_optimizer.memory_cleanup()

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"健康监控任务失败: {e}")
            await asyncio.sleep(60)


# ===== ✅ 在这里添加月度维护任务 =====
async def monthly_maintenance_task():
    """
    每月维护任务：
    1. 每月1号自动导出上个月数据（包含跨天夜班）
    2. 每天凌晨3点自动清理指定天数前的数据
    """
    logger.info("📅 月度维护任务已启动")

    # 记录上次执行清理的日期，避免重复执行
    last_cleanup_date = None
    last_export_date = None

    while True:
        try:
            now = get_beijing_time()
            today = now.date()

            # ===== 1. 每天凌晨3点执行清理 =====
            if (
                now.hour == Config.CLEANUP_HOUR
                and now.minute == Config.CLEANUP_MINUTE
                and last_cleanup_date != today
            ):  # 确保每天只执行一次

                if Config.AUTO_CLEANUP_ENABLED:
                    logger.info(
                        f"🧹 开始自动清理\n"
                        f"   ├─ 日常保留: {Config.DATA_RETENTION_DAYS}天\n"
                        f"   └─ 月度保留: {Config.MONTHLY_DATA_RETENTION_DAYS}天"
                    )

                    # 清理日常数据（传天数）
                    daily_deleted = await db.cleanup_old_data(
                        Config.DATA_RETENTION_DAYS
                    )

                    # ✅ 修复：清理月度数据（传天数）
                    monthly_deleted = await db.cleanup_monthly_data(
                        Config.MONTHLY_DATA_RETENTION_DAYS
                    )

                    logger.info(
                        f"✅ 自动清理完成\n"
                        f"   ├─ 日常数据: {daily_deleted} 条\n"
                        f"   └─ 月度数据: {monthly_deleted} 条"
                    )

                    last_cleanup_date = today  # 记录今天已清理

            # ===== 2. 每月1号凌晨2点执行导出 =====
            if (
                now.day == 1
                and now.hour == Config.MONTHLY_EXPORT_HOUR
                and now.minute == Config.MONTHLY_EXPORT_MINUTE
                and last_export_date != today
            ):  # 确保每月只执行一次

                if Config.MONTHLY_EXPORT_ENABLED:
                    # 计算上个月的年月
                    if now.month == 1:
                        year = now.year - 1
                        month = 12
                    else:
                        year = now.year
                        month = now.month - 1

                    logger.info(f"📊 开始导出 {year}年{month}月 数据")

                    all_groups = await db.get_all_groups()
                    success_count = 0
                    failed_count = 0

                    for chat_id in all_groups:
                        try:
                            await db.init_group(chat_id)

                            file_name = f"monthly_{chat_id}_{year}{month:02d}.csv"
                            csv_content = await optimized_monthly_export(
                                chat_id, year, month
                            )

                            if csv_content:
                                # 发送文件
                                temp_file = f"temp_{file_name}"
                                async with aiofiles.open(
                                    temp_file, "w", encoding="utf-8-sig"
                                ) as f:
                                    await f.write(csv_content)

                                # 获取群组标题
                                try:
                                    chat_info = await bot.get_chat(chat_id)
                                    chat_title = chat_info.title or str(chat_id)
                                except:
                                    chat_title = str(chat_id)

                                caption = (
                                    f"📊 月度数据导出\n"
                                    f"🏢 群组：<code>{chat_title}</code>\n"
                                    f"📅 统计月份：<code>{year}年{month}月</code>\n"
                                    f"⏰ 导出时间：<code>{now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                                    f"{MessageFormatter.create_dashed_line()}\n"
                                    f"💾 包含跨天夜班的完整工作时长"
                                )

                                input_file = FSInputFile(temp_file, filename=file_name)
                                await bot.send_document(
                                    chat_id,
                                    input_file,
                                    caption=caption,
                                    parse_mode="HTML",
                                )

                                # 清理临时文件
                                os.remove(temp_file)

                                success_count += 1
                                logger.info(f"✅ 群组 {chat_id} 导出成功")
                            else:
                                logger.debug(f"📭 群组 {chat_id} 本月无数据")

                            # 避免请求过快
                            await asyncio.sleep(1)

                        except Exception as e:
                            failed_count += 1
                            logger.error(f"❌ 群组 {chat_id} 导出失败: {e}")
                            logger.error(traceback.format_exc())

                    logger.info(
                        f"📊 月度导出完成\n"
                        f"   ├─ 成功: {success_count} 个群组\n"
                        f"   ├─ 失败: {failed_count} 个群组\n"
                        f"   └─ 总计: {len(all_groups)} 个群组"
                    )

                    last_export_date = today  # 记录今天已导出

            # 每分钟检查一次
            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"❌ 月度维护任务异常: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(60)  # 出错后等待1分钟再试，而不是1小时


# ========== Web服务器 ==========
async def health_check(request):
    """增强版健康检查接口"""
    try:
        # 检查数据库连接
        db_healthy = await db.health_check()

        # 检查Bot状态
        bot_healthy = (
            bot_manager.is_healthy() if hasattr(bot_manager, "is_healthy") else True
        )

        # 获取详细内存信息
        memory_info = performance_optimizer.get_memory_info()  # 👈 获取详细信息
        memory_ok = memory_info.get("status") == "healthy"  # 👈 从详细info中获取状态

        status = "healthy" if all([db_healthy, bot_healthy, memory_ok]) else "degraded"

        return web.json_response(
            {
                "status": status,
                "timestamp": time.time(),
                "services": {
                    "database": db_healthy,
                    "bot": bot_healthy,
                    "memory": memory_info,  # 👈 返回详细内存信息
                },
                "version": "1.0",
                "environment": os.environ.get("BOT_MODE", "polling"),
            }
        )
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return web.json_response(
            {"status": "unhealthy", "error": str(e), "timestamp": time.time()},
            status=500,
        )


async def start_health_server():
    """优化后的健康检查服务器 - 解决 404 并保留完整功能"""
    port = int(os.getenv("PORT", 10000))
    app = web.Application()

    # 1. 根路径处理函数
    async def root_handle(request):
        return web.Response(text="Bot is running!", status=200)

    # 2. 绑定路由 (核心修复)
    app.router.add_get("/", root_handle)
    # 完美对接 keepalive_loop 的请求路径
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    # 监听 0.0.0.0 确保外部可穿透
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"✅ 健康检查服务器已在端口 {port} 启动: / 和 /health")
    return site


# ========== 服务初始化 ==========
async def initialize_services():
    """初始化所有服务 - 最终完整版（包含班次状态管理器）"""
    logger.info("🔄 初始化服务...")

    try:
        # 1. 初始化数据库
        await db.initialize()
        logger.info("✅ 数据库初始化完成")

        # 2. 启动数据库连接维护任务
        await db.start_connection_maintenance()
        logger.info("✅ 数据库维护任务已启动")

        # 3. 初始化Bot管理器
        await bot_manager.initialize()
        logger.info("✅ Bot管理器初始化完成")

        # 4. 重新获取初始化的bot和dispatcher
        global bot, dp
        bot = bot_manager.bot
        dp = bot_manager.dispatcher

        # 🎯 关键修复：导入 utils 中的全局 notification_service 实例
        from utils import notification_service as utils_notification_service
        from utils import init_notification_service

        global notification_service

        # ✅ 使用 utils 中的全局实例，而不是创建新的
        notification_service = utils_notification_service

        # ✅ 调用初始化函数设置 bot_manager 和 bot
        init_notification_service(bot_manager_instance=bot_manager, bot_instance=bot)

        # 🎯 验证设置是否成功
        if not notification_service.bot_manager:
            logger.error("❌ notification_service.bot_manager 设置失败")
        if not notification_service.bot:
            logger.error("❌ notification_service.bot 设置失败")
        else:
            logger.info(
                f"✅ 通知服务配置完成: bot_manager={notification_service.bot_manager is not None}, bot={notification_service.bot is not None}"
            )

        # 6. 设置定时器回调
        timer_manager.set_activity_timer_callback(activity_timer)
        logger.info("✅ 定时器管理器配置完成")

        # 7. 初始化心跳管理器
        await heartbeat_manager.initialize()
        logger.info("✅ 心跳管理器初始化完成")

        # 8. 启动Bot健康监控
        await bot_manager.start_health_monitor()
        logger.info("✅ Bot健康监控已启动")

        # 9. 注册日志中间件
        dp.message.middleware(LoggingMiddleware())
        logger.info("✅ 日志中间件已注册")

        # 10. 注册所有消息处理器
        await register_handlers()
        logger.info("✅ 消息处理器注册完成")

        # ========== ⭐ 新增：启动班次状态管理器 ==========
        from utils import shift_state_manager

        await shift_state_manager.start()
        logger.info("✅ 班次状态管理器已启动")

        # 11. 恢复过期活动
        recovered_count = await recover_expired_activities()
        logger.info(f"✅ 过期活动恢复完成: {recovered_count} 个活动已处理")

        # 12. 恢复班次状态
        from dual_shift_reset import recover_shift_states

        shift_recovered = await recover_shift_states()
        logger.info(f"✅ 班次状态恢复完成: {shift_recovered} 个群组")

        # 13. 🎯 最终健康检查
        health_status = await check_services_health()
        if all(health_status.values()):
            logger.info("🎉 所有服务初始化完成且健康")
        else:
            logger.warning(f"⚠️ 服务初始化完成但有警告: {health_status}")

        # ========== ✅ 14. 启动月度维护任务（导出+清理）==========
        asyncio.create_task(monthly_maintenance_task(), name="monthly_maintenance")
        logger.info("✅ 月度维护任务已启动")

        # 记录配置信息
        from config import Config

        logger.info(
            f"📅 月度维护任务配置:\n"
            f"   ├─ 清理时间: 每天 {getattr(Config, 'CLEANUP_HOUR', 3):02d}:{getattr(Config, 'CLEANUP_MINUTE', 0):02d}\n"
            f"   ├─ 日常保留: {getattr(Config, 'DATA_RETENTION_DAYS', 90)} 天\n"
            f"   ├─ 月度保留: {getattr(Config, 'MONTHLY_DATA_RETENTION_DAYS', 90)} 天\n"
            f"   └─ 导出时间: 每月1号 {getattr(Config, 'MONTHLY_EXPORT_HOUR', 2):02d}:{getattr(Config, 'MONTHLY_EXPORT_MINUTE', 0):02d}"
        )

    except Exception as e:
        logger.error(f"❌ 服务初始化失败: {e}")
        # 🎯 记录详细的调试信息
        logger.error(f"调试信息 - bot: {bot}, bot_manager: {bot_manager}")
        logger.error(
            f"调试信息 - notification_service.bot_manager: {getattr(notification_service, 'bot_manager', '未设置')}"
        )
        logger.error(
            f"调试信息 - notification_service.bot: {getattr(notification_service, 'bot', '未设置')}"
        )
        raise


async def check_services_health():
    """完整的服务健康检查"""

    health_status = {
        "database": await db.health_check(),
        "bot_manager_exists": bot_manager is not None,
        "bot_manager_has_bot": hasattr(bot_manager, "bot") if bot_manager else False,
        "bot_instance": bot is not None,
        "notification_service_bot_manager": notification_service.bot_manager
        is not None,
        "notification_service_bot": notification_service.bot is not None,
        "notification_service_has_methods": all(
            hasattr(notification_service, attr)
            for attr in ["_last_notification_time", "_rate_limit_window"]
        ),
        "timestamp": time.time(),
    }

    # 记录详细的健康状态
    healthy_services = [k for k, v in health_status.items() if v]
    unhealthy_services = [
        k for k, v in health_status.items() if not v and k != "timestamp"
    ]

    if unhealthy_services:
        logger.warning(f"⚠️ 不健康服务: {unhealthy_services}")
    else:
        logger.info(f"✅ 所有服务健康: {healthy_services}")

    return health_status


async def register_handlers():
    """注册所有消息处理器"""
    # 命令处理器
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_ci, Command("ci"))
    dp.message.register(cmd_at, Command("at"))
    dp.message.register(cmd_workstart, Command("workstart"))
    dp.message.register(cmd_workend, Command("workend"))
    dp.message.register(cmd_admin, Command("admin"))

    # ========== 固定活动命令处理器 ==========
    dp.message.register(handle_fixed_activity, Command("wc"))
    dp.message.register(handle_fixed_activity, Command("bigwc"))
    dp.message.register(handle_fixed_activity, Command("eat"))
    dp.message.register(handle_fixed_activity, Command("smoke"))
    dp.message.register(handle_fixed_activity, Command("rest"))
    dp.message.register(handle_myinfo_command, Command("myinfo"))
    dp.message.register(handle_ranking_command, Command("ranking"))
    # =======================================

    dp.message.register(cmd_export, Command("export"))
    dp.message.register(cmd_monthlyreport, Command("monthlyreport"))
    dp.message.register(cmd_exportmonthly, Command("exportmonthly"))
    dp.message.register(cmd_addactivity, Command("addactivity"))
    dp.message.register(cmd_delactivity, Command("delactivity"))
    dp.message.register(cmd_setworktime, Command("setworktime"))
    dp.message.register(cmd_setresettime, Command("setresettime"))
    dp.message.register(cmd_resettime, Command("resettime"))
    dp.message.register(cmd_setchannel, Command("setchannel"))
    dp.message.register(cmd_setgroup, Command("setgroup"))
    dp.message.register(cmd_actnum, Command("actnum"))
    dp.message.register(cmd_actstatus, Command("actstatus"))
    dp.message.register(cmd_setfines_all, Command("setfines_all"))
    dp.message.register(cmd_setfine, Command("setfine"))
    dp.message.register(cmd_finesstatus, Command("finesstatus"))
    dp.message.register(cmd_setworkfine, Command("setworkfine"))
    dp.message.register(cmd_showsettings, Command("showsettings"))
    dp.message.register(cmd_worktime, Command("worktime"))
    dp.message.register(cmd_delwork_clear, Command("delwork_clear"))
    dp.message.register(cmd_cleanup_monthly, Command("cleanup_monthly"))
    dp.message.register(cmd_monthly_stats_status, Command("monthly_stats_status"))
    dp.message.register(cmd_cleanup_inactive, Command("cleanup_inactive"))
    dp.message.register(cmd_reset_user, Command("resetuser"))
    dp.message.register(cmd_setsoftresettime, Command("setsoftresettime"))
    dp.message.register(cmd_softresettime, Command("softresettime"))
    dp.message.register(cmd_fix_message_refs, Command("fixmessages"))

    dp.message.register(cmd_setdualmode, Command("setdualmode"))
    dp.message.register(cmd_setshiftgrace, Command("setshiftgrace"))
    dp.message.register(handle_ranking_shift_command, Command("ranking"))
    dp.message.register(handle_ranking_day_command, Command("rankingday"))
    dp.message.register(handle_ranking_night_command, Command("rankingnight"))
    dp.message.register(handle_myinfo_day_command, Command("myinfoday"))
    dp.message.register(handle_myinfo_night_command, Command("myinfonight"))
    dp.message.register(cmd_addextraworkgroup, Command("addextraworkgroup"))
    dp.message.register(cmd_clearextraworkgroup, Command("clearextraworkgroup"))
    dp.message.register(cmd_showeverypush, Command("showeverypush"))
    dp.message.register(cmd_checkdualsetup, Command("checkdual"))
    dp.message.register(cmd_testgroupaccess, Command("testgroupaccess"))
    dp.message.register(cmd_checkbotpermissions, Command("checkperms"))
    dp.message.register(cmd_setworkendgrace, Command("setworkendgrace"))

    # 按钮处理器
    dp.message.register(
        handle_back_command,
        lambda message: message.text and message.text.strip() in ["✅ 回座", "回座"],
    )
    dp.message.register(
        handle_work_buttons,
        lambda message: message.text and message.text.strip() in ["🟢 上班", "🔴 下班"],
    )
    dp.message.register(
        handle_export_button,
        lambda message: message.text and message.text.strip() in ["📤 导出数据"],
    )
    dp.message.register(
        handle_my_record,
        lambda message: message.text and message.text.strip() in ["📊 我的记录"],
    )
    dp.message.register(
        handle_rank,
        lambda message: message.text and message.text.strip() in ["🏆 排行榜"],
    )
    dp.message.register(
        handle_admin_panel_button,
        lambda message: message.text and message.text.strip() in ["👑 管理员面板"],
    )
    dp.message.register(
        handle_back_to_main_menu,
        lambda message: message.text and message.text.strip() in ["🔙 返回主菜单"],
    )
    dp.message.register(
        handle_all_text_messages, lambda message: message.text and message.text.strip()
    )

    # 回调处理器
    dp.callback_query.register(
        handle_quick_back, lambda c: c.data.startswith("quick_back:")
    )

    logger.info("✅ 所有消息处理器注册完成")


async def keepalive_loop():
    """完整的保活循环: 外部保活 + 内部检查 + 数据库保活 + 内存回收"""
    external_url = os.environ.get("RENDER_EXTERNAL_URL") or getattr(
        Config, "WEBHOOK_URL", None
    )
    if external_url:
        external_url = external_url.rstrip("/")

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 保活循环启动 | 外部URL: {external_url or '未设置'} | 端口: {port}")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20),
        headers={"User-Agent": "Bot-KeepAlive-Service"},
    ) as session:
        while True:
            try:
                # 保持原有的 5 分钟频率
                await asyncio.sleep(300)

                # 1. 外部公网保活
                if external_url:
                    try:
                        async with session.get(f"{external_url}/health") as resp:
                            if resp.status != 200:
                                logger.warning(
                                    f"🌍 外部保活异常 | 状态码: {resp.status}"
                                )
                            else:
                                logger.debug("🌍 外部保活成功")
                    except Exception as e:
                        logger.warning(f"🌍 外部保活失败: {e}")

                # 2. 内部健康检查
                try:
                    async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                        if resp.status != 200:
                            logger.warning(
                                f"🏠 内部健康检查异常 | 状态码: {resp.status}"
                            )
                except Exception as e:
                    logger.warning(f"🏠 内部健康检查失败: {e}")

                # 3. ✅ 补回：数据库连接保活
                try:
                    if hasattr(db, "connection_health_check"):
                        await db.connection_health_check()
                except Exception as e:
                    logger.warning(f"🗄️ 数据库保活异常: {e}")

                # 4. ✅ 补回：内存回收 (GC)
                try:
                    collected = gc.collect()
                    if collected > 0:
                        logger.debug(f"🧹 GC 回收对象数: {collected}")
                except Exception:
                    pass

            except asyncio.CancelledError:
                logger.info("🛑 保活循环已取消")
                break
            except Exception as e:
                logger.error(f"⚠️ 保活循环遇到异常: {e}")
                await asyncio.sleep(60)


# ========== 启动流程 =========
async def on_startup():
    """启动时执行 - 解决冲突并保留完整指令逻辑"""
    logger.info("🎯 机器人启动中...")
    try:
        # ✅ 新增：强行踢掉其他冲突实例，确保线上唯一运行
        await bot_manager.bot.delete_webhook(drop_pending_updates=True)

        # 1. 定义指令列表
        user_commands = [
            BotCommand(command="wc", description="🚽 小厕"),
            BotCommand(command="bigwc", description="🚻 大厕"),
            BotCommand(command="eat", description="🍚 吃饭"),
            BotCommand(command="smoke", description="🚬 抽烟"),
            BotCommand(command="rest", description="🛌 休息"),
            BotCommand(command="workstart", description="🟢 上班打卡"),
            BotCommand(command="workend", description="🔴 下班打卡"),
            BotCommand(command="at", description="✅ 回座"),
            BotCommand(command="myinfo", description="📊 我的记录"),
            BotCommand(command="ranking", description="🏆 排行榜"),
            BotCommand(command="help", description="❓ 使用帮助"),
        ]

        admin_commands = user_commands + [
            BotCommand(command="actstatus", description="📊 活跃活动统计"),
            BotCommand(command="showsettings", description="⚙️ 查看系统配置"),
            BotCommand(command="finesstatus", description="📈 罚款费率查询"),
            BotCommand(command="worktime", description="⌚ 考勤时间设置"),
            BotCommand(command="export", description="📤 导出今日报表"),
            BotCommand(command="checkdb", description="🏥 数据库体检"),
            BotCommand(command="admin", description="🛠 管理员全指令指南"),
        ]

        # ✅ 打印你需要的注册日志
        logger.info(f"📋 要注册的命令列表: {[cmd.command for cmd in user_commands]}")

        # 2. 注册普通用户菜单
        res_user = await bot_manager.bot.set_my_commands(commands=user_commands)
        logger.info(f"✅ 普通用户命令注册结果: {res_user}")

        # 3. 注册管理员菜单
        res_admin = await bot_manager.bot.set_my_commands(
            commands=admin_commands, scope=BotCommandScopeAllChatAdministrators()
        )
        logger.info(f"✅ 管理员指令菜单注册结果: {res_admin}")

        # 4. 初始化数据库
        if hasattr(db, "init"):
            await db.init()

        await send_startup_notification()
        logger.info("✅ 系统启动完成，准备接收消息")

    except Exception as e:
        logger.error(f"❌ 启动过程异常: {e}")
        raise


async def on_shutdown():
    """关闭时执行 - 更新版本（包含班次状态管理器）"""
    logger.info("🛑 机器人正在关闭...")
    try:
        # 停止数据库维护任务
        await db.stop_connection_maintenance()
        logger.info("✅ 数据库维护任务已停止")

        # 停止Bot管理器
        await bot_manager.stop()
        logger.info("✅ Bot管理器已停止")

        # 取消所有定时器
        cancelled_count = await timer_manager.cancel_all_timers()
        logger.info(f"✅ 已取消 {cancelled_count} 个活动定时器")

        # 停止心跳管理器
        await heartbeat_manager.stop()
        logger.info("✅ 心跳管理器已停止")

        # ========== ⭐ 新增：停止班次状态管理器 ==========
        from utils import shift_state_manager

        await shift_state_manager.stop()
        logger.info("✅ 班次状态管理器已停止")

        # 发送关闭通知
        await send_shutdown_notification()
        logger.info("✅ 关闭通知已发送")

        logger.info("🎉 所有服务已优雅关闭")
    except Exception as e:
        logger.error(f"关闭清理过程中出错: {e}")


async def main():
    """全环境通用 - 工业级稳固版 (适配 Render/VPS/Docker)"""
    # 1. 环境检测
    is_render = "RENDER" in os.environ
    health_server_site = None  # 用于存储健康服务器实例

    if is_render:
        logger.info("🎯 检测到 Render 环境，应用低功耗安全配置")
        Config.DB_MAX_CONNECTIONS = 3
        Config.ENABLE_FILE_LOGGING = False

    try:
        logger.info("🚀 启动打卡机器人系统...")

        # 2. 初始化核心服务（数据库等）
        await initialize_services()

        # 3. 启动健康检查服务器 (适配 Render 端口)
        # 修改点：保存返回值 site，以便后续安全关闭
        health_server_site = await start_health_server()

        # 4. 启动周期性后台任务
        background_tasks = [
            asyncio.create_task(daily_reset_task(), name="daily_reset"),
            asyncio.create_task(soft_reset_task(), name="soft_reset"),
            asyncio.create_task(memory_cleanup_task(), name="memory_cleanup"),
            asyncio.create_task(health_monitoring_task(), name="health_monitor"),
            asyncio.create_task(
                monthly_maintenance_task(), name="monthly_maintenance"
            ),  # 如果有的话
        ]

        # 针对 Render 的保活任务
        if is_render:
            background_tasks.append(
                asyncio.create_task(keepalive_loop(), name="render_keepalive")
            )

        # 5. 启动机器人逻辑
        await on_startup()

        # 将 Polling 放入后台独立任务
        polling_task = asyncio.create_task(
            bot_manager.start_polling_with_retry(), name="telegram_polling"
        )

        logger.info("🤖 机器人系统全功能已就绪")

        # 6. 核心：钉死进程，不让程序退出
        # 这样即便 Polling 崩溃重启，主程序和 Web Server 依然活着
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info("👋 收到系统关闭指令")
    except Exception as e:
        logger.error(f"❌ 系统运行异常: {e}")
        if is_render:
            sys.exit(1)  # 告诉 Render 启动失败，触发自动重启
    finally:
        logger.info("🛑 开始清理并优雅关闭...")

        # A. 停止轮询
        if "polling_task" in locals():
            polling_task.cancel()
            with suppress(asyncio.CancelledError):
                await polling_task

        # B. 关闭健康服务器（关键：防止重启时端口占用）
        if health_server_site:
            with suppress(Exception):
                await health_server_site.stop()
                logger.info("✅ 健康检查服务器已释放端口")

        # C. 停止所有后台任务
        if "background_tasks" in locals():
            for task in background_tasks:
                task.cancel()

        # D. 执行统一的清理逻辑（关闭数据库等）
        await on_shutdown()
        logger.info("🎉 进程已安全结束")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("机器人已被用户中断")
    except Exception as e:
        logger.error(f"机器人运行异常: {e}")
