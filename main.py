import asyncio
import logging
import sys
import time
import traceback
from functools import wraps
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List
from contextlib import suppress


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
    NotificationService,
    get_beijing_time,
    calculate_cross_day_time_diff,
    is_valid_checkin_time,
    rate_limit,
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

import csv
import os
from io import StringIO
import aiofiles

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
    """自动结束当前活动"""
    try:
        act = user_data["current_activity"]
        start_time_dt = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = int((now - start_time_dt).total_seconds())

        # 完成活动（不计算罚款，因为是自动结束）
        await db.complete_user_activity(chat_id, uid, act, elapsed, 0, False)

        # 取消定时器
        await timer_manager.cancel_timer(f"{chat_id}-{uid}")

        logger.info(f"自动结束活动: {chat_id}-{uid} - {act}")

    except Exception as e:
        logger.error(f"自动结束活动失败 {chat_id}-{uid}: {e}")


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
        f"• 总罚款金额：<code>{total_fines}</code> 元\n\n"
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
            f"• 上下班罚款：<code>{total_work_fines}</code> 元\n\n"
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
    """处理已过期的活动（用于服务重启后的恢复）"""
    try:
        now = get_beijing_time()
        elapsed = int((now - start_time).total_seconds())
        nickname = "用户"

        # 获取用户信息
        user_data = await db.get_user_cached(chat_id, user_id)
        if user_data:
            nickname = user_data.get("nickname", str(user_id))

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
            chat_id, user_id, activity, elapsed, fine_amount, is_overtime
        )

        # 发送恢复通知
        timeout_msg = (
            f"🔄 <b>系统恢复通知</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
            f"📝 检测到未结束的活动：<code>{activity}</code>\n"
            f"⏰ 活动开始时间：<code>{start_time.strftime('%m/%d %H:%M:%S')}</code>\n"
            f"⏱️ 活动总时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>\n"
            f"⚠️ 由于服务重启，您的活动已自动结束"
        )

        if fine_amount > 0:
            timeout_msg += f"\n💰 超时罚款：<code>{fine_amount}</code> 元"

        await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
        logger.info(f"已处理过期活动: {chat_id}-{user_id} - {activity}")

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
    """精确版每日数据重置"""
    try:
        now = get_beijing_time()

        # 获取群组自定义重置时间
        group_info = await db.get_group_cached(chat_id)
        if not group_info:
            await db.init_group(chat_id)
            group_info = await db.get_group_cached(chat_id)

        reset_hour = group_info.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_info.get("reset_minute", Config.DAILY_RESET_MINUTE)

        # 计算当前重置周期开始时间
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        if now < reset_time_today:
            current_period_start = reset_time_today - timedelta(days=1)
        else:
            current_period_start = reset_time_today

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        if not user_data:
            await db.init_user(chat_id, uid, "用户")
            return

        last_updated_str = user_data.get("last_updated")
        if not last_updated_str:
            await db.reset_user_daily_data(chat_id, uid, now.date())
            await db.update_user_last_updated(chat_id, uid, now.date())
            return

        # 解析最后更新时间
        last_updated = None
        if isinstance(last_updated_str, str):
            try:
                last_updated = datetime.fromisoformat(
                    str(last_updated_str).replace("Z", "+00:00")
                )
            except ValueError:
                try:
                    last_updated = datetime.strptime(str(last_updated_str), "%Y-%m-%d")
                except ValueError:
                    last_updated = now
        elif isinstance(last_updated_str, datetime):
            last_updated = last_updated_str
        else:
            last_updated = now

        # 比较最后更新时间是否在当前重置周期之前
        if last_updated.date() < current_period_start.date():
            logger.info(f"重置用户数据: {chat_id}-{uid}")
            await db.reset_user_daily_data(chat_id, uid, current_period_start.date())
            await db.update_user_last_updated(chat_id, uid, now.date())

    except Exception as e:
        logger.error(f"重置检查失败 {chat_id}-{uid}: {e}")
        try:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, datetime.now().date())
        except Exception as init_error:
            logger.error(f"用户初始化也失败: {init_error}")


async def check_activity_limit(
    chat_id: int, uid: int, act: str
) -> tuple[bool, int, int]:
    """检查活动次数是否达到上限"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)

    current_count = await db.get_user_activity_count(chat_id, uid, act)
    max_times = await db.get_activity_max_times(act)

    return current_count < max_times, current_count, max_times


async def has_active_activity(chat_id: int, uid: int) -> tuple[bool, Optional[str]]:
    """检查用户是否有活动正在进行"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)
    return user_data["current_activity"] is not None, user_data["current_activity"]


async def can_perform_activities(chat_id: int, uid: int) -> tuple[bool, str]:
    """快速检查是否可以执行活动 - 修复版：使用重置周期"""
    if not await db.has_work_hours_enabled(chat_id):
        return True, ""

    # 🆕 先执行重置检查，确保数据状态正确
    await reset_daily_data_if_needed(chat_id, uid)

    # 使用修复后的 get_today_work_records（现在基于重置周期）
    today_records = await db.get_today_work_records(chat_id, uid)

    if "work_start" not in today_records:
        return False, "❌ 请先打上班卡！"

    if "work_end" in today_records:
        return False, "❌ 已下班，无法进行活动！"

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
async def activity_timer(chat_id: int, uid: int, act: str, limit: int):
    """活动定时提醒任务"""
    try:
        one_minute_warning_sent = False
        timeout_immediate_sent = False
        timeout_5min_sent = False
        last_reminder_minute = 0

        while True:
            user_lock = user_lock_manager.get_lock(chat_id, uid)
            async with user_lock:
                user_data = await db.get_user_cached(chat_id, uid)
                if not user_data or user_data["current_activity"] != act:
                    break

                start_time = datetime.fromisoformat(user_data["activity_start_time"])
                elapsed = (get_beijing_time() - start_time).total_seconds()
                remaining = limit * 60 - elapsed

                nickname = user_data.get("nickname", str(uid))

            # 1分钟前警告
            if 0 < remaining <= 60 and not one_minute_warning_sent:
                warning_msg = (
                    f"⏳ <b>即将超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"🕓 您本次 {MessageFormatter.format_copyable_text(act)} 还有 <code>1</code> 分钟即将超时！\n"
                    f"💡 请及时回座，避免超时罚款"
                )
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )
                await bot.send_message(
                    chat_id, warning_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                one_minute_warning_sent = True

            # 超时提醒
            if remaining <= 0:
                overtime_minutes = int(-remaining // 60)

                if overtime_minutes == 0 and not timeout_immediate_sent:
                    timeout_msg = (
                        f"⚠️ <b>超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经<code>超时</code>！\n"
                        f"🏃‍♂️ 请立即回座，避免产生更多罚款！"
                    )
                    back_keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="👉 点击✅立即回座 👈",
                                    callback_data=f"quick_back:{chat_id}:{uid}",
                                )
                            ]
                        ]
                    )
                    await bot.send_message(
                        chat_id,
                        timeout_msg,
                        parse_mode="HTML",
                        reply_markup=back_keyboard,
                    )
                    timeout_immediate_sent = True
                    last_reminder_minute = 0

                elif overtime_minutes == 5 and not timeout_5min_sent:
                    timeout_msg = (
                        f"🔔 <b>超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>5</code> 分钟！\n"
                        f"😤 请立即回座，避免罚款增加！"
                    )
                    back_keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="👉 点击✅立即回座 👈",
                                    callback_data=f"quick_back:{chat_id}:{uid}",
                                )
                            ]
                        ]
                    )
                    await bot.send_message(
                        chat_id,
                        timeout_msg,
                        parse_mode="HTML",
                        reply_markup=back_keyboard,
                    )
                    timeout_5min_sent = True
                    last_reminder_minute = 5

                elif (
                    overtime_minutes >= 10
                    and overtime_minutes % 10 == 0
                    and overtime_minutes > last_reminder_minute
                ):
                    timeout_msg = (
                        f"🚨 <b>超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>{overtime_minutes}</code> 分钟！\n"
                        f"💢 请立即回座！"
                    )
                    back_keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="👉 点击✅立即回座 👈",
                                    callback_data=f"quick_back:{chat_id}:{uid}",
                                )
                            ]
                        ]
                    )
                    await bot.send_message(
                        chat_id,
                        timeout_msg,
                        parse_mode="HTML",
                        reply_markup=back_keyboard,
                    )
                    last_reminder_minute = overtime_minutes

            # 检查超时强制回座
            user_lock = user_lock_manager.get_lock(chat_id, uid)
            async with user_lock:
                user_data = await db.get_user_cached(chat_id, uid)
                if user_data and user_data["current_activity"] == act:
                    if remaining <= -120 * 60:  # 2小时强制回座
                        overtime_minutes = 120
                        overtime_seconds = 120 * 60
                        fine_amount = await calculate_fine(act, overtime_minutes)

                        elapsed = (
                            get_beijing_time()
                            - datetime.fromisoformat(user_data["activity_start_time"])
                        ).total_seconds()

                        await db.complete_user_activity(
                            chat_id, uid, act, int(elapsed), fine_amount, True
                        )

                        auto_back_msg = (
                            f"🛑 <b>自动安全回座</b>\n"
                            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                            f"📝 活动：<code>{act}</code>\n"
                            f"⚠️ 由于超时超过2小时，系统已自动为您回座\n"
                            f"⏰ 超时时长：<code>120</code> 分钟\n"
                            f"💰 本次罚款：<code>{fine_amount}</code> 元\n"
                            f"💢 请检查是否忘记回座！"
                        )
                        await bot.send_message(
                            chat_id, auto_back_msg, parse_mode="HTML"
                        )
                        await timer_manager.cancel_timer(f"{chat_id}-{uid}")
                        break

            await asyncio.sleep(30)

    except asyncio.CancelledError:
        logger.info(f"定时器 {chat_id}-{uid} 被取消")
    except Exception as e:
        logger.error(f"定时器错误: {e}")


# ========== 核心打卡功能 ==========
async def start_activity(message: types.Message, act: str):
    """开始活动"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await reset_daily_data_if_needed(chat_id, uid)
        # 快速检查
        if not await db.activity_exists(act):
            await message.answer(f"❌ 活动 '{act}' 不存在")
            return

        # 检查活动限制
        can_perform, reason = await can_perform_activities(chat_id, uid)
        if not can_perform:
            await message.answer(reason)
            return

        # 开始活动逻辑
        name = message.from_user.full_name
        now = get_beijing_time()

        # 检查活动人数限制
        user_limit = await db.get_activity_user_limit(act)
        if user_limit > 0:
            current_users = await db.get_current_activity_users(chat_id, act)
            if current_users >= user_limit:
                await message.answer(
                    f"❌ 打卡失败~ 活动 '<code>{act}</code>' 人数已满！\n\n"
                    f"📊 当前状态：\n"
                    f"• 限制人数：<code>{user_limit}</code> 人\n"
                    f"• 当前进行：<code>{current_users}</code> 人\n"
                    f"• 剩余名额：<code>0</code> 人\n\n"
                    f"💡 请等待其他用户回座后再打卡进行此活动",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
                return

        has_active, current_act = await has_active_activity(chat_id, uid)
        if has_active:
            await message.answer(
                Config.MESSAGES["has_activity"].format(current_act),
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        # 先重置数据（如果需要）
        await reset_daily_data_if_needed(chat_id, uid)

        can_start, current_count, max_times = await check_activity_limit(
            chat_id, uid, act
        )
        if not can_start:
            await message.answer(
                Config.MESSAGES["max_times_reached"].format(act, max_times),
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        await db.update_user_activity(chat_id, uid, act, str(now), name)
        time_limit = await db.get_activity_time_limit(act)

        await timer_manager.start_timer(chat_id, uid, act, time_limit)

        await message.answer(
            MessageFormatter.format_activity_message(
                uid,
                name,
                act,
                now.strftime("%m/%d %H:%M:%S"),
                current_count + 1,
                max_times,
                time_limit,
            ),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )


# ========== 回座功能 ==========
async def process_back(message: types.Message):
    """回座打卡"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = user_lock_manager.get_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)


async def _process_back_locked(message: types.Message, chat_id: int, uid: int):
    """线程安全的回座逻辑"""
    start_time = time.time()
    key = f"{chat_id}:{uid}"

    # 防重入检测
    if active_back_processing.get(key):
        await message.answer("⚠️ 您的回座请求正在处理中，请稍候。")
        return
    active_back_processing[key] = True

    try:
        now = get_beijing_time()

        user_data = await db.get_user_cached(chat_id, uid)
        if not user_data or not user_data.get("current_activity"):
            await message.answer(
                Config.MESSAGES["no_activity"],
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        act = user_data["current_activity"]
        start_time_dt = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = (now - start_time_dt).total_seconds()

        # 获取活动时间限制
        time_limit_minutes = await db.get_activity_time_limit(act)
        time_limit_seconds = time_limit_minutes * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        # 完成活动
        await db.complete_user_activity(
            chat_id, uid, act, int(elapsed), fine_amount, is_overtime
        )

        # 取消定时任务
        await timer_manager.cancel_timer(f"{chat_id}-{uid}")

        # 获取用户最新数据
        user_data = await db.get_user_cached(chat_id, uid)
        user_activities = await db.get_user_all_activities(chat_id, uid)

        activity_counts = {
            a: info.get("count", 0) for a, info in user_activities.items()
        }

        await message.answer(
            MessageFormatter.format_back_message(
                user_id=uid,
                user_name=user_data.get("nickname", "未知用户"),
                activity=act,
                time_str=now.strftime("%m/%d %H:%M:%S"),
                elapsed_time=MessageFormatter.format_time(int(elapsed)),
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
            ),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )

        # 超时通知推送
        if is_overtime and fine_amount > 0:
            try:
                chat_title = str(chat_id)
                try:
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = chat_info.title or chat_title
                except Exception:
                    pass

                notif_text = (
                    f"🚨 <b>超时回座通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, user_data.get('nickname', '未知用户'))}\n"
                    f"📝 活动：<code>{act}</code>\n"
                    f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
                    f"⏱️ 超时：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code>\n"
                    f"💰 罚款：<code>{fine_amount}</code> 元"
                )
                await notification_service.send_notification(chat_id, notif_text)
            except Exception as e:
                logger.error(f"超时通知推送异常: {e}")

    except Exception as e:
        logger.error(f"回座处理异常: {e}")
        await message.answer("❌ 回座失败，请稍后重试。")

    finally:
        active_back_processing.pop(key, None)
        duration = round(time.time() - start_time, 2)
        logger.info(f"回座结束 chat_id={chat_id}, uid={uid}，耗时 {duration}s")


# ========== 上下班打卡功能 ==========


async def process_work_checkin(message: types.Message, checkin_type: str):
    """
    智能化上下班打卡系统（跨天安全修复版）
    保留全部原有功能 + 增强智能判断、错误容错、日志追踪。
    """

    chat_id = message.chat.id
    uid = message.from_user.id
    name = message.from_user.full_name
    now = get_beijing_time()
    current_time = now.strftime("%H:%M")
    today = str(now.date())
    trace_id = f"{chat_id}-{uid}-{int(time.time())}"

    logger.info(f"🟢[{trace_id}] 开始处理 {checkin_type} 打卡请求：{name}({uid})")

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        # ✅ 初始化群组与用户数据
        await reset_daily_data_if_needed(chat_id, uid)
        try:
            await db.init_group(chat_id)
            await db.init_user(chat_id, uid)
            user_data = await db.get_user_cached(chat_id, uid)
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 初始化用户/群组失败: {e}")
            await message.answer("⚠️ 数据初始化失败，请稍后再试。")
            return

        # ✅ 检查是否重复打卡
        try:
            has_record_today = await db.has_work_record_today(
                chat_id, uid, checkin_type
            )
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 检查重复打卡失败: {e}")
            has_record_today = False  # 允许继续执行但记录日志

        if has_record_today:
            today_records = await db.get_today_work_records(chat_id, uid)
            existing_record = today_records.get(checkin_type)
            action_text = "上班" if checkin_type == "work_start" else "下班"
            status_msg = f"🚫 您今天已经打过{action_text}卡了！"

            if existing_record:
                existing_time = existing_record["checkin_time"]
                existing_status = existing_record["status"]
                status_msg += f"\n⏰ 打卡时间：<code>{existing_time}</code>"
                status_msg += f"\n📊 状态：{existing_status}"

            await message.answer(
                status_msg,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )
            logger.info(f"[{trace_id}] 🔁 检测到重复{action_text}打卡，终止处理。")
            return

        # 🆕 添加异常情况检查：已经下班但又打上班卡
        if checkin_type == "work_start":
            has_work_end_today = await db.has_work_record_today(
                chat_id, uid, "work_end"
            )
            if has_work_end_today:
                today_records = await db.get_today_work_records(chat_id, uid)
                end_record = today_records.get("work_end")
                end_time = end_record["checkin_time"] if end_record else "未知时间"

                await message.answer(
                    f"🚫 您今天已经在 <code>{end_time}</code> 打过下班卡，无法再打上班卡！\n"
                    f"💡 如需重新打卡，请联系管理员或等待次日自动重置",
                    reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
                    parse_mode="HTML",
                )
                logger.info(f"[{trace_id}] 🔁 检测到异常：下班后再次上班打卡")
                return

        # ✅ 自动结束活动（仅下班）
        current_activity = user_data.get("current_activity")
        activity_auto_ended = False
        if checkin_type == "work_end" and current_activity:
            with suppress(Exception):
                await auto_end_current_activity(chat_id, uid, user_data, now, message)
                activity_auto_ended = True
                logger.info(f"[{trace_id}] 🔄 已自动结束活动：{current_activity}")

        # ✅ 下班前检查上班记录
        if checkin_type == "work_end":
            has_work_start_today = await db.has_work_record_today(
                chat_id, uid, "work_start"
            )
            if not has_work_start_today:
                await message.answer(
                    "❌ 您今天还没有打上班卡，无法打下班卡！\n"
                    "💡 请先使用'🟢 上班'按钮或 /workstart 命令打上班卡",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
                logger.warning(f"[{trace_id}] ⚠️ 用户试图下班打卡但未上班")
                return

        # 🆕 添加时间范围检查（放在获取工作时间设置之前）
        try:
            valid_time, expected_dt = await is_valid_checkin_time(
                chat_id, checkin_type, now
            )
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ is_valid_checkin_time 调用失败: {e}")
            valid_time, expected_dt = True, now  # 避免误伤，默认允许

        if not valid_time:
            # 计算可打卡窗口的起止时间（基于选中的 expected_dt）
            allowed_start = (expected_dt - timedelta(hours=7)).strftime(
                "%Y-%m-%d %H:%M"
            )
            allowed_end = (expected_dt + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M")

            # 显示更友好的本地化提示（包含日期，避免跨天误解）
            await message.answer(
                f"⏰ 当前时间不在允许的打卡范围内（前后7小时规则）！\n\n"
                f"📅 期望打卡时间（参考）：<code>{expected_dt.strftime('%H:%M')}</code>\n"
                f"🕒 允许范围（含日期）：\n"
                f"   • 开始：<code>{allowed_start}</code>\n"
                f"   • 结束：<code>{allowed_end}</code>\n\n"
                f"💡 如果你确认时间有特殊情况，请联系管理员处理。",
                reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
                parse_mode="HTML",
            )
            logger.info(
                f"[{trace_id}] ⏰ 打卡时间范围检查失败（不在 ±7 小时内），终止处理"
            )
            return

        # ✅ 获取工作时间设置
        work_hours = await db.get_group_work_time(chat_id)
        expected_time = work_hours[checkin_type]

        # ✅ 计算时间差（含跨天）
        time_diff_minutes, expected_dt = calculate_cross_day_time_diff(
            now, expected_time, checkin_type
        )
        time_diff_hours = abs(time_diff_minutes / 60)

        # ✅ 时间异常修正
        if time_diff_hours > 24:
            logger.warning(
                f"[{trace_id}] ⏰ 异常时间差检测 {time_diff_hours}小时，自动纠正为0"
            )
            time_diff_minutes = 0

        # ✅ 格式化时间差
        def format_time_diff(minutes: float) -> str:
            mins = int(abs(minutes))
            h, m = divmod(mins, 60)
            if h > 0:
                return f"{h}小时{m}分"
            return f"{m}分钟"

        time_diff_str = format_time_diff(time_diff_minutes)
        fine_amount = 0
        is_late_early = False

        # ✅ 打卡状态判断
        if checkin_type == "work_start":
            if time_diff_minutes > 0:
                fine_amount = await calculate_work_fine("work_start", time_diff_minutes)
                status = f"🚨 迟到 {time_diff_str}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "😅"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "上班"
        else:
            if time_diff_minutes < 0:
                fine_amount = await calculate_work_fine(
                    "work_end", abs(time_diff_minutes)
                )
                status = f"🚨 早退 {time_diff_str}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "🏃"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "下班"

        # ✅ 安全写入数据库（含重试）
        for attempt in range(2):
            try:
                await db.add_work_record(
                    chat_id,
                    uid,
                    today,
                    checkin_type,
                    current_time,
                    status,
                    time_diff_minutes,
                    fine_amount,
                )
                break
            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 数据写入失败，第{attempt+1}次尝试: {e}")
                if attempt == 1:
                    await message.answer("⚠️ 数据保存失败，请稍后再试。")
                    return
                await asyncio.sleep(0.5)

        expected_time_display = expected_dt.strftime("%m/%d %H:%M")
        result_msg = (
            f"{emoji} <b>{action_text}打卡完成</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
            f"⏰ 打卡时间：<code>{current_time}</code>\n"
            f"📅 期望时间：<code>{expected_time_display}</code>\n"
            f"📊 状态：{status}"
        )

        if checkin_type == "work_end" and activity_auto_ended and current_activity:
            result_msg += (
                f"\n\n🔄 检测到未结束活动 <code>{current_activity}</code>，已自动结束"
            )

        await message.answer(
            result_msg,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )

        # ✅ 智能通知模块
        if is_late_early:
            try:
                status_type = "迟到" if checkin_type == "work_start" else "早退"
                time_detail = f"{status_type} {time_diff_str}"

                with suppress(Exception):
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = getattr(chat_info, "title", str(chat_id))
                notif_text = (
                    f"⚠️ <b>{action_text}{status_type}通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                    f"⏰ 打卡时间：<code>{current_time}</code>\n"
                    f"📅 期望时间：<code>{expected_time_display}</code>\n"
                    f"⏱️ {time_detail}"
                )
                if fine_amount:
                    notif_text += f"\n💰 罚款金额：<code>{fine_amount}</code> 元"

                sent = await notification_service.send_notification(chat_id, notif_text)
                if not sent:
                    logger.warning(f"[{trace_id}] ⚠️ 通知发送失败，尝试管理员兜底。")
                    for admin_id in Config.ADMINS:
                        with suppress(Exception):
                            await bot.send_message(
                                admin_id, notif_text, parse_mode="HTML"
                            )

            except Exception as e:
                logger.error(
                    f"[{trace_id}] ❌ 通知发送失败: {e}\n{traceback.format_exc()}"
                )

    logger.info(f"✅[{trace_id}] {action_text}打卡流程完成")


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
        parse_mode="HTML",
    )


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
        )
        return

    act = args[1].strip()
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
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
    await message.answer("👑 管理员面板", reply_markup=get_admin_keyboard())


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
            )
            logger.warning(f"👑 管理员 {message.from_user.id} 清理了所有月度统计数据")
            return
        except Exception as e:
            await message.answer(f"❌ 清理所有数据失败: {e}")
            return

    await message.answer("⏳ 正在清理月度统计数据...")

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
            )

    except Exception as e:
        logger.error(f"❌ 清理月度数据失败: {e}")
        await message.answer(f"❌ 清理月度数据失败: {e}")


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_monthly_stats_status(message: types.Message):
    """查看月度统计数据状态"""
    chat_id = message.chat.id

    try:
        async with db.pool.acquire() as conn:
            # 获取月度统计的日期范围
            date_range = await conn.fetch(
                "SELECT MIN(statistic_date) as earliest, MAX(statistic_date) as latest, COUNT(*) as total FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

            # 获取各月份数据量
            monthly_counts = await conn.fetch(
                "SELECT statistic_date, COUNT(*) as count FROM monthly_statistics WHERE chat_id = $1 GROUP BY statistic_date ORDER BY statistic_date DESC",
                chat_id,
            )

            # 获取总用户数
            user_count = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

            # 获取活动类型数量
            activity_count = await conn.fetchval(
                "SELECT COUNT(DISTINCT activity_name) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

        if not date_range or not date_range[0]["earliest"]:
            await message.answer(
                "📊 <b>月度统计数据状态</b>\n\n" "暂无月度统计数据", parse_mode="HTML"
            )
            return

        earliest = date_range[0]["earliest"]
        latest = date_range[0]["latest"]
        total_records = date_range[0]["total"]

        status_text = (
            f"📊 <b>月度统计数据状态</b>\n\n"
            f"📅 数据范围: <code>{earliest.strftime('%Y年%m月')}</code> - <code>{latest.strftime('%Y年%m月')}</code>\n"
            f"👥 统计用户: <code>{user_count}</code> 人\n"
            f"📝 活动类型: <code>{activity_count}</code> 种\n"
            f"💾 总记录数: <code>{total_records}</code> 条\n\n"
            f"<b>各月份数据量:</b>\n"
        )

        for row in monthly_counts[:12]:  # 显示最近12个月
            month_str = row["statistic_date"].strftime("%Y年%m月")
            count = row["count"]
            status_text += f"• {month_str}: <code>{count}</code> 条\n"

        if len(monthly_counts) > 12:
            status_text += f"• ... 还有 {len(monthly_counts) - 12} 个月份\n"

        status_text += (
            f"\n💡 <b>可用命令:</b>\n"
            f"• <code>/cleanup_monthly</code> - 自动清理（保留3个月）\n"
            f"• <code>/cleanup_monthly 2024 1</code> - 清理指定月份\n"
            f"• <code>/cleanup_monthly all</code> - 清理所有数据（危险）"
        )

        await message.answer(status_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"❌ 查看月度统计状态失败: {e}")
        await message.answer(f"❌ 查看月度统计状态失败: {e}")


@admin_required
async def cmd_cleanup_inactive(message: types.Message):
    """清理长期未活动的用户数据"""
    args = message.text.split()

    # 默认清理 30 天未活动的用户
    days = 30

    # 如果用户手动传入天数
    if len(args) > 1:
        try:
            days = int(args[1])
            if days < 7:
                await message.answer("❌ 天数不能少于7天，避免误删活跃用户")
                return
        except ValueError:
            await message.answer("❌ 天数必须是数字，例如：/cleanup_inactive 60")
            return

    await message.answer(f"⏳ 正在清理 {days} 天未活动的用户，请稍候...")

    try:
        cutoff_date = (get_beijing_time() - timedelta(days=days)).date()

        async with db.pool.acquire() as conn:
            # 删除长期未活动的用户
            result = await conn.execute(
                "DELETE FROM users WHERE last_updated < $1", cutoff_date
            )
            deleted_users = (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )

            # 删除对应的活动记录
            result2 = await conn.execute(
                "DELETE FROM user_activities WHERE activity_date < $1", cutoff_date
            )
            deleted_activities = (
                int(result2.split()[-1])
                if result2 and result2.startswith("DELETE")
                else 0
            )

            # 删除对应的工作记录
            result3 = await conn.execute(
                "DELETE FROM work_records WHERE record_date < $1", cutoff_date
            )
            deleted_work_records = (
                int(result3.split()[-1])
                if result3 and result3.startswith("DELETE")
                else 0
            )

        total_deleted = deleted_users + deleted_activities + deleted_work_records

        await message.answer(
            f"🧹 <b>长期未活动用户清理完成</b>\n\n"
            f"📅 清理截止: <code>{cutoff_date}</code> 之前\n"
            f"🗑️ 删除用户: <code>{deleted_users}</code> 个\n"
            f"🗑️ 删除活动记录: <code>{deleted_activities}</code> 条\n"
            f"🗑️ 删除工作记录: <code>{deleted_work_records}</code> 条\n\n"
            f"📊 总计删除: <code>{total_deleted}</code> 条记录",
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"❌ 清理未活动用户失败: {e}")
        await message.answer(f"❌ 清理未活动用户失败: {e}")


# ========== 重置用户命令 ==========
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_reset_user(message: types.Message):
    """重置指定用户的今日数据"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/resetuser <用户ID>\n" "💡 示例：/resetuser 123456789",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        chat_id = message.chat.id
        target_user_id = int(args[1])

        await message.answer(f"⏳ 正在重置用户 {target_user_id} 的今日数据...")

        # 执行重置
        success = await db.reset_user_daily_data(chat_id, target_user_id)

        if success:
            await message.answer(
                f"✅ 已重置用户 <code>{target_user_id}</code> 的今日数据\n\n"
                f"🗑️ 已清除：\n"
                f"• 今日活动记录\n"
                f"• 今日统计计数\n"
                f"• 当前活动状态\n"
                f"• 罚款计数（保留总罚款）",
                parse_mode="HTML",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            logger.info(
                f"👑 管理员 {message.from_user.id} 重置了用户 {target_user_id} 的数据"
            )
        else:
            await message.answer(
                f"❌ 重置用户 {target_user_id} 数据失败",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )

    except ValueError:
        await message.answer(
            "❌ 用户ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        logger.error(f"重置用户数据失败: {e}")
        await message.answer(
            f"❌ 重置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ========== 导出每日数据命令 ==========
@admin_required
@rate_limit(rate=2, per=60)
@track_performance("cmd_export")
async def cmd_export(message: types.Message):
    """导出数据"""
    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据，请稍候...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


# ========== 月度报告函数 ==========
async def optimized_monthly_export(chat_id: int, year: int, month: int):
    """紧急修复版月度数据导出"""
    try:
        # 获取活动配置
        activity_limits = await db.get_activity_limits_cached()
        activity_names = list(activity_limits.keys())

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        # 构建表头
        headers = ["用户ID", "用户昵称"]
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
            ]
        )
        writer.writerow(headers)

        # 使用现有的月度统计方法
        monthly_stats = await db.get_monthly_statistics(chat_id, year, month)

        if not monthly_stats:
            logger.warning(f"月度统计表中没有找到 {year}年{month}月 的数据")
            return None

        # 🆕 紧急修复：检查数据结构
        logger.info(
            f"月度统计数据样本类型: {type(monthly_stats[0]) if monthly_stats else '无数据'}"
        )
        logger.info(
            f"月度统计数据样本: {monthly_stats[0] if monthly_stats else '无数据'}"
        )

        # 处理每个用户的数据
        for user_stat in monthly_stats:
            # 🆕 紧急修复：确保 user_stat 是字典
            if not isinstance(user_stat, dict):
                logger.warning(
                    f"跳过非字典类型的用户数据: {type(user_stat)} - {user_stat}"
                )
                continue

            # 🆕 安全获取字段
            user_id = user_stat.get("user_id", "未知")
            nickname = user_stat.get("nickname", "未知用户")

            row = [user_id, nickname]

            # 🆕 紧急修复：安全获取 activities
            user_activities = user_stat.get("activities", {})
            if isinstance(user_activities, str):
                try:
                    import json

                    user_activities = json.loads(user_activities)
                except:
                    user_activities = {}
            elif not isinstance(user_activities, dict):
                user_activities = {}

            # 填充活动数据
            for act in activity_names:
                activity_info = user_activities.get(act, {})
                if not isinstance(activity_info, dict):
                    activity_info = {}

                count = activity_info.get("count", 0)
                time_seconds = activity_info.get("time", 0)
                time_formatted = db.format_time_for_csv(time_seconds)

                row.append(count)
                row.append(time_formatted)

            # 🆕 安全获取统计字段
            row.extend(
                [
                    user_stat.get("total_activity_count", 0),
                    db.format_time_for_csv(user_stat.get("total_accumulated_time", 0)),
                    user_stat.get("total_fines", 0),
                    user_stat.get("overtime_count", 0),
                    db.format_time_for_csv(user_stat.get("total_overtime_time", 0)),
                    user_stat.get("work_days", 0),
                    db.format_time_for_csv(user_stat.get("work_hours", 0)),
                ]
            )

            writer.writerow(row)

        return csv_buffer.getvalue()

    except Exception as e:
        logger.error(f"❌ 月度导出优化版失败: {e}")
        import traceback

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
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在生成月度报告，请稍候...")

    try:
        # 生成报告
        report = await generate_monthly_report(chat_id, year, month)
        if report:
            await message.answer(report, parse_mode="HTML")

            # 导出CSV
            await export_monthly_csv(chat_id, year, month)
            await message.answer("✅ 月度数据已导出并推送！")
        else:
            time_desc = f"{year}年{month}月" if year and month else "最近一个月"
            await message.answer(f"⚠️ {time_desc}没有数据需要报告")

    except Exception as e:
        await message.answer(f"❌ 生成月度报告失败：{e}")


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
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        await message.answer(f"❌ 添加/修改活动失败：{e}")


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
        )
        return
    act = args[1]
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 <code>{act}</code> 不存在",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
        return

    await db.delete_activity_config(act)
    await db.force_refresh_activity_cache()  # 确保缓存立即更新

    await message.answer(
        f"✅ 活动 <code>{act}</code> 已删除",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
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
        )
        return

    try:
        work_start = args[1]
        work_end = args[2]

        # 验证时间格式
        import re

        time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")

        if not time_pattern.match(work_start) or not time_pattern.match(work_end):
            await message.answer(
                "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n"
                "📝 示例：09:00、18:30",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
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
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"设置工作时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_reset_time(chat_id, hour, minute)
            await message.answer(
                f"✅ 每日重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>\n\n"
                f"💡 每天此时将自动重置所有用户的打卡数据",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
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
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！\n" "💡 示例：/setresettime 4 0 （凌晨4点重置）",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        logger.error(f"设置重置时间失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
        )
    except Exception as e:
        logger.error(f"查看重置时间失败: {e}")
        await message.answer(f"❌ 获取重置时间失败：{e}")


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
        )

        logger.info(
            f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能，清除 {records_cleared} 条记录"
        )

    except Exception as e:
        logger.error(f"移除上下班功能失败: {e}")
        await message.answer(
            f"❌ 移除失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
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
            parse_mode="HTML",
        )

        logger.info(f"频道绑定成功: 群组 {chat_id} -> 频道 {channel_id}")

    except ValueError:
        await message.answer(
            "❌ 频道ID必须是数字格式\n" "💡 示例：/setchannel -1001234567890",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        logger.error(f"设置频道失败: {e}")
        await message.answer(
            f"❌ 绑定频道失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
            parse_mode="HTML",
        )

        logger.info(f"群组绑定成功: 主群组 {chat_id} -> 通知群组 {group_id}")

    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字格式",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        logger.error(f"设置群组失败: {e}")
        await message.answer(
            f"❌ 绑定群组失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
            )
            logger.info(f"设置活动人数限制: {activity} -> {max_users}人")

    except ValueError:
        await message.answer(
            "❌ 人数限制必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        logger.error(f"设置活动人数限制失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_actstatus(message: types.Message):
    """查看活动人数状态"""
    chat_id = message.chat.id

    try:
        # 获取所有活动限制
        activity_limits = await db.get_all_activity_limits()

        if not activity_limits:
            await message.answer(
                "📊 当前没有设置任何活动人数限制\n"
                "💡 使用 /actnum <活动名> <人数> 来设置限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            )
            return

        status_text = "📊 活动人数限制状态\n\n"

        for activity, max_users in activity_limits.items():
            current_users = await db.get_current_activity_users(chat_id, activity)
            remaining = max_users - current_users

            status_icon = "🟢" if remaining > 0 else "🔴"

            status_text += (
                f"{status_icon} <code>{activity}</code>\n"
                f"   • 限制：<code>{max_users}</code> 人\n"
                f"   • 当前：<code>{current_users}</code> 人\n"
                f"   • 剩余：<code>{remaining}</code> 人\n\n"
            )

        status_text += "💡 绿色表示还有名额，红色表示已满员"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
        )

        logger.info(f"查看活动状态: {chat_id}")

    except Exception as e:
        logger.error(f"获取活动状态失败: {e}")
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
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
                )
                return
            segments[str(t)] = f

        activity_limits = await db.get_activity_limits_cached()
        for act in activity_limits.keys():
            for time_segment, amount in segments.items():
                await db.update_fine_config(act, time_segment, amount)

        segments_text = " ".join(
            [f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()]
        )
        await message.answer(
            f"✅ 已为所有活动设置分段罚款：{segments_text}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
        )
        return

    try:
        activity = args[1]
        time_segment = args[2]
        amount = int(args[3])

        # 检查活动是否存在
        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
            return

        if amount < 0:
            await message.answer(
                "❌ 罚款金额不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        await db.update_fine_config(activity, time_segment, amount)

        await message.answer(
            f"✅ 已设置活动 '<code>{activity}</code>' 的罚款：\n"
            f"⏱️ 时间段：<code>{time_segment}</code>\n"
            f"💰 金额：<code>{amount}</code> 元",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer(
            "❌ 金额必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_finesstatus(message: types.Message):
    """查看所有活动的罚款设置状态"""
    try:
        # 获取所有活动和罚款配置
        activity_limits = await db.get_activity_limits_cached()
        fine_rates = await db.get_fine_rates()

        if not activity_limits:
            await message.answer(
                "⚠️ 当前没有配置任何活动",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        status_text = "💰 活动罚款设置状态\n\n"

        for activity in activity_limits.keys():
            activity_fines = fine_rates.get(activity, {})
            status_text += f"🔹 <code>{activity}</code>\n"

            if activity_fines:
                for time_seg, amount in sorted(
                    activity_fines.items(), key=lambda x: int(x[0].replace("min", ""))
                ):
                    status_text += f"   • {time_seg}: <code>{amount}</code>元\n"
            else:
                status_text += f"   • 未设置罚款\n"

            status_text += "\n"

        status_text += "💡 使用以下命令设置：\n"
        status_text += "• /setfine <活动> <时间> <金额> - 设置单个活动\n"
        status_text += "• /setfines_all <t1> <f1> [t2 f2...] - 统一设置所有活动"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"查看罚款状态失败: {e}")
        await message.answer(
            f"❌ 获取罚款状态失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
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
    if len(args) < 4 or len(args) % 2 != 0:
        await message.answer(
            "❌ 用法错误\n正确格式：/setworkfine <work_start|work_end> <分钟1> <罚款1> [分钟2 罚款2 ...]",
            reply_markup=get_admin_keyboard(),
        )
        return

    checkin_type = args[1]
    if checkin_type not in ["work_start", "work_end"]:
        await message.answer(
            "❌ 类型必须是 work_start 或 work_end",
            reply_markup=get_admin_keyboard(),
        )
        return

    # 解析分钟阈值和罚款金额
    fine_segments = {}
    try:
        for i in range(2, len(args), 2):
            minute = int(args[i])
            amount = int(args[i + 1])
            fine_segments[str(minute)] = amount

        # 更新数据库配置（重写整个罚款配置）
        await db.clear_work_fine_rates(checkin_type)
        for minute_str, fine_amount in fine_segments.items():
            await db.update_work_fine_rate(checkin_type, minute_str, fine_amount)

        segments_text = "\n".join(
            [f"⏰ 超过 {m} 分钟 → 💰 {a} 元" for m, a in fine_segments.items()]
        )

        type_text = "上班迟到" if checkin_type == "work_start" else "下班早退"

        await message.answer(
            f"✅ 已设置{type_text}罚款规则：\n{segments_text}",
            reply_markup=get_admin_keyboard(),
        )

        logger.info(f"设置上下班罚款: {checkin_type} -> {fine_segments}")

    except ValueError:
        await message.answer(
            "❌ 分钟和罚款必须是数字",
            reply_markup=get_admin_keyboard(),
        )
    except Exception as e:
        logger.error(f"设置上下班罚款失败: {e}")
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=get_admin_keyboard(),
        )


@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showsettings(message: types.Message):
    """显示目前的设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id)

    if group_data and not isinstance(group_data, dict):
        group_data = dict(group_data)

    activity_limits = await db.get_activity_limits_cached()
    fine_rates = await db.get_fine_rates()
    work_fine_rates = await db.get_work_fine_rates()

    # 生成输出文本
    text = f"🔧 当前群设置（当前群ID {chat_id}）\n\n"

    # 基本设置
    text += "📋 基本设置：\n"
    text += f"• 绑定频道ID: <code>{group_data.get('channel_id', '未设置')}</code>\n"
    text += f"• 通知群组ID: <code>{group_data.get('notification_group_id', '未设置')}</code>\n\n"
    text += "⏰ 重置设置：\n"
    text += f"• 每日重置时间: <code>{group_data.get('reset_hour', 0):02d}:{group_data.get('reset_minute', 0):02d}</code>\n"
    text += f"• 上班时间: <code>{group_data.get('work_start_time', '09:00')}</code>\n"
    text += f"• 下班时间: <code>{group_data.get('work_end_time', '18:00')}</code>\n\n"

    # 活动设置
    text += "🎯 活动设置：\n"
    for act, v in activity_limits.items():
        text += f"• <code>{act}</code>：次数上限 <code>{v['max_times']}</code>，时间限制 <code>{v['time_limit']}</code> 分钟\n"

    # 活动罚款设置
    text += "\n💰 活动罚款分段：\n"
    has_fine_settings = False
    for act, fr in fine_rates.items():
        if fr:
            has_fine_settings = True
            sorted_fines = sorted(
                fr.items(), key=lambda x: int(x[0].replace("min", ""))
            )
            fines_text = " | ".join([f"{k}:{v}元" for k, v in sorted_fines])
            text += f"• <code>{act}</code>：{fines_text}\n"

    if not has_fine_settings:
        text += "• 暂无活动罚款设置\n"

    # 上下班罚款设置
    text += "\n⏰ 上下班罚款设置：\n"
    start_fines = work_fine_rates.get("work_start", {})
    if start_fines:
        sorted_start = sorted(start_fines.items(), key=lambda x: int(x[0]))
        start_text = " | ".join([f"{k}分:{v}元" for k, v in sorted_start])
        text += f"• 上班迟到：{start_text}\n"
    else:
        text += "• 上班迟到：未设置\n"

    end_fines = work_fine_rates.get("work_end", {})
    if end_fines:
        sorted_end = sorted(end_fines.items(), key=lambda x: int(x[0]))
        end_text = " | ".join([f"{k}分:{v}元" for k, v in sorted_end])
        text += f"• 下班早退：{end_text}\n"
    else:
        text += "• 下班早退：未设置\n"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


# ========== 查看工作时间命令 =========
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_worktime(message: types.Message):
    """查看当前工作时间设置"""
    chat_id = message.chat.id
    try:
        work_hours = await db.get_group_work_time(chat_id)
        has_enabled = await db.has_work_hours_enabled(chat_id)

        status = "🟢 已启用" if has_enabled else "🔴 未启用（使用默认时间）"

        await message.answer(
            f"🕒 当前工作时间设置\n\n"
            f"📊 状态：{status}\n"
            f"🟢 上班时间：<code>{work_hours['work_start']}</code>\n"
            f"🔴 下班时间：<code>{work_hours['work_end']}</code>\n\n"
            f"💡 使用 /setworktime 09:00 18:00 来修改",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"查看工作时间失败: {e}")
        await message.answer(
            f"❌ 获取工作时间失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ========== 按钮处理 ==========
@rate_limit(rate=10, per=60)
async def handle_back_command(message: types.Message):
    """处理回座命令"""
    await process_back(message)


@rate_limit(rate=5, per=60)
async def handle_work_buttons(message: types.Message):
    """处理上下班按钮"""
    text = message.text.strip()
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
    await message.answer("⏳ 正在导出数据，请稍候...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


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
    """处理管理员面板按钮"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    admin_text = (
        "👑 管理员面板\n\n"
        "📢 频道与推送管理：\n"
        "• /setchannel <频道ID> - 绑定提醒频道\n"
        "• /setgroup <群组ID> - 绑定通知群组\n"
        "• /setpush <channel|group|admin> <on|off> - 设置推送开关\n"
        "• /showpush - 显示推送设置状态\n\n"
        "🎯 活动管理：\n"
        "• /addactivity <活动名> <次数> <分钟> - 添加或修改活动\n"
        "• /delactivity <活动名> - 删除活动\n"
        "• /actnum <活动名> <人数> - 设置活动人数限制\n"
        "• /actstatus - 查看活动人数状态\n\n"
        "💰 罚款管理：\n"
        "• /setfine <活动名> <时间段> <金额> - 设置单个活动罚款\n"
        "• /setfines_all <t1> <f1> [t2 f2...] - 统一设置所有活动罚款\n"
        "• /setworkfine <work_start|work_end> <分钟1> <罚款1> [分钟2 罚款2...] - 设置上下班罚款\n"
        "• /finesstatus - 查看罚款设置状态\n\n"
        "⏰ 重置设置：：\n"
        "• /setresettime <小时> <分钟> - 设置每日重置时间\n"
        "• /reset <用户ID> - 重置用户数据\n"
        "• /resettime - 查看当前重置时间\n\n"
        "🕒 上下班管理：\n"
        "• /setworktime <上班时间> <下班时间> - 设置上下班时间\n"
        "• /worktime - 查看当前工作时间设置\n"
        "• /delwork - 移除功能(保留记录)\n"
        "• /delwork_clear - 移除功能(不保留记录)\n\n"
        "📊 数据管理：\n"
        "• /export - 导出当前数据\n"
        "• /exportmonthly [年份] [月份] - 导出月度数据\n"
        "• /monthlyreport [年份] [月份] - 生成月度报告\n"
        "• /cleanup_monthly [年份] [月份] - 清理月度数据\n"
        "• /monthly_stats_status - 查看月度统计状态\n"
        "• /cleanup_inactive [天数] - 清理未活动用户\n\n"
        "💾 数据显示：\n"
        "• /performance - 查看性能报告\n"
        "• /showsettings - 显示所有当前设置\n\n"
    )
    await message.answer(admin_text, reply_markup=get_admin_keyboard())


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
        parse_mode="HTML",
    )


# ========== 用户功能 ==========
async def show_history(message: types.Message):
    """显示用户历史记录"""
    chat_id = message.chat.id
    uid = message.from_user.id

    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)

    first_line = (
        f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}"
    )
    text = f"{first_line}\n📊 今日记录：\n\n"

    has_records = False
    activity_limits = await db.get_activity_limits_cached()
    user_activities = await db.get_user_all_activities(chat_id, uid)

    for act in activity_limits.keys():
        activity_info = user_activities.get(act, {})
        total_time = activity_info.get("time", 0)
        count = activity_info.get("count", 0)
        max_times = activity_limits[act]["max_times"]
        if total_time > 0 or count > 0:
            status = "✅" if count < max_times else "❌"
            time_str = MessageFormatter.format_time(int(total_time))
            text += f"• <code>{act}</code>：<code>{time_str}</code>，次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
            has_records = True

    total_time_all = user_data.get("total_accumulated_time", 0)
    total_count_all = user_data.get("total_activity_count", 0)
    total_fine = user_data.get("total_fines", 0)

    text += f"\n📈 今日总统计：\n"
    text += f"• 总累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
    text += f"• 总活动次数：<code>{total_count_all}</code> 次\n"
    if total_fine > 0:
        text += f"• 累计罚款：<code>{total_fine}</code> 元"

    if not has_records and total_count_all == 0:
        text += "暂无记录，请先进行打卡活动"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


async def show_rank(message: types.Message):
    """显示排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id

    await db.init_group(chat_id)
    activity_limits = await db.get_activity_limits_cached()

    if not activity_limits:
        await message.answer("⚠️ 当前没有配置任何活动，无法生成排行榜。")
        return

    rank_text = "🏆 今日活动排行榜\n\n"
    today = db.get_beijing_date()
    found_any_data = False

    for act in activity_limits.keys():
        # 获取活动排名数据
        group_members = await db.get_group_members(chat_id)
        activity_users = []

        for user_data in group_members:
            user_activities = await db.get_user_all_activities(
                chat_id, user_data["user_id"]
            )
            activity_info = user_activities.get(act, {})
            total_time = activity_info.get("time", 0)
            count = activity_info.get("count", 0)

            if total_time > 0 or user_data.get("current_activity") == act:
                activity_users.append(
                    {
                        "user_id": user_data["user_id"],
                        "nickname": user_data["nickname"],
                        "total_time": total_time,
                        "count": count,
                        "is_active": user_data.get("current_activity") == act,
                    }
                )

        # 按时间排序
        activity_users.sort(key=lambda x: x["total_time"], reverse=True)

        if activity_users:
            found_any_data = True
            rank_text += f"📈 <code>{act}</code>：\n"

            for i, user in enumerate(activity_users[:5], 1):
                if user["is_active"]:
                    rank_text += f"  <code>{i}.</code> 🟡 {MessageFormatter.format_user_link(user['user_id'], user['nickname'])} - 进行中\n"
                elif user["total_time"] > 0:
                    time_str = MessageFormatter.format_time(int(user["total_time"]))
                    rank_text += f"  <code>{i}.</code> 🟢 {MessageFormatter.format_user_link(user['user_id'], user['nickname'])} - {time_str} ({user['count']}次)\n"

            rank_text += "\n"

    if not found_any_data:
        rank_text = (
            "🏆 今日活动排行榜\n\n"
            "📊 今日还没有活动记录\n"
            "💪 开始第一个活动吧！\n\n"
            "💡 提示：开始活动后会立即显示在这里"
        )

    await message.answer(
        rank_text,
        reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
        parse_mode="HTML",
    )


# ========== 快速回座回调 ==========
async def handle_quick_back(callback_query: types.CallbackQuery):
    """处理快速回座按钮"""
    try:
        data_parts = callback_query.data.split(":")
        if len(data_parts) < 3:
            await callback_query.answer("❌ 数据格式错误", show_alert=True)
            return

        chat_id = int(data_parts[1])
        uid = int(data_parts[2])

        # 检查消息是否过期
        msg_ts = callback_query.message.date.timestamp()
        if time.time() - msg_ts > 600:
            await callback_query.answer(
                "⚠️ 此按钮已过期，请重新输入 /回座", show_alert=True
            )
            return

        # 检查是否是用户本人点击
        if callback_query.from_user.id != uid:
            await callback_query.answer("❌ 这不是您的回座按钮！", show_alert=True)
            return

        # 执行回座逻辑
        user_lock = user_lock_manager.get_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or not user_data.get("current_activity"):
                await callback_query.answer("❌ 您当前没有活动在进行", show_alert=True)
                return

            await _process_back_locked(callback_query.message, chat_id, uid)

        # 更新按钮状态
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            logger.warning(f"无法更新按钮状态: {e}")

        await callback_query.answer("✅ 已成功回座")

    except Exception as e:
        logger.error(f"快速回座失败: {e}")
        try:
            await callback_query.answer(
                "❌ 回座失败，请手动输入 /回座", show_alert=True
            )
        except Exception:
            pass


# ========== 日常数据导出处理函数 =========
async def get_group_stats_from_monthly(chat_id: int, target_date: date) -> List[Dict]:
    """从月度统计表获取群组统计数据（用于重置后导出）"""
    try:
        # 获取目标日期对应的月份
        month_start = target_date.replace(day=1)

        logger.info(
            f"🔍 从月度表查询数据: 群组{chat_id}, 日期{target_date}, 月份{month_start}"
        )

        # 从月度表获取数据
        monthly_stats = await db.get_monthly_statistics(
            chat_id, month_start.year, month_start.month
        )

        if not monthly_stats:
            logger.warning(f"⚠️ 月度表中没有找到 {month_start} 的数据")
            return []

        result = []
        for stat in monthly_stats:
            # 🆕 调试日志：检查工作相关字段
            logger.debug(
                f"📊 用户 {stat['user_id']} 工作数据: "
                f"工作天数={stat.get('work_days', 0)}, "
                f"工作时长={stat.get('work_hours', 0)}秒"
            )

            user_data = {
                "user_id": stat["user_id"],
                "nickname": stat.get("nickname", f"用户{stat['user_id']}"),
                "total_accumulated_time": stat.get("total_accumulated_time", 0),
                "total_activity_count": stat.get("total_activity_count", 0),
                "total_fines": stat.get("total_fines", 0),
                "overtime_count": stat.get("overtime_count", 0),
                "total_overtime_time": stat.get("total_overtime_time", 0),
                "work_days": stat.get("work_days", 0),  # 🆕 新增工作天数
                "work_hours": stat.get("work_hours", 0),  # 🆕 新增工作时长
                "activities": stat.get("activities", {}),
            }

            result.append(user_data)

        logger.info(
            f"✅ 从月度表成功获取 {target_date} 的数据，共 {len(result)} 个用户"
        )
        return result

    except Exception as e:
        logger.error(f"❌ 从月度表获取数据失败: {e}")
        return []


# ========== 数据导出功能 ==========
async def export_and_push_csv(
    chat_id: int,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
    target_date=None,
):
    """导出群组数据为 CSV 并推送 - 支持从月度表恢复数据"""
    await db.init_group(chat_id)

    # 规范 target_date
    if target_date is not None and hasattr(target_date, "date"):
        target_date = target_date.date()

    if not file_name:
        if target_date is not None:
            date_str = target_date.strftime("%Y%m%d")
        else:
            date_str = get_beijing_time().strftime("%Y%m%d_%H%M%S")
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"

    # 🆕 关键修复：检查是否是重置后的导出（目标日期是昨天）
    now = get_beijing_time()
    is_reset_export = False
    if target_date and target_date == (now - timedelta(days=1)).date():
        is_reset_export = True
        logger.info(f"🔄 检测到重置后导出，将从月度表恢复 {target_date} 的数据")

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    activity_limits = await db.get_activity_limits_cached()
    headers = ["用户ID", "用户昵称"]
    for act in activity_limits.keys():
        headers.extend([f"{act}次数", f"{act}总时长"])
    headers.extend(
        ["活动次数总计", "活动用时总计", "罚款总金额", "超时次数", "总超时时间"]
    )
    writer.writerow(headers)

    has_data = False

    if is_reset_export:
        # 🆕 重置后导出：从月度表获取数据
        group_stats = await get_group_stats_from_monthly(chat_id, target_date)
    else:
        # 正常导出：从日常表获取数据
        group_stats = await db.get_group_statistics(chat_id, target_date)

    # 后续代码保持不变...
    for user_data in group_stats:
        # 🆕 最小修复：只在需要的地方添加保护
        if not isinstance(user_data, dict):
            continue

        # 安全处理 activities 字段
        user_activities = user_data.get("activities", {})
        if not isinstance(user_activities, dict):
            user_activities = {}

        total_count = user_data.get("total_activity_count", 0)
        total_time = user_data.get("total_accumulated_time", 0)
        if total_count > 0 or total_time > 0:
            has_data = True

        row = [user_data.get("user_id", "未知"), user_data.get("nickname", "未知用户")]

        for act in activity_limits.keys():
            activity_info = user_activities.get(act, {})
            # 🆕 安全获取活动数据
            if not isinstance(activity_info, dict):
                activity_info = {}

            count = activity_info.get("count", 0)
            total_seconds = int(activity_info.get("time", 0))
            time_str = MessageFormatter.format_time_for_csv(total_seconds)
            row.append(count)
            row.append(time_str)

        total_seconds_all = int(user_data.get("total_accumulated_time", 0) or 0)
        total_time_str = MessageFormatter.format_time_for_csv(total_seconds_all)

        overtime_seconds = int(user_data.get("total_overtime_time", 0) or 0)
        overtime_str = MessageFormatter.format_time_for_csv(overtime_seconds)

        row.extend(
            [
                total_count,
                total_time_str,
                user_data.get("total_fines", 0),
                user_data.get("overtime_count", 0),
                overtime_str,
            ]
        )
        writer.writerow(row)

    if not has_data:
        await bot.send_message(chat_id, "⚠️ 当前群组没有数据需要导出")
        return

    csv_content = csv_buffer.getvalue()
    csv_buffer.close()

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
            f"📊 群组：<b>{chat_title}</b>\n"
            f"📅 统计日期：<code>{(target_date.strftime('%Y-%m-%d') if target_date else get_beijing_time().strftime('%Y-%m-%d'))}</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"💾 包含每个用户的每日活动统计"
        )

        # 先把文件发回到当前 chat（可选）
        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"发送到当前聊天失败: {e}")

        # 使用统一的 NotificationService 推送到绑定的频道/群组/管理员
        await notification_service.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption=caption
        )

        logger.info(f"✅ 数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


# ========== 定时任务 ==========
async def daily_reset_task():
    """每日自动重置任务"""
    while True:
        now = get_beijing_time()
        logger.info(f"重置任务检查，当前时间: {now}")

        try:
            all_groups = await db.get_all_groups()
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}")
            await asyncio.sleep(60)
            continue

        for chat_id in all_groups:
            try:
                group_data = await db.get_group_cached(chat_id)
                if not group_data:
                    continue

                reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
                reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

                # 到达重置时间
                if now.hour == reset_hour and now.minute == reset_minute:
                    logger.info(f"到达重置时间，正在重置群组 {chat_id} 的数据...")

                    # 执行每日数据重置
                    group_members = await db.get_group_members(chat_id)
                    reset_count = 0
                    for user_data in group_members:
                        user_lock = user_lock_manager.get_lock(
                            chat_id, user_data["user_id"]
                        )
                        async with user_lock:
                            success = await db.reset_user_daily_data(
                                chat_id, user_data["user_id"]
                            )
                            if success:
                                reset_count += 1

                    logger.info(
                        f"群组 {chat_id} 数据重置完成，重置了 {reset_count} 个用户"
                    )

                    # 启动延迟导出任务
                    asyncio.create_task(delayed_export(chat_id, 30))

            except Exception as e:
                logger.error(f"群组 {chat_id} 重置失败: {e}")

        # 每分钟检查一次
        await asyncio.sleep(60)


async def delayed_export(chat_id: int, delay_minutes: int = 30):
    """在每日重置后延迟导出昨日数据 - 安全修复版"""
    try:
        logger.info(f"群组 {chat_id} 将在 {delay_minutes} 分钟后导出昨日数据...")
        await asyncio.sleep(delay_minutes * 60)

        yesterday_dt = get_beijing_time() - timedelta(days=1)
        yesterday_date = yesterday_dt.date()

        # 🆕 先检查是否有数据
        monthly_stats = await db.get_monthly_statistics(
            chat_id, yesterday_date.year, yesterday_date.month
        )

        if not monthly_stats:
            logger.info(f"群组 {chat_id} 昨日无数据，跳过导出")
            return

        file_name = f"group_{chat_id}_statistics_{yesterday_dt.strftime('%Y%m%d')}.csv"
        await export_and_push_csv(chat_id, True, file_name, yesterday_date)
        logger.info(f"✅ 群组 {chat_id} 昨日数据导出完成")

    except Exception as e:
        logger.error(f"❌ 群组 {chat_id} 延迟导出昨日数据失败: {e}")
        # 不重新抛出异常，避免影响其他任务


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

        # 检查内存状态
        memory_ok = performance_optimizer.memory_usage_ok()

        status = "healthy" if all([db_healthy, bot_healthy, memory_ok]) else "degraded"

        return web.json_response(
            {
                "status": status,
                "timestamp": time.time(),
                "services": {
                    "database": db_healthy,
                    "bot": bot_healthy,
                    "memory": memory_ok,
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
    """启动健康检查服务器"""
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", Config.WEB_SERVER_CONFIG["PORT"]))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

    return site


# ========== 服务初始化 ==========
async def initialize_services():
    """初始化所有服务 - 最终完整版"""
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

        # 🎯 关键：验证 bot 和 bot_manager 是否真的初始化了
        global notification_service
        notification_service = NotificationService(bot_manager=bot_manager)
        notification_service.bot = bot

        # 5. 🎯 核心修复：双重设置 NotificationService
        notification_service.bot_manager = bot_manager
        notification_service.bot = bot  # 直接使用上面获取的 bot 实例

        # 🎯 验证设置是否成功
        if not notification_service.bot_manager:
            logger.error("❌ notification_service.bot_manager 设置失败")
        if not notification_service.bot:
            logger.error("❌ notification_service.bot 设置失败")

        logger.info("✅ 通知服务配置完成")

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

        # 11. 恢复过期活动
        recovered_count = await recover_expired_activities()
        logger.info(f"✅ 过期活动恢复完成: {recovered_count} 个活动已处理")

        # 12. 🎯 最终健康检查
        health_status = await check_services_health()
        if all(health_status.values()):
            logger.info("🎉 所有服务初始化完成且健康")
        else:
            logger.warning(f"⚠️ 服务初始化完成但有警告: {health_status}")

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


# ========= render部署用的代码 ========
async def external_keepalive():
    """外部保活服务调用 - 防止 Render 休眠"""
    keepalive_urls = [
        # 可以添加 UptimeRobot 或其他免费监控服务
    ]

    for url in keepalive_urls:
        try:
            # 使用 aiohttp 发起请求
            pass
        except Exception as e:
            logger.debug(f"保活请求失败 {url}: {e}")


async def keepalive_loop():
    """Render 专用保活循环 - 防止免费服务休眠"""
    while True:
        try:
            # 🆕 每5分钟执行一次保活（Render 免费版15分钟不活动会休眠）
            await asyncio.sleep(300)

            current_time = get_beijing_time()
            logger.debug(
                f"🔵 Render 保活检查: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # 1. 调用自己的健康检查端点
            try:
                import aiohttp

                port = int(os.environ.get("PORT", 8080))
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as session:
                    async with session.get(f"http://localhost:{port}/health") as resp:
                        if resp.status == 200:
                            logger.debug("✅ 内部健康检查保活成功")
            except Exception as e:
                logger.warning(f"内部保活检查失败: {e}")

            # 2. 数据库连接保活
            try:
                await db.connection_health_check()
                logger.debug("✅ 数据库连接保活成功")
            except Exception as e:
                logger.warning(f"数据库保活失败: {e}")

            # 3. 内存清理
            try:
                await performance_optimizer.memory_cleanup()
                # 🆕 强制垃圾回收
                import gc

                collected = gc.collect()
                if collected > 0:
                    logger.debug(f"🧹 保活期间GC回收 {collected} 个对象")
            except Exception as e:
                logger.debug(f"保活期间内存清理失败: {e}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Render 保活循环异常: {e}")
            await asyncio.sleep(60)  # 异常后等待1分钟


# ========== 启动流程 ==========
async def on_startup():
    """启动时执行 - 更新版本"""
    logger.info("🎯 机器人启动中...")
    try:
        # 删除webhook确保使用轮询模式（已在bot_manager中处理）
        # 初始化服务（已在main中调用initialize_services）
        logger.info("✅ 系统启动完成，准备接收消息")

        # 发送启动通知给管理员
        await send_startup_notification()

    except Exception as e:
        logger.error(f"启动过程异常: {e}")
        raise


async def on_shutdown():
    """关闭时执行 - 更新版本"""
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

        # 发送关闭通知
        await send_shutdown_notification()
        logger.info("✅ 关闭通知已发送")

        logger.info("🎉 所有服务已优雅关闭")
    except Exception as e:
        logger.error(f"关闭清理过程中出错: {e}")


async def main():
    """主函数 - Render 适配版"""
    # Render 环境检测
    is_render = os.environ.get("RENDER", False) or "RENDER" in os.environ

    if is_render:
        logger.info("🎯 检测到 Render 环境，应用优化配置")
        # 应用 Render 特定配置
        Config.DB_MAX_CONNECTIONS = 3
        Config.ENABLE_FILE_LOGGING = False

    try:
        logger.info("🚀 启动打卡机器人系统...")

        # 初始化服务
        await initialize_services()

        # 启动健康检查服务器（Render 必需）
        await start_health_server()

        # 🆕 Render 必需：更频繁的保活
        keepalive_task = asyncio.create_task(keepalive_loop(), name="render_keepalive")

        # 启动定时任务
        asyncio.create_task(daily_reset_task(), name="daily_reset")
        asyncio.create_task(memory_cleanup_task(), name="memory_cleanup")
        asyncio.create_task(health_monitoring_task(), name="health_monitoring")

        # 启动机器人
        logger.info("🤖 启动机器人（带自动重连机制）...")
        await on_startup()

        # 开始轮询
        await bot_manager.start_polling_with_retry()

    except KeyboardInterrupt:
        logger.info("🛑 机器人被用户中断")
    except Exception as e:
        logger.error(f"❌ 机器人启动失败: {e}")
        # 🆕 Render 环境下需要正常退出码
        if is_render:
            sys.exit(1)
        raise
    finally:
        # 🆕 确保保活任务被正确取消
        if "keepalive_task" in locals():
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass

        await on_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("机器人已被用户中断")
    except Exception as e:
        logger.error(f"机器人运行异常: {e}")

