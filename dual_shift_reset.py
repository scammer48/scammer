import logging
import asyncio
import time
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any, List
from performance import global_cache

from database import db


logger = logging.getLogger("GroupCheckInBot.DualShiftReset")


# ========== 1. 调度入口 ==========
async def handle_hard_reset(
    chat_id: int,
    operator_id: Optional[int] = None,
    target_date: Optional[date] = None,
) -> bool:
    """
    硬重置总调度入口 - 纯双班模式
    """
    try:
        logger.info(f"🔄 [双班模式] 群组 {chat_id} 执行双班硬重置")

        if target_date:
            success = await _dual_shift_hard_reset(chat_id, operator_id, target_date)
        else:
            success = await _dual_shift_hard_reset(chat_id, operator_id)

        if success:
            logger.info(f"✅ [双班硬重置] 群组 {chat_id} 执行成功")
        else:
            logger.error(f"❌ [双班硬重置] 群组 {chat_id} 执行失败")

        return success

    except Exception as e:
        logger.error(f"❌ [双班硬重置] 群组 {chat_id} 异常: {e}")
        logger.error(traceback.format_exc())
        return False


# ========== 2. 双班硬重置核心流程 ==========
async def _dual_shift_hard_reset(
    chat_id: int,
    operator_id: Optional[int] = None,
    forced_target_date: Optional[date] = None,
) -> bool:
    """双班硬重置主流程（带幂等性）"""
    try:
        now = db.get_beijing_time()

        date_range = await db.get_business_date_range(chat_id, now)
        business_today = date_range["business_today"]
        business_yesterday = date_range["business_yesterday"]
        business_day_before = date_range["business_day_before"]
        natural_today = date_range["natural_today"]

        logger.info(
            f"📅 [双班重置] 日期信息:\n"
            f"   • 自然今天: {natural_today}\n"
            f"   • 业务今天: {business_today}\n"
            f"   • 业务昨天: {business_yesterday}"
        )

        if forced_target_date:
            target_date = forced_target_date
            logger.info(f"🎯 [双班重置] 使用强制目标日期: {target_date}")
        else:
            group_data = await db.get_group_cached(chat_id)
            reset_hour = group_data.get("reset_hour", 0)
            reset_minute = group_data.get("reset_minute", 0)

            reset_time_natural_today = datetime.combine(
                natural_today,
                datetime.strptime(
                    f"{reset_hour:02d}:{reset_minute:02d}", "%H:%M"
                ).time(),
            ).replace(tzinfo=now.tzinfo)

            execute_time_today = reset_time_natural_today + timedelta(hours=2)

            reset_time_natural_yesterday = datetime.combine(
                natural_today - timedelta(days=1),
                datetime.strptime(
                    f"{reset_hour:02d}:{reset_minute:02d}", "%H:%M"
                ).time(),
            ).replace(tzinfo=now.tzinfo)

            execute_time_yesterday = reset_time_natural_yesterday + timedelta(hours=2)

            EXECUTION_WINDOW = 300

            time_to_today = abs((now - execute_time_today).total_seconds())
            time_to_yesterday = abs((now - execute_time_yesterday).total_seconds())

            logger.info(
                f"📅 重置窗口检查:\n"
                f"   ├─ 当前时间: {now.strftime('%H:%M:%S')}\n"
                f"   ├─ 今日执行窗口: {execute_time_today.strftime('%H:%M')} ±{EXECUTION_WINDOW/60}分钟\n"
                f"   ├─ 昨日执行窗口: {execute_time_yesterday.strftime('%H:%M')} ±{EXECUTION_WINDOW/60}分钟\n"
                f"   ├─ 今日时间差: {time_to_today:.0f}秒\n"
                f"   └─ 昨日时间差: {time_to_yesterday:.0f}秒"
            )

            if time_to_today <= EXECUTION_WINDOW:
                target_date = business_yesterday
                period_info = "正常执行"
                logger.info(f"📅 正常执行窗口，目标日期: {target_date}")

            elif time_to_yesterday <= EXECUTION_WINDOW:
                # 改进补执行逻辑：检查昨天是否真的执行过
                is_completed = await db.is_reset_completed(chat_id, business_yesterday)

                if not is_completed:
                    target_date = business_yesterday  # 补执行昨天的
                    period_info = "补执行"
                    logger.warning(
                        f"⚠️ 补执行场景，目标日期: {target_date}（昨天未执行）"
                    )
                else:
                    logger.info(f"✅ 昨天已执行，跳过补执行")
                    return False
            else:
                logger.debug(f"⏳ 不在执行窗口内")
                return False

        reset_flag_key = f"dual_reset:{chat_id}:{target_date.strftime('%Y%m%d')}"
        if await global_cache.get(reset_flag_key):
            logger.info(f"⏭️ 群组 {chat_id} 今天已完成双班重置，跳过")
            return True

        await db.init_group(chat_id)
        group_data = await db.get_group_cached(chat_id)
        if not group_data:
            logger.warning(f"⚠️ [双班硬重置] 群组 {chat_id} 没有配置数据，跳过重置")
            return False

        reset_hour = group_data.get("reset_hour", 0)
        reset_minute = group_data.get("reset_minute", 0)

        logger.info(
            f"🚀 [双班硬重置] 开始执行\n"
            f"   ┌─────────────────────────────────\n"
            f"   ├─ 群组ID: {chat_id}\n"
            f"   ├─ 当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   ├─ 自然今天: {natural_today}\n"
            f"   ├─ 业务今天: {business_today}\n"
            f"   ├─ 目标日期: {target_date}\n"
            f"   ├─ 重置时间: {reset_hour:02d}:{reset_minute:02d}\n"
            f"   └─ 操作员: {operator_id or '系统'}"
        )

        total_start_time = time.time()

        force_stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "day_shift": {"total": 0, "success": 0, "failed": 0},
            "night_shift": {"total": 0, "success": 0, "failed": 0},
            "details": [],
        }

        complete_stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "day_shift": {"total": 0, "success": 0, "failed": 0},
            "night_shift": {"total": 0, "success": 0, "failed": 0},
            "details": [],
        }

        logger.info(f"📊 [步骤1-2/5] 并发处理未完成活动及补全下班记录...")
        task1 = asyncio.create_task(
            _force_end_all_unfinished_shifts(chat_id, now, target_date, business_today)
        )
        task2 = asyncio.create_task(
            _complete_missing_work_ends(chat_id, target_date, business_today)
        )

        results = await asyncio.gather(task1, task2, return_exceptions=True)

        if not isinstance(results[0], Exception):
            force_stats = results[0]
            logger.info(
                f"✅ 强制结束活动完成: {force_stats['success']}/{force_stats['total']}"
            )
        else:
            logger.error(f"❌ [强制结束活动] 失败: {results[0]}")
            logger.error(traceback.format_exc())

        if not isinstance(results[1], Exception):
            complete_stats = results[1]
            logger.info(
                f"✅ 补全下班记录完成: {complete_stats['success']}/{complete_stats['total']}"
            )
        else:
            logger.error(f"❌ [补全下班记录] 失败: {results[1]}")
            logger.error(traceback.format_exc())

        logger.info(f"📊 [步骤3/5] 导出目标日期数据...")
        export_start = time.time()
        try:
            export_success = await _export_yesterday_data_concurrent(
                chat_id, target_date
            )
        except Exception as e:
            logger.error(f"❌ [数据导出] 失败: {e}")
            logger.error(traceback.format_exc())
            export_success = False
        export_time = time.time() - export_start

        logger.info(f"📊 [步骤4/5] 清除目标日期数据...")
        cleanup_start = time.time()
        try:
            cleanup_stats = await _cleanup_old_data(
                chat_id, target_date, business_today
            )
        except Exception as e:
            logger.error(f"❌ [数据清理] 失败: {e}")
            logger.error(traceback.format_exc())
            cleanup_stats = {
                "user_activities": 0,
                "work_records": 0,
                "daily_statistics": 0,
                "users_reset": 0,
            }
        cleanup_time = time.time() - cleanup_start

        deleted_count = 0
        try:
            if not db.pool or not db._initialized:
                logger.error("数据库连接池未初始化")
                return
            async with db.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    DELETE FROM group_shift_state 
                    WHERE chat_id = $1 AND record_date < $2
                    """,
                    chat_id,
                    business_today,
                )
                deleted_count = _parse_delete_count(result)

                if deleted_count > 0:
                    logger.info(f"✅ 已清除 {deleted_count} 个过期班次状态")

                    keys_to_remove = [
                        key
                        for key in db._cache.keys()
                        if key.startswith(f"shift_state:{chat_id}:")
                    ]
                    for key in keys_to_remove:
                        db._cache.pop(key, None)
                        db._cache_ttl.pop(key, None)
                else:
                    logger.info("✅ 没有需要清除的班次状态")

        except Exception as e:
            logger.error(f"❌ [清除班次状态] 失败: {e}")

        try:
            asyncio.create_task(
                _send_reset_notification(
                    chat_id,
                    force_stats,
                    complete_stats,
                    export_success,
                    cleanup_stats,
                    now,
                )
            )
        except Exception as e:
            logger.error(f"❌ [发送通知] 失败: {e}")

        await global_cache.set(reset_flag_key, True, ttl=86400)

        # ===== 新增：持久化到数据库 =====
        await db.mark_reset_completed(chat_id, target_date)

        logger.info(f"✅ [双班重置] 群组 {chat_id} 执行成功，已设置幂等标记")

        total_time = time.time() - total_start_time
        logger.info(
            f"🎉 [双班硬重置完成] 群组 {chat_id}\n"
            f"   ├─ 目标日期: {target_date}\n"
            f"   ├─ 强制结束: {force_stats['success']}/{force_stats['total']}\n"
            f"   ├─ 补全下班: {complete_stats['success']}/{complete_stats['total']}\n"
            f"   ├─ 导出成功: {export_success}\n"
            f"   ├─ 清理记录: {cleanup_stats.get('user_activities', 0)}条\n"
            f"   ├─ 清除班次状态: {deleted_count}个\n"
            f"   └─ 总耗时: {total_time:.2f}秒"
        )

        return True

    except Exception as e:
        logger.error(
            f"❌ [双班硬重置] 失败 {chat_id}\n"
            f"   ├─ 错误类型: {type(e).__name__}\n"
            f"   ├─ 错误信息: {e}\n"
            f"   └─ 堆栈: {traceback.format_exc()}"
        )
        return False


# ========== 新增：批量获取活动配置 ==========
async def _get_activity_configs_batch(activities: List[str]) -> Dict[str, Dict]:
    """
    批量获取活动配置
    返回格式: {
        '活动名': {
            'time_limit_seconds': int,
            'fine_rates': Dict[str, int]
        }
    }
    """
    if not activities:
        return {}

    # 从缓存获取所有活动配置（只查询一次数据库）
    all_limits = await db.get_activity_limits_cached()
    all_fines = await db.get_fine_rates()

    result = {}
    unique_activities = set(activities)

    for activity in unique_activities:
        time_limit_min = all_limits.get(activity, {}).get("time_limit", 0)
        result[activity] = {
            "time_limit_seconds": time_limit_min * 60,
            "fine_rates": all_fines.get(activity, {}),
        }

    logger.debug(f"📊 批量加载活动配置: {len(result)} 个活动: {list(result.keys())}")
    return result


# ========== 3. 统一强制结束所有未完成活动（优化版）==========
async def _force_end_all_unfinished_shifts(
    chat_id: int, now: datetime, target_date: date, business_today: date
) -> Dict[str, Any]:
    """强制结束所有进行中的活动（优化版，批量加载配置）"""
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "day_shift": {"total": 0, "success": 0, "failed": 0},
        "night_shift": {"total": 0, "success": 0, "failed": 0},
        "details": [],
    }

    try:
        if not db.pool or not db._initialized:
            logger.error("数据库连接池未初始化")
            return stats

        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, 
                       activity_start_time, shift
                FROM users 
                WHERE chat_id = $1 
                  AND current_activity IS NOT NULL
                """,
                chat_id,
            )

            stats["total"] = len(rows)

            if not rows:
                logger.info(f"📊 群组 {chat_id} 没有进行中的活动")
                return stats

            # ===== 批量获取活动配置 =====
            activities = list(set(row["current_activity"] for row in rows))
            activity_configs = await _get_activity_configs_batch(activities)

            logger.info(f"📊 发现 {len(rows)} 个进行中的活动，开始并发处理...")

            tasks = []
            for row in rows:
                task = asyncio.create_task(
                    _force_end_single_activity_optimized(
                        conn,
                        chat_id,
                        row,
                        now,
                        target_date,
                        business_today,
                        activity_configs,
                    )
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    if rows[i]["shift"] == "day":
                        stats["day_shift"]["failed"] += 1
                    else:
                        stats["night_shift"]["failed"] += 1
                    logger.error(f"❌ 处理用户 {rows[i]['user_id']} 失败: {result}")
                else:
                    stats["success"] += 1
                    if result["shift"] == "day":
                        stats["day_shift"]["success"] += 1
                    else:
                        stats["night_shift"]["success"] += 1
                    stats["details"].append(result)

            stats["day_shift"]["total"] = sum(1 for r in rows if r["shift"] == "day")
            stats["night_shift"]["total"] = sum(
                1 for r in rows if r["shift"] == "night"
            )

        logger.info(
            f"✅ [强制结束活动完成] 群组 {chat_id}\n"
            f"   ├─ 总计: {stats['total']} 人\n"
            f"   ├─ 成功: {stats['success']} 人\n"
            f"   ├─ 失败: {stats['failed']} 人\n"
            f"   ├─ 白班: {stats['day_shift']['success']}/{stats['day_shift']['total']}\n"
            f"   └─ 夜班: {stats['night_shift']['success']}/{stats['night_shift']['total']}"
        )

    except Exception as e:
        logger.error(f"❌ [强制结束活动] 失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())

    return stats


# ========== 新增：优化版的强制结束单个活动 ==========
async def _force_end_single_activity_optimized(
    conn,
    chat_id: int,
    user_row: dict,
    now: datetime,
    target_date: date,
    business_today: date,
    activity_configs: Dict[str, Dict],
) -> Dict[str, Any]:
    """强制结束单个活动（优化版，使用预加载配置）"""
    result = {
        "user_id": user_row["user_id"],
        "shift": user_row["shift"],
        "activity": user_row["current_activity"],
        "elapsed": 0,
        "fine": 0,
        "is_overtime": False,
        "success": False,
    }

    try:
        activity = user_row["current_activity"]
        start_time = datetime.fromisoformat(user_row["activity_start_time"])
        start_date = start_time.date()

        if start_date < business_today:
            if start_date <= target_date:
                forced_date = target_date
            else:
                forced_date = business_today - timedelta(days=1)
        else:
            logger.debug(f"⏭️ 保留今天活动: 用户{user_row['user_id']}")
            result["success"] = True
            return result

        elapsed = int((now - start_time).total_seconds())

        # 使用预加载的配置
        config = activity_configs.get(activity, {})
        time_limit_seconds = config.get("time_limit_seconds", 0)
        fine_rates = config.get("fine_rates", {})

        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, elapsed - time_limit_seconds)
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0 and fine_rates:
            segments = []
            for k in fine_rates.keys():
                try:
                    v = int(str(k).lower().replace("min", ""))
                    segments.append(v)
                except:
                    pass
            segments.sort()
            for s in segments:
                if overtime_minutes <= s:
                    fine_amount = fine_rates.get(str(s), fine_rates.get(f"{s}min", 0))
                    break
            if fine_amount == 0 and segments:
                m = segments[-1]
                fine_amount = fine_rates.get(str(m), fine_rates.get(f"{m}min", 0))

        result["elapsed"] = elapsed
        result["fine"] = fine_amount
        result["is_overtime"] = is_overtime

        await db.complete_user_activity(
            chat_id=chat_id,
            user_id=user_row["user_id"],
            activity=activity,
            elapsed_time=elapsed,
            fine_amount=fine_amount,
            is_overtime=is_overtime,
            shift=user_row["shift"],
            forced_date=forced_date,
        )

        result["success"] = True

        logger.info(
            f"✅ [强制结束] 用户{user_row['user_id']} | "
            f"活动:{activity} | 班次:{user_row['shift']} | "
            f"归到:{forced_date} | 时长:{elapsed}s | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"❌ [强制结束] 用户{user_row['user_id']} 失败: {e}")
        raise

    return result


# ========== 保留原版强制结束单个活动（兼容性）==========
async def _force_end_single_activity(
    conn,
    chat_id: int,
    user_row: dict,
    now: datetime,
    target_date: date,
    business_today: date,
) -> Dict[str, Any]:
    """强制结束单个活动（保留原版，用于兼容性）"""
    # 调用优化版，但需要临时获取配置
    activities = [user_row["current_activity"]]
    activity_configs = await _get_activity_configs_batch(activities)

    return await _force_end_single_activity_optimized(
        conn, chat_id, user_row, now, target_date, business_today, activity_configs
    )


# ========== 4. 补全未打卡的下班记录（优化版）==========
async def _complete_missing_work_ends(
    chat_id: int, target_date: date, business_today: date
) -> Dict[str, Any]:
    """为昨天有上班记录但没有下班记录的用户补全下班记录（优化版）"""
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "day_shift": {"total": 0, "success": 0, "failed": 0},
        "night_shift": {"total": 0, "success": 0, "failed": 0},
        "details": [],
    }

    try:
        if not db.pool or not db._initialized:
            logger.error("数据库连接池未初始化")
            return stats

        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    wr.shift,
                    wr.shift_detail,
                    wr.checkin_time as work_start_time,
                    u.nickname
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1
                  AND wr.record_date = $2
                  AND wr.checkin_type = 'work_start'
                  AND NOT EXISTS(
                      SELECT 1 FROM work_records wr2
                      WHERE wr2.chat_id = wr.chat_id
                        AND wr2.user_id = wr.user_id
                        AND wr2.record_date = wr.record_date
                        AND wr2.shift = wr.shift
                        AND wr2.checkin_type = 'work_end'
                  )
                """,
                chat_id,
                target_date,
            )

            stats["total"] = len(rows)

            if not rows:
                logger.info(f"📝 群组 {chat_id} 昨日没有未下班的用户")
                return stats

            logger.info(f"📝 发现 {len(rows)} 个昨日未下班的用户，开始补全记录...")

            group_data = await db.get_group_cached(chat_id)
            reset_hour = group_data.get("reset_hour", 0)
            reset_minute = group_data.get("reset_minute", 0)
            auto_end_time = f"{reset_hour:02d}:{reset_minute:02d}"

            shift_config = await db.get_shift_config(chat_id)

            # ===== 批量获取工作罚款配置 =====
            work_fine_rates = await db.get_work_fine_rates_for_type("work_end")

            tasks = []
            for row in rows:
                task = asyncio.create_task(
                    _complete_single_work_end_optimized(
                        conn,
                        chat_id,
                        row,
                        target_date,
                        auto_end_time,
                        shift_config,
                        work_fine_rates,
                    )
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    if rows[i]["shift"] == "day":
                        stats["day_shift"]["failed"] += 1
                    else:
                        stats["night_shift"]["failed"] += 1
                    logger.error(
                        f"❌ 补全用户 {rows[i]['user_id']} 下班记录失败: {result}"
                    )
                else:
                    stats["success"] += 1
                    if result["shift"] == "day":
                        stats["day_shift"]["success"] += 1
                    else:
                        stats["night_shift"]["success"] += 1
                    stats["details"].append(result)

            stats["day_shift"]["total"] = sum(1 for r in rows if r["shift"] == "day")
            stats["night_shift"]["total"] = sum(
                1 for r in rows if r["shift"] == "night"
            )

        logger.info(
            f"✅ [补全下班记录完成] 群组 {chat_id}\n"
            f"   ├─ 总计: {stats['total']} 人\n"
            f"   ├─ 成功: {stats['success']} 人\n"
            f"   ├─ 失败: {stats['failed']} 人\n"
            f"   ├─ 白班: {stats['day_shift']['success']}/{stats['day_shift']['total']}\n"
            f"   └─ 夜班: {stats['night_shift']['success']}/{stats['night_shift']['total']}"
        )

    except Exception as e:
        logger.error(f"❌ [补全下班记录] 失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())

    return stats


# ========== 新增：优化版补全单个下班记录 ==========
async def _complete_single_work_end_optimized(
    conn,
    chat_id: int,
    row: dict,
    target_date: date,
    auto_end_time: str,
    shift_config: dict,
    work_fine_rates: Dict[str, int],
) -> Dict[str, Any]:
    """优化版：补全单个用户的下班记录"""
    result = {
        "user_id": row["user_id"],
        "shift": row["shift"],
        "work_start_time": row["work_start_time"],
        "work_end_time": auto_end_time,
        "fine": 0,
        "success": False,
    }

    try:
        if row["shift"] == "day":
            expected_end_time = shift_config.get("day_end", "18:00")
            work_end_date = target_date
        else:
            expected_end_time = shift_config.get("day_start", "09:00")
            work_end_date = target_date + timedelta(days=1)

        work_start_time = datetime.strptime(row["work_start_time"], "%H:%M").time()
        work_start_dt = datetime.combine(target_date, work_start_time)

        expected_end_dt = datetime.combine(
            work_end_date, datetime.strptime(expected_end_time, "%H:%M").time()
        )

        auto_end_dt = datetime.combine(
            work_end_date, datetime.strptime(auto_end_time, "%H:%M").time()
        )

        time_diff_seconds = int((auto_end_dt - expected_end_dt).total_seconds())
        time_diff_minutes = time_diff_seconds / 60

        fine_amount = 0
        if time_diff_seconds < 0 and work_fine_rates:
            thresholds = sorted([int(k) for k in work_fine_rates.keys()])
            for threshold in thresholds:
                if abs(time_diff_minutes) >= threshold:
                    fine_amount = work_fine_rates[str(threshold)]

        work_duration = int((auto_end_dt - work_start_dt).total_seconds())

        if time_diff_seconds < 0:
            status = f"🚨 自动下班（早退 {abs(time_diff_minutes):.1f}分钟）"
        elif time_diff_seconds > 0:
            status = f"✅ 自动下班（加班 {time_diff_minutes:.1f}分钟）"
        else:
            status = "✅ 自动下班（准时）"

        await db.add_work_record(
            chat_id=chat_id,
            user_id=row["user_id"],
            record_date=target_date,
            checkin_type="work_end",
            checkin_time=auto_end_time,
            status=status,
            time_diff_minutes=time_diff_minutes,
            fine_amount=fine_amount,
            shift=row["shift"],
            shift_detail=row.get("shift_detail", row["shift"]),
        )

        await conn.execute(
            """
            INSERT INTO daily_statistics
            (chat_id, user_id, record_date, activity_name, accumulated_time, shift)
            VALUES ($1, $2, $3, 'work_hours', $4, $5)
            ON CONFLICT (chat_id, user_id, record_date, activity_name, shift)
            DO UPDATE SET
                accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                updated_at = CURRENT_TIMESTAMP
            """,
            chat_id,
            row["user_id"],
            target_date,
            work_duration,
            row["shift"],
        )

        result["fine"] = fine_amount
        result["success"] = True

        logger.info(
            f"✅ [补全下班] 用户{row['user_id']} | "
            f"班次:{row['shift']} | 上班:{row['work_start_time']} | "
            f"自动下班:{auto_end_time} | 罚款:{fine_amount}"
        )

    except Exception as e:
        logger.error(f"❌ [补全下班] 用户{row['user_id']} 失败: {e}")
        raise

    return result


# ========== 保留原版补全单个下班记录（兼容性）==========
async def _complete_single_work_end(
    conn,
    chat_id: int,
    row: dict,
    target_date: date,
    auto_end_time: str,
    shift_config: dict,
) -> Dict[str, Any]:
    """保留原版函数，但内部调用优化版"""
    work_fine_rates = await db.get_work_fine_rates_for_type("work_end")
    return await _complete_single_work_end_optimized(
        conn, chat_id, row, target_date, auto_end_time, shift_config, work_fine_rates
    )


# ========== 5. 导出数据 ==========
async def _export_yesterday_data_concurrent(
    chat_id: int, target_date: date, from_monthly: bool = False
) -> bool:
    """并发导出数据，成功一次就推送"""
    from main import export_and_push_csv

    source = "月度表" if from_monthly else "日常表"

    # 使用锁确保只有一个任务能执行推送
    push_lock = asyncio.Lock()
    push_completed = False

    async def task_wrapper(attempt: int) -> bool:
        nonlocal push_completed

        file_name = f"dual_shift_backup_{chat_id}_{target_date.strftime('%Y%m%d')}.csv"

        try:
            # 执行导出，但还不确定是否推送
            result = await export_and_push_csv(
                chat_id=chat_id,
                target_date=target_date,
                file_name=file_name,
                is_daily_reset=True,
                from_monthly_table=True,
                push_file=False,  # 先不推送，只生成数据
            )

            if result:
                # 数据生成成功，现在决定是否推送
                should_push = False
                async with push_lock:
                    if not push_completed:
                        should_push = True
                        push_completed = True

                if should_push:
                    # 需要推送：重新调用但只推送（可以优化为直接使用已生成的文件）
                    await export_and_push_csv(
                        chat_id=chat_id,
                        target_date=target_date,
                        file_name=file_name,
                        is_daily_reset=True,
                        from_monthly_table=True,
                        push_file=True,
                    )
                    logger.info(
                        f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功，已推送"
                    )
                else:
                    logger.info(
                        f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功，已跳过"
                    )

                return True
            return False

        except Exception as e:
            logger.warning(f"⚠️ [数据导出] 第{attempt+1}次尝试失败: {e}")
            return False

    tasks = [asyncio.create_task(task_wrapper(i)) for i in range(3)]
    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r is True)

    if push_completed:
        logger.info(f"📊 [数据导出] 群组{chat_id} 共 {success_count} 次成功，已推送1次")
        return True
    else:
        logger.error(f"❌ [数据导出] 群组{chat_id} 所有3次尝试均失败")
        return False


# ========== 6. 数据清理 ==========
async def _cleanup_old_data(
    chat_id: int, target_date: date, business_today: date
) -> Dict[str, int]:
    """数据清理"""
    stats = {
        "user_activities": 0,
        "work_records": 0,
        "daily_statistics": 0,
        "users_reset": 0,
    }

    try:
        if not db.pool or not db._initialized:
            logger.error("数据库连接池未初始化")
            return stats

        async with db.pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.execute(
                    """
                    DELETE FROM user_activities 
                    WHERE chat_id = $1 AND activity_date = $2
                    """,
                    chat_id,
                    target_date,
                )
                stats["user_activities"] = _parse_delete_count(result)

                result = await conn.execute(
                    """
                    DELETE FROM work_records 
                    WHERE chat_id = $1 AND record_date = $2
                    """,
                    chat_id,
                    target_date,
                )
                stats["work_records"] = _parse_delete_count(result)

                result = await conn.execute(
                    """
                    DELETE FROM daily_statistics 
                    WHERE chat_id = $1 AND record_date = $2
                    """,
                    chat_id,
                    target_date,
                )
                stats["daily_statistics"] = _parse_delete_count(result)

                result = await conn.execute(
                    """
                    UPDATE users 
                    SET current_activity = NULL, 
                        activity_start_time = NULL,
                        last_updated = $2
                    WHERE chat_id = $1 
                      AND last_updated <= $3
                      AND current_activity IS NOT NULL
                    """,
                    chat_id,
                    business_today,
                    target_date,
                )
                stats["users_reset"] = _parse_update_count(result)

        total_deleted = (
            stats["user_activities"] + stats["work_records"] + stats["daily_statistics"]
        )

        logger.info(
            f"🧹 [数据清理] 群组{chat_id}\n"
            f"   • 删除用户活动: {stats['user_activities']} 条\n"
            f"   • 删除工作记录: {stats['work_records']} 条\n"
            f"   • 删除日统计: {stats['daily_statistics']} 条\n"
            f"   • 重置用户状态: {stats['users_reset']} 人\n"
            f"   • 总计删除: {total_deleted} 条\n"
            f"   • 今天数据: ✅ 完整保留 (业务今天 = {business_today})"
        )

    except Exception as e:
        logger.error(f"❌ [数据清理] 失败 {chat_id}: {e}")
        logger.error(traceback.format_exc())

    return stats


# ========== 7. 发送通知 ==========
async def _send_reset_notification(
    chat_id: int,
    force_stats: Dict[str, Any],
    complete_stats: Dict[str, Any],
    export_success: bool,
    cleanup_stats: Dict[str, int],
    reset_time: datetime,
):
    """发送重置通知"""
    try:
        from main import send_reset_notification

        notification_data = {
            "force_activities": force_stats,
            "complete_records": complete_stats,
            "export": export_success,
            "cleanup": cleanup_stats,
            "reset_time": reset_time.strftime("%Y-%m-%d %H:%M:%S"),
            "day_shift": {
                "forced": force_stats.get("day_shift", {}).get("success", 0),
                "completed": complete_stats.get("day_shift", {}).get("success", 0),
            },
            "night_shift": {
                "forced": force_stats.get("night_shift", {}).get("success", 0),
                "completed": complete_stats.get("night_shift", {}).get("success", 0),
            },
        }

        await send_reset_notification(chat_id, notification_data, reset_time)
        logger.info(f"   ✅ 重置通知已发送")

    except Exception as e:
        logger.warning(f"   ⚠️ 发送重置通知失败: {e}")


# ========== 8. 辅助函数 ==========
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


def _parse_update_count(result: str) -> int:
    """解析 UPDATE 语句返回的行数"""
    if not result or not isinstance(result, str):
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0] == "UPDATE":
            return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 0


# ========== 9. 恢复班次状态 ==========
async def recover_shift_states():
    """系统启动时恢复所有用户的班次状态"""
    logger.info("🔄 开始恢复用户班次状态...")
    recovered_count = 0

    try:
        all_groups = await db.get_all_groups()

        for chat_id in all_groups:
            try:
                if not await db.is_dual_mode_enabled(chat_id):
                    continue

                if not db.pool or not db._initialized:
                    logger.error("数据库连接池未初始化")
                    return

                async with db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT 
                            wr.user_id, 
                            wr.shift, 
                            wr.record_date,
                            MIN(wr.created_at) as earliest_time
                        FROM work_records wr
                        WHERE wr.chat_id = $1
                          AND wr.checkin_type = 'work_start'
                          AND NOT EXISTS (
                              SELECT 1 FROM work_records wr2
                              WHERE wr2.chat_id = wr.chat_id
                                AND wr2.user_id = wr.user_id
                                AND wr2.record_date = wr.record_date
                                AND wr2.shift = wr.shift
                                AND wr2.checkin_type = 'work_end'
                          )
                        GROUP BY wr.user_id, wr.shift, wr.record_date
                        """,
                        chat_id,
                    )

                    for row in rows:
                        await db.set_user_shift_state(
                            chat_id=chat_id,
                            user_id=row["user_id"],
                            shift=row["shift"],
                            record_date=row["record_date"],
                        )
                        recovered_count += 1
                        logger.info(
                            f"✅ 恢复用户班次状态: 群组={chat_id}, "
                            f"用户={row['user_id']}, 班次={row['shift']}"
                        )

            except Exception as e:
                logger.error(f"❌ 恢复群组 {chat_id} 班次状态失败: {e}")

        logger.info(f"✅ 用户班次状态恢复完成，共恢复 {recovered_count} 个班次")
        return recovered_count

    except Exception as e:
        logger.error(f"❌ 用户班次状态恢复过程失败: {e}")
        return 0


# ========== 10. 新增：启动时检查未完成重置 ==========
async def check_missed_resets_on_startup():
    """系统启动时检查是否有错过的重置（高性能并发版）"""
    logger.info("🔍 开始检查是否有未完成的重置...")

    try:
        now = db.get_beijing_time()
        all_groups = await db.get_all_groups()

        if not all_groups:
            logger.info("✅ 没有需要检查的群组")
            return

        # ===== 1. 统计变量 =====
        stats = {
            "total": len(all_groups),
            "completed": 0,  # 已完成的
            "executed": 0,  # 自动执行的
            "missed": 0,  # 需手动处理的
            "skipped": 0,  # 无配置的
            "errors": 0,  # 检查失败的
            "failed": 0,  # 执行失败的
        }

        # ===== 2. 并发控制 =====
        sem = asyncio.Semaphore(5)

        async def check_group(chat_id):
            async with sem:
                try:
                    # 获取群组配置
                    group_data = await db.get_group_cached(chat_id)
                    if not group_data:
                        stats["skipped"] += 1
                        return

                    reset_hour = group_data.get("reset_hour", 0)
                    reset_minute = group_data.get("reset_minute", 0)

                    # 获取业务日期
                    date_range = await db.get_business_date_range(chat_id, now)
                    business_yesterday = date_range["business_yesterday"]

                    # 检查是否已完成
                    if await db.is_reset_completed(chat_id, business_yesterday):
                        stats["completed"] += 1
                        return

                    # 计算时间差
                    reset_time_today = now.replace(
                        hour=reset_hour, minute=reset_minute, second=0, microsecond=0
                    )
                    hours_since = (now - reset_time_today).total_seconds() / 3600

                    logger.warning(
                        f"⚠️ 未完成重置: 群组={chat_id}, 日期={business_yesterday}, "
                        f"已过{hours_since:.1f}小时"
                    )

                    # ===== 3. 阈值判断 =====
                    if 0 <= hours_since <= 4:
                        cache_key = f"dual_reset:{chat_id}:{business_yesterday.strftime('%Y%m%d')}"

                        # 检查缓存
                        if await global_cache.get(cache_key):
                            logger.debug(f"⏭️ 缓存已标记: {chat_id}")
                            stats["completed"] += 1
                            return

                        # 执行重置
                        logger.info(f"🔄 自动执行重置: 群组 {chat_id}")
                        success = await handle_hard_reset(
                            chat_id, None, target_date=business_yesterday
                        )

                        if success:
                            stats["executed"] += 1
                            logger.info(f"✅ 自动执行成功")
                        else:
                            stats["failed"] += 1
                            logger.error(f"❌ 自动执行失败")

                    elif hours_since > 4:
                        stats["missed"] += 1
                        logger.info(f"⏭️ 超过4小时，需手动处理")
                    else:
                        stats["completed"] += 1  # 未来时间视为已完成

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"❌ 检查群组 {chat_id} 失败: {e}")

        # ===== 4. 并发执行 =====
        tasks = [check_group(cid) for cid in all_groups]
        await asyncio.gather(*tasks, return_exceptions=True)

        # ===== 5. 生成报告 =====
        report = (
            f"📊 **启动自检报告**\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 总计检查: {stats['total']} 个群组\n"
            f"✅ 已完成: {stats['completed']} 个\n"
            f"🔄 自动执行: {stats['executed']} 个\n"
            f"⚠️ 需手动处理: {stats['missed']} 个\n"
            f"⏭️ 跳过: {stats['skipped']} 个\n"
            f"❌ 执行失败: {stats['failed']} 个\n"
            f"❗ 检查失败: {stats['errors']} 个\n"
            f"━━━━━━━━━━━━━━━━"
        )

        logger.info(f"\n{report}")

        # ===== 6. 如果有失败，发送通知给管理员 =====
        if stats["failed"] > 0 or stats["errors"] > 0:
            from utils import notification_service

            await notification_service.send_notification(
                chat_id=None,  # 发送给所有管理员
                text=f"⚠️ **启动自检警告**\n{stats['failed']} 个重置失败，{stats['errors']} 个检查失败",
                notification_type="admin",
            )

    except Exception as e:
        logger.error(f"❌ 启动检查异常: {e}")
        logger.error(traceback.format_exc())
