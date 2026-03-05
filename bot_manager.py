import asyncio
import logging
import time
from typing import Optional, Dict, Any
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import Config

from fault_tolerance import telegram_circuit_breaker

logger = logging.getLogger("GroupCheckInBot")


class RobustBotManager:
    """健壮的Bot管理器 - 带自动重连"""

    def __init__(self, token: str):
        self.token = token
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self._is_running = False
        self._polling_task: Optional[asyncio.Task] = None
        self._max_retries = 10
        self._base_delay = 2.0
        self._current_retry = 0
        self._last_successful_connection = 0
        self._connection_check_interval = 300

    async def initialize(self):
        """初始化Bot"""
        self.bot = Bot(token=self.token)
        self.dispatcher = Dispatcher(storage=MemoryStorage())
        logger.info("Bot管理器初始化完成")

    async def start_polling_with_retry(self):
        """带重试的轮询启动"""
        self._is_running = True
        self._current_retry = 0

        while self._is_running and self._current_retry < self._max_retries:
            try:
                self._current_retry += 1
                logger.info(
                    f"🤖 启动Bot轮询 (尝试 {self._current_retry}/{self._max_retries})"
                )

                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info("✅ Webhook已删除，使用轮询模式")

                await self.dispatcher.start_polling(
                    self.bot,
                    skip_updates=True,
                    allowed_updates=["message", "callback_query", "chat_member"],
                )

                self._last_successful_connection = time.time()
                logger.info("Bot轮询正常结束")
                break

            except asyncio.CancelledError:
                logger.info("Bot轮询被取消")
                break

            except Exception as e:
                logger.error(f"❌ Bot轮询失败 (尝试 {self._current_retry}): {e}")

                if self._current_retry >= self._max_retries:
                    logger.critical(
                        f"🚨 Bot启动重试{self._max_retries}次后失败，停止尝试"
                    )
                    break

                delay = self._base_delay * (2 ** (self._current_retry - 1))
                delay = min(delay, 300)

                logger.info(f"⏳ {delay:.1f}秒后第{self._current_retry + 1}次重试...")
                await asyncio.sleep(delay)

    async def stop(self):
        """停止Bot"""
        self._is_running = False

        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                logger.info("Bot轮询任务已取消")

        if self.bot:
            await self.bot.session.close()
            logger.info("Bot会话已关闭")

    async def send_message_with_retry(self, chat_id: int, text: str, **kwargs) -> bool:
        """带重试的消息发送"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True

            except Exception as e:
                error_msg = str(e).lower()

                if any(
                    keyword in error_msg
                    for keyword in [
                        "timeout",
                        "connection",
                        "network",
                        "flood",
                        "retry",
                        "cannot connect",
                        "connectorerror",
                        "ssl",
                        "socket",
                    ]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * (2 ** (attempt - 1))
                    delay = min(delay, 30)

                    logger.warning(
                        f"📤 发送消息失败(网络问题)，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                elif any(
                    keyword in error_msg
                    for keyword in [
                        "forbidden",
                        "blocked",
                        "unauthorized",
                        "chat not found",
                        "bot was blocked",
                        "user is deactivated",
                    ]
                ):
                    logger.warning(f"📤 发送消息失败(权限问题): {e}")
                    return False

                else:
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * attempt
                    logger.warning(
                        f"📤 发送消息失败，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

        return False

    async def send_document_with_retry(
        self, chat_id: int, document, caption: str = "", **kwargs
    ) -> bool:
        """带重试的文档发送"""
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_document(
                    chat_id, document, caption=caption, **kwargs
                )
                return True

            except Exception as e:
                error_msg = str(e).lower()

                if any(
                    keyword in error_msg
                    for keyword in [
                        "timeout",
                        "connection",
                        "network",
                        "flood",
                        "retry",
                    ]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📎 发送文档重试{max_attempts}次后失败: {e}")
                        return False

                    delay = attempt * 2
                    logger.warning(
                        f"📎 发送文档失败，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"📎 发送文档失败（不重试）: {e}")
                    return False

        return False

    async def send_message_with_protection(
        self, chat_id: int, text: str, **kwargs
    ) -> bool:
        """
        带熔断器保护的消息发送

        使用熔断器防止Telegram API故障导致雪崩效应

        Args:
            chat_id: 目标聊天ID
            text: 消息文本
            **kwargs: 其他参数 (parse_mode, reply_markup等)

        Returns:
            bool: 是否发送成功
        """

        async def _send():
            """实际发送函数"""
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True
            except Exception as e:
                logger.error(f"❌ 发送消息失败: {e}")
                raise  # 重新抛出异常，让熔断器捕获

        try:
            return await telegram_circuit_breaker.call(_send)
        except Exception as e:
            logger.error(f"❌ 熔断器保护的消息发送失败: {e}")
            return False

    def is_healthy(self) -> bool:
        """检查Bot健康状态"""
        if not self._last_successful_connection:
            return False

        time_since_last_success = time.time() - self._last_successful_connection
        return time_since_last_success < self._connection_check_interval

    async def restart_polling(self):
        """重启轮询"""
        logger.info("🔄 重启Bot轮询...")
        await self.stop()
        await asyncio.sleep(2)
        await self.start_polling_with_retry()

    async def start_health_monitor(self):
        """启动健康监控"""
        asyncio.create_task(self._health_monitor_loop())

    async def _health_monitor_loop(self):
        """健康监控循环"""
        while self._is_running:
            try:
                await asyncio.sleep(60)

                if not self.is_healthy():
                    logger.warning("Bot连接不健康，尝试重启...")
                    await self.restart_polling()

            except Exception as e:
                logger.error(f"健康监控异常: {e}")
                await asyncio.sleep(30)


bot_manager = RobustBotManager(Config.TOKEN)
