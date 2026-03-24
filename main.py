import os
import re
import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone
from collections import deque
from telethon import TelegramClient, events, connection
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, Channel
from telethon.errors import FloodWaitError
# ========== 配置项 ==========
api_id = 25559912  # 请立即替换为你重置后的新api_id
api_hash = '22d3bb9665ad7e6a86e89c1445672e07'  # 请立即替换为你重置后的新api_hash
session_name = "session"
# 频道转发配对，可自由增减
channels = [
    {'source': '@wenan77','target': '@wnffx'},
    {'source': '@chigua_a','target': '@hrgxx'}
]
max_text_length = 300  # 最大允许的文本长度
forward_interval = 5  # 转发间隔（秒），已调大降低限流风险
media_group_wait_time = 12  # 媒体组等待时长（秒），核心修复项，确保同组内容完整接收
max_cache_size = 2000  # 已处理消息ID缓存上限
restart_interval_hours = 12  # 定时重启间隔（小时）
max_retry = 5  # 发送失败最大重试次数
ALLOWED_VIDEO_MIMES = {'video/mp4', 'video/mov', 'video/avi', 'video/mkv', 'video/webm', 'video/flv'}
# ========== 全局状态 ==========
stop_event = asyncio.Event()
shutdown_lock = asyncio.Lock()
is_shutting_down = False
last_forward_time = 0
forward_lock = asyncio.Lock()
processed_msg_ids = deque(maxlen=max_cache_size)
media_group_cache = {}
media_group_lock = asyncio.Lock()
valid_channels = []
channel_map = {}
valid_source_ids = []
active_tasks = set()
client = None
# ========== 工具函数 ==========
def log_with_time(msg: str):
    beijing_time = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{beijing_time}] {msg}")
def clean_text(text):
    if not text:
        return ""
    # 清除链接、@用户名、多余换行（已修复正则转义警告）
    text = re.sub(r"https?://[^\s\u4e00-\u9fa5，。！？；：\"'()（）、]+|t\.me/[^\s\u4e00-\u9fa5，。！？；：\"'()（）、]+", '', text)
    text = re.sub(r"@[a-zA-Z0-9_]{5,32}", '', text)
    return re.sub(r"\n+", '\n', text).strip()
async def rate_limit_wait():
    global last_forward_time
    async with forward_lock:
        now = time.time()
        wait_time = forward_interval - (now - last_forward_time)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        last_forward_time = time.time()
def track_task(task):
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)
# ========== 定时重启 ==========
async def auto_restart_scheduler():
    while True:
        await asyncio.sleep(restart_interval_hours * 3600)
        if is_shutting_down:
            break
        log_with_time("⏰ 到达定时重启时间，准备优雅重启服务...")
        stop_event.set()
        break
# ========== 优雅关闭（已修复死锁+加超时保护） ==========
async def graceful_shutdown(client: TelegramClient):
    global is_shutting_down
    async with shutdown_lock:
        if is_shutting_down:
            return
        is_shutting_down = True
    log_with_time("🔌 开始优雅关闭，等待所有活跃任务完成...")
    if active_tasks:
        try:
            # 最多等待30秒，超时强制继续关闭，避免无限卡住
            await asyncio.wait_for(asyncio.gather(*active_tasks, return_exceptions=True), timeout=30)
            log_with_time("✅ 所有活跃任务已完成")
        except asyncio.TimeoutError:
            log_with_time("⚠️  等待活跃任务超时（30秒），强制继续关闭流程")
    log_with_time("✅ 正在断开客户端连接...")
    try:
        # 断开连接也加超时，避免网络异常卡死
        await asyncio.wait_for(client.disconnect(), timeout=10)
        log_with_time("✅ 客户端已正常断开，进程即将退出")
    except Exception as e:
        log_with_time(f"⚠️  断开连接时出错：{str(e)}，强制退出进程")
async def stop_watcher(client: TelegramClient):
    await stop_event.wait()
    await graceful_shutdown(client)
# ========== 频道校验 ==========
async def check_channels(client: TelegramClient, me):
    log_with_time("=== 正在检查频道配置 ===")
    global valid_channels, channel_map, valid_source_ids
    valid_list, channel_map, valid_source_ids = [], {}, []
    for idx, channel in enumerate(channels):
        source_config, target_config = channel['source'], channel['target']
        log_with_time(f"\n--- 检查配对{idx+1}：监听 {source_config} → 转发到 {target_config} ---")
        # 源频道校验
        try:
            source_chat = await client.get_entity(source_config)
            if not isinstance(source_chat, Channel) or not source_chat.broadcast:
                log_with_time(f"⚠️  警告：配对{idx+1}的源 {source_config} 不是频道类型，已跳过")
                continue
            real_username = source_chat.username if source_chat.username else '私有频道'
            log_with_time(f"ℹ️  频道真实ID：{source_chat.id} | 用户名：@{real_username}")
            log_with_time(f"✅ 源频道校验通过，access_hash已缓存")
        except Exception as e:
            log_with_time(f"❌ 源频道 {source_config} 访问失败 | 详情：{str(e)}")
            continue
        # 目标频道校验
        try:
            target_chat = await client.get_entity(target_config)
            if not isinstance(target_chat, Channel):
                log_with_time(f"⚠️  警告：配对{idx+1}的目标 {target_config} 不是频道类型，已跳过")
                continue
            permissions = await client.get_permissions(target_chat, me)
            if not permissions.post_messages:
                log_with_time(f"❌ 目标频道 {target_config} 校验失败：账号无发帖权限，已跳过")
                continue
            log_with_time(f"✅ 目标频道校验通过，发帖权限正常，access_hash已缓存")
        except Exception as e:
            log_with_time(f"❌ 目标频道 {target_config} 访问失败 | 详情：{str(e)}")
            continue
        # 存入配置
        valid_item = {'source_config': source_config,'target': target_config,'source_id': source_chat.id,'target_entity': target_chat}
        valid_list.append(valid_item)
        channel_map[source_chat.id] = valid_item
        valid_source_ids.append(source_chat.id)
    valid_channels = valid_list
    if len(valid_channels) > 0:
        log_with_time(f"\n✅ 共 {len(valid_channels)} 组频道配置生效")
        for item in valid_channels:
            try:
                latest_msg = await client.get_messages(item['source_id'], limit=1)
                if latest_msg:
                    processed_msg_ids.append( (item['source_id'], latest_msg[0].id) )
            except Exception as e:
                log_with_time(f"⚠️  预加载最新消息失败 | 源：{item['source_config']} | 详情：{str(e)}")
    else:
        log_with_time("\n❌ 无可用频道配置，程序无法启动")
    return len(valid_channels) > 0
# ========== 媒体组处理 ==========
async def process_media_group(grouped_id):
    global client
    try:
        async with media_group_lock:
            if grouped_id not in media_group_cache:
                return
            group_data = media_group_cache.pop(grouped_id)
        msg_list, source_chat, target_item, source_name = group_data['msg_list'], group_data['source_chat'], group_data['target_item'], group_data['source_name']
        
        first_msg = msg_list[0]
        if (source_chat.id, first_msg.id) in processed_msg_ids:
            log_with_time(f"⏭️  已跳过 | 源：{source_name} | 同一条消息已转发")
            return
        
        # 拦截带按钮的媒体组消息，同组任意一条消息带按钮直接拦截
        has_button = False
        for msg in msg_list:
            if msg.reply_markup and hasattr(msg.reply_markup, 'rows') and len(msg.reply_markup.rows) > 0:
                has_button = True
                break
        if has_button:
            log_with_time(f"⏭️  已拦截 | 源：{source_name} | 媒体组消息带有按钮，不符合转发规则")
            return
        
        processed_msg_ids.append( (source_chat.id, first_msg.id) )
        
        # 过滤有效媒体
        valid_media = []
        for msg in msg_list:
            try:
                if isinstance(msg.media, MessageMediaPhoto):
                    valid_media.append(msg.media)
                elif isinstance(msg.media, MessageMediaDocument):
                    mime_type = msg.media.document.mime_type
                    if mime_type in ALLOWED_VIDEO_MIMES:
                        valid_media.append(msg.media)
            except Exception as e:
                log_with_time(f"⚠️  跳过无效媒体 | 源：{source_name} | 详情：{str(e)}")
                continue
        if not valid_media:
            log_with_time(f"⏭️  已拦截 | 源：{source_name} | 无有效图片/视频媒体")
            return
        
        log_with_time(f"ℹ️  媒体组收集完成 | 源：{source_name} | 共收集到{len(valid_media)}个媒体文件")
        
        # 文本处理
        raw_text = "\n".join([msg.text for msg in msg_list if msg.text.strip()])
        cleaned_text = clean_text(raw_text)
        if len(cleaned_text) > max_text_length:
            log_with_time(f"⏭️  已拦截 | 源：{source_name} | 文本长度{len(cleaned_text)}，超过限制")
            return
        
        await rate_limit_wait()
        # 全有或全无重试逻辑，修复部分发送导致的拆分
        retry_count = 0
        send_success = False
        while retry_count < max_retry and not send_success:
            try:
                await client.send_message(target_item['target_entity'], message=cleaned_text, file=valid_media, silent=True)
                log_with_time(f"✅ 媒体组转发成功 | 源：{source_name} → 目标：{target_item['target']} | 媒体数：{len(valid_media)}")
                send_success = True
                break
            except FloodWaitError as e:
                retry_count += 1
                wait_time = e.seconds + 5  # 额外增加等待时长，避免再次触发限流
                log_with_time(f"⚠️  触发限流，等待{wait_time}秒后重试（第{retry_count}次）")
                await asyncio.sleep(wait_time)
            except Exception as e:
                retry_count += 1
                log_with_time(f"❌ 媒体组转发失败，第{retry_count}次重试 | 详情：{str(e)}")
                await asyncio.sleep(3)
        if not send_success:
            log_with_time(f"❌ 媒体组最终转发失败，已跳过 | 源：{source_name}")
    except Exception as e:
        # 全局兜底，任何异常不导致程序崩溃
        if "Could not find a matching Constructor ID" in str(e):
            log_with_time(f"⚠️  跳过无法解析的媒体组消息 | 详情：Telegram协议不兼容，已跳过该条消息")
        else:
            log_with_time(f"❌ 媒体组处理失败 | 详情：{str(e)}")
        async with media_group_lock:
            if grouped_id in media_group_cache:
                del media_group_cache[grouped_id]
# ========== 主程序 ==========
async def main():
    global client
    # 已优化连接配置，提升稳定性，减少断开重连
    client = TelegramClient(
        session_name, api_id, api_hash,
        auto_reconnect=True, connection_retries=None, retry_delay=5, timeout=60,
        flood_sleep_threshold=120, catch_up=True,
        device_model="Pixel 7", system_version="Android 14", app_version="10.13.0",
        lang_code="zh-CN", system_lang_code="zh-CN",
        connection=connection.TCPFull,  # 更稳定的全连接模式
        use_ipv6=False,  # 强制IPv4，避免IPv6网络波动导致断开
        receive_updates=True
    )
    async with client:
        me = await client.get_me()
        log_with_time(f"✅ 已登录账号：@{me.username} | 用户ID：{me.id}")
        # 预加载所有对话，缓存access_hash
        log_with_time("正在预加载所有对话，缓存access_hash...")
        try:
            await client.get_dialogs(limit=None)
            log_with_time("✅ 所有对话预加载完成，access_hash已全部缓存")
        except Exception as e:
            log_with_time(f"⚠️  对话预加载失败，程序仍可正常运行 | 详情：{str(e)}")
        
        # 频道校验
        check_result = await check_channels(client, me)
        if not check_result:
            return
        
        # 重复配置提醒
        if len(valid_source_ids) != len(set(valid_source_ids)):
            log_with_time("⚠️  检测到重复的源频道，重复项仅第一个生效")
        
        # 规则打印
        log_with_time("\n=== 转发规则已生效 ===")
        log_with_time(f"✅ 允许转发：带图片/视频的消息（含多图媒体组），清洗后文本≤{max_text_length}字，无按钮")
        log_with_time(f"❌ 禁止转发：纯文字消息、文本超{max_text_length}字的消息、非图片/视频媒体、带按钮的消息")
        log_with_time(f"⏰ 定时重启：已开启，每{restart_interval_hours}小时自动重启一次")
        log_with_time(f"🕵️  无来源转发：已开启，转发消息无任何原频道标识")
        for idx, channel in enumerate(valid_channels):
            log_with_time(f"配对{idx+1}：监听 {channel['source_config']} → 转发到 {channel['target']}")
        log_with_time("\n机器人已启动，正在监听消息...\n")
        
        # 启动定时任务（已修复死锁：stop_watcher不加入活跃任务）
        track_task(asyncio.create_task(auto_restart_scheduler()))
        asyncio.create_task(stop_watcher(client))
        
        # 消息监听器
        @client.on(events.NewMessage(chats=valid_source_ids))
        async def handler(event):
            if is_shutting_down:
                return
            # 全局兜底，单条消息异常不导致程序崩溃
            try:
                msg = event.message
                source_chat = event.chat
                source_id = source_chat.id
                source_name = f"@{source_chat.username}" if source_chat.username else f"频道ID:{source_id}"
                grouped_id = msg.grouped_id
                target_item = channel_map.get(source_id)
                
                if not target_item:
                    log_with_time(f"⏭️  已拦截 | 源：{source_name} | 无匹配目标频道")
                    return
                
                # 拦截带按钮的单条消息，优先执行
                if msg.reply_markup and hasattr(msg.reply_markup, 'rows') and len(msg.reply_markup.rows) > 0:
                    log_with_time(f"⏭️  已拦截 | 源：{source_name} | 消息带有按钮，不符合转发规则")
                    return
                
                # 媒体组处理逻辑
                if grouped_id:
                    async with media_group_lock:
                        is_new_group = grouped_id not in media_group_cache
                        if is_new_group:
                            # 新分组初始化缓存
                            media_group_cache[grouped_id] = {
                                'msg_list': [], 'source_chat': source_chat,
                                'target_item': target_item, 'source_name': source_name
                            }
                        # 追加当前消息到分组
                        media_group_cache[grouped_id]['msg_list'].append(msg)
                        # 仅新分组创建延迟处理任务，确保所有同组消息都能追加完成
                        if is_new_group:
                            async def delayed_process():
                                # 等待完整时长后再处理，确保同组所有消息已接收
                                await asyncio.sleep(media_group_wait_time)
                                await process_media_group(grouped_id)
                            track_task(asyncio.create_task(delayed_process()))
                    return
                
                # 处理单媒体消息（单图/单视频）
                if (source_id, msg.id) in processed_msg_ids:
                    log_with_time(f"⏭️  已跳过 | 源：{source_name} | 同一条消息已转发")
                    return
                processed_msg_ids.append( (source_id, msg.id) )
                
                # 媒体校验
                if not msg.media:
                    log_with_time(f"⏭️  已拦截 | 源：{source_name} | 纯文字消息")
                    return
                valid_media = None
                if isinstance(msg.media, MessageMediaPhoto):
                    valid_media = msg.media
                elif isinstance(msg.media, MessageMediaDocument):
                    mime_type = msg.media.document.mime_type
                    if mime_type in ALLOWED_VIDEO_MIMES:
                        valid_media = msg.media
                if not valid_media:
                    log_with_time(f"⏭️  已拦截 | 源：{source_name} | 非图片/视频媒体")
                    return
                
                # 文本处理
                raw_text = msg.text or ""
                cleaned_text = clean_text(raw_text)
                if len(cleaned_text) > max_text_length:
                    log_with_time(f"⏭️  已拦截 | 源：{source_name} | 文本长度{len(cleaned_text)}，超过限制")
                    return
                
                await rate_limit_wait()
                # 单媒体重试逻辑
                retry_count = 0
                send_success = False
                while retry_count < max_retry and not send_success:
                    try:
                        await client.send_message(target_item['target_entity'], message=cleaned_text, file=valid_media, silent=True)
                        log_with_time(f"✅ 单媒体转发成功 | 源：{source_name} → 目标：{target_item['target']}")
                        send_success = True
                        break
                    except FloodWaitError as e:
                        retry_count += 1
                        wait_time = e.seconds + 5
                        log_with_time(f"⚠️  触发限流，等待{wait_time}秒后重试（第{retry_count}次）")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        retry_count += 1
                        log_with_time(f"❌ 单媒体转发失败，第{retry_count}次重试 | 详情：{str(e)}")
                        await asyncio.sleep(3)
                if not send_success:
                    log_with_time(f"❌ 单媒体最终转发失败，已跳过 | 源：{source_name}")
            except Exception as e:
                # 核心兜底，捕获协议解析错误
                if "Could not find a matching Constructor ID" in str(e):
                    log_with_time(f"⚠️  跳过无法解析的消息 | 详情：Telegram协议不兼容，已跳过该条消息")
                else:
                    log_with_time(f"❌ 消息处理失败 | 详情：{str(e)}")
        
        await client.run_until_disconnected()
if __name__ == "__main__":
    try:
        asyncio.run(main())
        log_with_time("✅ 程序已正常退出")
        sys.exit(0)
    except KeyboardInterrupt:
        log_with_time("\n✅ 程序已手动停止")
    except Exception as e:
        log_with_time(f"❌ 程序异常退出：{str(e)}")
        sys.exit(1)
