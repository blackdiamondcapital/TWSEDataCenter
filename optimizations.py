#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统优化模块
提供数据库锁、缓存、进度追踪等功能
"""

import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# 1. 数据库并发控制
# ============================================================================

class DatabaseLockManager:
    """数据库锁管理器 - 防止并发死锁"""
    
    def __init__(self):
        self.table_lock = threading.Lock()
        self.write_lock = threading.Semaphore(3)  # 最多3个并发写入
        
    def acquire_table_lock(self):
        """获取表结构修改锁"""
        return self.table_lock.acquire(timeout=30)
    
    def release_table_lock(self):
        """释放表结构修改锁"""
        self.table_lock.release()
    
    def acquire_write_lock(self):
        """获取写入锁"""
        return self.write_lock.acquire(timeout=60)
    
    def release_write_lock(self):
        """释放写入锁"""
        self.write_lock.release()


# 全局锁实例
db_lock_manager = DatabaseLockManager()


# ============================================================================
# 2. API 请求缓存
# ============================================================================

class APICache:
    """API 请求缓存 - 减少重复请求"""
    
    def __init__(self, ttl=3600):
        self.cache = {}
        self.ttl = ttl  # 缓存有效期（秒）
        self.lock = threading.Lock()
    
    def get(self, key):
        """获取缓存"""
        with self.lock:
            if key in self.cache:
                data, timestamp = self.cache[key]
                # 检查是否过期
                if time.time() - timestamp < self.ttl:
                    logger.debug(f"缓存命中: {key}")
                    return data
                else:
                    # 过期，删除
                    del self.cache[key]
        return None
    
    def set(self, key, data):
        """设置缓存"""
        with self.lock:
            self.cache[key] = (data, time.time())
            logger.debug(f"缓存设置: {key}")
    
    def clear(self):
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            logger.info("缓存已清空")
    
    def get_stats(self):
        """获取缓存统计"""
        with self.lock:
            total = len(self.cache)
            expired = sum(
                1 for _, timestamp in self.cache.values()
                if time.time() - timestamp >= self.ttl
            )
            return {
                'total': total,
                'valid': total - expired,
                'expired': expired
            }


# 全局缓存实例
api_cache = APICache(ttl=3600)  # 1小时过期


# ============================================================================
# 3. 进度追踪器
# ============================================================================

class ProgressTracker:
    """进度追踪器 - 实时显示更新进度"""
    
    def __init__(self, total, task_name="更新"):
        self.total = total
        self.task_name = task_name
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = datetime.now()
        self.results = []
        self.lock = threading.Lock()
    
    def update(self, success=True, symbol=None, count=0, error=None):
        """更新进度"""
        with self.lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1
            
            # 记录结果
            self.results.append({
                'symbol': symbol,
                'success': success,
                'count': count,
                'error': str(error) if error else None,
                'time': datetime.now()
            })
            
            # 每10个或失败时输出
            if self.completed % 10 == 0 or not success:
                self.print_progress()
    
    def skip(self, symbol=None, reason=None):
        """跳过某个项目"""
        with self.lock:
            self.skipped += 1
            self.results.append({
                'symbol': symbol,
                'success': False,
                'skipped': True,
                'reason': reason,
                'time': datetime.now()
            })
    
    def print_progress(self):
        """打印进度信息"""
        processed = self.completed + self.failed + self.skipped
        percentage = (processed / self.total * 100) if self.total > 0 else 0
        
        # 计算剩余时间
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if processed > 0:
            avg_time = elapsed / processed
            remaining_seconds = (self.total - processed) * avg_time
            remaining_str = f"{int(remaining_seconds // 60)}分{int(remaining_seconds % 60)}秒"
        else:
            remaining_str = "计算中..."
        
        logger.info(
            f"📊 {self.task_name} 进度: {processed}/{self.total} ({percentage:.1f}%) | "
            f"✅ {self.completed} ❌ {self.failed} ⏭️ {self.skipped} | "
            f"⏱️ 预计剩余: {remaining_str}"
        )
    
    def get_summary(self):
        """获取摘要"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'total': self.total,
            'completed': self.completed,
            'failed': self.failed,
            'skipped': self.skipped,
            'success_rate': (self.completed / self.total * 100) if self.total > 0 else 0,
            'elapsed_seconds': elapsed,
            'elapsed_str': f"{int(elapsed // 60)}分{int(elapsed % 60)}秒"
        }
    
    def print_summary(self):
        """打印摘要"""
        summary = self.get_summary()
        
        logger.info("=" * 60)
        logger.info(f"🎯 {self.task_name} 完成摘要")
        logger.info("=" * 60)
        logger.info(f"总数: {summary['total']}")
        logger.info(f"✅ 成功: {summary['completed']}")
        logger.info(f"❌ 失败: {summary['failed']}")
        logger.info(f"⏭️ 跳过: {summary['skipped']}")
        logger.info(f"成功率: {summary['success_rate']:.1f}%")
        logger.info(f"⏱️ 总耗时: {summary['elapsed_str']}")
        logger.info("=" * 60)


# ============================================================================
# 4. 错误分类器
# ============================================================================

class ErrorClassifier:
    """错误分类器 - 智能分类和处理错误"""
    
    ERROR_TYPES = {
        'API_TIMEOUT': {
            'name': 'API 超时',
            'keywords': ['timeout', 'timed out'],
            'severity': 'warning',
            'retry': True
        },
        'API_500': {
            'name': 'API 服务器错误',
            'keywords': ['500', 'internal server error'],
            'severity': 'error',
            'retry': True
        },
        'API_404': {
            'name': 'API 未找到',
            'keywords': ['404', 'not found'],
            'severity': 'info',
            'retry': False
        },
        'PARSE_ERROR': {
            'name': '数据解析失败',
            'keywords': ['parse', 'convert', 'float'],
            'severity': 'warning',
            'retry': False
        },
        'DB_DEADLOCK': {
            'name': '数据库死锁',
            'keywords': ['deadlock', 'transaction'],
            'severity': 'critical',
            'retry': True
        },
        'DB_CONSTRAINT': {
            'name': '数据库约束',
            'keywords': ['constraint', 'unique', 'foreign key'],
            'severity': 'warning',
            'retry': False
        },
        'NO_DATA': {
            'name': '无数据（停牌/下市）',
            'keywords': ['no data', '没有抓到', '---'],
            'severity': 'info',
            'retry': False
        },
        'NETWORK_ERROR': {
            'name': '网络错误',
            'keywords': ['connection', 'network', 'dns'],
            'severity': 'error',
            'retry': True
        }
    }
    
    def __init__(self):
        self.error_counts = defaultdict(int)
        self.lock = threading.Lock()
    
    def classify(self, error):
        """分类错误"""
        error_str = str(error).lower()
        
        for error_type, config in self.ERROR_TYPES.items():
            if any(keyword in error_str for keyword in config['keywords']):
                with self.lock:
                    self.error_counts[error_type] += 1
                return error_type, config
        
        # 未知错误
        with self.lock:
            self.error_counts['UNKNOWN'] += 1
        return 'UNKNOWN', {
            'name': '未知错误',
            'severity': 'error',
            'retry': False
        }
    
    def should_retry(self, error):
        """判断是否应该重试"""
        error_type, config = self.classify(error)
        return config.get('retry', False)
    
    def get_statistics(self):
        """获取错误统计"""
        with self.lock:
            return dict(self.error_counts)
    
    def print_statistics(self):
        """打印错误统计"""
        stats = self.get_statistics()
        if not stats:
            logger.info("✅ 无错误记录")
            return
        
        logger.info("=" * 60)
        logger.info("⚠️ 错误统计")
        logger.info("=" * 60)
        
        total = sum(stats.values())
        for error_type, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
            config = self.ERROR_TYPES.get(error_type, {})
            name = config.get('name', error_type)
            percentage = (count / total * 100) if total > 0 else 0
            logger.info(f"{name}: {count} 次 ({percentage:.1f}%)")
        
        logger.info("=" * 60)


# 全局错误分类器
error_classifier = ErrorClassifier()


# ============================================================================
# 5. 智能批次计算器
# ============================================================================

class BatchOptimizer:
    """批次优化器 - 智能计算最优批次大小"""
    
    def __init__(self):
        self.history = []  # 记录历史性能
    
    def calculate_batch_size(self, total_items, days_per_item, target_time=45):
        """
        计算最优批次大小
        
        Args:
            total_items: 总项目数
            days_per_item: 每个项目的天数
            target_time: 目标批次时间（秒）
        
        Returns:
            批次大小
        """
        # 估算单个项目时间
        time_per_item = days_per_item * 0.5 + 3  # API 0.5秒/天 + 写入3秒
        
        # 计算批次大小
        batch_size = int(target_time / time_per_item)
        
        # 限制范围
        batch_size = max(5, min(20, batch_size))
        
        # 根据历史调整
        if len(self.history) > 5:
            avg_actual = sum(h['actual_time'] for h in self.history[-5:]) / 5
            if avg_actual > target_time * 1.5:
                batch_size = int(batch_size * 0.8)  # 减小批次
            elif avg_actual < target_time * 0.5:
                batch_size = int(batch_size * 1.2)  # 增大批次
        
        return batch_size
    
    def record_performance(self, batch_size, items_count, actual_time):
        """记录实际性能"""
        self.history.append({
            'batch_size': batch_size,
            'items_count': items_count,
            'actual_time': actual_time,
            'timestamp': datetime.now()
        })
        
        # 只保留最近20条记录
        if len(self.history) > 20:
            self.history = self.history[-20:]
    
    def get_recommendation(self, total_items, days_per_item):
        """获取批次建议"""
        batch_size = self.calculate_batch_size(total_items, days_per_item)
        num_batches = (total_items + batch_size - 1) // batch_size
        
        time_per_batch = (days_per_item * 0.5 + 3) * batch_size
        total_time = time_per_batch * num_batches
        
        return {
            'batch_size': batch_size,
            'num_batches': num_batches,
            'estimated_time_per_batch': f"{int(time_per_batch // 60)}分{int(time_per_batch % 60)}秒",
            'estimated_total_time': f"{int(total_time // 60)}分{int(total_time % 60)}秒"
        }


# 全局批次优化器
batch_optimizer = BatchOptimizer()


# ============================================================================
# 6. 使用示例
# ============================================================================

if __name__ == '__main__':
    # 示例：使用进度追踪器
    print("\n示例 1: 进度追踪器")
    print("=" * 60)
    
    tracker = ProgressTracker(100, "测试更新")
    for i in range(100):
        success = i % 10 != 7  # 模拟偶尔失败
        tracker.update(success=success, symbol=f"TEST{i}", count=5 if success else 0)
        time.sleep(0.01)
    
    tracker.print_summary()
    
    # 示例：批次优化器
    print("\n示例 2: 批次优化器")
    print("=" * 60)
    
    recommendation = batch_optimizer.get_recommendation(
        total_items=857,  # 所有上櫃股票
        days_per_item=30   # 最近 30 天
    )
    
    print(f"总股票数: 857")
    print(f"日期范围: 30 天")
    print(f"建议批次大小: {recommendation['batch_size']}")
    print(f"预计批次数: {recommendation['num_batches']}")
    print(f"预计每批时间: {recommendation['estimated_time_per_batch']}")
    print(f"预计总时间: {recommendation['estimated_total_time']}")
    
    # 示例：错误分类
    print("\n示例 3: 错误分类器")
    print("=" * 60)
    
    test_errors = [
        Exception("Request timeout"),
        Exception("HTTP 500 Internal Server Error"),
        Exception("could not convert string to float: ' ---'"),
        Exception("deadlock detected"),
        Exception("Connection refused"),
    ]
    
    for error in test_errors:
        error_type, config = error_classifier.classify(error)
        print(f"错误: {error}")
        print(f"  类型: {config['name']}")
        print(f"  是否重试: {'是' if config['retry'] else '否'}")
        print()
    
    error_classifier.print_statistics()
