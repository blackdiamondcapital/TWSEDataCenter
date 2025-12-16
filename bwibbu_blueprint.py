# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, date
import time
from typing import Dict, Any

from flask import Blueprint, request, jsonify
import logging
logger = logging.getLogger(__name__)


def _date_range(start_str: str, end_str: str):
    start_dt = datetime.strptime(start_str, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_str, '%Y-%m-%d').date()
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
    cur = start_dt
    while cur <= end_dt:
        # 只處理工作日（依 server 現有邏輯，BWIBBU_d 僅交易日有資料；這裡不過濾週末，交給 API 回傳空）
        yield cur
        cur = cur + timedelta(days=1)

def create_bwibbu_blueprint(DatabaseManager, stock_api):
    """使用注入的 DatabaseManager 與 stock_api 建立 BWIBBU Blueprint。
    避免循環匯入，由 server.py 在定義完成後呼叫本工廠函式。
    """
    bwibbu_bp = Blueprint('bwibbu', __name__, url_prefix='/api/bwibbu')

    @bwibbu_bp.route('/backfill', methods=['POST'])
    def backfill():
        try:
            payload: Dict[str, Any] = request.get_json() or {}
            start_str = payload.get('start')
            end_str = payload.get('end')
            if not start_str or not end_str:
                return jsonify({'success': False, 'error': '缺少 start 或 end'}), 400
            try:
                datetime.strptime(start_str, '%Y-%m-%d')
                datetime.strptime(end_str, '%Y-%m-%d')
            except Exception:
                return jsonify({'success': False, 'error': '日期格式錯誤，需 YYYY-MM-DD'}), 400

            # 取得資料庫連線（優先使用主專案提供的工廠方法）
            db = None
            if hasattr(DatabaseManager, 'from_request_payload'):
                try:
                    db = DatabaseManager.from_request_payload(payload)
                except Exception:
                    db = None
            if db is None:
                db = DatabaseManager()

            if not db.connect():
                return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

            total_inserted = 0
            available_dates = []
            daily_stats = {}
            skip_existing = bool(payload.get('skip_existing', False))

            try:
                db.create_tables()
                for d in _date_range(start_str, end_str):
                    # 來源：TWSE + TPEX（對齊原專案行為）
                    twse_records = stock_api.fetch_twse_bwibbu_by_date(d)
                    tpex_records = []
                    try:
                        tpex_records = stock_api.fetch_tpex_bwibbu_by_date(d)
                    except Exception:
                        tpex_records = []
                    records = (twse_records or []) + (tpex_records or [])
                    twse_cnt = len(twse_records) if twse_records else 0
                    tpex_cnt = len(tpex_records) if tpex_records else 0
                    rec_len = len(records)
                    logger.info(f"BWIBBU backfill fetch {d}: twse={twse_cnt}, tpex={tpex_cnt}, total={rec_len}")
                    inserted = 0
                    if records:
                        inserted = stock_api.upsert_bwibbu_records(records, db_manager=db)
                    logger.info(f"BWIBBU backfill insert {d}: {inserted} rows")
                    total_inserted += inserted
                    available_dates.append(d.isoformat())
                    daily_stats[d.isoformat()] = {
                        'twse_count': twse_cnt,
                        'tpex_count': tpex_cnt,
                        'total_count': rec_len,
                        'inserted': inserted,
                    }
                    # 禮貌延遲避免被 TWSE 限速
                    time.sleep(0.6)
                write_mode = 'insert_only' if skip_existing else 'upsert'
                return jsonify({
                    'success': True,
                    'total_records': total_inserted,
                    'available_dates': available_dates,
                    'daily_stats': daily_stats,
                    'write_mode': write_mode,
                    'message': f'成功寫入 {total_inserted} 筆記錄'
                })
            finally:
                db.disconnect()
        except Exception as exc:
            return jsonify({'success': False, 'error': str(exc)}), 500

    @bwibbu_bp.route('/query', methods=['GET'])
    def query():
        try:
            db = None
            if hasattr(DatabaseManager, 'from_request_args'):
                try:
                    db = DatabaseManager.from_request_args(request.args)
                except Exception:
                    db = None
            if db is None:
                db = DatabaseManager()
            if not db.connect():
                return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
            try:
                cur = db.connection.cursor()
                start_str = request.args.get('start')
                end_str = request.args.get('end')
                if start_str and end_str:
                    cur.execute(
                        """
                        SELECT DISTINCT date FROM tw_stock_bwibbu
                        WHERE date BETWEEN %s AND %s
                        ORDER BY date DESC
                        """,
                        (start_str, end_str)
                    )
                else:
                    cur.execute("SELECT DISTINCT date FROM tw_stock_bwibbu ORDER BY date DESC")
                dates = [(r['date'] if isinstance(r, dict) else r[0]).isoformat() for r in cur.fetchall()]
                cur.execute("SELECT COUNT(*) FROM tw_stock_bwibbu")
                total_count = (cur.fetchone()[0])
                return jsonify({'success': True, 'dates': dates, 'total_count': total_count})
            finally:
                db.disconnect()
        except Exception as exc:
            return jsonify({'success': False, 'error': str(exc)}), 500

    @bwibbu_bp.route('/ping', methods=['GET'])
    def ping():
        return jsonify({'success': True, 'message': 'bwibbu blueprint ok'})

    @bwibbu_bp.route('/debug_fetch', methods=['GET'])
    def debug_fetch():
        """僅抓取指定日期的 BWIBBU 原始資料（不寫入），用於診斷來源是否為空。
        Query: date=YYYY-MM-DD
        回傳: {success, date, count, sample}
        """
        try:
            date_str = request.args.get('date')
            if not date_str:
                return jsonify({'success': False, 'error': '需要參數 date=YYYY-MM-DD'}), 400
            try:
                d = datetime.strptime(date_str, '%Y-%m-%d').date()
            except Exception:
                return jsonify({'success': False, 'error': '日期格式錯誤，需 YYYY-MM-DD'}), 400
            # 來源：TWSE + TPEX
            twse = stock_api.fetch_twse_bwibbu_by_date(d)
            try:
                tpex = stock_api.fetch_tpex_bwibbu_by_date(d)
            except Exception:
                tpex = []
            records = (twse or []) + (tpex or [])
            twse_cnt = len(twse) if twse else 0
            tpex_cnt = len(tpex) if tpex else 0
            cnt = len(records)
            logger.info(f"BWIBBU debug_fetch {date_str}: twse={twse_cnt}, tpex={tpex_cnt}, total={cnt}")
            sample = records[:5] if records else []
            return jsonify({'success': True, 'date': date_str, 'count': cnt, 'twse_count': twse_cnt, 'tpex_count': tpex_cnt, 'sample': sample})
        except Exception as exc:
            logger.exception("debug_fetch 錯誤")
            return jsonify({'success': False, 'error': str(exc)}), 500

    @bwibbu_bp.route('/by-date', methods=['POST'])
    def upsert_by_date():
        """抓取單一日期並寫入 DB（upsert）。
        JSON: { date: 'YYYY-MM-DD', use_local_db?: bool }
        回傳: { success, date, fetched, inserted }
        """
        try:
            body = request.get_json(silent=True) or {}
            date_str = body.get('date')
            if not date_str:
                return jsonify({'success': False, 'error': '需要 date'}), 400
            try:
                d = datetime.strptime(date_str, '%Y-%m-%d').date()
            except Exception:
                return jsonify({'success': False, 'error': '日期格式錯誤，需 YYYY-MM-DD'}), 400

            db = None
            if hasattr(DatabaseManager, 'from_request_payload'):
                try:
                    db = DatabaseManager.from_request_payload(body)
                except Exception:
                    db = None
            if db is None:
                db = DatabaseManager()
            if not db.connect():
                return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
            try:
                db.create_tables()
                twse = stock_api.fetch_twse_bwibbu_by_date(d)
                try:
                    tpex = stock_api.fetch_tpex_bwibbu_by_date(d)
                except Exception:
                    tpex = []
                recs = (twse or []) + (tpex or [])
                fetched = len(recs) if recs else 0
                inserted = 0
                if recs:
                    inserted = stock_api.upsert_bwibbu_records(recs, db_manager=db)
                logger.info(f"BWIBBU by-date {date_str}: twse={len(twse) if twse else 0}, tpex={len(tpex) if tpex else 0}, total={fetched}, inserted={inserted}")
                return jsonify({'success': True, 'date': date_str, 'fetched': fetched, 'inserted': inserted, 'twse_count': len(twse) if twse else 0, 'tpex_count': len(tpex) if tpex else 0})
            finally:
                db.disconnect()
        except Exception as exc:
            logger.exception("by-date 錯誤")
            return jsonify({'success': False, 'error': str(exc)}), 500

    # 方便對齊舊習慣：提供 /refresh_range 作為 /backfill 的別名
    @bwibbu_bp.route('/refresh_range', methods=['POST'])
    def refresh_range_alias():
        return backfill()

    @bwibbu_bp.route('/backfill_stream', methods=['GET'])
    def backfill_stream():
        try:
            start_str = request.args.get('start')
            end_str = request.args.get('end')
            if not start_str or not end_str:
                return Response("data: {\"event\":\"error\",\"error\":\"缺少 start 或 end\"}\n\n", mimetype='text/event-stream')
            try:
                datetime.strptime(start_str, '%Y-%m-%d')
                datetime.strptime(end_str, '%Y-%m-%d')
            except Exception:
                return Response("data: {\"event\":\"error\",\"error\":\"日期格式錯誤\"}\n\n", mimetype='text/event-stream')

            use_local = (request.args.get('use_local_db', 'false').lower() == 'true')
            skip_existing = (request.args.get('skip_existing', 'false').lower() == 'true')

            def sse(obj):
                import json
                return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

            def generate():
                db = DatabaseManager(use_local) if hasattr(DatabaseManager, '__call__') else DatabaseManager
                try:
                    if hasattr(db, 'connect') and not db.connect():
                        yield sse({ 'event': 'error', 'error': '資料庫連線失敗' })
                        return
                    if hasattr(db, 'create_tables'):
                        try:
                            db.create_tables()
                        except Exception:
                            pass

                    dates = []
                    try:
                        s = datetime.strptime(start_str, '%Y-%m-%d').date()
                        e = datetime.strptime(end_str, '%Y-%m-%d').date()
                        if s > e:
                            s, e = e, s
                        cur = s
                        while cur <= e:
                            dates.append(cur)
                            cur = cur + timedelta(days=1)
                    except Exception:
                        dates = []

                    yield sse({ 'event': 'start', 'start': start_str, 'end': end_str, 'totalDays': len(dates) })

                    processed = 0
                    total_inserted = 0
                    for d in dates:
                        twse = stock_api.fetch_twse_bwibbu_by_date(d)
                        try:
                            tpex = stock_api.fetch_tpex_bwibbu_by_date(d)
                        except Exception:
                            tpex = []
                        recs = (twse or []) + (tpex or [])
                        fetched = len(recs)
                        inserted = 0
                        if recs:
                            try:
                                inserted = stock_api.upsert_bwibbu_records(recs, db_manager=db)
                            except Exception as w:
                                yield sse({ 'event': 'warn', 'date': d.isoformat(), 'fetched': fetched, 'inserted': 0, 'error': str(w) })
                                inserted = 0
                        total_inserted += inserted
                        processed += 1
                        yield sse({ 'event': 'day', 'date': d.isoformat(), 'twse_count': len(twse) if twse else 0, 'tpex_count': len(tpex) if tpex else 0, 'fetched': fetched, 'inserted': inserted, 'progress': { 'processed': processed, 'total': len(dates) } })
                        time.sleep(0.3)

                    yield sse({ 'event': 'done', 'success': True, 'totalDays': len(dates), 'totalInserted': total_inserted })
                finally:
                    try:
                        db.disconnect()
                    except Exception:
                        pass

            headers = {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*'
            }
            return Response(generate(), headers=headers)
        except Exception as exc:
            return Response(f"data: {{\"event\":\"error\",\"error\":\"{str(exc)}\"}}\n\n", mimetype='text/event-stream')

    return bwibbu_bp
