"""HTTP 路由：页面 + JSON API。

并发口径（详见 README「复核单生命周期与事件顺序」）：
* 写操作（导入/签发/更正）一律走 BEGIN IMMEDIATE 写事务，按入库先后核定；
* 更正分两阶段：旧单复核在写事务外的一致性读快照里做（复核期间新单照常签发），
  写事务里只补核复核期间新签的单；
* 查峰/验单的多次读固定在同一个 WAL 快照（同一次提交）上。
"""
import json
import sqlite3
from contextlib import contextmanager

from flask import (
    Blueprint,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from . import ingest, solver
from .db import get_db, is_seed_loaded

bp = Blueprint("main", __name__)


@contextmanager
def _snapshot(conn):
    """WAL 读快照：把多次读固定在同一次提交上（归属与单状态一致）。"""
    conn.execute("BEGIN")
    try:
        yield
    finally:
        conn.rollback()


def _revoke_form(conn, form, correction_event, reason):
    """在更正写事务里撤销一张复核单，返回撤销记录。

    单状态若在复核期间被意外改动则抛错，由调用方整体回滚——
    写入失败时本次更正不得改变钟差和复核单。
    """
    rev = conn.execute(
        "INSERT INTO events(kind, detail) VALUES ('revoke', ?)",
        (json.dumps({"form_no": form["form_no"],
                     "peak_id": form["peak_id"],
                     "correction_event": correction_event,
                     "reason": reason}, ensure_ascii=False),))
    revoke_event = rev.lastrowid
    cur = conn.execute(
        "UPDATE forms SET status = 'revoked', revoked_event = ?, "
        "revoke_reason = ? WHERE form_no = ? AND status = 'active'",
        (revoke_event, reason, form["form_no"]))
    if cur.rowcount != 1:
        raise sqlite3.IntegrityError(
            f"复核单 {form['form_no']} 状态在更正期间变化，更正整体回滚")
    return {"form_no": form["form_no"], "peak_id": form["peak_id"],
            "event_id": revoke_event}


@bp.app_errorhandler(413)
def too_large(exc):
    if request.path.startswith("/api/"):
        return jsonify(error="上传文件超过 256MB 限制"), 413
    return render_template("upload.html", loaded=False,
                           error="上传文件超过 256MB 限制", result=None), 413


@bp.app_errorhandler(409)
def conflict(exc):
    if request.path.startswith("/api/"):
        return jsonify(error=exc.description), 409
    return f"<h1>409</h1><p>{exc.description}</p>", 409


# --------------------------------------------------------------------------
# 页面
# --------------------------------------------------------------------------
@bp.route("/")
def index():
    conn = get_db()
    loaded = is_seed_loaded(conn)
    stats = None
    if loaded:
        with _snapshot(conn):
            stats = {
                "peaks": conn.execute("SELECT COUNT(*) FROM peaks").fetchone()[0],
                "intervals": conn.execute(
                    "SELECT COUNT(*) FROM work_intervals").fetchone()[0],
                "cranes": solver.list_cranes(conn),
                "clock_active": conn.execute(
                    "SELECT COUNT(*) FROM clock_records WHERE active = 1").fetchone()[0],
                "forms_active": conn.execute(
                    "SELECT COUNT(*) FROM forms WHERE status = 'active'").fetchone()[0],
            }
    return render_template("index.html", loaded=loaded, stats=stats)


@bp.route("/upload", methods=["GET", "POST"])
def upload_page():
    conn = get_db()
    if request.method == "GET":
        return render_template("upload.html",
                               loaded=is_seed_loaded(conn), error=None, result=None)
    try:
        # 写事务：重复导入校验与入库原子完成，并发导入按入库先后拒绝
        conn.execute("BEGIN IMMEDIATE")
        result = ingest.load_season(
            conn,
            request.files.get("peaks"),
            request.files.get("work"),
            request.files.get("clocks"),
        )
        conn.commit()
    except solver.IngestError as exc:
        conn.rollback()
        return render_template("upload.html", loaded=False, error=str(exc),
                               result=None), 400
    except sqlite3.Error:
        conn.rollback()
        return render_template("upload.html", loaded=False,
                               error="数据库写入失败，本次导入未生效",
                               result=None), 500
    return render_template("upload.html", loaded=True, error=None, result=result)


@bp.route("/peaks")
def peaks_page():
    conn = get_db()
    if not is_seed_loaded(conn):
        return redirect(url_for("main.upload_page"))
    q = request.args.get("q", "").strip()
    peaks = conn.execute(
        "SELECT peak_id, peak_sec FROM peaks ORDER BY peak_sec, peak_id LIMIT 200"
    ).fetchall()
    return render_template("peaks.html", peaks=peaks, q=q)


@bp.route("/peaks/<peak_id>")
def peak_detail_page(peak_id):
    conn = get_db()
    if not is_seed_loaded(conn):
        return redirect(url_for("main.upload_page"))
    with _snapshot(conn):
        peak = conn.execute("SELECT * FROM peaks WHERE peak_id = ?",
                            (peak_id,)).fetchone()
        if peak is None:
            abort(404, description=f"峰号 {peak_id} 不存在")
        determination, conflict = _determinate_or_conflict(conn, peak)
        forms = conn.execute(
            "SELECT * FROM forms WHERE peak_id = ? ORDER BY created_event",
            (peak_id,)).fetchall()
    return render_template("peak_detail.html", peak=peak,
                           determination=determination, conflict=conflict,
                           forms=forms)


@bp.route("/clocks")
def clocks_page():
    conn = get_db()
    if not is_seed_loaded(conn):
        return redirect(url_for("main.upload_page"))
    records = conn.execute(
        "SELECT * FROM clock_records ORDER BY rec_id").fetchall()
    return render_template("clocks.html", records=records)


@bp.route("/forms")
def forms_page():
    conn = get_db()
    if not is_seed_loaded(conn):
        return redirect(url_for("main.upload_page"))
    status = request.args.get("status", "")
    sql = "SELECT * FROM forms"
    params = ()
    if status in ("active", "revoked"):
        sql += " WHERE status = ?"
        params = (status,)
    sql += " ORDER BY created_event"
    forms = conn.execute(sql, params).fetchall()
    return render_template("forms.html", forms=forms, status=status)


@bp.route("/forms/<form_no>")
def form_detail_page(form_no):
    conn = get_db()
    with _snapshot(conn):
        form = conn.execute("SELECT * FROM forms WHERE form_no = ?",
                            (form_no,)).fetchone()
        if form is None:
            abort(404, description=f"复核单 {form_no} 不存在")
        peak = conn.execute("SELECT * FROM peaks WHERE peak_id = ?",
                            (form["peak_id"],)).fetchone()
        recheck = None
        if is_seed_loaded(conn):
            determination, conflict = _determinate_or_conflict(conn, peak)
            recheck = {"determination": determination, "conflict": conflict}
        events = conn.execute(
            "SELECT * FROM events WHERE id IN (?, ?) ORDER BY id",
            (form["created_event"], form["revoked_event"])).fetchall()
    return render_template("form_detail.html", form=form, peak=peak,
                           recheck=recheck, events=events)


@bp.route("/events")
def events_page():
    conn = get_db()
    events = conn.execute(
        "SELECT * FROM events ORDER BY id DESC LIMIT 300").fetchall()
    parsed = [(e["id"], e["kind"], json.loads(e["detail"])) for e in events]
    return render_template("events.html", events=parsed)


# --------------------------------------------------------------------------
# JSON API
# --------------------------------------------------------------------------
@bp.get("/api/health")
def api_health():
    return jsonify(ok=True, loaded=is_seed_loaded(get_db()))


@bp.get("/api/peaks/<peak_id>")
def api_peak(peak_id):
    """按峰号查询当前归属判定。读取量只与该峰/吊机相关，不随全季区间数增长。

    归属判定与有效单状态取自同一个 WAL 快照（同一次提交）。
    """
    conn = get_db()
    _require_loaded(conn)
    with _snapshot(conn):
        peak = conn.execute("SELECT * FROM peaks WHERE peak_id = ?",
                            (peak_id,)).fetchone()
        if peak is None:
            return jsonify(error=f"峰号 {peak_id} 不存在"), 404
        determination, conflict = _determinate_or_conflict(conn, peak)
        active_form = conn.execute(
            "SELECT form_no FROM forms WHERE peak_id = ? AND status = 'active'",
            (peak_id,)).fetchone()
    return jsonify(
        peak_id=peak_id,
        peak_sec=peak["peak_sec"],
        determination=determination,
        conflict_records=conflict,
        active_form=active_form["form_no"] if active_form else None,
    )


@bp.post("/api/forms")
def api_issue_form():
    """对峰号签发复核单；待核峰拒绝签发。

    核定与落库在同一个 BEGIN IMMEDIATE 写事务里完成：签发按入库先后
    （写锁获取顺序 = 事件序号顺序）核定，核定所依据的钟差在写入前
    不会被并发更正替换——不会出现“峰已待核，单却照样签出”。
    """
    data = request.get_json(silent=True) or {}
    peak_id = (data.get("peak_id") or "").strip()
    if not peak_id:
        return jsonify(error="缺少 peak_id"), 400
    conn = get_db()
    _require_loaded(conn)
    try:
        conn.execute("BEGIN IMMEDIATE")
        peak = conn.execute("SELECT * FROM peaks WHERE peak_id = ?",
                            (peak_id,)).fetchone()
        if peak is None:
            conn.rollback()
            return jsonify(error=f"峰号 {peak_id} 不存在"), 404

        bounds, conflict = solver.current_bounds(conn)
        if conflict is not None:
            conn.rollback()
            return jsonify(
                error="当前钟差无解，不能签发复核单",
                conflict_records=conflict,
            ), 409
        determination = solver.determine_peak(
            conn, peak, solver.list_cranes(conn), bounds)
        if determination["result"] != "issue":
            conn.rollback()
            return jsonify(error="该峰为待核峰，不能签发",
                           reason=determination["reason"],
                           windows=determination["windows"]), 409

        existing = conn.execute(
            "SELECT form_no FROM forms WHERE peak_id = ? AND status = 'active'",
            (peak_id,)).fetchone()
        if existing:
            conn.rollback()
            return jsonify(error="该峰已有有效复核单",
                           form_no=existing["form_no"]), 409

        cur = conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('issue', ?)",
            (json.dumps({"peak_id": peak_id, "crane_id": determination["crane"]},
                        ensure_ascii=False),))
        event_id = cur.lastrowid
        form_no = f"RC-{event_id:06d}"
        conn.execute(
            "INSERT INTO forms(form_no, peak_id, crane_id, status, created_event) "
            "VALUES (?, ?, ?, 'active', ?)",
            (form_no, peak_id, determination["crane"], event_id))
        conn.commit()
    except sqlite3.IntegrityError:
        # 并发签发同一峰：部分唯一索引兜底，后到者按重复签发处理
        conn.rollback()
        return jsonify(error="该峰已有有效复核单"), 409
    except sqlite3.Error:
        conn.rollback()
        return jsonify(error="签发写入失败，请重试"), 503
    return jsonify(form_no=form_no, peak_id=peak_id,
                   crane_id=determination["crane"], event_id=event_id), 201


@bp.get("/api/forms/<form_no>")
def api_form_status(form_no):
    """按单号查状态（主键查询，O(1) 级）。"""
    conn = get_db()
    _require_loaded(conn)
    form = conn.execute("SELECT * FROM forms WHERE form_no = ?",
                        (form_no,)).fetchone()
    if form is None:
        return jsonify(error=f"复核单 {form_no} 不存在"), 404
    return jsonify(dict(form))


@bp.get("/api/clock-records")
def api_clock_records():
    conn = get_db()
    _require_loaded(conn)
    rows = conn.execute(
        "SELECT * FROM clock_records ORDER BY rec_id").fetchall()
    return jsonify([dict(r) for r in rows])


@bp.post("/api/clock-records/<int:rec_id>/correct")
def api_correct_clock(rec_id):
    """逐条更正钟差。无解则返回一组冲突记录，数据与复核单原样不动。

    事务边界：
    * 阶段一（读快照，不占写锁）：试解新钟差、按新钟差复核全部现存有效单。
      复核期间新单照常签发（WAL 读写不互斥）。
    * 阶段二（BEGIN IMMEDIATE 写事务）：落库更正、撤销失归属旧单，
      并只补核复核期间新签的单（写事务里只为它们读取作业区间）。
    * 两条更正交错时，按入库前的最新钟差重新试解（重试阶段一）；
      复核期间只有新单入库则照常完成，已复核的旧单不再重算。
    * 无解或写入失败：整体回滚，本次更正不得改变钟差和复核单。
    """
    data = request.get_json(silent=True) or {}
    try:
        low = int(data["low_sec"])
        high = int(data["high_sec"])
    except (KeyError, TypeError, ValueError):
        return jsonify(error="需要整数参数 low_sec, high_sec"), 400
    if high < low:
        return jsonify(error=f"上界 {high} 小于下界 {low}，无解"), 400

    conn = get_db()
    _require_loaded(conn)

    for _attempt in range(3):
        # ---- 阶段一：一致性读快照（不写库，签发不阻塞）----
        phase = None
        phase_error = None
        conn.execute("BEGIN")
        try:
            record = conn.execute(
                "SELECT * FROM clock_records WHERE rec_id = ? AND active = 1",
                (rec_id,)).fetchone()
            if record is None:
                phase_error = (jsonify(
                    error=f"有效钟差记录 {rec_id} 不存在（可能已被更正）"), 404)
            else:
                # 先在临时改写后的图上试解；发现负环直接拒绝，库内数据与复核单保持原样
                trial = [dict(r) for r in solver.active_clock_records(conn)
                         if r["rec_id"] != rec_id]
                trial.append({"rec_id": rec_id, "device_a": record["device_a"],
                              "device_b": record["device_b"],
                              "low_sec": low, "high_sec": high})
                nodes, edges = solver.build_graph(trial)
                conflict = solver.find_negative_cycle(edges, nodes)
                if conflict is not None:
                    phase_error = (jsonify(
                        error="更正后钟差无解（存在负环），更正未生效，数据与复核单保持原样",
                        conflict_records=conflict), 409)
                else:
                    cranes = solver.list_cranes(conn)
                    bounds = solver.meter_relative_bounds(nodes, edges, cranes)
                    # 按入库先后复核所有现存有效单（读作业区间在写事务外做）
                    old_revokes = []
                    old_forms = conn.execute(
                        "SELECT * FROM forms WHERE status = 'active' "
                        "ORDER BY created_event").fetchall()
                    for form in old_forms:
                        peak = conn.execute(
                            "SELECT * FROM peaks WHERE peak_id = ?",
                            (form["peak_id"],)).fetchone()
                        det = solver.determine_peak(conn, peak, cranes, bounds)
                        if not (det["result"] == "issue"
                                and det["crane"] == form["crane_id"]):
                            old_revokes.append(form)
                    # 同一快照上的核定水位：之后入库的单在阶段二补核
                    snapshot_event = conn.execute(
                        "SELECT COALESCE(MAX(id), 0) FROM events").fetchone()[0]
                    corrections = conn.execute(
                        "SELECT COUNT(*) FROM events WHERE kind = 'correction'"
                    ).fetchone()[0]
                    phase = (record, bounds, cranes, old_revokes,
                             snapshot_event, corrections)
        finally:
            conn.rollback()
        if phase_error is not None:
            return phase_error
        record, bounds, cranes, old_revokes, snapshot_event, corrections = phase

        # ---- 阶段二：写事务（入库即核定）----
        try:
            conn.execute("BEGIN IMMEDIATE")
            now_corrections = conn.execute(
                "SELECT COUNT(*) FROM events WHERE kind = 'correction'"
            ).fetchone()[0]
            if now_corrections != corrections:
                # 两条更正交错：阶段一结论已过时，按入库前最新钟差重新核定
                conn.rollback()
                continue

            detail = {
                "rec_id": rec_id,
                "device_a": record["device_a"],
                "device_b": record["device_b"],
                "old_low": record["low_sec"], "old_high": record["high_sec"],
                "new_low": low, "new_high": high,
            }
            cur = conn.execute(
                "INSERT INTO events(kind, detail) VALUES ('correction', ?)",
                (json.dumps(detail, ensure_ascii=False),))
            correction_event = cur.lastrowid

            conn.execute(
                "UPDATE clock_records SET active = 0, superseded_by = ? "
                "WHERE rec_id = ?", (correction_event, rec_id))
            conn.execute(
                "INSERT INTO clock_records(rec_id, device_a, device_b, low_sec, "
                "high_sec, active, created_event) VALUES (?, ?, ?, ?, ?, 1, ?)",
                (rec_id, record["device_a"], record["device_b"], low, high,
                 correction_event))

            reason = "钟差更正后该峰不再唯一归属原班组"
            revoked = []
            # 旧单：阶段一已复核，按结论撤销，不再重算
            for form in old_revokes:
                revoked.append(_revoke_form(conn, form, correction_event, reason))
            # 复核期间新签的单：更正入库前签出，也按新钟差核定；
            # 写事务里只为这些单读取作业区间
            new_forms = conn.execute(
                "SELECT * FROM forms WHERE status = 'active' "
                "AND created_event > ? ORDER BY created_event",
                (snapshot_event,)).fetchall()
            for form in new_forms:
                peak = conn.execute("SELECT * FROM peaks WHERE peak_id = ?",
                                    (form["peak_id"],)).fetchone()
                det = solver.determine_peak(conn, peak, cranes, bounds)
                if not (det["result"] == "issue"
                        and det["crane"] == form["crane_id"]):
                    revoked.append(_revoke_form(conn, form, correction_event, reason))

            detail["revoked_form_nos"] = [r["form_no"] for r in revoked]
            conn.execute("UPDATE events SET detail = ? WHERE id = ?",
                         (json.dumps(detail, ensure_ascii=False), correction_event))
            conn.commit()
        except sqlite3.Error:
            # 写入失败：整体回滚，钟差与复核单一字不动
            conn.rollback()
            return jsonify(
                error="更正写入失败，本次更正未生效，钟差与复核单保持原样"), 500

        return jsonify(
            ok=True,
            correction_event=correction_event,
            rec_id=rec_id,
            old={"low_sec": record["low_sec"], "high_sec": record["high_sec"]},
            new={"low_sec": low, "high_sec": high},
            revoked=revoked,
            revoked_count=len(revoked),
            revoked_form_nos=[r["form_no"] for r in revoked],
            note="撤销的旧单永久保留；即使之后改回原归属也不恢复旧单，需重新签发新单",
        )
    return jsonify(error="更正与其他更正交错，请稍后重试"), 409


# --------------------------------------------------------------------------
# 辅助
# --------------------------------------------------------------------------
def _require_loaded(conn):
    if not is_seed_loaded(conn):
        abort(409, description="尚未导入本季 CSV 数据")


def _determinate_or_conflict(conn, peak):
    """返回 (determination_or_None, conflict_rec_ids_or_None)。"""
    bounds, conflict = solver.current_bounds(conn)
    if conflict is not None:
        return None, conflict
    return solver.determine_peak(conn, peak, solver.list_cranes(conn), bounds), None
