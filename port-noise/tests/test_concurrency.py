"""并发核定测试：签发/更正按入库先后核定，交错时各自看到正确的钟差。

覆盖：
1. 签发核定与落库同一事务——更正先入库则签发按新钟差拒签；
2. 更正复核期间新签的单，在更正写事务里按新钟差补核，失归属随更正撤销；
3. 复核期间只有新单入库时更正照常完成，已复核旧单不重算；
4. 两条更正交错时，后入库者按入库前最新钟差重新试解；
5. 同峰并发签发只有一单成功。
"""
import sqlite3
import threading

from app import solver
from tests.conftest import upload


def _issue(client, peak_id):
    return client.post("/api/forms", json={"peak_id": peak_id})


def _correct(client, rec_id, low, high):
    return client.post(f"/api/clock-records/{rec_id}/correct",
                       json={"low_sec": low, "high_sec": high})


def test_issue_validated_against_clock_at_ingestion(client):
    """任务场景：C1 作业区间 [100,110)，峰时刻 100，钟差 0 可签；
    钟差先改成 20 后峰已待核，签发必须按入库时的最新钟差拒签。"""
    resp = upload(client, [("P1", 100)], [("C1", 100, 110)],
                  [(1, "S", "C1", 0, 0)])
    assert resp.status_code == 200, resp.data.decode()

    # 钟差先入库（0 -> 20）：峰窗口 [120,120] 无人作业，峰变待核
    assert _correct(client, 1, 20, 20).status_code == 200
    peak = client.get("/api/peaks/P1").get_json()
    assert peak["determination"]["result"] == "pending"
    # 签发按入库先后核定：此时只能看到新钟差，必须拒签
    resp = _issue(client, "P1")
    assert resp.status_code == 409
    assert client.get("/api/peaks/P1").get_json()["active_form"] is None

    # 改回 0 后可签；再更正则已签出的单按新钟差撤销并列出单号
    assert _correct(client, 1, 0, 0).status_code == 200
    form_no = _issue(client, "P1").get_json()["form_no"]
    resp = _correct(client, 1, 20, 20)
    assert resp.status_code == 200
    assert resp.get_json()["revoked_form_nos"] == [form_no]
    status = client.get(f"/api/forms/{form_no}").get_json()
    assert status["status"] == "revoked"
    assert "钟差更正" in status["revoke_reason"]


def test_correction_revokes_form_issued_during_review(
        app, loaded_client, monkeypatch):
    """更正复核旧单期间仍可签发；复核期间签出的单在更正写事务里
    按新钟差补核，失去原归属的随更正撤销并列出单号。"""
    client = loaded_client
    old_form = _issue(client, "P1").get_json()["form_no"]

    entered = threading.Event()
    release = threading.Event()
    orig = solver.meter_relative_bounds

    def patched(nodes, edges, cranes):
        # 只在本次更正（r1 -> [100,200]）的试解图上暂停，
        # 放行签发线程对 current_bounds 的调用
        if any(rid == 1 and w == 200 for _u, _v, w, rid in edges):
            entered.set()
            release.wait(timeout=10)
        return orig(nodes, edges, cranes)

    monkeypatch.setattr(solver, "meter_relative_bounds", patched)

    outcome = {}

    def run_correction():
        c = app.test_client()
        outcome["resp"] = _correct(c, 1, 100, 200)

    t = threading.Thread(target=run_correction)
    t.start()
    assert entered.wait(timeout=10), "更正未进入复核阶段"

    # 复核期间签发照常（按入库时的旧钟差核定）
    resp = _issue(client, "P7")
    assert resp.status_code == 201
    new_form = resp.get_json()["form_no"]
    release.set()
    t.join(timeout=10)

    resp = outcome["resp"]
    assert resp.status_code == 200
    body = resp.get_json()
    # 旧单（复核失归属）与复核期间新签的单都按新钟差撤销，
    # 单号按入库先后列出
    assert body["revoked_form_nos"] == [old_form, new_form]
    for fno in (old_form, new_form):
        assert client.get(f"/api/forms/{fno}").get_json()["status"] == "revoked"
    # 更正事件留痕撤销单号
    with sqlite3.connect(app.config["DATABASE"]) as raw:
        detail = raw.execute(
            "SELECT detail FROM events WHERE kind = 'correction' "
            "ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert old_form in detail and new_form in detail


def test_correction_completes_when_only_new_forms_ingested(
        app, loaded_client, monkeypatch):
    """复核期间只有新单入库：更正照常完成；归属未变的单（旧单与新单）都保留。"""
    client = loaded_client
    old_form = _issue(client, "P1").get_json()["form_no"]

    entered = threading.Event()
    release = threading.Event()
    orig = solver.meter_relative_bounds

    def patched(nodes, edges, cranes):
        if any(rid == 1 and w == 8 for _u, _v, w, rid in edges):
            entered.set()
            release.wait(timeout=10)
        return orig(nodes, edges, cranes)

    monkeypatch.setattr(solver, "meter_relative_bounds", patched)

    outcome = {}

    def run_correction():
        c = app.test_client()
        outcome["resp"] = _correct(c, 1, 2, 8)  # 收窄 C1 偏移，归属不变

    t = threading.Thread(target=run_correction)
    t.start()
    assert entered.wait(timeout=10)
    # 复核期间只入库新单：P7 在新钟差 [2,8] 下窗口 [7002,7008] 仍全覆盖
    resp = _issue(client, "P7")
    assert resp.status_code == 201
    new_form = resp.get_json()["form_no"]
    release.set()
    t.join(timeout=10)

    resp = outcome["resp"]
    assert resp.status_code == 200
    assert resp.get_json()["revoked_count"] == 0
    assert client.get(f"/api/forms/{old_form}").get_json()["status"] == "active"
    assert client.get(f"/api/forms/{new_form}").get_json()["status"] == "active"


def test_interleaved_corrections_validated_against_latest_clock(
        app, loaded_client, monkeypatch):
    """两条更正交错：后入库者按入库前的最新钟差重新试解；
    无解则拒绝且钟差与复核单一字不动。"""
    client = loaded_client
    form_no = _issue(client, "P1").get_json()["form_no"]

    entered = threading.Event()
    release = threading.Event()
    orig = solver.meter_relative_bounds

    def patched(nodes, edges, cranes):
        # 只暂停更正 A（r2 -> [20,30]，新上界边权 30）的试解；
        # 重试时 release 已置位直接放行
        if any(rid == 2 and w == 30 for _u, _v, w, rid in edges):
            entered.set()
            release.wait(timeout=10)
        return orig(nodes, edges, cranes)

    monkeypatch.setattr(solver, "meter_relative_bounds", patched)

    outcome = {}

    def run_correction_a():
        c = app.test_client()
        outcome["resp"] = _correct(c, 2, 20, 30)

    t = threading.Thread(target=run_correction_a)
    t.start()
    assert entered.wait(timeout=10), "更正 A 未进入试解阶段"

    # 更正 B 先入库：r4 S->R 收窄为 [3,4]（单独可行）
    resp_b = _correct(client, 4, 3, 4)
    assert resp_b.status_code == 200
    release.set()
    t.join(timeout=10)

    # A 按入库前最新钟差（含 B）重新试解：
    # r2 要求 o(C2) ∈ [20,30]，而 r4+r5 链只给 [-7,14] => 负环，必须拒绝
    resp_a = outcome["resp"]
    assert resp_a.status_code == 409
    body = resp_a.get_json()
    assert set(body["conflict_records"]) <= {2, 4, 5}

    # A 未生效：r2 仍是 [-5,5]，r4 是 B 的 [3,4]，复核单原样有效
    records = {r["rec_id"]: r for r in client.get("/api/clock-records").get_json()
               if r["active"] == 1}
    assert (records[2]["low_sec"], records[2]["high_sec"]) == (-5, 5)
    assert (records[4]["low_sec"], records[4]["high_sec"]) == (3, 4)
    assert client.get(f"/api/forms/{form_no}").get_json()["status"] == "active"


def test_concurrent_issues_same_peak_exactly_one_wins(app, loaded_client):
    """同峰并发签发：按入库先后只有一单成功，另一单 409。"""
    start = threading.Barrier(3)
    results = []

    def run_issue():
        c = app.test_client()
        start.wait(timeout=10)
        results.append(c.post("/api/forms", json={"peak_id": "P1"}))

    threads = [threading.Thread(target=run_issue) for _ in range(2)]
    for t in threads:
        t.start()
    start.wait(timeout=10)
    for t in threads:
        t.join(timeout=10)

    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409]
    peak = loaded_client.get("/api/peaks/P1").get_json()
    assert peak["active_form"] is not None
    # 只有一张有效单
    with sqlite3.connect(app.config["DATABASE"]) as raw:
        n = raw.execute(
            "SELECT COUNT(*) FROM forms WHERE peak_id = 'P1' "
            "AND status = 'active'").fetchone()[0]
    assert n == 1


def test_peak_query_consistent_under_correction(app, loaded_client):
    """查峰时归属与单状态取自同一次提交：更正提交前后查询结果各自一致。"""
    client = loaded_client
    form_no = _issue(client, "P1").get_json()["form_no"]

    before = client.get("/api/peaks/P1").get_json()
    assert before["determination"]["result"] == "issue"
    assert before["active_form"] == form_no

    assert _correct(client, 1, 100, 200).status_code == 200

    after = client.get("/api/peaks/P1").get_json()
    assert after["determination"]["result"] == "pending"
    assert after["active_form"] is None
    assert client.get(f"/api/forms/{form_no}").get_json()["status"] == "revoked"
