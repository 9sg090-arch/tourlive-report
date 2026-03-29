"""
투어라이브 대시보드 — CSV → Supabase 업로드 스크립트
=====================================================
사용법:
  pip install requests

  python tourslive_upload.py \\
    --active-users "Active Users_Insights_....csv" \\
    --events       "events-export-....csv"

실행하면:
  1. 두 CSV를 파싱·분석
  2. 이슈 자동 감지 (로그인 루프, DAU 하락 등)
  3. Supabase에 결과 저장
  4. 대시보드 HTML을 열면 히스토리에 자동 추가됨
"""

import argparse, csv, json, sys, os
from collections import Counter, defaultdict
from datetime import datetime
import requests

# ── Supabase 설정 ────────────────────────────────────────────────────
SUPABASE_URL = "https://udbslvrmlqtcltpnkenw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVkYnNsdnJtbHF0Y2x0cG5rZW53Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ3ODM0MDEsImV4cCI6MjA5MDM1OTQwMX0.ALYRcSaSvi2udbR1J2EwltgDe2Qh0kLqJz4MUkyaRAY"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

def sb_insert(table, rows):
    """Supabase REST API로 데이터 insert"""
    if not rows:
        return []
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        json=rows,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()

# ── CSV 파싱 ─────────────────────────────────────────────────────────
def parse_active_users(path):
    """Mixpanel Active Users Insights CSV → {hour: (today, prev)}"""
    today_col, prev_col = {}, {}
    date_today, date_prev = "", ""
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        # header[2] = prev date label, header[3] = today date label
        if len(header) >= 4:
            date_prev  = header[2].split(",")[0].strip()
            date_today = header[3].split(",")[0].strip()
        for row in reader:
            if len(row) < 4 or not row[0]:
                continue
            hour = int(row[0][11:13])
            try:
                prev_col[hour]  = int(row[2]) if row[2] else 0
                today_col[hour] = int(row[3]) if row[3] else 0
            except ValueError:
                pass
    return today_col, prev_col, date_today, date_prev


def parse_events(path):
    """Mixpanel events export CSV → 집계 결과"""
    event_counts    = Counter()
    tour_views      = Counter()
    tour_names      = {}
    tour_prices     = {}
    platform_counts = Counter()
    country_counts  = Counter()
    country_names   = {}
    login_per_user  = defaultdict(int)
    user_events     = defaultdict(list)
    total = 0

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            event = row.get("Event Name", "").strip()
            did   = row.get("Distinct ID", "")
            event_counts[event] += 1

            lib = row.get("Mixpanel Library", "")
            if lib == "flutter":
                platform_counts[row.get("Operating System", "Unknown")] += 1
            else:
                platform_counts["web"] += 1

            cc = row.get("Country") or row.get("country_name") or ""
            if cc:
                country_counts[cc] += 1
                country_names[cc] = row.get("country_name") or cc

            if event == "PageView_Tour":
                tid   = row.get("tour_id") or row.get("Tour_id") or ""
                tname = row.get("tour_name") or row.get("Tour_name") or ""
                price = row.get("price") or ""
                if tid:
                    tour_views[tid] += 1
                    if tname: tour_names[tid] = tname
                    if price:
                        try: tour_prices[tid] = int(float(price))
                        except: pass

            if event == "PageView_Login":
                login_per_user[did] += 1

            user_events[did].append({
                "event": event,
                "time":  float(row.get("Time") or 0),
            })

    return {
        "total": total,
        "event_counts":    event_counts,
        "tour_views":      tour_views,
        "tour_names":      tour_names,
        "tour_prices":     tour_prices,
        "platform_counts": platform_counts,
        "country_counts":  country_counts,
        "country_names":   country_names,
        "login_per_user":  login_per_user,
        "user_events":     user_events,
        "unique_users":    len(user_events),
    }


# ── 이슈 자동 감지 ───────────────────────────────────────────────────
def detect_issues(ev, dau_today, dau_prev):
    issues = []

    # 1. 로그인 루프
    for uid, cnt in ev["login_per_user"].items():
        if cnt >= 10:
            u_evts = sorted(ev["user_events"][uid], key=lambda x: x["time"])
            times = [e["time"] for e in u_evts]
            span = times[-1] - times[0] if len(times) > 1 else 0
            # 직전 이벤트 시퀀스
            pre = [e["event"] for e in u_evts if e["event"] != "PageView_Login"][:3]
            issues.append({
                "severity": "critical",
                "issue_type": "login_loop",
                "title": f"PageView_Login 루프 버그 — 유저 #{uid}",
                "description": (
                    f"유저 #{uid}가 {span:.0f}초 내 PageView_Login을 {cnt}회 발화. "
                    f"직전 이벤트: {' → '.join(pre) if pre else '없음'}. "
                    "세션 만료 시 Flutter 위젯 rebuild 루프 추정."
                ),
                "affected_user_id": str(uid),
                "metadata": json.dumps({
                    "event_count": cnt,
                    "time_span_sec": round(span, 1),
                    "preceding_events": pre,
                }),
            })

    # 2. DAU 하락
    today_sum = sum(dau_today.values())
    prev_sum  = sum(dau_prev.values())
    if prev_sum > 0:
        drop_pct = (today_sum - prev_sum) / prev_sum * 100
        if drop_pct < -10:
            issues.append({
                "severity": "critical",
                "issue_type": "dau_drop",
                "title": f"DAU 전일 대비 {abs(drop_pct):.1f}% 급락",
                "description": f"오늘 {today_sum:,}명 vs 전일 {prev_sum:,}명. 즉시 원인 파악 필요.",
                "affected_user_id": None,
                "metadata": json.dumps({"today": today_sum, "yesterday": prev_sum, "drop_pct": round(drop_pct, 1)}),
            })
        elif drop_pct < 0:
            issues.append({
                "severity": "warning",
                "issue_type": "dau_drop",
                "title": f"DAU 전일 대비 {abs(drop_pct):.1f}% 하락",
                "description": f"오늘 {today_sum:,}명 vs 전일 {prev_sum:,}명. 지속 모니터링 필요.",
                "affected_user_id": None,
                "metadata": json.dumps({"today": today_sum, "yesterday": prev_sum, "drop_pct": round(drop_pct, 1)}),
            })

    # 3. UTM 미태깅
    total = ev["total"]
    login_cnt = ev["event_counts"].get("PageView_Login", 0)
    non_login = total - login_cnt
    # (UTM 집계는 별도로 할 수 있으나 여기서는 비율만)
    if total > 0 and login_cnt / total > 0.5:
        issues.append({
            "severity": "warning",
            "issue_type": "event_noise",
            "title": f"이벤트 노이즈 비율 {login_cnt/total*100:.0f}% (PageView_Login 과다)",
            "description": f"전체 {total}건 중 PageView_Login {login_cnt}건. 루프 버그에 의한 노이즈로 다른 이벤트 분석 신뢰도 저하.",
            "affected_user_id": None,
            "metadata": json.dumps({"total": total, "login_count": login_cnt}),
        })

    return issues


# ── 메인 ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="투어라이브 CSV → Supabase 업로드")
    parser.add_argument("--active-users", required=True, help="Active Users Insights CSV 경로")
    parser.add_argument("--events",       required=True, help="Events Export CSV 경로")
    parser.add_argument("--label",        default="",    help="이번 업로드 메모 (선택)")
    args = parser.parse_args()

    print("📂 CSV 파싱 중…")
    dau_today, dau_prev, date_today, date_prev = parse_active_users(args.active_users)
    ev = parse_events(args.events)

    print(f"  Active Users: {date_prev} → {date_today}")
    print(f"  Events: 총 {ev['total']}건, 유니크 유저 {ev['unique_users']}명")

    print("🔍 이슈 감지 중…")
    issues = detect_issues(ev, dau_today, dau_prev)
    print(f"  감지된 이슈: {len(issues)}건")

    # 날짜 파싱
    try:
        date_from = datetime.strptime(date_today, "%b %d %Y").strftime("%Y-%m-%d")
    except:
        date_from = datetime.now().strftime("%Y-%m-%d")

    label = args.label or f"{date_from} Mixpanel CSV 업로드"

    print("☁️  Supabase에 저장 중…")

    # 1. analysis_run 생성
    run_res = sb_insert("analysis_runs", [{
        "date_from":    date_from,
        "date_to":      date_from,
        "label":        label,
        "total_events": ev["total"],
        "unique_users": ev["unique_users"],
        "notes":        f"이슈 {len(issues)}건 감지",
    }])
    run_id = run_res[0]["id"]
    print(f"  ✅ analysis_run 생성: {run_id}")

    # 2. DAU hourly
    dau_rows = [
        {"run_id": run_id, "hour": h, "today_count": dau_today.get(h, 0), "prev_count": dau_prev.get(h, 0)}
        for h in range(24)
    ]
    sb_insert("dau_hourly", dau_rows)
    print(f"  ✅ DAU hourly {len(dau_rows)}행 저장")

    # 3. event stats
    evt_rows = [{"run_id": run_id, "event_name": k, "count": v} for k, v in ev["event_counts"].most_common()]
    sb_insert("event_stats", evt_rows)
    print(f"  ✅ event_stats {len(evt_rows)}행 저장")

    # 4. platform
    plat_rows = [{"run_id": run_id, "platform": k, "count": v} for k, v in ev["platform_counts"].most_common()]
    sb_insert("platform_stats", plat_rows)

    # 5. country
    ctry_rows = [
        {"run_id": run_id, "country_code": k, "country_name": ev["country_names"].get(k, k), "count": v}
        for k, v in ev["country_counts"].most_common()
    ]
    sb_insert("country_stats", ctry_rows)

    # 6. tour views
    tour_rows = [
        {"run_id": run_id, "tour_id": tid, "tour_name": ev["tour_names"].get(tid), "view_count": cnt, "price": ev["tour_prices"].get(tid)}
        for tid, cnt in ev["tour_views"].most_common()
    ]
    if tour_rows:
        sb_insert("tour_view_stats", tour_rows)

    # 7. issues
    if issues:
        issue_rows = [{**i, "run_id": run_id} for i in issues]
        sb_insert("detected_issues", issue_rows)
        for iss in issues:
            badge = "🚨" if iss["severity"] == "critical" else "⚠️"
            print(f"  {badge} {iss['title']}")

    print(f"\n🎉 완료! 대시보드를 새로고침하면 '{label}' 항목이 추가됩니다.")
    print(f"   대시보드 파일: tourslive_dashboard.html")


if __name__ == "__main__":
    main()
