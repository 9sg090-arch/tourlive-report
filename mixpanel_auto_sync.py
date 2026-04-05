"""
투어라이브 Mixpanel 자동 동기화 스크립트
=========================================
GitHub Actions에서 매일 자동 실행됩니다.

필요 환경변수 (GitHub Secrets에 설정):
  MIXPANEL_PROJECT_ID           - Mixpanel 프로젝트 ID
  MIXPANEL_PROJECT_SECRET       - Mixpanel 프로젝트 시크릿
                                  (Settings > Project Settings > Project Secret)
  SUPABASE_URL                  - Supabase 프로젝트 URL (선택, 기본값 내장)
  SUPABASE_KEY                  - Supabase anon key   (선택, 기본값 내장)
  LOOKBACK_DAYS                 - 조회 기간 일수 (선택, 기본 1 = 어제 하루)

로컬 실행:
  export MIXPANEL_PROJECT_ID="..."
  export MIXPANEL_PROJECT_SECRET="..."
  pip install requests
  python mixpanel_auto_sync.py
"""

import os, sys, json, base64, time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter, defaultdict

# ──────────────────────────────────────────────
# 0. 설정
# ──────────────────────────────────────────────
TZ = ZoneInfo("Asia/Seoul")
NOW = datetime.now(tz=TZ)

MP_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "")
MP_SECRET     = os.environ.get("MIXPANEL_PROJECT_SECRET", "")

# Mixpanel Export API는 보통 24~48시간 딜레이 있음 → 2일 전 ~ 2일 전이 안전
LOOKBACK      = int(os.environ.get("LOOKBACK_DAYS", "3"))
DATE_TO       = (NOW - timedelta(days=2)).strftime("%Y-%m-%d")   # 2일 전 (딜레이 감안)
DATE_PREV     = (NOW - timedelta(days=3)).strftime("%Y-%m-%d")   # 전일 비교용 (3일 전)
DATE_FROM     = (NOW - timedelta(days=LOOKBACK + 1)).strftime("%Y-%m-%d")

# Supabase — 환경변수 우선, 없으면 기존 하드코딩 값 폴백
SB_URL = os.environ.get("SUPABASE_URL", "https://udbslvrmlqtcltpnkenw.supabase.co")
SB_KEY = os.environ.get("SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVkYnNsdnJtbHF0Y2x0cG5rZW53Iiwicm9sZSI6"
    "ImFub24iLCJpYXQiOjE3NzQ3ODM0MDEsImV4cCI6MjA5MDM1OTQwMX0."
    "ALYRcSaSvi2udbR1J2EwltgDe2Qh0kLqJz4MUkyaRAY"
)

if not MP_PROJECT_ID or not MP_SECRET:
    print("❌ MIXPANEL_PROJECT_ID / MIXPANEL_PROJECT_SECRET 환경변수가 필요합니다.")
    sys.exit(1)

# Mixpanel 인증
_mp_auth    = base64.b64encode(f"{MP_SECRET}:".encode()).decode()
MP_HEADERS  = {"Authorization": f"Basic {_mp_auth}", "Accept": "text/plain"}
SB_HEADERS  = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

print(f"📅 동기화 기간: {DATE_FROM} ~ {DATE_TO}")

# ──────────────────────────────────────────────
# 1. Mixpanel Export API — 원시 이벤트 수집
# ──────────────────────────────────────────────
def fetch_raw_events(date_from: str = DATE_FROM, date_to: str = DATE_TO) -> list[dict]:
    """지정한 기간의 원시 이벤트를 Export API로 가져옵니다."""
    print(f"📡 Mixpanel Export API 호출 중 ({date_from} ~ {date_to})…")
    resp = requests.get(
        "https://data-eu.mixpanel.com/api/2.0/export",
        headers=MP_HEADERS,
        params={"from_date": date_from, "to_date": date_to},
        stream=True,
        timeout=180,
    )
    if resp.status_code == 400:
        print(f"  ⚠️  Export API 오류: {resp.text[:200]}")
        return []
    resp.raise_for_status()

    events = []
    for line in resp.iter_lines():
        if line:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    print(f"  ✅ {len(events):,}건 수집")
    return events


# ──────────────────────────────────────────────
# 2. 원시 이벤트에서 시간별 DAU 계산 (Segmentation API 불필요)
# ──────────────────────────────────────────────
def calc_hourly_dau(raw: list[dict]) -> tuple[dict, dict]:
    """Export API 원시 이벤트에서 날짜별·시간별 유니크 유저 수를 계산합니다.
    오늘(DATE_TO) 데이터를 today, 하루 전(DATE_PREV) 데이터를 prev로 반환합니다."""
    prev_date = DATE_PREV
    # {date: {hour: set(distinct_id)}}
    buckets: dict[str, dict[int, set]] = {}

    for row in raw:
        props = row.get("properties", {})
        did   = str(props.get("distinct_id", ""))
        ts    = props.get("time", 0)
        if not ts:
            continue
        try:
            dt = datetime.fromtimestamp(float(ts), tz=TZ)
            date_str = dt.strftime("%Y-%m-%d")
            hour     = dt.hour
            buckets.setdefault(date_str, {}).setdefault(hour, set()).add(did)
        except (ValueError, OSError):
            pass

    def _to_count(date_str: str) -> dict:
        day_data = buckets.get(date_str, {})
        return {h: len(users) for h, users in day_data.items()}

    today_dau = _to_count(DATE_TO)
    prev_dau  = _to_count(prev_date)
    print(f"  ✅ DAU — {DATE_TO}: {sum(today_dau.values()):,}명, {prev_date}: {sum(prev_dau.values()):,}명")
    return today_dau, prev_dau


# ──────────────────────────────────────────────
# 3. 이벤트 집계
# ──────────────────────────────────────────────
def aggregate_events(raw: list[dict]) -> dict:
    """원시 이벤트 리스트 → 집계 딕셔너리"""
    event_counts    = Counter()
    platform_counts = Counter()
    country_counts  = Counter()
    country_names   = {}
    tour_views      = Counter()
    tour_names      = {}
    tour_prices     = {}
    login_per_user  = defaultdict(int)
    user_events     = defaultdict(list)

    for row in raw:
        props   = row.get("properties", {})
        event   = row.get("event", "").strip()
        did     = str(props.get("distinct_id", ""))
        mp_lib  = props.get("$lib", props.get("mp_lib", ""))

        if not event:
            continue

        event_counts[event] += 1

        # 플랫폼 분류
        if mp_lib in ("flutter", "android", "iphone", "swift"):
            os_name = props.get("$os", mp_lib)
            platform_counts[os_name] += 1
        else:
            platform_counts["web"] += 1

        # 국가
        cc = props.get("$country_code") or props.get("country_code") or ""
        if cc:
            country_counts[cc] += 1
            country_names[cc] = props.get("$region", cc) or cc

        # 투어 조회
        if event == "PageView_Tour":
            tid   = str(props.get("tour_id") or props.get("Tour_id") or "")
            tname = props.get("tour_name") or props.get("Tour_name") or ""
            price = props.get("price") or props.get("Price") or ""
            if tid:
                tour_views[tid] += 1
                if tname:
                    tour_names[tid] = tname
                if price:
                    try:
                        tour_prices[tid] = int(float(price))
                    except (ValueError, TypeError):
                        pass

        # 로그인 루프 감지용
        if event == "PageView_Login":
            login_per_user[did] += 1

        user_events[did].append({
            "event": event,
            "time":  float(props.get("time", 0)),
        })

    return {
        "total":           len(raw),
        "event_counts":    event_counts,
        "platform_counts": platform_counts,
        "country_counts":  country_counts,
        "country_names":   country_names,
        "tour_views":      tour_views,
        "tour_names":      tour_names,
        "tour_prices":     tour_prices,
        "login_per_user":  login_per_user,
        "user_events":     user_events,
        "unique_users":    len(user_events),
    }


# ──────────────────────────────────────────────
# 4. PageView_Tour 속성별 집계 (도시 / 국가 / 투어)
# ──────────────────────────────────────────────
def calc_tour_view_props(raw_today: list[dict], raw_prev: list[dict], top_n: int = 10) -> list[dict]:
    """PageView_Tour 이벤트의 속성별(도시·국가·투어) 상위 N개를 집계합니다."""
    def extract(raw: list[dict]) -> tuple[Counter, Counter, Counter, dict, dict]:
        cities    = Counter()   # city_name → count
        countries = Counter()   # country_name → count
        tours     = Counter()   # tour_id → count
        city_ids  = {}          # city_name → city_id
        tour_names_map = {}     # tour_id → tour_name

        for row in raw:
            if row.get("event", "").strip() != "PageView_Tour":
                continue
            p = row.get("properties", {})

            cname = p.get("cityName") or p.get("City_name") or p.get("city_name") or ""
            cid   = p.get("cityId")   or p.get("City_id")   or p.get("city_id")   or ""
            ctry  = (p.get("countryName") or p.get("Country_name") or
                     p.get("country_name") or p.get("$country_code") or "")
            tid   = str(p.get("tourId") or p.get("Tour_id") or p.get("tour_id") or "")
            tname = p.get("tourName") or p.get("Tour_name") or p.get("tour_name") or ""

            if cname:
                cities[cname] += 1
                if cid:
                    city_ids[cname] = str(cid)
            if ctry:
                countries[ctry] += 1
            if tid:
                tours[tid] += 1
                if tname:
                    tour_names_map[tid] = tname

        return cities, countries, tours, city_ids, tour_names_map

    t_cities, t_countries, t_tours, t_city_ids, t_tour_names = extract(raw_today)
    p_cities, p_countries, p_tours, _,            _           = extract(raw_prev)

    rows = []

    # 도시별
    all_cities = set(list(t_cities.keys())[:top_n]) | set(list(p_cities.keys())[:top_n])
    for name in sorted(all_cities, key=lambda x: -t_cities.get(x, 0))[:top_n]:
        rows.append({
            "prop_type":   "city",
            "prop_value":  name,
            "prop_id":     t_city_ids.get(name, ""),
            "today_count": t_cities.get(name, 0),
            "prev_count":  p_cities.get(name, 0),
        })

    # 국가별
    all_ctry = set(list(t_countries.keys())[:top_n]) | set(list(p_countries.keys())[:top_n])
    for name in sorted(all_ctry, key=lambda x: -t_countries.get(x, 0))[:top_n]:
        rows.append({
            "prop_type":   "country",
            "prop_value":  name,
            "prop_id":     "",
            "today_count": t_countries.get(name, 0),
            "prev_count":  p_countries.get(name, 0),
        })

    # 투어별
    all_tours = set(list(t_tours.keys())[:top_n]) | set(list(p_tours.keys())[:top_n])
    for tid in sorted(all_tours, key=lambda x: -t_tours.get(x, 0))[:top_n]:
        rows.append({
            "prop_type":   "tour",
            "prop_value":  t_tour_names.get(tid, tid),
            "prop_id":     tid,
            "today_count": t_tours.get(tid, 0),
            "prev_count":  p_tours.get(tid, 0),
        })

    print(f"  ✅ tour_view_props 집계 완료 — 도시 {sum(1 for r in rows if r['prop_type']=='city')}개 / "
          f"국가 {sum(1 for r in rows if r['prop_type']=='country')}개 / "
          f"투어 {sum(1 for r in rows if r['prop_type']=='tour')}개")
    return rows


# ──────────────────────────────────────────────
# 5. 퍼널 집계 (투어 상세 → 구매 시작 → 콘텐츠 접근 → 재생)
# ──────────────────────────────────────────────
FUNNEL_STEPS = [
    ("tour_view",         "투어 상세",   "PageView_Tour"),
    ("complete_purchase", "구매 완료",   "EventOn_CompletePurchase"),
    ("purchased_content", "콘텐츠 접근", "PageView_PurchasedContent"),
    ("player",            "재생",        "PageView_Player"),
]

def calc_funnel(raw_today: list[dict], raw_prev: list[dict]) -> list[dict]:
    """오늘/전일 원시 이벤트에서 퍼널 단계별 이벤트 수를 집계합니다."""
    def count_events(raw: list[dict]) -> dict:
        counts: dict[str, int] = {}
        for row in raw:
            e = row.get("event", "").strip()
            if e:
                counts[e] = counts.get(e, 0) + 1
        return counts

    today_counts = count_events(raw_today)
    prev_counts  = count_events(raw_prev)

    rows = []
    for step_key, label, event_name in FUNNEL_STEPS:
        rows.append({
            "step":        step_key,
            "step_label":  label,
            "today_count": today_counts.get(event_name, 0),
            "prev_count":  prev_counts.get(event_name, 0),
        })
    summary = [f"{r['step_label']}({r['today_count']})" for r in rows]
    print(f"  ✅ 퍼널 집계 완료: {summary}")
    return rows


# ──────────────────────────────────────────────
# 5. 이슈 자동 감지
# ──────────────────────────────────────────────
def detect_issues(agg: dict, dau_today: dict, dau_prev: dict) -> list[dict]:
    issues = []

    # 1. 로그인 루프
    for uid, cnt in agg["login_per_user"].items():
        if cnt >= 10:
            u_evts = sorted(agg["user_events"][uid], key=lambda x: x["time"])
            times  = [e["time"] for e in u_evts]
            span   = times[-1] - times[0] if len(times) > 1 else 0
            pre    = [e["event"] for e in u_evts if e["event"] != "PageView_Login"][:3]
            issues.append({
                "severity":         "critical",
                "issue_type":       "login_loop",
                "title":            f"PageView_Login 루프 — 유저 #{uid}",
                "description":      (
                    f"유저 #{uid}가 {span:.0f}초 내 PageView_Login {cnt}회 발화. "
                    f"직전 이벤트: {' → '.join(pre) if pre else '없음'}. "
                    "세션 만료 시 Flutter 위젯 rebuild 루프 추정."
                ),
                "affected_user_id": str(uid),
                "metadata":         json.dumps({
                    "event_count":       cnt,
                    "time_span_sec":     round(span, 1),
                    "preceding_events":  pre,
                }),
            })

    # 2. DAU 급락
    today_sum = sum(dau_today.values())
    prev_sum  = sum(dau_prev.values())
    if prev_sum > 0:
        drop_pct = (today_sum - prev_sum) / prev_sum * 100
        if drop_pct < -10:
            issues.append({
                "severity":         "critical",
                "issue_type":       "dau_drop",
                "title":            f"DAU 전일 대비 {abs(drop_pct):.1f}% 급락",
                "description":      f"오늘 {today_sum:,}명 vs 전일 {prev_sum:,}명. 즉시 원인 파악 필요.",
                "affected_user_id": None,
                "metadata":         json.dumps({"today": today_sum, "yesterday": prev_sum, "drop_pct": round(drop_pct, 1)}),
            })
        elif drop_pct < 0:
            issues.append({
                "severity":         "warning",
                "issue_type":       "dau_drop",
                "title":            f"DAU 전일 대비 {abs(drop_pct):.1f}% 하락",
                "description":      f"오늘 {today_sum:,}명 vs 전일 {prev_sum:,}명. 지속 모니터링 권장.",
                "affected_user_id": None,
                "metadata":         json.dumps({"today": today_sum, "yesterday": prev_sum, "drop_pct": round(drop_pct, 1)}),
            })

    # 3. 이벤트 노이즈 (PageView_Login 과다)
    total     = agg["total"]
    login_cnt = agg["event_counts"].get("PageView_Login", 0)
    if total > 0 and login_cnt / total > 0.5:
        issues.append({
            "severity":         "warning",
            "issue_type":       "event_noise",
            "title":            f"이벤트 노이즈 {login_cnt / total * 100:.0f}% (PageView_Login 과다)",
            "description":      f"전체 {total}건 중 PageView_Login {login_cnt}건. 루프 버그 영향으로 분석 신뢰도 저하.",
            "affected_user_id": None,
            "metadata":         json.dumps({"total": total, "login_count": login_cnt}),
        })

    return issues


# ──────────────────────────────────────────────
# 6. Supabase 저장
# ──────────────────────────────────────────────
def sb_insert(table: str, rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    resp = requests.post(
        f"{SB_URL}/rest/v1/{table}",
        headers=SB_HEADERS,
        json=rows,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ⚠️  Supabase 오류 [{table}]: {resp.status_code} — {resp.text[:200]}")
        return []
    return resp.json()


# ──────────────────────────────────────────────
# 7. 메인
# ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("🤖 TourLive Mixpanel 자동 동기화 시작")
    print(f"   {NOW.strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 50)

    # ─ 데이터 수집 (오늘/전일 별도 호출 → prev 데이터 확실 보장)
    print(f"📅 오늘: {DATE_TO} / 전일: {DATE_PREV}")
    raw_today_only = fetch_raw_events(date_from=DATE_TO,   date_to=DATE_TO)
    raw_prev_only  = fetch_raw_events(date_from=DATE_PREV, date_to=DATE_PREV)

    if not raw_today_only:
        print("⚠️  오늘 데이터가 없습니다. 종료.")
        return

    print(f"  ℹ️  오늘({DATE_TO}): {len(raw_today_only):,}건 / 전일({DATE_PREV}): {len(raw_prev_only):,}건")

    # 전체 집계용 (오늘 + 전일 통합)
    raw_events = raw_today_only + raw_prev_only

    # ─ 집계
    print("🔢 이벤트 집계 중…")
    agg = aggregate_events(raw_today_only)   # 이벤트 통계는 오늘 기준
    dau_today, dau_prev = calc_hourly_dau(raw_events)
    print(f"  총 이벤트: {agg['total']:,}건 | 유니크 유저: {agg['unique_users']:,}명")
    print(f"  상위 이벤트: {', '.join(f'{k}({v})' for k,v in agg['event_counts'].most_common(5))}")

    # ─ PageView_Tour 속성별 집계
    print("🗺  투어 상세 속성 집계 중…")
    tour_prop_rows = calc_tour_view_props(raw_today_only, raw_prev_only)

    # ─ 퍼널 집계
    print("🔀 퍼널 집계 중…")
    funnel_rows = calc_funnel(raw_today_only, raw_prev_only)

    # ─ 이슈 감지
    print("🔍 이슈 감지 중…")
    issues = detect_issues(agg, dau_today, dau_prev)
    print(f"  감지된 이슈: {len(issues)}건")

    # ─ Supabase 저장
    print("☁️  Supabase 저장 중…")

    label = f"🤖 자동 동기화 · {DATE_TO} (API)"

    run_res = sb_insert("analysis_runs", [{
        "date_from":    DATE_FROM,
        "date_to":      DATE_TO,
        "label":        label,
        "total_events": agg["total"],
        "unique_users": agg["unique_users"],
        "notes":        f"[API 자동] 이슈 {len(issues)}건 감지 | {NOW.strftime('%Y-%m-%d %H:%M KST')}",
    }])
    if not run_res:
        print("❌ analysis_runs 저장 실패. 중단.")
        sys.exit(1)
    run_id = run_res[0]["id"]
    print(f"  ✅ analysis_run 생성: id={run_id}")

    # DAU hourly
    dau_rows = [
        {
            "run_id":      run_id,
            "hour":        h,
            "today_count": dau_today.get(h, 0),
            "prev_count":  dau_prev.get(h, 0),
        }
        for h in range(24)
    ]
    sb_insert("dau_hourly", dau_rows)
    print(f"  ✅ dau_hourly 저장 ({len(dau_rows)}행)")

    # event_stats
    evt_rows = [
        {"run_id": run_id, "event_name": k, "count": v}
        for k, v in agg["event_counts"].most_common()
    ]
    sb_insert("event_stats", evt_rows)
    print(f"  ✅ event_stats 저장 ({len(evt_rows)}행)")

    # platform_stats
    plat_rows = [
        {"run_id": run_id, "platform": k, "count": v}
        for k, v in agg["platform_counts"].most_common()
    ]
    sb_insert("platform_stats", plat_rows)
    print(f"  ✅ platform_stats 저장 ({len(plat_rows)}행)")

    # country_stats
    ctry_rows = [
        {
            "run_id":       run_id,
            "country_code": k,
            "country_name": agg["country_names"].get(k, k),
            "count":        v,
        }
        for k, v in agg["country_counts"].most_common()
    ]
    sb_insert("country_stats", ctry_rows)
    print(f"  ✅ country_stats 저장 ({len(ctry_rows)}행)")

    # tour_view_stats
    tour_rows = [
        {
            "run_id":     run_id,
            "tour_id":    tid,
            "tour_name":  agg["tour_names"].get(tid),
            "view_count": cnt,
            "price":      agg["tour_prices"].get(tid),
        }
        for tid, cnt in agg["tour_views"].most_common()
    ]
    if tour_rows:
        sb_insert("tour_view_stats", tour_rows)
        print(f"  ✅ tour_view_stats 저장 ({len(tour_rows)}행)")

    # detected_issues
    issue_rows = [{**i, "run_id": run_id} for i in issues]
    if issue_rows:
        sb_insert("detected_issues", issue_rows)
        print(f"  ✅ detected_issues 저장 ({len(issue_rows)}행)")

    # funnel_daily
    fn_rows = [{**r, "run_id": run_id} for r in funnel_rows]
    sb_insert("funnel_daily", fn_rows)
    print(f"  ✅ funnel_daily 저장 ({len(fn_rows)}행)")

    # tour_view_props
    tp_rows = [{**r, "run_id": run_id} for r in tour_prop_rows]
    if tp_rows:
        sb_insert("tour_view_props", tp_rows)
        print(f"  ✅ tour_view_props 저장 ({len(tp_rows)}행)")

    print("=" * 50)
    print(f"✅ 자동 동기화 완료! run_id={run_id}")
    print("=" * 50)


if __name__ == "__main__":
    main()
