"""
투어라이브 대시보드 데이터 수집 & HTML 생성 스크립트
------------------------------------------------------
사용법:
  1. tourslive_config.json 에 API 키/URL 입력
  2. pip install requests jinja2
  3. python tourslive_fetch.py

결과: tourslive_dashboard.html 생성
"""

import requests
import json
import base64
import sys
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ──────────────────────────────────────────────
# 0. 설정 로드
# ──────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "tourslive_config.json")

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ {CONFIG_PATH} 파일이 없습니다. 먼저 config 파일을 만들어주세요.")
    sys.exit(1)

MP_PROJECT_ID  = config["mixpanel"]["project_id"]
MP_SECRET      = config["mixpanel"]["project_secret"]
MP_EVENTS      = config["mixpanel"]["events"]

RADASH_URL     = config["radash"]["base_url"].rstrip("/")
RADASH_KEY     = config["radash"]["api_key"]
RADASH_EP      = config["radash"]["endpoints"]

LOOKBACK       = config["dashboard"]["lookback_days"]
CURRENCY       = config["dashboard"]["currency"]
TZ             = ZoneInfo(config["dashboard"]["timezone"])
OUTPUT_FILE    = config["dashboard"]["output_file"]

NOW            = datetime.now(tz=TZ)
END_DATE       = NOW.strftime("%Y-%m-%d")
START_DATE     = (NOW - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")

# ──────────────────────────────────────────────
# 1. Mixpanel API 헬퍼
# ──────────────────────────────────────────────
_mp_auth = base64.b64encode(f"{MP_SECRET}:".encode()).decode()
_mp_headers = {"Authorization": f"Basic {_mp_auth}"}

def mp_segmentation(event_name: str, unit: str = "day", count_type: str = "general") -> dict:
    """이벤트 발생 수 / 고유 사용자 수를 일별로 가져옴"""
    params = {
        "event":     f'["{event_name}"]',
        "from_date": START_DATE,
        "to_date":   END_DATE,
        "type":      count_type,   # "general" = 총 횟수, "unique" = 유니크 유저
        "unit":      unit,
    }
    resp = requests.get(
        "https://mixpanel.com/api/2.0/segmentation",
        headers=_mp_headers,
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()

def mp_funnel(funnel_steps: list[str]) -> dict:
    """다단계 퍼널 전환율"""
    steps = [{"event": e} for e in funnel_steps]
    body = {
        "project_id": MP_PROJECT_ID,
        "steps":      steps,
        "from_date":  START_DATE,
        "to_date":    END_DATE,
        "unit":       "day",
    }
    resp = requests.post(
        "https://mixpanel.com/api/2.0/funnels",
        headers=_mp_headers,
        json=body,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()

# ──────────────────────────────────────────────
# 2. Radash API 헬퍼
# ──────────────────────────────────────────────
_radash_headers = {
    "Authorization": f"Bearer {RADASH_KEY}",
    "Content-Type":  "application/json",
}

def radash_get(endpoint: str, params: dict = None) -> dict:
    url = RADASH_URL + endpoint
    resp = requests.get(url, headers=_radash_headers, params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()

# ──────────────────────────────────────────────
# 3. 데이터 수집
# ──────────────────────────────────────────────
def fetch_all() -> dict:
    print("📡 데이터 수집 시작…")
    data = {}

    # ── Mixpanel: DAU (Session Start 유니크 유저)
    print("  [Mixpanel] DAU 데이터 수집…")
    try:
        raw_dau = mp_segmentation(MP_EVENTS["session_start"], count_type="unique")
        series  = raw_dau.get("data", {}).get("series", {})
        dates   = sorted(series.keys())
        dau_values = [series[d] for d in dates]
        data["dau"] = {"dates": dates, "values": dau_values}
        data["mau"] = max(dau_values[-30:]) if dau_values else 0   # 간이 MAU (30일 최고 DAU 기준)
    except Exception as e:
        print(f"  ⚠️  DAU 수집 실패: {e}")
        data["dau"] = {"dates": [], "values": []}
        data["mau"] = 0

    # ── Mixpanel: 투어 조회 수
    print("  [Mixpanel] 투어 조회 수집…")
    try:
        raw_view = mp_segmentation(MP_EVENTS["tour_view"])
        series   = raw_view.get("data", {}).get("series", {})
        dates_v  = sorted(series.keys())
        data["tour_views"] = {
            "dates":  dates_v,
            "values": [series[d] for d in dates_v],
        }
    except Exception as e:
        print(f"  ⚠️  투어 조회 수집 실패: {e}")
        data["tour_views"] = {"dates": [], "values": []}

    # ── Mixpanel: 퍼널 (Session → Tour View → Purchase)
    print("  [Mixpanel] 전환 퍼널 수집…")
    try:
        funnel_steps = [
            MP_EVENTS["session_start"],
            MP_EVENTS["tour_view"],
            MP_EVENTS["purchase"],
        ]
        raw_funnel = mp_funnel(funnel_steps)
        meta = raw_funnel.get("meta", {})
        step_counts = raw_funnel.get("data", {}).get("steps", [])
        funnel_labels = [
            "앱/웹 방문 (Session Start)",
            "투어 상세 조회 (Tour View)",
            "예약/결제 완료 (Purchase)",
        ]
        funnel_values = [s.get("count", 0) for s in step_counts]
        data["funnel"] = {"labels": funnel_labels, "values": funnel_values}
    except Exception as e:
        print(f"  ⚠️  퍼널 수집 실패: {e}")
        data["funnel"] = {
            "labels": ["앱/웹 방문", "투어 상세 조회", "예약/결제 완료"],
            "values": [0, 0, 0],
        }

    # ── Radash: 예약/주문 요약
    print("  [Radash] 예약 데이터 수집…")
    try:
        bookings = radash_get(RADASH_EP["bookings"], {
            "start_date": START_DATE, "end_date": END_DATE
        })
        data["bookings"] = bookings
        data["total_bookings"] = bookings.get("total", 0)
    except Exception as e:
        print(f"  ⚠️  예약 수집 실패: {e}")
        data["bookings"] = {}
        data["total_bookings"] = 0

    # ── Radash: 일별 매출
    print("  [Radash] 매출 데이터 수집…")
    try:
        revenue = radash_get(RADASH_EP["revenue"], {
            "start_date": START_DATE, "end_date": END_DATE, "unit": "day"
        })
        rev_series = revenue.get("daily", [])   # [{"date": "2024-01-01", "amount": 1234567}, ...]
        data["revenue"] = {
            "dates":  [r["date"] for r in rev_series],
            "values": [r["amount"] for r in rev_series],
        }
        data["total_revenue"] = revenue.get("total", 0)
    except Exception as e:
        print(f"  ⚠️  매출 수집 실패: {e}")
        data["revenue"] = {"dates": [], "values": []}
        data["total_revenue"] = 0

    # ── Radash: 사용자 통계
    print("  [Radash] 사용자 데이터 수집…")
    try:
        users = radash_get(RADASH_EP["users"])
        data["users"] = users
        data["total_users"] = users.get("total", 0)
        data["new_users"]   = users.get("new_this_period", 0)
    except Exception as e:
        print(f"  ⚠️  사용자 수집 실패: {e}")
        data["users"] = {}
        data["total_users"] = 0
        data["new_users"]   = 0

    # ── Radash: 투어 상품 통계
    print("  [Radash] 투어 상품 데이터 수집…")
    try:
        tours = radash_get(RADASH_EP["tours"])
        data["tours"] = tours
        data["active_tours"] = tours.get("active_count", 0)
        data["top_tours"]    = tours.get("top_by_bookings", [])[:5]
    except Exception as e:
        print(f"  ⚠️  투어 수집 실패: {e}")
        data["tours"] = {}
        data["active_tours"] = 0
        data["top_tours"]    = []

    # ── 전환율 계산
    funnel_v = data["funnel"]["values"]
    if len(funnel_v) >= 2 and funnel_v[0] > 0:
        data["conversion_rate"] = round(funnel_v[-1] / funnel_v[0] * 100, 2)
    else:
        data["conversion_rate"] = 0.0

    data["generated_at"] = NOW.strftime("%Y-%m-%d %H:%M:%S KST")
    data["period"]       = f"{START_DATE} ~ {END_DATE}"
    data["currency"]     = CURRENCY
    print("✅ 데이터 수집 완료!")
    return data


# ──────────────────────────────────────────────
# 4. HTML 대시보드 생성
# ──────────────────────────────────────────────
HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>투어라이브 프로덕트 대시보드</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3e;
    --text: #e2e8f0;
    --muted: #94a3b8;
    --accent: #6366f1;
    --green: #10b981;
    --red: #f43f5e;
    --yellow: #f59e0b;
    --blue: #3b82f6;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: -apple-system, 'Pretendard', sans-serif; padding: 24px; }}
  h1 {{ font-size: 22px; font-weight: 700; margin-bottom: 4px; }}
  .subtitle {{ color: var(--muted); font-size: 13px; margin-bottom: 28px; }}

  /* KPI 카드 */
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 28px; }}
  .kpi-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
  .kpi-card .label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 8px; }}
  .kpi-card .value {{ font-size: 28px; font-weight: 700; }}
  .kpi-card .sub   {{ font-size: 12px; color: var(--muted); margin-top: 6px; }}
  .kpi-card.green  {{ border-top: 3px solid var(--green); }}
  .kpi-card.blue   {{ border-top: 3px solid var(--blue); }}
  .kpi-card.accent {{ border-top: 3px solid var(--accent); }}
  .kpi-card.yellow {{ border-top: 3px solid var(--yellow); }}
  .kpi-card.red    {{ border-top: 3px solid var(--red); }}

  /* 차트 섹션 */
  .charts-row {{ display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 20px; }}
  .charts-row2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }}
  .chart-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
  .chart-card h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 16px; }}
  .chart-card canvas {{ max-height: 240px; }}

  /* Top 투어 테이블 */
  .table-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
  .table-card h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 14px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; color: var(--muted); font-weight: 500; padding: 6px 8px; border-bottom: 1px solid var(--border); }}
  td {{ padding: 8px; border-bottom: 1px solid var(--border); }}
  tr:last-child td {{ border-bottom: none; }}

  /* 문제 인식 섹션 */
  .insight-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }}
  .insight-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
  .insight-card h3 {{ font-size: 14px; font-weight: 600; margin-bottom: 12px; }}
  .insight-item {{ display: flex; align-items: flex-start; gap: 10px; margin-bottom: 12px; font-size: 13px; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px; font-weight: 600; }}
  .badge.warn {{ background: #f59e0b22; color: var(--yellow); }}
  .badge.ok   {{ background: #10b98122; color: var(--green); }}
  .badge.crit {{ background: #f43f5e22; color: var(--red); }}

  footer {{ text-align: center; color: var(--muted); font-size: 11px; margin-top: 32px; }}
</style>
</head>
<body>

<h1>🌏 투어라이브 프로덕트 대시보드</h1>
<p class="subtitle">기간: {period} &nbsp;|&nbsp; 마지막 업데이트: {generated_at}</p>

<!-- KPI 카드 -->
<div class="kpi-grid">
  <div class="kpi-card blue">
    <div class="label">MAU (30일 피크 DAU)</div>
    <div class="value">{mau:,}</div>
    <div class="sub">월간 활성 사용자</div>
  </div>
  <div class="kpi-card green">
    <div class="label">총 예약 건수</div>
    <div class="value">{total_bookings:,}</div>
    <div class="sub">기간 내 확정 예약</div>
  </div>
  <div class="kpi-card accent">
    <div class="label">최종 전환율</div>
    <div class="value">{conversion_rate}%</div>
    <div class="sub">방문 → 결제 완료</div>
  </div>
  <div class="kpi-card yellow">
    <div class="label">총 매출</div>
    <div class="value">{total_revenue_display}</div>
    <div class="sub">기간 내 결제 매출 (KRW)</div>
  </div>
  <div class="kpi-card green">
    <div class="label">활성 투어 수</div>
    <div class="value">{active_tours:,}</div>
    <div class="sub">현재 판매 중인 상품</div>
  </div>
  <div class="kpi-card blue">
    <div class="label">신규 가입자</div>
    <div class="value">{new_users:,}</div>
    <div class="sub">기간 내 신규 가입</div>
  </div>
</div>

<!-- 차트 Row 1: DAU 트렌드 + 퍼널 -->
<div class="charts-row">
  <div class="chart-card">
    <h3>📈 DAU 트렌드 (일별 활성 사용자)</h3>
    <canvas id="dauChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>🔽 전환 퍼널</h3>
    <canvas id="funnelChart"></canvas>
  </div>
</div>

<!-- 차트 Row 2: 매출 트렌드 + 투어 조회 -->
<div class="charts-row2">
  <div class="chart-card">
    <h3>💰 일별 매출 트렌드</h3>
    <canvas id="revenueChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>👁 일별 투어 조회 수</h3>
    <canvas id="viewChart"></canvas>
  </div>
</div>

<!-- 인사이트 + 상위 투어 -->
<div class="insight-row">
  <div class="insight-card">
    <h3>🔍 프로덕트 현황 체크리스트</h3>
    <div id="insightList"></div>
  </div>
  <div class="table-card">
    <h3>🏆 Top 5 예약 투어</h3>
    <table>
      <thead><tr><th>#</th><th>투어명</th><th>예약 수</th><th>매출</th></tr></thead>
      <tbody id="topToursBody"></tbody>
    </table>
  </div>
</div>

<footer>투어라이브 대시보드 · 데이터 소스: Mixpanel + Radash · {generated_at}</footer>

<script>
// ── 임베드 데이터 ──────────────────────────────────
const D = {data_json};

// ── 공통 스타일 ──────────────────────────────────
const gridColor = 'rgba(255,255,255,0.06)';
const commonScales = {{
  x: {{ ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }}, grid: {{ color: gridColor }} }},
  y: {{ ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }}, grid: {{ color: gridColor }} }},
}};

// ── DAU 차트 ──────────────────────────────────────
new Chart(document.getElementById('dauChart'), {{
  type: 'line',
  data: {{
    labels: D.dau.dates.map(d => d.slice(5)),
    datasets: [{{
      label: 'DAU',
      data: D.dau.values,
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59,130,246,0.12)',
      fill: true, tension: 0.4, pointRadius: 2,
    }}],
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: commonScales,
  }},
}});

// ── 퍼널 차트 ──────────────────────────────────────
new Chart(document.getElementById('funnelChart'), {{
  type: 'bar',
  data: {{
    labels: D.funnel.labels,
    datasets: [{{
      data: D.funnel.values,
      backgroundColor: ['#3b82f6', '#6366f1', '#10b981'],
      borderRadius: 6,
    }}],
  }},
  options: {{
    indexAxis: 'y',
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          afterLabel: (ctx) => {{
            const first = D.funnel.values[0] || 1;
            return `전환율: ${{(ctx.raw / first * 100).toFixed(1)}}%`;
          }},
        }},
      }},
    }},
    scales: commonScales,
  }},
}});

// ── 매출 차트 ──────────────────────────────────────
new Chart(document.getElementById('revenueChart'), {{
  type: 'bar',
  data: {{
    labels: D.revenue.dates.map(d => d.slice(5)),
    datasets: [{{
      label: '매출',
      data: D.revenue.values,
      backgroundColor: 'rgba(245,158,11,0.7)',
      borderRadius: 4,
    }}],
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      ...commonScales,
      y: {{ ...commonScales.y, ticks: {{ ...commonScales.y.ticks,
        callback: v => (v >= 1000000 ? (v/1000000).toFixed(1)+'M' : v >= 1000 ? (v/1000).toFixed(0)+'K' : v) + ' ₩'
      }} }},
    }},
  }},
}});

// ── 투어 조회 차트 ──────────────────────────────────────
new Chart(document.getElementById('viewChart'), {{
  type: 'line',
  data: {{
    labels: D.tour_views.dates.map(d => d.slice(5)),
    datasets: [{{
      label: '투어 조회',
      data: D.tour_views.values,
      borderColor: '#10b981',
      backgroundColor: 'rgba(16,185,129,0.10)',
      fill: true, tension: 0.4, pointRadius: 2,
    }}],
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: commonScales,
  }},
}});

// ── Top 투어 테이블 ──────────────────────────────────────
const tbody = document.getElementById('topToursBody');
const fmt = n => n ? new Intl.NumberFormat('ko-KR').format(n) : '-';
(D.top_tours || []).forEach((t, i) => {{
  tbody.innerHTML += `<tr>
    <td>${{i+1}}</td>
    <td>${{t.name || t.title || '-'}}</td>
    <td>${{fmt(t.bookings || t.booking_count)}}</td>
    <td>${{t.revenue ? fmt(t.revenue)+'원' : '-'}}</td>
  </tr>`;
}});
if (!D.top_tours || !D.top_tours.length) {{
  tbody.innerHTML = '<tr><td colspan="4" style="color:#94a3b8;text-align:center">데이터 없음</td></tr>';
}}

// ── 인사이트 체크리스트 ──────────────────────────────────
const insights = [];
const cr = D.conversion_rate;
const dau = D.dau.values;
const avgDau = dau.length ? dau.reduce((a,b)=>a+b,0)/dau.length : 0;
const lastDau = dau.length ? dau[dau.length-1] : 0;
const dauTrend = dau.length > 7
  ? (lastDau - dau[dau.length-8]) / (dau[dau.length-8] || 1) * 100
  : 0;

// 전환율 판단
if (cr < 1) insights.push({{ badge:'crit', text:`전환율 ${{cr}}% — 매우 낮음. 결제 플로우/랜딩 페이지 점검 필요` }});
else if (cr < 3) insights.push({{ badge:'warn', text:`전환율 ${{cr}}% — 개선 여지 있음. 상세 페이지 or CTA 최적화 검토` }});
else insights.push({{ badge:'ok', text:`전환율 ${{cr}}% — 양호` }});

// DAU 추세
if (dauTrend < -10) insights.push({{ badge:'crit', text:`DAU 전주 대비 ${{dauTrend.toFixed(1)}}% 하락 중 — 리텐션/마케팅 확인` }});
else if (dauTrend < 0) insights.push({{ badge:'warn', text:`DAU 소폭 감소 (${{dauTrend.toFixed(1)}}%) — 트렌드 모니터링 필요` }});
else insights.push({{ badge:'ok', text:`DAU 성장 중 (+${{dauTrend.toFixed(1)}}%)` }});

// 퍼널 드롭
const fv = D.funnel.values;
if (fv.length >= 2 && fv[0] > 0) {{
  const viewRate = (fv[1]/fv[0]*100).toFixed(1);
  if (viewRate < 30) insights.push({{ badge:'warn', text:`방문→투어조회 전환 ${{viewRate}}% — 홈 UI/탐색 경험 개선 고려` }});
  else insights.push({{ badge:'ok', text:`방문→투어조회 전환 ${{viewRate}}% — 양호` }});
}}

const il = document.getElementById('insightList');
insights.forEach(ins => {{
  il.innerHTML += `<div class="insight-item">
    <span class="badge ${{ins.badge}}">${{ins.badge==='crit'?'🚨 긴급':ins.badge==='warn'?'⚠️ 주의':'✅ 정상'}}</span>
    <span>${{ins.text}}</span>
  </div>`;
}});
</script>
</body>
</html>
"""

def format_revenue(amount: int, currency: str) -> str:
    if currency == "KRW":
        if amount >= 100_000_000:
            return f"{amount/100_000_000:.1f}억"
        elif amount >= 10_000:
            return f"{amount/10_000:.0f}만"
        return f"{amount:,}원"
    return f"{amount:,.0f}"

def generate_html(data: dict) -> str:
    return HTML_TEMPLATE.format(
        period              = data["period"],
        generated_at        = data["generated_at"],
        mau                 = data["mau"],
        total_bookings      = data["total_bookings"],
        conversion_rate     = data["conversion_rate"],
        total_revenue_display = format_revenue(data["total_revenue"], CURRENCY),
        active_tours        = data["active_tours"],
        new_users           = data["new_users"],
        data_json           = json.dumps(data, ensure_ascii=False, default=str),
    )


# ──────────────────────────────────────────────
# 5. 메인
# ──────────────────────────────────────────────
if __name__ == "__main__":
    data = fetch_all()
    html = generate_html(data)

    out_path = os.path.join(os.path.dirname(__file__), OUTPUT_FILE)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n🎉 대시보드 생성 완료!")
    print(f"   → {out_path}")
    print(f"\n주요 지표 요약:")
    print(f"   MAU            : {data['mau']:,}")
    print(f"   총 예약        : {data['total_bookings']:,}")
    print(f"   전환율         : {data['conversion_rate']}%")
    print(f"   총 매출        : {format_revenue(data['total_revenue'], CURRENCY)}")
