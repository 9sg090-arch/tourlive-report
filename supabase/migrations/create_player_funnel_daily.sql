-- player_funnel_daily 테이블
-- Supabase 대시보드 > SQL Editor에서 실행하세요

CREATE TABLE IF NOT EXISTS player_funnel_daily (
    id           bigserial    PRIMARY KEY,
    run_id       bigint       REFERENCES analysis_runs(id) ON DELETE CASCADE,
    funnel_key   text         NOT NULL,  -- 'entry' | 'engagement' | 'free_trial'
    funnel_name  text         NOT NULL,  -- 한국어 퍼널명
    step_order   int          NOT NULL,  -- 0부터 시작하는 단계 순서
    step_key     text         NOT NULL,  -- 'tour_view' | 'player_open' | ...
    step_label   text         NOT NULL,  -- 화면 표시용 한국어 레이블
    today_count  int          NOT NULL DEFAULT 0,
    prev_count   int          NOT NULL DEFAULT 0,
    created_at   timestamptz  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_funnel_run_id     ON player_funnel_daily(run_id);
CREATE INDEX IF NOT EXISTS idx_player_funnel_funnel_key ON player_funnel_daily(funnel_key);
