# 속성 (Properties)

`event_types/` 폴더 내의 각 이벤트 클래스들이 트래킹될 때(`trackEvent`) 함께 전송하는 파라미터들입니다. 

---

**투어 정보 관련**

- `Tour_id` / `Tour_name`: 투어 ID 및 이름
- `City_id` / `City_name`: 도시/지역 ID 및 이름
- `Country_name`: 국가 이름
- `Price`: 가격
- `is_purchased`: 구매 여부

**콘텐츠 및 플레이어 관련**

- `type`: 플레이어 타입 (radio, video) 또는 이벤트 타입
- `Place_id` / `Place_name`: 장소 ID 및 이름
- `Place_type_id` / `Place_type_name`: 장소의 타입(카테고리)
- `track_index` — 트랙 번호
- `exit_position_sec` — 종료 시점 재생 위치
- `play_duration_sec` — 실제 청취한 시간
- `listen_duration_sec` — 해당 트랙 청취 시간

**검색 및 리뷰 관련**

- `Search_keyword`: 검색어
- `Result_count`: 검색 결과 개수
- `When_write_review`: 리뷰를 작성한 시점/맥락
- `rating`: 남긴 별점
- `is_write_review_contents`: 리뷰 내용 작성 여부

**기기/기타 속성**

- `Platform`: 사용 플랫폼 (iOS, Android 등)
- `Coupon_name` / `Coupon_price`: 등록/사용된 쿠폰 정보