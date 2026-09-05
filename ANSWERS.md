# ANSWERS — Day 28 Track 2

**Người thực hiện:** Đỗ Quang Huy — MSSV 2A202601896 (làm cá nhân)

## 1. Bằng chứng chạy được (tóm tắt)

- `pytest starter-tests tests -q`: 87/87 passed.
- `pytest integration-tests/test_j1_golden_path.py -q`: 12 passed, 3 skipped (gpu gate — không có vLLM thật).
- `pytest integration-tests/test_j2_idempotent_replay.py -q`: 9/9 passed.
- `pytest integration-tests -m "not gpu and not langsmith" -q`: 56 passed, 0 failed.
- 10/10 evidence file trong `evidence/` (`ip01`…`ip10`) + `integration-report.json` + `load-profile.json`.
- Môi trường chạy: GitHub Codespaces (4 vCPU/16GB) do máy cá nhân chỉ có 7.7GB RAM, không đủ cho profile đầy đủ (Airflow + Spark Connect) theo khuyến nghị 12–16GB của README.

## 2. Trade-off kỹ thuật

### IP01/IP10 — Header Kafka và trace
`event_headers` bắt buộc `idempotency-key` nhưng bỏ hẳn `traceparent` khi không có trace, thay vì gửi chuỗi rỗng. Trade-off: consumer phải tự xử lý trường hợp thiếu header thay vì luôn có mặt — nhưng gửi một `traceparent` rỗng/không hợp lệ còn tệ hơn, vì nó có thể được hiểu nhầm là một trace hợp lệ ở phía downstream (Jaeger/OTel sẽ tạo span "ma" thay vì báo rõ "không có trace").

### IP03 — Idempotency ở tầng ứng dụng, không phải tầng Kafka
`dedupe_latest` giữ bản ghi mới nhất theo `(occurred_at, event_id)` trước khi đưa vào Spark MERGE, cố tình **không** dựa vào Kafka để loại trùng (Kafka chỉ đảm bảo *at-least-once*, không đảm bảo *exactly-once* theo ngữ nghĩa nghiệp vụ). Trade-off: mỗi lần consume phải giữ toàn bộ batch trong bộ nhớ để so sánh trước khi ghi, giới hạn kích thước batch mà pipeline có thể xử lý một lần — chấp nhận được ở quy mô lab, nhưng ở production cần MERGE theo cửa sổ thời gian hoặc watermark thay vì toàn batch.

### IP04 — Tách online/offline feature path
`feast_online_request` chỉ đọc 4 đặc trưng đã vật chất hoá (`feedback_count`, `avg_rating`, `negative_ratio`, `delta_version`) thay vì tính lại từ Delta mỗi request. Trade-off: độ tươi của đặc trưng phụ thuộc tần suất `refresh_online_features` chạy trong Airflow (hiện chạy theo mỗi batch, không theo lịch cố định) — đổi lại latency phục vụ không phụ thuộc kích thước bảng Delta.

### IP07/IP08 — Readiness ba mức thay vì boolean
`readiness_status` phân biệt `not_ready`/`degraded`/`ready` dựa trên cờ `mandatory` của từng probe, thay vì gộp chung "healthy/unhealthy". Trade-off: thêm độ phức tạp cấu hình (phải gắn đúng `mandatory=True/False` cho từng dependency), nhưng đổi lại gateway không rút hẳn pod khỏi rotation chỉ vì một phụ thuộc không quan trọng (ví dụ Feast) tạm thời lỗi — đúng tinh thần probe Kubernetes readiness/liveness tách biệt.

### Rate limiting (IP08) — chấp nhận từ chối thay vì hàng đợi
Envoy dùng local rate limit token-bucket (10 req/s, không hàng đợi). Load test 200 request/8 worker cho thấy rõ: chỉ ~21/200 request qua được, phần còn lại nhận `429` (đã xác nhận trực tiếp bằng curl song song, không phải lỗi hệ thống). Trade-off: bảo vệ backend khỏi quá tải bằng cách từ chối sớm và rẻ (tại gateway) thay vì cho request xếp hàng và làm tăng P99 toàn hệ thống — đánh đổi là client phải tự cài retry-with-backoff, điều mà bài lab chưa triển khai ở phía client mẫu.

## 3. Production gaps (những gì còn thiếu so với hệ thống thật)

| Hạng mục | Hiện trạng trong lab | Thiếu gì so với production |
|---|---|---|
| Failure injection / recovery (J4) | Chưa chạy — chỉ có idempotency (J2) | Chưa có kịch bản tắt một dependency giữa chừng và đo thời gian phục hồi + chứng minh không mất dữ liệu |
| Serving thật (IP07) | vLLM không kết nối (không có GPU) | Chưa đo được latency/throughput thật của tầng inference, chưa có evidence `is_real_vllm: true` |
| K8s/GitOps rollback | Đã validate tĩnh manifest (`validate_manifests.py`) | Chưa demo drift + self-heal + rollback sống trên cụm K8s thật |
| Retry/backoff phía client | Không có | Client (`load-tests/run_profile.py`, `lab28 seed`) gửi request không giãn cách, dựa hoàn toàn vào rate limiter phía server để bảo vệ — production cần retry-with-jitter ở client |
| Airflow task timeout | Mới thêm `execution_timeout=3 phút` cho `index_new_documents` sau khi gặp sự cố CDN treo task | Các task còn lại (`drain_kafka_into_delta`, `refresh_online_features`) chưa có timeout tương tự — nguy cơ treo tương tự nếu phụ thuộc mạng ngoài chậm |
| Multi-broker Kafka | 1 broker, replication_factor=1 | Production cần ≥3 broker để chịu được mất 1 node mà không mất dữ liệu |
| Quan sát chi phí GPU | Không đo | Chưa có cơ chế theo dõi chi phí/giờ GPU khi nối vLLM thật |

## 4. Sự cố thực tế đã gặp và cách xử lý (đáng nêu trong Q&A)

1. **Máy cá nhân thiếu RAM (7.7GB) cho full profile** → chuyển sang GitHub Codespaces (16GB), có điều chỉnh `hostRequirements` trong `devcontainer.json`.
2. **Devcontainer build fail hoàn toàn** do apt source `dl.yarnpkg.com` không có chữ ký GPG hợp lệ, làm bước cài feature Docker-in-Docker fail → xoá source đó trong `Dockerfile` trước khi feature chạy `apt-get update`.
3. **`docker.io` bị chặn mạng hoàn toàn** từ Codespace (xác nhận bằng curl tới nhiều IP đều timeout) → cấu hình `registry-mirrors: ["https://mirror.gcr.io"]` cho Docker daemon, không cần đổi tên image nào trong `compose.yaml`.
4. **CDN HuggingFace (`us.aws.cdn.hf.co`) treo tác vụ nhúng vector vô thời hạn** → thêm `execution_timeout` cho task Airflow tương ứng để fail nhanh thay vì treo; CDN sau đó tự phục hồi và toàn bộ pipeline chạy sạch.

## 5. Đóng góp cá nhân

Bài làm cá nhân — Đỗ Quang Huy (2A202601896) thực hiện toàn bộ 10 điểm kết nối, với sự hỗ trợ của Claude Code (Anthropic) trong việc viết mã, gỡ lỗi hạ tầng Docker/Codespaces, và thu thập bằng chứng.

## 6. Phản tư (tự viết)

> _Phần dưới đây cần góc nhìn cá nhân của bạn — hãy tự viết vì bạn sẽ cần bảo vệ nó khi thuyết trình/Q&A._

- Phần nào của bài lab khó nhất với bạn, và vì sao?
- Nếu có thêm 1 tuần, bạn sẽ cải thiện điểm gì trước tiên trong danh sách "Production gaps" ở trên?
- Sự cố hạ tầng nào (mục 4) giúp bạn hiểu rõ nhất một khái niệm trong bài học?
