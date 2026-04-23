.PHONY: build test run run-cluster stop-cluster run-docker stop-docker chaos chaos-v2 chaos-report clean

build:
	go build -o bin/lbserver    ./cmd/lbserver
	go build -o bin/echobackend ./cmd/echobackend
	go build -o bin/chaos       ./cmd/chaos

test:
	go test -race -count=1 ./...

# Single LB + one local backend, foreground LB. Smoke test only.
run: build
	./bin/echobackend --listen=:9001 --id=local & echo $$! > .pid
	./bin/lbserver --backends=http://localhost:9001

# 3 local backends + 1 LB, all backgrounded. Mirrors key-value-store/run-cluster.
# Use `make stop-cluster` to clean up.
run-cluster: build
	./bin/echobackend --listen=:9001 --id=b1 & echo $$! >  .pids
	./bin/echobackend --listen=:9002 --id=b2 & echo $$! >> .pids
	./bin/echobackend --listen=:9003 --id=b3 & echo $$! >> .pids
	./bin/lbserver \
		--listen=:7080 --admin-listen=:7090 \
		--backends=http://localhost:9001,http://localhost:9002,http://localhost:9003 \
		& echo $$! >> .pids

stop-cluster:
	-pkill -f "bin/lbserver"    || true
	-pkill -f "bin/echobackend" || true
	rm -f .pid .pids

run-docker:
	docker compose up --build -d

stop-docker:
	docker compose down

# 60-second vegeta load test against a self-spawned 3-backend cluster, with
# chaos kill/revive every 10s. Writes reports/<tag>-<timestamp>/.
# Stop any running cluster first to free ports.
chaos: build stop-cluster
	./bin/chaos --tag=v1 --seed=42

# V2 acceptance run — same seed as the V1 baseline so kill/revive timelines
# align for like-for-like comparison. spec.md §8 names the thresholds.
chaos-v2: build stop-cluster
	./bin/chaos --tag=v2 --seed=42

# Print the latest report's summary and chaos timeline.
chaos-report:
	@latest=$$(ls -1dt reports/*/ 2>/dev/null | head -1); \
	if [ -z "$$latest" ]; then echo "no reports yet — run 'make chaos'"; exit 1; fi; \
	echo "=== $$latest ==="; \
	cat $$latest/summary.txt; \
	echo; \
	echo "--- chaos timeline ---"; \
	cat $$latest/chaos.log

clean:
	rm -rf bin/ .pid .pids
