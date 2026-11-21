IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac.
PLATFORM ?= linux/amd64
DEV    ?= ../dev
ANSWER ?= ../dev-answer
BIN    := target/release/shopping-sessions
export IMAGE CPUS MEM PLATFORM

.PHONY: native fixtures-native check-native build fixtures run-dev check-dev

## Without Docker: compile, run the 36 fixtures, run + check the dev set
native:
	cargo build --release
fixtures-native: native
	python3 scripts/run_fixtures.py fixtures -- $(BIN)
check-native: native
	rm -rf out && time env DATA_DIR=$(DEV) OUTPUT_DIR=out TIMING=1 $(BIN)
	python3 scripts/compare.py out $(ANSWER)

## With Docker, exactly like the scorer
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out
check-dev:
	python3 scripts/compare.py out $(ANSWER)
