SHELL := /bin/bash
MAVEN := mvn

.PHONY: install test build bundle clean lint format \
        install-all test-all build-all help

help:
	@echo "Available targets:"
	@echo "  install      Install artifact to local .m2 (skip tests)"
	@echo "  test         Run unit and integration tests"
	@echo "  build        Run lint + install"
	@echo "  bundle       Build shaded fat-jar (skip tests)"
	@echo "  clean        Remove target/"
	@echo "  lint         Run Spotless and Checkstyle checks"
	@echo "  format       Apply Spotless formatting in place"
	@echo "  install-all  Alias of install (reserved for future multi-module matrix)"
	@echo "  test-all     Alias of test (reserved for future multi-module matrix)"
	@echo "  build-all    Alias of build (reserved for future multi-module matrix)"

install:
	$(MAVEN) install -DskipTests

test:
	$(MAVEN) test

build:
	$(MAKE) lint
	$(MAKE) install

bundle:
	$(MAVEN) package -DskipTests

clean:
	$(MAVEN) clean

lint:
	$(MAVEN) spotless:check checkstyle:check

format:
	$(MAVEN) spotless:apply

install-all: install
test-all: test
build-all: build
