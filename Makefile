
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# Build commands
# =============================================================================

.PHONY: install
install:
	./mvnw install -DskipTests

.PHONY: test
test:
	./mvnw test

.PHONY: build
build: lint install

.PHONY: package
package:
	./mvnw package -DskipTests

# =============================================================================
# Code style
# =============================================================================

.PHONY: lint
lint:
	./mvnw checkstyle:check spotless:check

.PHONY: format
format:
	./mvnw spotless:apply

# =============================================================================
# Clean
# =============================================================================

.PHONY: clean
clean:
	./mvnw clean

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help:
	@echo "Lance Flink Makefile"
	@echo ""
	@echo "Build commands:"
	@echo "  install    - Install without tests"
	@echo "  test       - Run tests"
	@echo "  build      - Lint and install"
	@echo "  package    - Package without tests"
	@echo ""
	@echo "Code style:"
	@echo "  lint       - Check code style (checkstyle + spotless)"
	@echo "  format     - Apply spotless formatting"
	@echo ""
	@echo "Clean:"
	@echo "  clean      - Clean build artifacts"
