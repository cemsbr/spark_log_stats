RESOURCES := $(shell find ../sparklogstats -name \*.py)


help:
	@echo '        tests: run all tests in this folder'
	@echo '     coverage: prints coverage report'
	@echo 'coverage-html: generates report in coverage_html/index.html'
	@echo '        clean: deletes coverage data and html report'

tests:
	@PYTHONPATH="$$PYTHON_PATH:.." python3 -m unittest

# Runs coverage report and changes path from absolute to relative
coverage: .coverage
	@prefix="$$(readlink -e $$(dirname $$(pwd)))/sparklogstats/"; \
	        coverage report \
	        | sed "s|$$prefix||" \
	        | sed "s/^-\{$${#prefix}\}//" \
	        | sed "s/^Name \{$${#prefix}\}/Name/" \
	        | sed "s/^TOTAL \{$${#prefix}\}/TOTAL/"

.coverage: test_*.py $(RESOURCES)
	@-rm -f .coverage
	@export PYTHONPATH="$$PYTHON_PATH:.."; \
	for test in test_*.py; do \
		coverage run -a $$test; \
	done

coverage-html: .coverage
	coverage html
	@echo 'Output in coverage_html/index.html'

clean:
	rm -rf coverage_html .coverage __pycache__
