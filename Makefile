define run_on_sources
find sparklogstats tests -name "*.py" -type f | xargs $(1)
endef

help:
	@echo '           all: lint, format and coverage'
	@echo '          lint: run pylama on *.py files'
	@echo '        format: format all *.py files inline using yapf'
	@echo '      coverage: run tests and print coverage report'
	@echo ' coverage-html: run tests and generate report in coverage_html/index.html'
	@echo '         tests: run tests'
	@echo '         clean: delete data generated by coverage'
	@echo 'clean_worktree: remove all files unknown to git'

all: lint format coverage

lint:
	pylama

format:
	$(call run_on_sources, yapf -i)

tests:
	python3 setup.py test

coverage: .coverage
	coverage3 report

.coverage: tests/test_*.py $(RESOURCES)
	coverage3 run setup.py test

coverage-html: .coverage
	coverage html
	@echo 'Output in coverage_html/index.html'

clean:
	rm -rf coverage_html .coverage

clean_worktree:
	git clean -dxf
