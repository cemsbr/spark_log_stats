define run_on_sources
find sparklogstats tests -name "*.py" -type f | xargs $(1)
endef

help:
	@echo '           all: run pylama and, if successful, run yapf_format'
	@echo '        pylama: run pylama on *.py files'
	@echo '   yapf_format: format all *.py files inline'
	@echo '        flake8: run flake8 on *.py files'
	@echo '          pep8: run pep8'
	@echo 'clean_worktree: remove all files not known by git'

all: pylama yapf_format

pylama:
	pylama --ignore I0011,C0111

yapf_format:
	$(call run_on_sources, yapf -i)

flake8:
	$(call run_on_sources, flake8 --exclude=__init__.py)

clean_worktree:
	git clean -dxf
