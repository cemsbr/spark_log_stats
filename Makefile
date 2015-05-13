define run_on_sources
find sparklogstats tests -name "*.py" -type f | xargs $(1)
endef

help:
	@echo '        flake8: runs flake8 on *.py files'
	@echo '          pep8: runs pep8'
	@echo '   yapf_format: formats all *.py files inline'
	@echo '           all: runs flake8 and, if successful, runs yapf_format'
	@echo 'clean_worktree: removes all files not known by git'

all: flake8 yapf_format

flake8:
	$(call run_on_sources, flake8 --exclude=__init__.py)

pep8:
	$(call run_on_sources, pep8)

yapf_format:
	$(call run_on_sources, yapf -i)

clean_worktree:
	git clean -dxf
