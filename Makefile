.PHONY: dgemm
.PHONY: dgemm_release
.PHONY: playground
.PHONY: run

playground:
	odin build ./tests/playground/ -out:$@ -debug -define:PROF_ENABLED=true

run:
	odin run ./tests/playground/ -debug
