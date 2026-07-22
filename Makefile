.PHONY: dgemm
.PHONY: dgemm_release
.PHONY: playground
.PHONY: run

dgemm:
	odin build ./tests/dgemm/ -out:$@ -debug -define:IMP_PROFILER_ENABLED=true

dgemm_release:
	odin build ./tests/dgemm/ -out:$@ -o:speed -define:IMP_PROFILER_ENABLED=false

playground:
	odin build ./tests/playground/ -out:$@ -debug -define:IMP_PROFILER_ENABLED=true

run:
	odin run ./tests/playground/ -debug
