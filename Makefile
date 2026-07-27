.PHONY: dgemm
.PHONY: dgemm_release
.PHONY: playground
.PHONY: run

dgemm:
	odin build ./tests/dgemm/ -out:$@ -debug -define:PROF_ENABLED=true

dgemm_release:
	odin build ./tests/dgemm/ -out:dgemm -o:speed -define:PROF_ENABLED=true -microarch=native -no-bounds-check

playground:
	odin build ./tests/playground/ -out:$@ -debug -define:PROF_ENABLED=true

run:
	odin run ./tests/playground/ -debug

test:
	odin test ./tests/dgemm/ -debug -thread-count:1 -define:PROF_ENABLED=true
