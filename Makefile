.PHONY: prog
.PHONY: run

prog:
	odin build ./tests/playground/ -out:$@ -debug

run:
	odin run ./tests/playground/ -debug
