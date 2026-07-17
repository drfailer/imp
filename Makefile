.PHONY: prog
.PHONY: run

prog:
	odin build . -out:$@ -debug

run:
	odin run . -debug
