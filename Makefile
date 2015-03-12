all: build

clean:
	./build.sh clean

build:
	./build.sh

.PHONY: update-deps clean

