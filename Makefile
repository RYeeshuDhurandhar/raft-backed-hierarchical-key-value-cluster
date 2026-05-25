# folder name of the package of interest
PKGNAME = hkvc
MKARGS = -timeout 120s

.PHONY: final checkpoint all final-race checkpoint-race all-race clean docs
.SILENT: final checkpoint all final-race checkpoint-race all-race clean docs

# run corresponding tests
checkpoint:
	go test -C $(PKGNAME) -v $(MKARGS) -run Checkpoint

final:
	go test -C $(PKGNAME) -v $(MKARGS) -run Final

all:
	go test -C $(PKGNAME) -v $(MKARGS)

# run corresponding tests using race detector
checkpoint-race:
	go test -C $(PKGNAME) -v $(MKARGS) -race -run Checkpoint

final-race:
	go test -C $(PKGNAME) -v $(MKARGS) -race -run Final

all-race:
	go test -C $(PKGNAME) -v $(MKARGS) -race

# delete all executables and docs, leaving only source
clean:
	rm -rf $(PKGNAME)-doc.md

# generate documentation for the package of interest
docs:
	gomarkdoc -u -o $(PKGNAME)-doc.md ./$(PKGNAME)

