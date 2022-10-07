
test:
	@go test -race -covermode=atomic -coverprofile=coverage.out

bench:
	@go test -bench=. -benchmem -run=^#