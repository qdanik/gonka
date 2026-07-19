.PHONY: proto test integration-test stress-test fault-test gateway gateway-test

proto:
	PATH="$$HOME/go/bin:$$PATH" protoc --proto_path=proto --go_out=. --go_opt=module=devshard proto/devshard/v1/*.proto

test:
	go test ./... -v

integration-test:
	go test ./user/ -tags stress -run 'TestStress|TestFault' -v -timeout 30m -count=1

stress-test:
	go test ./user/ ./storage/ -tags stress -run TestStress -v -timeout 30m -count=1

fault-test:
	go test ./user/ -tags stress -run TestFault -v -timeout 10m -count=1

gateway:
	go build -ldflags "-X main.Version=$$(git describe --tags --always 2>/dev/null || echo dev)" -o bin/gateway ./cmd/gateway

gateway-test:
	go test ./cmd/gateway/... -race -count=1
