clear
go env -w GOOS=darwin
go env -w GOARCH=arm64
go build
./producer