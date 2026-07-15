Start-Process powershell -ArgumentList '-NoExit', '-Command', '. .\master.ps1; go run ./cmd/master'
Start-Process powershell -ArgumentList '-NoExit', '-Command', '. .\envsc1.ps1; go run ./cmd/node'
Start-Process powershell -ArgumentList '-NoExit', '-Command', '. .\envsc2.ps1; go run ./cmd/node'
Start-Process powershell -ArgumentList '-NoExit', '-Command', '. .\envsc3.ps1; go run ./cmd/node'