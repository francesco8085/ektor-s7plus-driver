# EktorS7PlusDriver

**.NET 4.8 communication driver for Siemens S7-1200 and S7-1500 PLCs via S7CommPlus protocol.**

Built for the [EKTOR Industrial IoT Platform](https://entersrl.it) — a production-grade system for PLC connectivity, alarm management, and industrial data acquisition.

---

## Features

- **Connection pool** with health-check and auto-reconnect
- **Variable browse** with cache (full DB/tag tree)
- **Single and batch read/write** with timeout and thread safety
- **Block operations** — list blocks, read MC7 bytecode, upload OB/FB/FC/DB
- **LAD/FBD decompiler** — human-readable representation of MC7 code
- **MC7 annotator** — annotates raw bytecode with instruction names
- **S7 raw client** — SZL reads and block upload via classic S7comm (port 102, no TLS)
- **Priority lock** — prevents concurrent access on non-thread-safe native connections
- Handles `AccessViolationException` and SEH crashes from the native driver without crashing the host process

## Requirements

- .NET Framework 4.8 (Windows)
- x64 platform
- **S7CommPlusDriver** — external dependency, must be obtained and built separately
  - Place `S7CommPlusDriver.dll` and `zlib.net.dll` in the path referenced by the `.csproj`
  - OpenSSL DLLs (`libssl-3-x64.dll`, `libcrypto-3-x64.dll`) required for TLS connections

## Build

```bash
dotnet build EktorS7PlusDriver.csproj --configuration Release
```

## Quick Start

```csharp
using EnterSrl.Ektor.S7Plus;

// 1. Configure the PLC
var config = new S7PlusConfig
{
    Id       = "plc-01",
    Name     = "Line 1 Controller",
    Ip       = "192.168.1.10",
    Port     = 102,
    UseTls   = false,
    Timeout  = 30000,
    Enabled  = true
};

// 2. Start connection manager
var manager = new S7PlusConnectionManager();
await manager.ConnectAsync(config);

// 3. Read/Write via service
var service = new S7PlusService(manager);

// Browse all variables
var vars = await service.BrowseAsync("plc-01");

// Read a variable by name
var value = await service.ReadVariableAsync("plc-01", "DB1.Setpoint_Temp");

// Write a value
await service.WriteVariableAsync("plc-01", "DB1.Setpoint_Temp", 85.5);

// Batch read
var results = await service.ReadBatchAsync("plc-01", new[] {
    "DB1.Setpoint_Temp",
    "DB1.ActualPressure",
    "DB2.MotorRunning"
});
```

## Project Structure

| File | Description |
|------|-------------|
| `S7PlusConfig.cs` | Configuration POCO (IP, port, TLS, timeout, device profile) |
| `S7PlusConnectionManager.cs` | Connection pool, health check, auto-reconnect, browse cache |
| `S7PlusService.cs` | High-level operations: browse, read single/batch, write |
| `S7PlusApiHandler.cs` | HTTP-agnostic request handler (JSON in/out) |
| `S7PlusBlockOperations.cs` | Block list, MC7 upload, block info |
| `S7RawClient.cs` | S7comm classic client — SZL reads, block upload (port 102) |
| `LadFbdDecompiler.cs` | LAD/FBD decompiler from MC7 bytecode |
| `Mc7Annotator.cs` | MC7 bytecode annotator |
| `PriorityLock.cs` | Priority-aware mutex for non-thread-safe native connections |

## Supported PLC Models

| Model | Protocol | Notes |
|-------|----------|-------|
| S7-1500 (all variants) | S7CommPlus | Full browse + read/write |
| S7-1200 (FW >= 4.x) | S7CommPlus | Full browse + read/write |
| ET 200SP CPU 1512SP | S7CommPlus | Supported via device profile |
| S7-300 / S7-400 | S7 classic (raw) | SZL + block upload only |

## Author

**Francesco Cesarone**
f.cesarone@entersrl.it
Enter SRL — Italy

## License

MIT — see [LICENSE](LICENSE)

---

## Disclaimer

> **This project is not affiliated with, endorsed by, or connected to Siemens AG in any way.**
>
> The S7CommPlus communication protocol is implemented based on publicly available
> community research. Use of this driver to communicate with Siemens PLCs is entirely
> at your own risk.
>
> **This software is provided "AS IS", without warranty of any kind.**
> The author and contributors are not liable for any damage to equipment,
> production systems, or data arising from the use of this software.
>
> **Do not use in safety-critical systems** (SIL, functional safety, life support)
> without your own independent validation.
>
> All Siemens product names and trademarks are property of Siemens AG.
