// =============================================================================
// EktorS7PlusDriver — S7CommPlus Communication Driver for Siemens S7-1200/1500
// =============================================================================
// Copyright (c) 2025-2026 Francesco Cesarone <f.cesarone@entersrl.it>
// Azienda   : Enter SRL
// Progetto  : EKTOR Industrial IoT Platform
// Licenza   : Proprietaria — uso riservato Enter SRL
// =============================================================================

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using S7CommPlusDriver;
using S7CommPlusDriver.ClientApi;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Holds runtime state for a single S7CommPlus device connection.
    /// </summary>
    public class S7PlusConnectionState
    {
        public S7CommPlusConnection Connection { get; set; }
        public S7PlusConfig Config { get; set; }
        public bool IsConnected { get; set; }

        // Cumulative statistics
        public long TotalReads { get; set; }
        public long TotalWrites { get; set; }
        public long TotalErrors { get; set; }

        public string LastError { get; set; } = string.Empty;
        public DateTime ConnectedAt { get; set; }
        public DateTime LastActivity { get; set; }
        public int ConsecutiveHealthFailures { get; set; }

        // Browse cache
        public List<VarInfo> CachedVarInfoList { get; set; } = new List<VarInfo>();
        public Dictionary<string, VarInfo> CachedVarInfoByName { get; set; } = new Dictionary<string, VarInfo>(StringComparer.OrdinalIgnoreCase);
        public bool BrowseCacheValid { get; set; }
        public DateTime LastBrowseAttemptUtc { get; set; }
        public DateTime LastBrowseSuccessUtc { get; set; }
        public DateTime LastBrowseFailureUtc { get; set; }
        public int ConsecutiveBrowseFailures { get; set; }
        public string LastBrowseError { get; set; } = string.Empty;

        // Block list cache
        public List<S7CommPlusConnection.BlockInfo> CachedBlockList { get; set; }
        public DateTime LastBlockListSuccessUtc { get; set; }
        public DateTime LastBlockListFailureUtc { get; set; }
        public int ConsecutiveBlockListFailures { get; set; }
        public string LastBlockListError { get; set; } = string.Empty;

        // Deduplica richieste GetAllBlocks concorrenti sulla stessa CPU
        public object BlockTaskSync { get; } = new object();
        public Task<(bool success, string? error, List<object>? blocks)> PendingBlockTask { get; set; }

        // Block snapshot cache for diff operations
        public ConcurrentDictionary<string, List<BlockSnapshot>> BlockSnapshots { get; }
            = new ConcurrentDictionary<string, List<BlockSnapshot>>(StringComparer.OrdinalIgnoreCase);

        // Deduplica browse concorrenti sulla stessa CPU
        public object BrowseTaskSync { get; } = new object();
        public Task<(bool success, string? error, List<object>? variables)> PendingBrowseTask { get; set; }

        // S7 Raw Classic client (parallel connection for SZL + block upload)
        public S7RawClient RawClient { get; set; }

        // Priority lock per connection: S7CommPlusConnection is NOT thread-safe
        // HIGH priority: reads/writes (fast, <5s timeout)
        // LOW priority: browse/blocks/explore (slow, up to 30s timeout)
        public PriorityLock PrioLock { get; } = new PriorityLock();

        // Circuit breaker for per-block operations (InterfaceXml, BodyXml)
        // If these fail/timeout on a PLC, don't retry for CooldownMinutes
        public int InterfaceXmlFailures { get; set; }
        public DateTime InterfaceXmlLastFailUtc { get; set; }
        public bool InterfaceXmlDisabled =>
            InterfaceXmlFailures >= 2 &&
            DateTime.UtcNow - InterfaceXmlLastFailUtc < TimeSpan.FromMinutes(10);

        // Backward compat — old code that uses state.Lock still works but should migrate
        [System.Obsolete("Use PrioLock.WaitHighAsync/WaitLowAsync instead")]
        public SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);
    }

    /// <summary>
    /// Result object returned by connection operations (avoids throwing exceptions).
    /// </summary>
    public class S7PlusResult
    {
        public bool Success { get; set; }
        public string Error { get; set; } = string.Empty;

        public static S7PlusResult Ok() => new S7PlusResult { Success = true };
        public static S7PlusResult Fail(string error) => new S7PlusResult { Success = false, Error = error };
    }

    /// <summary>
    /// Snapshot of a connection's current status — safe to serialize / return via HTTP.
    /// </summary>
    public class S7PlusConnectionStatus
    {
        public string DeviceId { get; set; }
        public string Name { get; set; }
        public string Ip { get; set; }
        public bool IsConnected { get; set; }
        public long TotalReads { get; set; }
        public long TotalWrites { get; set; }
        public long TotalErrors { get; set; }
        public string LastError { get; set; }
        public DateTime ConnectedAt { get; set; }
        public DateTime LastActivity { get; set; }
        public int CachedVariables { get; set; }
        public int ConsecutiveConnectFailures { get; set; }
        public bool PreferClassicFallback { get; set; }
        public string FallbackReason { get; set; }
        public DateTime? PreferClassicFallbackUntilUtc { get; set; }
        public string LastConnectTrace { get; set; }
        public string LastCreateObjectRequestSummary { get; set; }
        public string LastCreateObjectRequestHex { get; set; }
        public string LastCreateObjectMatrixSummary { get; set; }
        public string ModuleTypeName { get; set; }
        public string SerialNumber { get; set; }
        public string PlantId { get; set; }
        public string Copyright { get; set; }
        public string ModuleName { get; set; }
        public string OrderCode { get; set; }
        public string MemoryCardOrderCode { get; set; }
        public string FirmwareVersion { get; set; }
        public string BootLoaderVersion { get; set; }
        public DateTime? CpuInfoUpdatedAtUtc { get; set; }
        public string DeviceProfileKey { get; set; }
        public string DeviceProfileLabel { get; set; }
        public string ConnectStrategyHint { get; set; }
    }

    internal sealed class S7PlusDeviceRuntime
    {
        public S7PlusConfig Config { get; set; }
        public DateTime LastConnectAttemptUtc { get; set; }
        public DateTime LastConnectFailureUtc { get; set; }
        public int ConsecutiveConnectFailures { get; set; }
        public string LastConnectError { get; set; } = string.Empty;
        public DateTime? PreferClassicFallbackUntilUtc { get; set; }
        public List<S7CommPlusConnection.BlockInfo> LastGoodBlockList { get; set; }
        public DateTime LastGoodBlockListUtc { get; set; }
        public string LastConnectTrace { get; set; } = string.Empty;
        public string LastCreateObjectRequestSummary { get; set; } = string.Empty;
        public string LastCreateObjectRequestHex { get; set; } = string.Empty;
        public string LastCreateObjectMatrixSummary { get; set; } = string.Empty;
        public string ModuleTypeName { get; set; } = string.Empty;
        public string SerialNumber { get; set; } = string.Empty;
        public string PlantId { get; set; } = string.Empty;
        public string Copyright { get; set; } = string.Empty;
        public string ModuleName { get; set; } = string.Empty;
        public string OrderCode { get; set; } = string.Empty;
        public string MemoryCardOrderCode { get; set; } = string.Empty;
        public string FirmwareVersion { get; set; } = string.Empty;
        public string BootLoaderVersion { get; set; } = string.Empty;
        public DateTime? CpuInfoUpdatedAtUtc { get; set; }
        public string DeviceProfileKey { get; set; } = string.Empty;
        public string DeviceProfileLabel { get; set; } = string.Empty;
        public string ConnectStrategyHint { get; set; } = string.Empty;
    }

    /// <summary>
    /// Manages the lifecycle of S7CommPlus connections.
    /// All public methods are safe to call from multiple threads.
    /// Each connection is protected by its own SemaphoreSlim(1,1) because
    /// S7CommPlusConnection itself is not thread-safe.
    /// </summary>
    public class S7PlusConnectionManager : IDisposable
    {
        private readonly ConcurrentDictionary<string, S7PlusConnectionState> _connections =
            new ConcurrentDictionary<string, S7PlusConnectionState>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, S7PlusDeviceRuntime> _deviceRuntime =
            new ConcurrentDictionary<string, S7PlusDeviceRuntime>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, Lazy<Task<S7PlusResult>>> _pendingConnects =
            new ConcurrentDictionary<string, Lazy<Task<S7PlusResult>>>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, string> _activeEndpoints =
            new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        private readonly Timer _healthCheckTimer;
        private const int HealthCheckIntervalMs = 15_000;
        private bool _disposed;

        public S7PlusConnectionManager()
        {
            _healthCheckTimer = new Timer(HealthCheckCallback, null, HealthCheckIntervalMs, HealthCheckIntervalMs);
        }

        // -------------------------------------------------------------------------
        // Public API
        // -------------------------------------------------------------------------

        /// <summary>
        /// Establishes a connection to the PLC described by <paramref name="config"/>.
        /// The blocking S7CommPlusConnection.Connect() call is wrapped in Task.Run so
        /// the caller's thread is not blocked.
        /// </summary>
        public async Task<S7PlusResult> ConnectAsync(S7PlusConfig config)
        {
            if (config == null)
                return S7PlusResult.Fail("Config is null.");

            if (string.IsNullOrWhiteSpace(config.Id))
                return S7PlusResult.Fail("Config.Id must not be empty.");

            if (string.IsNullOrWhiteSpace(config.Ip))
                return S7PlusResult.Fail("Config.Ip must not be empty.");

            config = NormalizeConfig(config);

            var pendingConnect = _pendingConnects.GetOrAdd(
                config.Id,
                _ => new Lazy<Task<S7PlusResult>>(() => ConnectCoreAsync(config), LazyThreadSafetyMode.ExecutionAndPublication));

            try
            {
                return await pendingConnect.Value;
            }
            finally
            {
                _pendingConnects.TryRemove(config.Id, out _);
            }
        }

        /// <summary>
        /// Disconnects and removes the connection identified by <paramref name="deviceId"/>.
        /// </summary>
        private async Task<S7PlusResult> ConnectCoreAsync(S7PlusConfig config)
        {
            string endpointKey = BuildEndpointKey(config.Ip, config.Port);
            var runtime = _deviceRuntime.AddOrUpdate(
                config.Id,
                _ => new S7PlusDeviceRuntime { Config = config },
                (_, existingRuntime) =>
                {
                    existingRuntime.Config = config;
                    return existingRuntime;
                });

            runtime.LastConnectAttemptUtc = DateTime.UtcNow;

            // Se il config specifica un profilo esplicito, usalo subito — prima ancora
            // che TryRefreshDeviceIdentityAsync sovrascriva il profilo con il rilevamento automatico.
            if (!string.IsNullOrWhiteSpace(config.DeviceProfileKey) &&
                !string.Equals(runtime.DeviceProfileKey, config.DeviceProfileKey, StringComparison.OrdinalIgnoreCase))
            {
                runtime.DeviceProfileKey = config.DeviceProfileKey;
                runtime.ConnectStrategyHint = $"Profilo forzato da configurazione: {config.DeviceProfileKey}";
                Console.WriteLine($"[S7PlusConnectionManager] Device '{config.Id}': profilo forzato a '{config.DeviceProfileKey}'.");
            }

            await TryRefreshDeviceIdentityAsync(config, runtime);

            if (IsClassicFallbackPreferred(runtime))
            {
                double remainingSeconds = runtime.PreferClassicFallbackUntilUtc.HasValue
                    ? Math.Max(1, Math.Ceiling((runtime.PreferClassicFallbackUntilUtc.Value - DateTime.UtcNow).TotalSeconds))
                    : 0;
                string reason = string.IsNullOrWhiteSpace(runtime.LastConnectError)
                    ? "timeout ripetuti su S7+"
                    : runtime.LastConnectError;
                string cooledDownMessage = $"S7+ temporaneamente in cooldown per questo device ({remainingSeconds:0}s). Usa PLC S7 fallback. Ultimo errore: {reason}";
                Console.WriteLine($"[S7PlusConnectionManager] {cooledDownMessage}");
                return S7PlusResult.Fail(cooledDownMessage);
            }

            if (_connections.TryGetValue(config.Id, out var existing))
            {
                if (existing.IsConnected)
                {
                    Console.WriteLine($"[S7PlusConnectionManager] Device '{config.Id}' is already connected.");
                    ClearConnectFailures(runtime);
                    return S7PlusResult.Ok();
                }

                await DisconnectAsync(config.Id);
            }

            if (!_activeEndpoints.TryAdd(endpointKey, config.Id))
            {
                _activeEndpoints.TryGetValue(endpointKey, out var activeDeviceId);
                string activeId = string.IsNullOrWhiteSpace(activeDeviceId) ? "unknown" : activeDeviceId;
                string message = $"Connection attempt already in progress for {config.Ip}:{config.Port} (device '{activeId}').";
                Console.WriteLine($"[S7PlusConnectionManager] {message}");
                return S7PlusResult.Fail(message);
            }

            Console.WriteLine($"[S7PlusConnectionManager] Connecting to '{config.Id}' at {config.Ip}...");

            var state = new S7PlusConnectionState
            {
                Config = config,
                Connection = new S7CommPlusConnection(),
                IsConnected = false,
            };

            if (runtime.LastGoodBlockList != null && runtime.LastGoodBlockList.Count > 0)
            {
                state.CachedBlockList = new List<S7CommPlusConnection.BlockInfo>(runtime.LastGoodBlockList);
                state.LastBlockListSuccessUtc = runtime.LastGoodBlockListUtc;
            }

            try
            {
                var connectAttempt = await TryConnectWithProfilesAsync(state, config, config.Timeout, GetHardConnectTimeoutMs(config));

                if (!connectAttempt.success && IsTimeoutLikeConnectFailure(connectAttempt.error))
                {
                    Console.WriteLine($"[S7PlusConnectionManager] Timeout-like connect failure for '{config.Id}', probing endpoint before retry...");
                    var endpointProbe = await ProbeEndpointAsync(config.Ip, config.Port, config.Timeout);
                    if (endpointProbe.Success)
                    {
                        try
                        {
                            state.Connection.Disconnect();
                        }
                        catch
                        {
                            // Ignore cleanup errors between attempts.
                        }

                        state.Connection = new S7CommPlusConnection();
                        await Task.Delay(750);

                        int retryDriverTimeoutMs = Math.Max(config.Timeout, 15000);
                        int retryHardTimeoutMs = Math.Max(GetHardConnectTimeoutMs(config), 25000);
                        Console.WriteLine($"[S7PlusConnectionManager] Retrying S7+ connect to '{config.Id}' with extended timeout ({retryDriverTimeoutMs} ms)...");
                        connectAttempt = await TryConnectWithProfilesAsync(state, config, retryDriverTimeoutMs, retryHardTimeoutMs);

                        if (!connectAttempt.success &&
                            IsTimeoutLikeConnectFailure(connectAttempt.error) &&
                            ShouldUseExperimentalEt1500Profile(config, runtime))
                        {
                            Console.WriteLine($"[S7PlusConnectionManager] Experimental ET1500 connect profile for '{config.Id}'...");

                            // Warm up the endpoint with a plain TCP open/close, then retry with a fresh object.
                            var warmProbe = await ProbeEndpointAsync(config.Ip, config.Port, retryDriverTimeoutMs);
                            Console.WriteLine($"[S7PlusConnectionManager] Experimental warm probe for '{config.Id}': {(warmProbe.Success ? "ok" : warmProbe.Error)}");

                            try
                            {
                                state.Connection.Disconnect();
                            }
                            catch
                            {
                                // Ignore cleanup errors between attempts.
                            }

                            state.Connection = new S7CommPlusConnection();
                            await Task.Delay(1500);

                            int experimentalDriverTimeoutMs = Math.Max(retryDriverTimeoutMs, 20000);
                            int experimentalHardTimeoutMs = Math.Max(retryHardTimeoutMs, 32000);
                            Console.WriteLine($"[S7PlusConnectionManager] Experimental retry S7+ connect to '{config.Id}' ({experimentalDriverTimeoutMs} ms / {experimentalHardTimeoutMs} ms)...");
                            connectAttempt = await TryConnectWithProfilesAsync(state, config, experimentalDriverTimeoutMs, experimentalHardTimeoutMs);

                            if (!connectAttempt.success &&
                                IsTimeoutLikeConnectFailure(connectAttempt.error) &&
                                ShouldUseExperimentalS71500DualStreamProfile(config, runtime))
                            {
                                Console.WriteLine($"[S7PlusConnectionManager] Experimental TIA-like dual ES bootstrap for '{config.Id}'...");
                                connectAttempt = await TryConnectWithDualEsBootstrapAsync(state, config, experimentalDriverTimeoutMs, experimentalHardTimeoutMs);
                            }

                            if (!connectAttempt.success &&
                                IsTimeoutLikeConnectFailure(connectAttempt.error))
                            {
                                Console.WriteLine($"[S7PlusConnectionManager] Experimental TIA-like companion channel for '{config.Id}'...");
                                connectAttempt = await TryConnectWithEsCompanionAsync(state, config, experimentalDriverTimeoutMs, experimentalHardTimeoutMs);
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[S7PlusConnectionManager] Endpoint probe failed for '{config.Id}': {endpointProbe.Error}");
                    }
                }

                if (!connectAttempt.success)
                {
                    await TryRefreshDeviceIdentityAsync(config, runtime);
                    state.LastError = RegisterConnectFailure(runtime, connectAttempt.error);
                    Console.WriteLine($"[S7PlusConnectionManager] {state.LastError} for device '{config.Id}'.");

                    // Diagnostica specifica per profili noti con problemi tipici
                    if (!string.IsNullOrWhiteSpace(runtime.ConnectStrategyHint) &&
                        IsTimeoutLikeConnectFailure(connectAttempt.error))
                    {
                        Console.WriteLine($"[S7PlusConnectionManager] HINT per '{config.Id}' ({runtime.DeviceProfileLabel}): {runtime.ConnectStrategyHint}");
                    }

                    return S7PlusResult.Fail(state.LastError);
                }

                state.IsConnected = true;
                state.ConnectedAt = DateTime.UtcNow;
                state.LastActivity = DateTime.UtcNow;
                state.LastError = string.Empty;

                ClearConnectFailures(runtime);
                _connections[config.Id] = state;
                await TryRefreshDeviceIdentityAsync(config, runtime);

                Console.WriteLine($"[S7PlusConnectionManager] Device '{config.Id}' connected successfully.");
                return S7PlusResult.Ok();
            }
            finally
            {
                _activeEndpoints.TryRemove(endpointKey, out _);
            }
        }

        private static bool IsTimeoutLikeConnectFailure(string error)
        {
            if (string.IsNullOrWhiteSpace(error))
                return false;

            string normalized = error.ToLowerInvariant();
            return normalized.Contains("code 5") ||
                   normalized.Contains("hard timeout") ||
                   normalized.Contains("did not respond") ||
                   normalized.Contains("timeout");
        }

        private static bool ShouldUseExperimentalEt1500Profile(S7PlusConfig config, S7PlusDeviceRuntime runtime)
        {
            if (config == null && runtime == null)
                return false;

            if (runtime != null && string.Equals(runtime.DeviceProfileKey, "et200sp-1512-fw3x", StringComparison.OrdinalIgnoreCase))
                return true;

            string orderCode = runtime?.OrderCode ?? string.Empty;
            string moduleType = runtime?.ModuleTypeName ?? string.Empty;
            string firmware = runtime?.FirmwareVersion ?? string.Empty;

            return ContainsIgnoreCase(orderCode, "6ES7 512-") ||
                   ContainsIgnoreCase(moduleType, "ET 200SP") ||
                   (ContainsIgnoreCase(orderCode, "6ES7 512-") && ContainsIgnoreCase(firmware, "V3."));
        }

        private static bool ShouldUseExperimentalS71500DualStreamProfile(S7PlusConfig config, S7PlusDeviceRuntime runtime)
        {
            if (config == null && runtime == null)
                return false;

            if (runtime != null && string.Equals(runtime.DeviceProfileKey, "s7-1500-1511-fw29", StringComparison.OrdinalIgnoreCase))
                return true;

            string orderCode = runtime?.OrderCode ?? string.Empty;
            string moduleType = runtime?.ModuleTypeName ?? string.Empty;

            return ContainsIgnoreCase(orderCode, "6ES7 511-") ||
                   ContainsIgnoreCase(moduleType, "S7-1500");
        }

        private static IEnumerable<string> GetRemoteTsapCandidates(S7PlusConfig config, S7PlusDeviceRuntime runtime, int driverTimeoutMs)
        {
            yield return "SIMATIC-ROOT-HMI";

            if (ShouldUseExperimentalEt1500Profile(config, runtime) && driverTimeoutMs >= 20000)
                yield return "SIMATIC-ROOT-ES";
        }

        private static IReadOnlyList<string> GetCreateObjectProfileCandidates(S7PlusConfig config, S7PlusDeviceRuntime runtime, int driverTimeoutMs)
        {
            if (ShouldUseExperimentalEt1500Profile(config, runtime) && driverTimeoutMs >= 20000)
            {
                return new[]
                {
                    "full-v1",
                    "minimal-v1",
                };
            }

            return new[] { string.Empty };
        }

        private async Task<(bool success, string error)> TryConnectWithEsCompanionAsync(
            S7PlusConnectionState state,
            S7PlusConfig config,
            int driverTimeoutMs,
            int hardTimeoutMs)
        {
            var companion = new S7CommPlusConnection();
            var companionTrace = new List<string>
            {
                $"target={config.Ip}:{config.Port}",
                "mode=tia-like-companion",
                "companionRemoteTsap=SIMATIC-ROOT-ES",
            };

            try
            {
                // Keep the ES channel alive briefly while the main HMI path attempts session creation.
                var companionTask = Task.Run(() =>
                    companion.Connect(config.Ip, config.Password, "", Math.Min(driverTimeoutMs, 10000), "SIMATIC-ROOT-ES", "full-v1"));

                await Task.Delay(300);

                var mainAttempt = await TryConnectOnceAsync(state, config, driverTimeoutMs, hardTimeoutMs, "SIMATIC-ROOT-HMI", "full-v1");
                companionTrace.Add(mainAttempt.success ? "main=hmi-full-v1:ok" : $"main=hmi-full-v1:{mainAttempt.error}");

                if (!mainAttempt.success && IsTimeoutLikeConnectFailure(mainAttempt.error))
                {
                    try
                    {
                        state.Connection.Disconnect();
                    }
                    catch
                    {
                        // Ignore cleanup errors between attempts.
                    }

                    state.Connection = new S7CommPlusConnection();
                    await Task.Delay(200);

                    mainAttempt = await TryConnectOnceAsync(state, config, driverTimeoutMs, hardTimeoutMs, "SIMATIC-ROOT-HMI", "minimal-v1");
                    companionTrace.Add(mainAttempt.success ? "main=hmi-minimal-v1:ok" : $"main=hmi-minimal-v1:{mainAttempt.error}");
                }

                var completed = await Task.WhenAny(companionTask, Task.Delay(Math.Min(hardTimeoutMs, 12000)));
                if (completed == companionTask)
                {
                    int companionResult = await companionTask;
                    companionTrace.Add($"companionResult={companionResult}");
                    companionTrace.Add("companionDriverTrace=" + (companion.LastConnectDebugTrace ?? string.Empty));
                    companionTrace.Add("companionDriverRx=" + (companion.LastReceiveDebugTrace ?? string.Empty));
                    if (!string.IsNullOrWhiteSpace(companion.LastCreateObjectRequestSummary))
                        companionTrace.Add("companionCreateObject=" + companion.LastCreateObjectRequestSummary);
                }
                else
                {
                    companionTrace.Add("companionResult=timeout");
                    companionTrace.Add("companionDriverTrace=" + (companion.LastConnectDebugTrace ?? string.Empty));
                    companionTrace.Add("companionDriverRx=" + (companion.LastReceiveDebugTrace ?? string.Empty));
                }

                _deviceRuntime.TryGetValue(config.Id, out var runtime);
                if (runtime != null)
                {
                    runtime.LastCreateObjectMatrixSummary = string.Join(" || ", companionTrace);
                }

                return mainAttempt;
            }
            finally
            {
                try
                {
                    companion.Disconnect();
                }
                catch
                {
                    // Ignore cleanup errors for the companion channel.
                }
            }
        }

        private async Task<(bool success, string error)> TryConnectWithDualEsBootstrapAsync(
            S7PlusConnectionState state,
            S7PlusConfig config,
            int driverTimeoutMs,
            int hardTimeoutMs)
        {
            var companion = new S7CommPlusConnection();
            var trace = new List<string>
            {
                $"target={config.Ip}:{config.Port}",
                "mode=tia-like-dual-es",
                "companionRemoteTsap=SIMATIC-ROOT-ES",
                "mainRemoteTsap=SIMATIC-ROOT-ES",
            };

            try
            {
                var companionTask = Task.Run(() =>
                    companion.Connect(config.Ip, config.Password, "", Math.Min(driverTimeoutMs, 10000), "SIMATIC-ROOT-ES", "full-v1"));

                await Task.Delay(180);

                var mainAttempt = await TryConnectOnceAsync(state, config, driverTimeoutMs, hardTimeoutMs, "SIMATIC-ROOT-ES", "full-v1");
                trace.Add(mainAttempt.success ? "main=es-full-v1:ok" : $"main=es-full-v1:{mainAttempt.error}");

                if (!mainAttempt.success && IsTimeoutLikeConnectFailure(mainAttempt.error))
                {
                    try
                    {
                        state.Connection.Disconnect();
                    }
                    catch
                    {
                        // Ignore cleanup errors between attempts.
                    }

                    state.Connection = new S7CommPlusConnection();
                    await Task.Delay(180);

                    mainAttempt = await TryConnectOnceAsync(state, config, driverTimeoutMs, hardTimeoutMs, "SIMATIC-ROOT-ES", "minimal-v1");
                    trace.Add(mainAttempt.success ? "main=es-minimal-v1:ok" : $"main=es-minimal-v1:{mainAttempt.error}");
                }

                var completed = await Task.WhenAny(companionTask, Task.Delay(Math.Min(hardTimeoutMs, 12000)));
                if (completed == companionTask)
                {
                    int companionResult = await companionTask;
                    trace.Add($"companionResult={companionResult}");
                    trace.Add("companionDriverTrace=" + (companion.LastConnectDebugTrace ?? string.Empty));
                    trace.Add("companionDriverRx=" + (companion.LastReceiveDebugTrace ?? string.Empty));
                    if (!string.IsNullOrWhiteSpace(companion.LastCreateObjectRequestSummary))
                        trace.Add("companionCreateObject=" + companion.LastCreateObjectRequestSummary);
                }
                else
                {
                    trace.Add("companionResult=timeout");
                    trace.Add("companionDriverTrace=" + (companion.LastConnectDebugTrace ?? string.Empty));
                    trace.Add("companionDriverRx=" + (companion.LastReceiveDebugTrace ?? string.Empty));
                }

                _deviceRuntime.TryGetValue(config.Id, out var runtime);
                if (runtime != null)
                    runtime.LastCreateObjectMatrixSummary = string.Join(" || ", trace);

                return mainAttempt;
            }
            finally
            {
                try
                {
                    companion.Disconnect();
                }
                catch
                {
                    // Ignore cleanup errors for the companion channel.
                }
            }
        }

        private async Task<(bool success, string error)> TryConnectOnceAsync(
            S7PlusConnectionState state,
            S7PlusConfig config,
            int driverTimeoutMs,
            int hardTimeoutMs,
            string remoteTsap,
            string createObjectProfile = null)
        {
            var trace = new List<string>
            {
                $"target={config.Ip}:{config.Port}",
                $"driverTimeoutMs={driverTimeoutMs}",
                $"hardTimeoutMs={hardTimeoutMs}",
                $"remoteTsap={remoteTsap}",
            };
            if (!string.IsNullOrWhiteSpace(createObjectProfile))
                trace.Add($"createObjectProfile={createObjectProfile}");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            _deviceRuntime.TryGetValue(config.Id, out var runtime);

            try
            {
                trace.Add("connect:start");
                var connectTask = Task.Run(() =>
                    state.Connection.Connect(config.Ip, config.Password, "", driverTimeoutMs, remoteTsap, createObjectProfile));
                var completedTask = await Task.WhenAny(connectTask, Task.Delay(hardTimeoutMs));

                if (completedTask != connectTask)
                {
                    trace.Add($"connect:hard-timeout elapsedMs={sw.ElapsedMilliseconds}");
                    try
                    {
                        state.Connection.Disconnect();
                        trace.Add("cleanup:disconnect-ok");
                    }
                    catch
                    {
                        // Ignore cleanup errors after timeout.
                        trace.Add("cleanup:disconnect-failed");
                    }

                    if (runtime != null)
                        CaptureDriverDebugSnapshot(runtime, state.Connection, trace);
                    Console.WriteLine($"[S7PlusConnectionManager] Connect trace '{config.Id}': {runtime?.LastConnectTrace}");
                    return (false, $"PLC {config.Ip}:{config.Port} did not respond within hard timeout.");
                }

                int res = await connectTask;
                trace.Add($"connect:completed elapsedMs={sw.ElapsedMilliseconds}");
                trace.Add($"connect:result={res}");
                if (res == 0)
                {
                    long sessionId = 0;
                    try
                    {
                        sessionId = state.Connection.SessionId2;
                        trace.Add($"sessionId=0x{sessionId:X}");
                    }
                    catch
                    {
                        trace.Add("sessionId=unavailable");
                    }

                    string invalidSessionReason = GetInvalidSessionReason(state.Connection, sessionId);
                    if (!string.IsNullOrWhiteSpace(invalidSessionReason))
                    {
                        trace.Add($"connect:invalid-session={invalidSessionReason}");
                        try
                        {
                            state.Connection.Disconnect();
                            trace.Add("cleanup:disconnect-ok");
                        }
                        catch
                        {
                            trace.Add("cleanup:disconnect-failed");
                        }

                        if (runtime != null)
                            CaptureDriverDebugSnapshot(runtime, state.Connection, trace);
                        Console.WriteLine($"[S7PlusConnectionManager] Connect trace '{config.Id}': {runtime?.LastConnectTrace}");
                        return (false, $"S7+ sessione non valida ({invalidSessionReason}).");
                    }

                    if (runtime != null)
                        CaptureDriverDebugSnapshot(runtime, state.Connection, trace);
                    Console.WriteLine($"[S7PlusConnectionManager] Connect trace '{config.Id}': {runtime?.LastConnectTrace}");
                    return (true, string.Empty);
                }

                string err = res == 5
                    ? $"PLC {config.Ip}:{config.Port} did not respond within timeout (code 5)."
                    : $"Connect returned error code {res}";

                if (runtime != null)
                    CaptureDriverDebugSnapshot(runtime, state.Connection, trace);
                Console.WriteLine($"[S7PlusConnectionManager] Connect trace '{config.Id}': {runtime?.LastConnectTrace}");
                return (false, err);
            }
            catch (Exception ex)
            {
                trace.Add($"connect:exception={ex.GetType().Name}");
                trace.Add($"elapsedMs={sw.ElapsedMilliseconds}");
                if (runtime != null)
                    CaptureDriverDebugSnapshot(runtime, state.Connection, trace);
                Console.WriteLine($"[S7PlusConnectionManager] Connect trace '{config.Id}': {runtime?.LastConnectTrace}");
                return (false, $"Connect exception: {ex.Message}");
            }
        }

        private static string GetInvalidSessionReason(S7CommPlusConnection connection, long sessionId)
        {
            if (connection == null)
                return "connection-null";

            if (sessionId == 0)
                return "sessionId=0";

            string driverTrace = connection.LastConnectDebugTrace ?? string.Empty;
            if (driverTrace.IndexOf("deserialize=null", StringComparison.OrdinalIgnoreCase) >= 0)
                return "initssl-deserialize-null";

            if (driverTrace.IndexOf("connect:success", StringComparison.OrdinalIgnoreCase) < 0)
                return "driver-trace-without-success";

            return string.Empty;
        }

        private async Task<(bool success, string error)> TryConnectWithProfilesAsync(
            S7PlusConnectionState state,
            S7PlusConfig config,
            int driverTimeoutMs,
            int hardTimeoutMs)
        {
            (bool success, string error) lastAttempt = (false, "No TSAP profiles available.");
            _deviceRuntime.TryGetValue(config.Id, out var runtime);

            if (ShouldUseExperimentalS71500DualStreamProfile(config, runtime) && driverTimeoutMs >= 20000)
            {
                lastAttempt = await TryConnectWithDualEsBootstrapAsync(state, config, driverTimeoutMs, hardTimeoutMs);
                if (lastAttempt.success)
                    return lastAttempt;

                try
                {
                    state.Connection.Disconnect();
                }
                catch
                {
                    // Ignore cleanup errors before falling back to the stable path.
                }

                state.Connection = new S7CommPlusConnection();
                await Task.Delay(250);

                if (runtime != null && string.IsNullOrWhiteSpace(runtime.LastCreateObjectMatrixSummary))
                    runtime.LastCreateObjectMatrixSummary = "tia-like-dual-es:fallback-to-stable";
            }

            foreach (var remoteTsap in GetRemoteTsapCandidates(config, runtime, driverTimeoutMs))
            {
                var createObjectProfiles = GetCreateObjectProfileCandidates(config, runtime, driverTimeoutMs);
                bool isMatrix = createObjectProfiles.Count > 1;
                var matrixSummary = new List<string>();

                foreach (var createObjectProfile in createObjectProfiles)
                {
                    lastAttempt = await TryConnectOnceAsync(state, config, driverTimeoutMs, hardTimeoutMs, remoteTsap, createObjectProfile);
                    string profileLabel = string.IsNullOrWhiteSpace(createObjectProfile) ? "default" : createObjectProfile;
                    matrixSummary.Add($"{remoteTsap}/{profileLabel}:{(lastAttempt.success ? "ok" : lastAttempt.error)}");

                    if (runtime != null)
                        runtime.LastCreateObjectMatrixSummary = string.Join(" || ", matrixSummary);

                    if (lastAttempt.success)
                        return lastAttempt;

                    if (!isMatrix && !IsTimeoutLikeConnectFailure(lastAttempt.error))
                        return lastAttempt;

                    try
                    {
                        state.Connection.Disconnect();
                    }
                    catch
                    {
                        // Ignore cleanup errors between attempts.
                    }

                    state.Connection = new S7CommPlusConnection();
                    await Task.Delay(isMatrix ? 100 : 250);
                }

                if (!isMatrix && !IsTimeoutLikeConnectFailure(lastAttempt.error))
                    return lastAttempt;
            }

            return lastAttempt;
        }

        private static bool IsClassicFallbackPreferred(S7PlusDeviceRuntime runtime)
        {
            return runtime?.PreferClassicFallbackUntilUtc.HasValue == true &&
                   runtime.PreferClassicFallbackUntilUtc.Value > DateTime.UtcNow;
        }

        private static TimeSpan GetClassicFallbackCooldown(int consecutiveFailures, string error)
        {
            if (string.IsNullOrWhiteSpace(error))
                return TimeSpan.Zero;

            string normalizedError = error.ToLowerInvariant();

            // Sessione invalida = PLC non supporta S7CommPlus (es. S7-300, firmware vecchio,
            // device non Siemens). Non è un problema transitorio: cooldown lungo dal primo tentativo.
            bool isInvalidSession = normalizedError.Contains("sessione non valida") ||
                                    normalizedError.Contains("sessionid=0") ||
                                    normalizedError.Contains("invalid-session") ||
                                    normalizedError.Contains("initssl-deserialize-null") ||
                                    normalizedError.Contains("driver-trace-without-success");

            if (isInvalidSession)
            {
                if (consecutiveFailures >= 3)
                    return TimeSpan.FromMinutes(15);
                if (consecutiveFailures >= 2)
                    return TimeSpan.FromMinutes(10);
                return TimeSpan.FromMinutes(5);
            }

            bool isTimeoutLike = normalizedError.Contains("code 5") ||
                                 normalizedError.Contains("timeout") ||
                                 normalizedError.Contains("did not respond") ||
                                 normalizedError.Contains("non ha risposto");

            if (isTimeoutLike)
            {
                if (consecutiveFailures >= 3)
                    return TimeSpan.FromMinutes(3);
                if (consecutiveFailures >= 2)
                    return TimeSpan.FromSeconds(90);
                return TimeSpan.FromSeconds(45);
            }

            if (consecutiveFailures >= 3)
                return TimeSpan.FromSeconds(30);

            return TimeSpan.Zero;
        }

        private async Task TryRefreshDeviceIdentityAsync(S7PlusConfig config, S7PlusDeviceRuntime runtime)
        {
            if (config == null || runtime == null)
                return;

            bool hasCachedIdentity =
                !string.IsNullOrWhiteSpace(runtime.ModuleTypeName) ||
                !string.IsNullOrWhiteSpace(runtime.OrderCode) ||
                !string.IsNullOrWhiteSpace(runtime.FirmwareVersion);

            if (hasCachedIdentity &&
                runtime.CpuInfoUpdatedAtUtc.HasValue &&
                DateTime.UtcNow - runtime.CpuInfoUpdatedAtUtc.Value < TimeSpan.FromMinutes(15))
            {
                return;
            }

            try
            {
                var cpuInfo = await Task.Run(() =>
                {
                    using (var raw = new S7RawClient())
                    {
                        raw.Timeout = Math.Max(3000, Math.Min(config.Timeout, 5000));
                        if (raw.Connect(config.Ip, 0, 1) != 0)
                            return null;

                        return raw.ReadCpuInfo();
                    }
                });

                if (cpuInfo == null)
                    return;

                runtime.ModuleTypeName = cpuInfo.ModuleTypeName ?? string.Empty;
                runtime.SerialNumber = cpuInfo.SerialNumber ?? string.Empty;
                runtime.PlantId = cpuInfo.PlantId ?? string.Empty;
                runtime.Copyright = cpuInfo.Copyright ?? string.Empty;
                runtime.ModuleName = cpuInfo.ModuleName ?? string.Empty;
                runtime.OrderCode = cpuInfo.OrderCode ?? string.Empty;
                runtime.MemoryCardOrderCode = cpuInfo.MemoryCardOrderCode ?? string.Empty;
                runtime.FirmwareVersion = cpuInfo.FirmwareVersion ?? string.Empty;
                runtime.BootLoaderVersion = cpuInfo.BootLoaderVersion ?? string.Empty;
                runtime.CpuInfoUpdatedAtUtc = DateTime.UtcNow;
                UpdateDeviceProfile(runtime);
            }
            catch
            {
                // Identity enrichment is best-effort only.
            }
        }

        private static void UpdateDeviceProfile(S7PlusDeviceRuntime runtime)
        {
            if (runtime == null)
                return;

            string orderCode = runtime.OrderCode ?? string.Empty;
            string moduleTypeName = runtime.ModuleTypeName ?? string.Empty;
            string firmwareVersion = runtime.FirmwareVersion ?? string.Empty;

            // ET200SP CPU 1511SP — deve stare PRIMA del check generico ET200SP 1512
            if (ContainsIgnoreCase(orderCode, "6ES7 511-1UK") ||
                ContainsIgnoreCase(orderCode, "6ES7 511-1TK") ||
                ContainsIgnoreCase(moduleTypeName, "CPU 1511SP"))
            {
                runtime.DeviceProfileKey = "et200sp-1511sp";
                runtime.DeviceProfileLabel = "ET200SP CPU 1511SP";
                runtime.ConnectStrategyHint = "ET200SP 1511SP: verifica TIA Portal → Protezione → 'Consenti accesso con pannello operatore (HMI)' e firmware >= V2.1";
                return;
            }

            if (ContainsIgnoreCase(orderCode, "6ES7 512-1DM03-0AB0") || ContainsIgnoreCase(moduleTypeName, "ET 200SP"))
            {
                runtime.DeviceProfileKey = "et200sp-1512-fw3x";
                runtime.DeviceProfileLabel = "ET200SP 1512 FW3.x";
                runtime.ConnectStrategyHint = "S7+ dedicato: ET200SP / bootstrap piu sensibile";
                return;
            }

            if (ContainsIgnoreCase(orderCode, "6ES7 511-1AK02-0AB0") || ContainsIgnoreCase(moduleTypeName, "S7-1500"))
            {
                runtime.DeviceProfileKey = "s7-1500-1511-fw29";
                runtime.DeviceProfileLabel = "S7-1500 1511 FW2.9";
                runtime.ConnectStrategyHint = "Profilo S7+ baseline";
                return;
            }

            if (!string.IsNullOrWhiteSpace(orderCode) || !string.IsNullOrWhiteSpace(moduleTypeName) || !string.IsNullOrWhiteSpace(firmwareVersion))
            {
                runtime.DeviceProfileKey = "generic-s7plus";
                runtime.DeviceProfileLabel = "Profilo generico";
                runtime.ConnectStrategyHint = "S7+ standard";
                return;
            }

            runtime.DeviceProfileKey = string.Empty;
            runtime.DeviceProfileLabel = string.Empty;
            runtime.ConnectStrategyHint = string.Empty;
        }

        private static bool ContainsIgnoreCase(string source, string value)
        {
            if (string.IsNullOrEmpty(source) || string.IsNullOrEmpty(value))
                return false;

            return source.IndexOf(value, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static void ClearConnectFailures(S7PlusDeviceRuntime runtime)
        {
            if (runtime == null)
                return;

            runtime.ConsecutiveConnectFailures = 0;
            runtime.LastConnectError = string.Empty;
            runtime.LastConnectFailureUtc = default(DateTime);
            runtime.PreferClassicFallbackUntilUtc = null;
        }

        private static string BuildConnectTrace(IEnumerable<string> lines)
        {
            if (lines == null)
                return string.Empty;

            return string.Join(" | ", lines);
        }

        private static void CaptureDriverDebugSnapshot(
            S7PlusDeviceRuntime runtime,
            S7CommPlusConnection connection,
            IEnumerable<string> traceLines)
        {
            if (runtime == null)
                return;

            var combinedTrace = new List<string>(traceLines ?? Array.Empty<string>())
            {
                "driverTrace=" + (connection?.LastConnectDebugTrace ?? string.Empty),
                "driverRx=" + (connection?.LastReceiveDebugTrace ?? string.Empty)
            };

            runtime.LastConnectTrace = BuildConnectTrace(combinedTrace);
            runtime.LastCreateObjectRequestSummary = connection?.LastCreateObjectRequestSummary ?? string.Empty;
            runtime.LastCreateObjectRequestHex = connection?.LastCreateObjectRequestHex ?? string.Empty;
        }

        private static string RegisterConnectFailure(S7PlusDeviceRuntime runtime, string error)
        {
            if (runtime == null)
                return error ?? string.Empty;

            runtime.LastConnectError = error ?? string.Empty;
            runtime.LastConnectFailureUtc = DateTime.UtcNow;
            runtime.ConsecutiveConnectFailures++;

            TimeSpan cooldown = GetClassicFallbackCooldown(runtime.ConsecutiveConnectFailures, runtime.LastConnectError);
            if (cooldown > TimeSpan.Zero)
            {
                runtime.PreferClassicFallbackUntilUtc = DateTime.UtcNow.Add(cooldown);
                return $"{runtime.LastConnectError} Fallback PLC S7 consigliato per {Math.Ceiling(cooldown.TotalSeconds):0}s.";
            }

            runtime.PreferClassicFallbackUntilUtc = null;
            return runtime.LastConnectError;
        }

        /// <summary>
        /// Disconnects and removes the connection identified by <paramref name="deviceId"/>.
        /// </summary>
        public async Task<S7PlusResult> DisconnectAsync(string deviceId)
        {
            if (!_connections.TryRemove(deviceId, out var state))
            {
                return S7PlusResult.Ok();
            }

            // Disconnect uses HIGH priority (must not be starved)
            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(10)))
            {
                Console.WriteLine($"[S7PlusConnectionManager] Disconnect timeout for '{deviceId}'.");
                return S7PlusResult.Fail("Lock timeout during disconnect");
            }
            try
            {
                if (state.IsConnected)
                {
                    await Task.Run(() => state.Connection.Disconnect());
                    state.IsConnected = false;
                    Console.WriteLine($"[S7PlusConnectionManager] Device '{deviceId}' disconnected.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusConnectionManager] Disconnect exception for '{deviceId}': {ex.Message}");
            }
            finally
            {
                state.PrioLock.Release();
            }

            return S7PlusResult.Ok();
        }

        /// <summary>
        /// Returns the connection state for <paramref name="deviceId"/>, or <c>null</c> if not found.
        /// </summary>
        public S7PlusConnectionState GetConnection(string deviceId)
        {
            _connections.TryGetValue(deviceId, out var state);
            return state;
        }

        public void RememberGoodBlockList(string deviceId, List<S7CommPlusConnection.BlockInfo> blockList, DateTime successUtc)
        {
            if (string.IsNullOrWhiteSpace(deviceId) || blockList == null || blockList.Count == 0)
                return;

            _deviceRuntime.AddOrUpdate(
                deviceId,
                _ => new S7PlusDeviceRuntime
                {
                    LastGoodBlockList = new List<S7CommPlusConnection.BlockInfo>(blockList),
                    LastGoodBlockListUtc = successUtc
                },
                (_, runtime) =>
                {
                    runtime.LastGoodBlockList = new List<S7CommPlusConnection.BlockInfo>(blockList);
                    runtime.LastGoodBlockListUtc = successUtc;
                    return runtime;
                });
        }

        /// <summary>
        /// Returns a snapshot of every registered connection's status.
        /// </summary>
        public List<S7PlusConnectionStatus> GetAllStatuses()
        {
            var list = new List<S7PlusConnectionStatus>();
            var deviceIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var key in _connections.Keys)
                deviceIds.Add(key);
            foreach (var key in _deviceRuntime.Keys)
                deviceIds.Add(key);

            foreach (var deviceId in deviceIds)
            {
                _connections.TryGetValue(deviceId, out var connectionState);
                _deviceRuntime.TryGetValue(deviceId, out var runtime);

                var config = connectionState?.Config ?? runtime?.Config;
                bool preferClassicFallback = IsClassicFallbackPreferred(runtime);
                UpdateDeviceProfile(runtime);

                list.Add(new S7PlusConnectionStatus
                {
                    DeviceId = deviceId,
                    Name = config?.Name ?? deviceId,
                    Ip = config?.Ip ?? string.Empty,
                    IsConnected = connectionState?.IsConnected ?? false,
                    TotalReads = connectionState?.TotalReads ?? 0,
                    TotalWrites = connectionState?.TotalWrites ?? 0,
                    TotalErrors = connectionState?.TotalErrors ?? 0,
                    LastError = connectionState?.LastError ?? runtime?.LastConnectError ?? string.Empty,
                    ConnectedAt = connectionState?.ConnectedAt ?? default(DateTime),
                    LastActivity = connectionState?.LastActivity ?? default(DateTime),
                    CachedVariables = connectionState?.CachedVarInfoList?.Count ?? 0,
                    ConsecutiveConnectFailures = runtime?.ConsecutiveConnectFailures ?? 0,
                    PreferClassicFallback = preferClassicFallback,
                    FallbackReason = preferClassicFallback ? (runtime?.LastConnectError ?? string.Empty) : string.Empty,
                    PreferClassicFallbackUntilUtc = preferClassicFallback ? runtime?.PreferClassicFallbackUntilUtc : null,
                    LastConnectTrace = runtime?.LastConnectTrace ?? string.Empty,
                    LastCreateObjectRequestSummary = runtime?.LastCreateObjectRequestSummary ?? string.Empty,
                    LastCreateObjectRequestHex = runtime?.LastCreateObjectRequestHex ?? string.Empty,
                    LastCreateObjectMatrixSummary = runtime?.LastCreateObjectMatrixSummary ?? string.Empty,
                    ModuleTypeName = runtime?.ModuleTypeName ?? string.Empty,
                    SerialNumber = runtime?.SerialNumber ?? string.Empty,
                    PlantId = runtime?.PlantId ?? string.Empty,
                    Copyright = runtime?.Copyright ?? string.Empty,
                    ModuleName = runtime?.ModuleName ?? string.Empty,
                    OrderCode = runtime?.OrderCode ?? string.Empty,
                    MemoryCardOrderCode = runtime?.MemoryCardOrderCode ?? string.Empty,
                    FirmwareVersion = runtime?.FirmwareVersion ?? string.Empty,
                    BootLoaderVersion = runtime?.BootLoaderVersion ?? string.Empty,
                    CpuInfoUpdatedAtUtc = runtime?.CpuInfoUpdatedAtUtc,
                    DeviceProfileKey = runtime?.DeviceProfileKey ?? string.Empty,
                    DeviceProfileLabel = runtime?.DeviceProfileLabel ?? string.Empty,
                    ConnectStrategyHint = runtime?.ConnectStrategyHint ?? string.Empty,
                });
            }

            return list;
        }

        // -------------------------------------------------------------------------
        // Health check
        // -------------------------------------------------------------------------
        // Health check
        // -------------------------------------------------------------------------

        private void HealthCheckCallback(object state)
        {
            foreach (var kv in _connections)
            {
                var cs = kv.Value;

                // Skip se un'operazione e' in corso
                if (!cs.PrioLock.WaitHighAsync(TimeSpan.Zero).GetAwaiter().GetResult())
                    continue;

                try
                {
                    if (cs.IsConnected)
                    {
                        // Verifica reale: prova una lettura leggera come ping
                        bool isAlive = false;
                        try
                        {
                            // Usa m_LastError come indicatore: se la connessione e' viva
                            // il campo SessionId2 e' accessibile senza errori
                            var sessionId = cs.Connection.SessionId2;
                            isAlive = (cs.Connection.m_LastError == 0);
                        }
                        catch
                        {
                            isAlive = false;
                        }

                        if (isAlive)
                        {
                            cs.ConsecutiveHealthFailures = 0;
                            Console.WriteLine($"[S7PlusConnectionManager] Health check OK: '{kv.Key}'");
                        }
                        else
                        {
                            cs.ConsecutiveHealthFailures++;
                            Console.WriteLine($"[S7PlusConnectionManager] Health check FAILED: '{kv.Key}' ({cs.ConsecutiveHealthFailures}/3)");

                            if (cs.ConsecutiveHealthFailures < 3)
                            {
                                continue;
                            }

                            cs.LastError = "Health check instabile: sessione mantenuta attiva fino a errore reale di lettura/scrittura";
                            cs.ConsecutiveHealthFailures = 0;
                            Console.WriteLine($"[S7PlusConnectionManager] Health check threshold reached for '{kv.Key}' - sessione mantenuta attiva.");
                        }
                    }
                    else if (cs.Config != null && cs.Config.Enabled)
                    {
                        // Dispositivo abilitato ma non connesso — tenta riconnessione
                        Console.WriteLine($"[S7PlusConnectionManager] Auto-reconnect tentativo per '{kv.Key}' (era disconnesso)...");
                        try
                        {
                            cs.Connection = new S7CommPlusDriver.S7CommPlusConnection();
                            int reconRes = cs.Connection.Connect(
                                cs.Config.Ip, cs.Config.Password ?? "", "", cs.Config.Timeout);

                            if (reconRes == 0)
                            {
                                cs.IsConnected = true;
                                cs.LastError = null;
                                cs.ConnectedAt = DateTime.UtcNow;
                                cs.ConsecutiveHealthFailures = 0;
                                Console.WriteLine($"[S7PlusConnectionManager] Auto-reconnect OK: '{kv.Key}'");
                            }
                            else
                            {
                                cs.LastError = $"Auto-reconnect failed (code {reconRes})";
                            }
                        }
                        catch (Exception ex)
                        {
                            cs.LastError = $"Auto-reconnect exception: {ex.Message}";
                        }
                    }
                }
                finally
                {
                    cs.PrioLock.Release();
                }
            }
        }

        // -------------------------------------------------------------------------
        // IDisposable
        // -------------------------------------------------------------------------

        private static string BuildEndpointKey(string ip, int port)
        {
            string safeIp = ip?.Trim() ?? string.Empty;
            return $"{safeIp}:{port}";
        }

        private static S7PlusConfig NormalizeConfig(S7PlusConfig config)
        {
            if (config == null)
                return null;

            int normalizedTimeoutMs = NormalizeTimeoutMs(config.Timeout);
            if (normalizedTimeoutMs == config.Timeout)
                return config;

            return new S7PlusConfig
            {
                Id = config.Id,
                Name = config.Name,
                Ip = config.Ip,
                Port = config.Port,
                Password = config.Password,
                UseTls = config.UseTls,
                Timeout = normalizedTimeoutMs,
                Enabled = config.Enabled,
                DeviceProfileKey = config.DeviceProfileKey
            };
        }

        private static int NormalizeTimeoutMs(int requestedTimeout)
        {
            if (requestedTimeout <= 0)
                return 10000;

            // Backward compatibility: older config payloads store timeout in seconds (e.g. 60).
            if (requestedTimeout <= 300)
                return requestedTimeout * 1000;

            return requestedTimeout;
        }

        private static int GetHardConnectTimeoutMs(S7PlusConfig config)
        {
            int safeTimeout = NormalizeTimeoutMs(config?.Timeout ?? 10000);

            return Math.Max(5000, Math.Min(safeTimeout + 2000, 20000));
        }

        private static async Task<S7PlusResult> ProbeEndpointAsync(string ip, int port, int timeoutMs)
        {
            int probeTimeoutMs = Math.Max(2000, Math.Min(NormalizeTimeoutMs(timeoutMs), 5000));

            using (var tcpClient = new TcpClient())
            {
                try
                {
                    var connectTask = tcpClient.ConnectAsync(ip, port);
                    var completedTask = await Task.WhenAny(connectTask, Task.Delay(probeTimeoutMs));

                    if (completedTask != connectTask || !tcpClient.Connected)
                    {
                        return S7PlusResult.Fail($"TCP {ip}:{port} non raggiungibile entro {probeTimeoutMs} ms.");
                    }

                    return S7PlusResult.Ok();
                }
                catch (Exception ex)
                {
                    return S7PlusResult.Fail($"TCP {ip}:{port} non raggiungibile: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _healthCheckTimer?.Dispose();

            foreach (var kv in _connections)
            {
                try
                {
                    if (kv.Value.IsConnected)
                        kv.Value.Connection.Disconnect();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[S7PlusConnectionManager] Dispose error for '{kv.Key}': {ex.Message}");
                }
            }

            _connections.Clear();
        }
    }
}
