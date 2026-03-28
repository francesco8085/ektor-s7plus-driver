// =============================================================================
// EktorS7PlusDriver — S7CommPlus Communication Driver for Siemens S7-1200/1500
// =============================================================================
// Copyright (c) 2025-2026 Francesco Cesarone <f.cesarone@entersrl.it>
// Azienda   : Enter SRL
// Progetto  : EKTOR Industrial IoT Platform
// Licenza   : Proprietaria — uso riservato Enter SRL
// =============================================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using S7CommPlusDriver;
using S7CommPlusDriver.ClientApi;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// High-level PLC operations: browse, read (single + batch), write.
    /// All methods acquire the per-connection SemaphoreSlim to guard the
    /// non-thread-safe S7CommPlusConnection, then offload the blocking driver
    /// call to Task.Run so the caller's thread is never blocked.
    /// </summary>
    public class S7PlusService
    {
        private readonly S7PlusConnectionManager _manager;
        private readonly S7PlusBlockOperations _blockOperations;

        public S7PlusService(S7PlusConnectionManager manager)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _blockOperations = new S7PlusBlockOperations(_manager, SafeNativeCallAsync);
        }

        // =====================================================================
        // SafeNativeCallAsync: isolates native S7CommPlus driver calls with
        // timeout and catches corrupted state exceptions (AccessViolation, SEH)
        // that would otherwise crash the entire process.
        // =====================================================================
        [HandleProcessCorruptedStateExceptions]
        private async Task<int> SafeNativeCallAsync(Func<int> nativeCall, string operationName, string deviceId, int timeoutMs = 25000)
        {
            try
            {
                var cts = new CancellationTokenSource(timeoutMs);
                var task = Task.Run(() =>
                {
                    try
                    {
                        return nativeCall();
                    }
                    catch (AccessViolationException avEx)
                    {
                        LogNativeCrash(operationName, deviceId, avEx);
                        return -9999;
                    }
                    catch (System.Runtime.InteropServices.SEHException sehEx)
                    {
                        LogNativeCrash(operationName, deviceId, sehEx);
                        return -9998;
                    }
                }, cts.Token);

                if (await Task.WhenAny(task, Task.Delay(timeoutMs, cts.Token)) == task)
                {
                    cts.Cancel(); // cancel delay
                    return task.Result;
                }

                // Timeout — native call is stuck
                Console.Error.WriteLine($"[S7PlusService] NATIVE TIMEOUT: {operationName} for '{deviceId}' did not complete in {timeoutMs}ms");
                Serilog.Log.Warning("Native call {Op} for {DeviceId} timed out after {Ms}ms", operationName, deviceId, timeoutMs);
                return -9997;
            }
            catch (AccessViolationException avEx)
            {
                LogNativeCrash(operationName, deviceId, avEx);
                return -9999;
            }
            catch (System.Runtime.InteropServices.SEHException sehEx)
            {
                LogNativeCrash(operationName, deviceId, sehEx);
                return -9998;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[S7PlusService] SafeNativeCall error in {operationName} for '{deviceId}': {ex.GetType().Name}: {ex.Message}");
                return -9996;
            }
        }

        private static void LogNativeCrash(string operation, string deviceId, Exception ex)
        {
            var msg = $"[NATIVE CRASH] {operation} for '{deviceId}': {ex.GetType().Name}: {ex.Message}";
            Console.Error.WriteLine(msg);
            Console.Error.WriteLine(ex.StackTrace);
            Serilog.Log.Fatal(ex, "Native crash in {Op} for {DeviceId}", operation, deviceId);
            try
            {
                System.IO.File.AppendAllText("logs/native-crash.log",
                    $"\n{DateTime.Now:yyyy-MM-dd HH:mm:ss} {msg}\n{ex.StackTrace}\n");
            }
            catch { }
        }

        // HasUsableBrowseCache → moved to S7PlusBlockOperations

        private static List<object> ToBrowseResult(IEnumerable<VarInfo> vars)
        {
            return (vars ?? Enumerable.Empty<VarInfo>())
                .Select(v => (object)new
                {
                    name = v.Name,
                    type = S7PlusBlockOperations.GetTypeName(v.Softdatatype),
                    accessSequence = v.AccessSequence,
                    softdatatype = v.Softdatatype,
                    s7Address = S7PlusBlockOperations.ComputeFullS7Address(v),
                    optAddress = v.OptAddress,
                    optBitoffset = v.OptBitoffset,
                    nonOptAddress = v.NonOptAddress,
                    nonOptBitoffset = v.NonOptBitoffset,
                })
                .ToList();
        }

        // BuildVarCounts, ToBlockResult, HasUsableBlockCache,
        // IsBlockDiscoveryTimeoutLike, GetBlocksCoreAsync → moved to S7PlusBlockOperations

        private async Task<bool> ReconnectForBrowseAsync(string deviceId, S7PlusConnectionState state)
        {
            Console.WriteLine($"[S7PlusService] BrowseAsync: reconnecting '{deviceId}' before retry...");
            try
            {
                state.Connection.Disconnect();
                state.Connection = new S7CommPlusDriver.S7CommPlusConnection();
                int reconRes = await Task.Run(() =>
                    state.Connection.Connect(state.Config.Ip, state.Config.Password ?? "", "", state.Config.Timeout));
                if (reconRes == 0)
                {
                    state.IsConnected = true;
                    Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect OK for '{deviceId}'");
                    await Task.Delay(350);
                    return true;
                }

                state.IsConnected = false;
                Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect FAILED for '{deviceId}' (code {reconRes})");
                return false;
            }
            catch (Exception reconEx)
            {
                Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect exception: {reconEx.Message}");
                state.IsConnected = false;
                return false;
            }
        }

        private async Task<(bool success, string? error, List<object>? variables)> BrowseCoreAsync(
            string deviceId,
            S7PlusConnectionState state)
        {
            var now = DateTime.UtcNow;
            var hasAnyCache = S7PlusBlockOperations.HasUsableBrowseCache(state);
            var recentFailureWindow = TimeSpan.FromSeconds(45);

            if (state.BrowseCacheValid && hasAnyCache)
            {
                Console.WriteLine($"[S7PlusService] BrowseAsync: returning {state.CachedVarInfoList.Count} cached variables for '{deviceId}'.");
                return (true, null, ToBrowseResult(state.CachedVarInfoList));
            }

            if (hasAnyCache &&
                state.LastBrowseFailureUtc != default(DateTime) &&
                now - state.LastBrowseFailureUtc < recentFailureWindow)
            {
                var staleReason = string.IsNullOrWhiteSpace(state.LastBrowseError)
                    ? "recent browse failure"
                    : state.LastBrowseError;
                Console.WriteLine($"[S7PlusService] BrowseAsync: using stale cache for '{deviceId}' after recent failure: {staleReason}");
                return (true, $"Using cached browse data after recent failure: {staleReason}", ToBrowseResult(state.CachedVarInfoList));
            }

            if (!hasAnyCache &&
                state.ConsecutiveBrowseFailures >= 2 &&
                state.LastBrowseFailureUtc != default(DateTime) &&
                now - state.LastBrowseFailureUtc < TimeSpan.FromSeconds(20))
            {
                var fastFailReason = string.IsNullOrWhiteSpace(state.LastBrowseError)
                    ? "recent repeated browse failures"
                    : state.LastBrowseError;
                Console.WriteLine($"[S7PlusService] BrowseAsync: fast-fail cooldown for '{deviceId}' after repeated failures: {fastFailReason}");
                return (false, $"Browse temporarily cooled down after repeated failures: {fastFailReason}", null);
            }

            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(18)))
            {
                if (hasAnyCache)
                {
                    Console.WriteLine($"[S7PlusService] BrowseAsync: low-priority lock timeout for '{deviceId}', returning stale cache.");
                    return (true, "Using cached browse data: PLC busy with other operations", ToBrowseResult(state.CachedVarInfoList));
                }

                return (false, "Lock timeout: PLC busy with other operations", null);
            }

            try
            {
                if (state.BrowseCacheValid && S7PlusBlockOperations.HasUsableBrowseCache(state))
                    return (true, null, ToBrowseResult(state.CachedVarInfoList));

                List<VarInfo> varList = null;
                int res = -1;
                int maxRetries = hasAnyCache ? 2 : 3;
                string lastErr = "";
                state.LastBrowseAttemptUtc = DateTime.UtcNow;

                for (int attempt = 1; attempt <= maxRetries; attempt++)
                {
                    Console.WriteLine($"[S7PlusService] BrowseAsync: attempt {attempt}/{maxRetries} for '{deviceId}'...");

                    try
                    {
                        res = await SafeNativeCallAsync(() =>
                        {
                            List<VarInfo> tempList;
                            int r = state.Connection.Browse(out tempList);
                            varList = tempList;
                            return r;
                        }, "Browse", deviceId, 30000);
                    }
                    catch (Exception browseEx)
                    {
                        res = -1;
                        lastErr = browseEx.Message;
                        Console.WriteLine($"[S7PlusService] BrowseAsync: exception on attempt {attempt}: {browseEx.Message}");
                    }

                    if (res == 0)
                        break;

                    // Native crash or timeout — don't retry
                    if (res <= -9997)
                    {
                        lastErr = $"Browse native crash/timeout (code {res})";
                        Console.WriteLine($"[S7PlusService] BrowseAsync: {lastErr} — aborting retries");
                        break;
                    }

                    lastErr = $"Browse failed with code {res}";
                    Console.WriteLine($"[S7PlusService] BrowseAsync: {lastErr} for '{deviceId}', attempt {attempt}/{maxRetries}");

                    if (attempt < maxRetries)
                    {
                        var reconnected = await ReconnectForBrowseAsync(deviceId, state);
                        if (!reconnected)
                            break;
                    }
                }

                if (res != 0)
                {
                    state.LastBrowseFailureUtc = DateTime.UtcNow;
                    state.LastBrowseError = lastErr;
                    state.ConsecutiveBrowseFailures++;
                    state.LastError = lastErr;
                    state.TotalErrors++;

                    if (hasAnyCache)
                    {
                        Console.WriteLine($"[S7PlusService] BrowseAsync: browse failed for '{deviceId}', serving stale cache.");
                        return (true, $"Using cached browse data after failure: {lastErr}", ToBrowseResult(state.CachedVarInfoList));
                    }

                    return (false, $"{lastErr} (dopo {maxRetries} tentativi)", null);
                }

                state.CachedVarInfoList = varList ?? new List<VarInfo>();
                state.CachedVarInfoByName.Clear();
                foreach (var v in state.CachedVarInfoList)
                {
                    if (!string.IsNullOrEmpty(v.Name))
                        state.CachedVarInfoByName[v.Name] = v;
                }

                state.BrowseCacheValid = true;
                state.LastBrowseSuccessUtc = DateTime.UtcNow;
                state.LastBrowseError = string.Empty;
                state.ConsecutiveBrowseFailures = 0;
                state.LastActivity = DateTime.UtcNow;

                Console.WriteLine($"[S7PlusService] Browse OK: {state.CachedVarInfoList.Count} variables for device '{deviceId}'.");
                return (true, string.Empty, ToBrowseResult(state.CachedVarInfoList));
            }
            catch (Exception ex)
            {
                state.LastBrowseFailureUtc = DateTime.UtcNow;
                state.LastBrowseError = ex.Message;
                state.ConsecutiveBrowseFailures++;
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] BrowseAsync exception for '{deviceId}': {ex.Message}");

                if (hasAnyCache)
                    return (true, $"Using cached browse data after exception: {ex.Message}", ToBrowseResult(state.CachedVarInfoList));

                return (false, ex.Message, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // -------------------------------------------------------------------------
        // Browse
        // -------------------------------------------------------------------------

        /// <summary>
        /// Browses all PLC variables, caches the result, and returns a simplified list.
        /// Each entry contains: Name, Type (human-readable), AccessSequence.
        /// </summary>
        public async Task<(bool success, string? error, List<object>? variables)> BrowseAsync(string deviceId)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found or not connected.", null);

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.", null);

            Task<(bool success, string? error, List<object>? variables)> browseTask;
            lock (state.BrowseTaskSync)
            {
                if (state.PendingBrowseTask != null && !state.PendingBrowseTask.IsCompleted)
                {
                    Console.WriteLine($"[S7PlusService] BrowseAsync: join pending browse for '{deviceId}'.");
                    browseTask = state.PendingBrowseTask;
                }
                else
                {
                    browseTask = BrowseCoreAsync(deviceId, state);
                    state.PendingBrowseTask = browseTask;
                }
            }

            try
            {
                return await browseTask;
            }
            finally
            {
                lock (state.BrowseTaskSync)
                {
                    if (ReferenceEquals(state.PendingBrowseTask, browseTask) && browseTask.IsCompleted)
                        state.PendingBrowseTask = null;
                }
            }

#if false

            // Se la cache e' valida, restituisci subito senza ri-scansionare
            if (state.BrowseCacheValid && state.CachedVarInfoList != null && state.CachedVarInfoList.Count > 0)
            {
                Console.WriteLine($"[S7PlusService] BrowseAsync: returning {state.CachedVarInfoList.Count} cached variables for '{deviceId}'.");
                var cachedResult = state.CachedVarInfoList.Select(v => (object)new
                {
                    name = v.Name,
                    type = Softdatatype.Types.ContainsKey(v.Softdatatype) ? Softdatatype.Types[v.Softdatatype] : $"Unknown({v.Softdatatype})",
                    accessSequence = v.AccessSequence,
                    softdatatype = v.Softdatatype
                }).ToList();
                return (true, null, cachedResult);
            }

            // Browse is a HEAVY operation — use LOW priority, 30s timeout
            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(30)))
                return (false, "Lock timeout: PLC busy with other operations", null);
            try
            {
                List<VarInfo> varList = null;
                int res = -1;
                int maxRetries = 3;
                string lastErr = "";

                for (int attempt = 1; attempt <= maxRetries; attempt++)
                {
                    Console.WriteLine($"[S7PlusService] BrowseAsync: attempt {attempt}/{maxRetries} for '{deviceId}'...");

                    try
                    {
                        res = await SafeNativeCallAsync(() =>
                        {
                            List<VarInfo> tempList;
                            int r = state.Connection.Browse(out tempList);
                            varList = tempList;
                            return r;
                        }, "Browse", deviceId, 30000);
                    }
                    catch (Exception browseEx)
                    {
                        res = -1;
                        lastErr = browseEx.Message;
                        Console.WriteLine($"[S7PlusService] BrowseAsync: exception on attempt {attempt}: {browseEx.Message}");
                    }

                    if (res == 0) break;

                    // Native crash or timeout — don't retry
                    if (res <= -9997)
                    {
                        lastErr = $"Browse native crash/timeout (code {res})";
                        Console.WriteLine($"[S7PlusService] BrowseAsync: {lastErr} — aborting retries");
                        break;
                    }

                    lastErr = $"Browse failed with code {res}";
                    Console.WriteLine($"[S7PlusService] BrowseAsync: {lastErr} for '{deviceId}', attempt {attempt}/{maxRetries}");

                    // Se fallisce, prova a riconnettere prima del prossimo tentativo
                    if (attempt < maxRetries)
                    {
                        Console.WriteLine($"[S7PlusService] BrowseAsync: reconnecting '{deviceId}' before retry...");
                        try
                        {
                            state.Connection.Disconnect();
                            state.Connection = new S7CommPlusDriver.S7CommPlusConnection();
                            int reconRes = state.Connection.Connect(
                                state.Config.Ip, state.Config.Password ?? "", "", state.Config.Timeout);
                            if (reconRes == 0)
                            {
                                state.IsConnected = true;
                                Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect OK for '{deviceId}'");
                                await Task.Delay(500); // pausa breve dopo riconnessione
                            }
                            else
                            {
                                state.IsConnected = false;
                                Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect FAILED for '{deviceId}' (code {reconRes})");
                                break;
                            }
                        }
                        catch (Exception reconEx)
                        {
                            Console.WriteLine($"[S7PlusService] BrowseAsync: reconnect exception: {reconEx.Message}");
                            break;
                        }
                    }
                }

                if (res != 0)
                {
                    state.LastError = lastErr;
                    state.TotalErrors++;
                    return (false, $"{lastErr} (dopo {maxRetries} tentativi)", null);
                }

                // Update cache
                state.CachedVarInfoList = varList ?? new List<VarInfo>();
                state.CachedVarInfoByName.Clear();
                foreach (var v in state.CachedVarInfoList)
                {
                    if (!string.IsNullOrEmpty(v.Name))
                        state.CachedVarInfoByName[v.Name] = v;
                }
                state.BrowseCacheValid = true;
                state.LastActivity = DateTime.UtcNow;

                Console.WriteLine($"[S7PlusService] Browse OK: {state.CachedVarInfoList.Count} variables for device '{deviceId}'.");

                var result = ToBrowseResult(state.CachedVarInfoList);

                return (true, string.Empty, result);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] BrowseAsync exception for '{deviceId}': {ex.Message}");
                return (false, ex.Message, null);
            }
            finally
            {
                state.PrioLock.Release();
            }

#endif
        }

        // -------------------------------------------------------------------------
        // Read (single)
        // -------------------------------------------------------------------------

        /// <summary>
        /// Reads a single variable by symbolic name.
        /// Auto-browses if the cache is empty (lazy browse).
        /// Returns value, type name, and quality string.
        /// </summary>
        public async Task<(bool success, string? error, object? value, string? typeName, string? quality)>
            ReadAsync(string deviceId, string variableName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null, null, null);

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.", null, null, null);

            // Lazy browse — skip if Browse has already failed on this PLC
            if (!state.BrowseCacheValid && state.ConsecutiveBrowseFailures == 0)
            {
                var (browseOk, browseErr, _) = await BrowseAsync(deviceId);
                if (!browseOk)
                    return (false, $"Auto-browse failed: {browseErr}", null, null, null);
            }

            VarInfo varInfo;
            if (!state.CachedVarInfoByName.TryGetValue(variableName, out varInfo))
                return (false, $"Variable '{variableName}' not found in browse cache.", null, null, null);

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy", null, null, null);
            try
            {
                var tag = PlcTags.TagFactory(varInfo.Name, new ItemAddress(varInfo.AccessSequence), varInfo.Softdatatype);
                var tagList = new List<PlcTag> { tag };

                int res = await Task.Run(() => state.Connection.ReadTags(tagList));

                state.LastActivity = DateTime.UtcNow;

                if (res != 0)
                {
                    string err = $"ReadTags returned code {res}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] ReadAsync failed: {err} for '{variableName}' on '{deviceId}'.");
                    return (false, err, null, null, null);
                }

                state.TotalReads++;

                bool goodQuality = (tag.Quality & PlcTagQC.TAG_QUALITY_MASK) == PlcTagQC.TAG_QUALITY_GOOD;
                string qualityStr = goodQuality ? "GOOD" : $"BAD (0x{tag.Quality:X2})";
                object value = SerializableValue(tag);
                string typeName = S7PlusBlockOperations.GetTypeName(tag.Datatype);

                Console.WriteLine($"[S7PlusService] Read '{variableName}' = {value} [{typeName}] Q={qualityStr}");
                return (true, string.Empty, value, typeName, qualityStr);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] ReadAsync exception for '{variableName}' on '{deviceId}': {ex.Message}");
                return (false, ex.Message, null, null, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // -------------------------------------------------------------------------
        // Read (batch)
        // -------------------------------------------------------------------------

        /// <summary>
        /// Reads multiple variables in a single driver call.
        /// Returns a list where each entry mirrors the request order and contains
        /// { name, value, typeName, quality, error }.
        /// </summary>
        public async Task<(bool success, string? error, List<object>? results)>
            ReadBatchAsync(string deviceId, IEnumerable<string> variableNames)
        {
            var nameList = variableNames?.ToList() ?? new List<string>();
            if (nameList.Count == 0)
                return (false, "No variable names provided.", null);

            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null);

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.", null);

            // Lazy browse — skip if Browse has already failed on this PLC
            if (!state.BrowseCacheValid && state.ConsecutiveBrowseFailures == 0)
            {
                var (browseOk, browseErr, _) = await BrowseAsync(deviceId);
                if (!browseOk)
                    return (false, $"Auto-browse failed: {browseErr}", null);
            }

            // Resolve names to VarInfo
            var resolved = new List<(string requestedName, VarInfo varInfo)>();
            var notFound = new List<string>();

            foreach (var name in nameList)
            {
                if (state.CachedVarInfoByName.TryGetValue(name, out var vi))
                    resolved.Add((name, vi));
                else
                    notFound.Add(name);
            }

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy", null);
            try
            {
                var tags = resolved
                    .Select(r => PlcTags.TagFactory(r.varInfo.Name, new ItemAddress(r.varInfo.AccessSequence), r.varInfo.Softdatatype))
                    .ToList();

                int res = await Task.Run(() => state.Connection.ReadTags(tags));

                state.LastActivity = DateTime.UtcNow;

                if (res != 0)
                {
                    string err = $"ReadTags returned code {res}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] ReadBatchAsync failed: {err} on '{deviceId}'.");
                    return (false, err, null);
                }

                state.TotalReads += tags.Count;

                var results = new List<object>();

                // Add successfully-read tags
                for (int i = 0; i < resolved.Count; i++)
                {
                    var tag = tags[i];
                    bool goodQuality = (tag.Quality & PlcTagQC.TAG_QUALITY_MASK) == PlcTagQC.TAG_QUALITY_GOOD;
                    results.Add(new
                    {
                        name = resolved[i].requestedName,
                        value = SerializableValue(tag),
                        typeName = S7PlusBlockOperations.GetTypeName(tag.Datatype),
                        quality = goodQuality ? "GOOD" : $"BAD (0x{tag.Quality:X2})",
                        error = (string)null,
                    });
                }

                // Add not-found placeholders
                foreach (var name in notFound)
                {
                    results.Add(new
                    {
                        name = name,
                        value = (object)null,
                        typeName = (string)null,
                        quality = (string)null,
                        error = $"Variable '{name}' not found in browse cache.",
                    });
                }

                Console.WriteLine($"[S7PlusService] ReadBatch: {resolved.Count} read, {notFound.Count} not found on '{deviceId}'.");
                return (true, string.Empty, results);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] ReadBatchAsync exception on '{deviceId}': {ex.Message}");
                return (false, ex.Message, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // -------------------------------------------------------------------------
        // Write
        // -------------------------------------------------------------------------

        /// <summary>
        /// Writes a value to the named variable.
        /// <paramref name="value"/> is a JSON-compatible object (string, bool, number).
        /// <paramref name="dataType"/> is the optional softdatatype override; if 0
        /// the type from the browse cache is used.
        /// </summary>
        public async Task<(bool success, string error)>
            WriteAsync(string deviceId, string variableName, object value, uint dataType = 0)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found.");

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.");

            // Lazy browse — skip if Browse has already failed on this PLC
            if (!state.BrowseCacheValid && state.ConsecutiveBrowseFailures == 0)
            {
                var (browseOk, browseErr, _) = await BrowseAsync(deviceId);
                if (!browseOk)
                    return (false, $"Auto-browse failed: {browseErr}");
            }

            if (!state.CachedVarInfoByName.TryGetValue(variableName, out var varInfo))
                return (false, $"Variable '{variableName}' not found in browse cache.");

            uint effectiveType = dataType != 0 ? dataType : varInfo.Softdatatype;

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy");
            try
            {
                var tag = PlcTags.TagFactory(varInfo.Name, new ItemAddress(varInfo.AccessSequence), effectiveType);
                SetTagValue(tag, value, effectiveType);

                var tagList = new List<PlcTag> { tag };
                int res = await Task.Run(() => state.Connection.WriteTags(tagList));

                state.LastActivity = DateTime.UtcNow;

                if (res != 0)
                {
                    string err = $"WriteTags returned code {res}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] WriteAsync failed: {err} for '{variableName}' on '{deviceId}'.");
                    return (false, err);
                }

                if (tag.LastWriteError != 0)
                {
                    string err = $"Write error for '{variableName}': 0x{tag.LastWriteError:X}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] {err} on '{deviceId}'.");
                    return (false, err);
                }

                state.TotalWrites++;
                Console.WriteLine($"[S7PlusService] Write '{variableName}' = {value} OK on '{deviceId}'.");
                return (true, string.Empty);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] WriteAsync exception for '{variableName}' on '{deviceId}': {ex.Message}");
                return (false, ex.Message);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // -------------------------------------------------------------------------
        // Direct I/O Read/Write by Absolute S7 Address
        // -------------------------------------------------------------------------

        /// <summary>
        /// Parses a standard S7 address string into its components.
        /// Supported formats:
        ///   Bit:   I0.0, Q1.3, M10.5, DB1.DBX0.0
        ///   Byte:  IB0, QB0, MB0, DB1.DBB0
        ///   Word:  IW0, QW0, MW0, DB1.DBW0
        ///   DWord: ID0, QD0, MD0, DB1.DBD0
        /// Returns (area, dbNum, byteOffset, bitOffset, byteCount, dataType).
        /// bitOffset = -1 means no bit addressing (byte/word/dword).
        /// Throws FormatException on unrecognized syntax.
        /// </summary>
        private (string area, int dbNum, int byteOffset, int bitOffset, int byteCount, string dataType)
            ParseS7Address(string address)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new FormatException("Address is null or empty.");

            address = address.Trim().ToUpperInvariant();

            // --- DB variants: DB<n>.DBX<b>.<bit>, DB<n>.DBB<b>, DB<n>.DBW<b>, DB<n>.DBD<b>
            if (address.StartsWith("DB"))
            {
                // Split at the dot separating DB number from sub-address
                int firstDot = address.IndexOf('.');
                if (firstDot < 0)
                    throw new FormatException($"Invalid DB address (missing '.'): {address}");

                string dbPart = address.Substring(0, firstDot);          // e.g. "DB1"
                string subPart = address.Substring(firstDot + 1);        // e.g. "DBX0.0"

                if (!int.TryParse(dbPart.Substring(2), out int dbNum) || dbNum < 1)
                    throw new FormatException($"Invalid DB number in: {address}");

                if (subPart.StartsWith("DBX"))
                {
                    // Bit: DB1.DBX<byte>.<bit>
                    string rest = subPart.Substring(3); // e.g. "0.0"
                    int dot2 = rest.IndexOf('.');
                    if (dot2 < 0)
                        throw new FormatException($"Bit address missing bit offset in: {address}");
                    if (!int.TryParse(rest.Substring(0, dot2), out int byteOff))
                        throw new FormatException($"Invalid byte offset in: {address}");
                    if (!int.TryParse(rest.Substring(dot2 + 1), out int bitOff) || bitOff < 0 || bitOff > 7)
                        throw new FormatException($"Invalid bit offset (0-7) in: {address}");
                    return ("DB", dbNum, byteOff, bitOff, 1, "Bool");
                }
                else if (subPart.StartsWith("DBB"))
                {
                    if (!int.TryParse(subPart.Substring(3), out int byteOff))
                        throw new FormatException($"Invalid byte offset in: {address}");
                    return ("DB", dbNum, byteOff, -1, 1, "Byte");
                }
                else if (subPart.StartsWith("DBW"))
                {
                    if (!int.TryParse(subPart.Substring(3), out int byteOff))
                        throw new FormatException($"Invalid byte offset in: {address}");
                    return ("DB", dbNum, byteOff, -1, 2, "Word");
                }
                else if (subPart.StartsWith("DBD"))
                {
                    if (!int.TryParse(subPart.Substring(3), out int byteOff))
                        throw new FormatException($"Invalid byte offset in: {address}");
                    return ("DB", dbNum, byteOff, -1, 4, "DWord");
                }
                else
                {
                    throw new FormatException($"Unrecognized DB sub-address format: {address}");
                }
            }

            // --- I/Q/M variants ---
            // Determine area letter
            string areaStr;
            string remainder;
            if (address.StartsWith("IB") || address.StartsWith("IW") || address.StartsWith("ID"))
            {
                areaStr = "I"; remainder = address.Substring(1); // e.g. "B0", "W0", "D0"
            }
            else if (address.StartsWith("QB") || address.StartsWith("QW") || address.StartsWith("QD"))
            {
                areaStr = "Q"; remainder = address.Substring(1);
            }
            else if (address.StartsWith("MB") || address.StartsWith("MW") || address.StartsWith("MD"))
            {
                areaStr = "M"; remainder = address.Substring(1);
            }
            else if (address.StartsWith("I"))
            {
                areaStr = "I"; remainder = address.Substring(1); // could be "0.0" (bit)
            }
            else if (address.StartsWith("Q"))
            {
                areaStr = "Q"; remainder = address.Substring(1);
            }
            else if (address.StartsWith("M"))
            {
                areaStr = "M"; remainder = address.Substring(1);
            }
            else
            {
                throw new FormatException($"Unrecognized area in address: {address}");
            }

            // remainder starts with B/W/D for byte/word/dword, or a digit for bit
            if (remainder.StartsWith("B"))
            {
                if (!int.TryParse(remainder.Substring(1), out int byteOff))
                    throw new FormatException($"Invalid byte offset in: {address}");
                return (areaStr, 0, byteOff, -1, 1, "Byte");
            }
            else if (remainder.StartsWith("W"))
            {
                if (!int.TryParse(remainder.Substring(1), out int byteOff))
                    throw new FormatException($"Invalid byte offset in: {address}");
                return (areaStr, 0, byteOff, -1, 2, "Word");
            }
            else if (remainder.StartsWith("D"))
            {
                if (!int.TryParse(remainder.Substring(1), out int byteOff))
                    throw new FormatException($"Invalid byte offset in: {address}");
                return (areaStr, 0, byteOff, -1, 4, "DWord");
            }
            else
            {
                // Bit address: e.g. I0.0, Q1.3, M10.5
                int dot = remainder.IndexOf('.');
                if (dot < 0)
                    throw new FormatException($"Bit address missing '.' in: {address}");
                if (!int.TryParse(remainder.Substring(0, dot), out int byteOff))
                    throw new FormatException($"Invalid byte offset in: {address}");
                if (!int.TryParse(remainder.Substring(dot + 1), out int bitOff) || bitOff < 0 || bitOff > 7)
                    throw new FormatException($"Invalid bit offset (0-7) in: {address}");
                return (areaStr, 0, byteOff, bitOff, 1, "Bool");
            }
        }

        /// <summary>
        /// Builds an ItemAddress for a classic-blob absolute read/write using the parsed address components.
        /// For DB: uses SetAccessAreaToDatablock(dbNum) + Ids.DB_ValueActual.
        /// For I/Q/M: uses the corresponding NativeObjects area + Ids.ControllerArea_ValueActual.
        /// LID pattern (ClassicBlob): [3, byteOffset, byteCount].
        /// </summary>
        private ItemAddress BuildAbsoluteItemAddress(string area, int dbNum, int byteOffset, int byteCount)
        {
            var addr = new ItemAddress();
            addr.SymbolCrc = 0;

            if (area == "DB")
            {
                addr.SetAccessAreaToDatablock((uint)dbNum);
                addr.AccessSubArea = Ids.DB_ValueActual;
            }
            else if (area == "I")
            {
                addr.AccessArea = (uint)Ids.NativeObjects_theIArea_Rid;
                addr.AccessSubArea = (uint)Ids.ControllerArea_ValueActual;
            }
            else if (area == "Q")
            {
                addr.AccessArea = (uint)Ids.NativeObjects_theQArea_Rid;
                addr.AccessSubArea = (uint)Ids.ControllerArea_ValueActual;
            }
            else if (area == "M")
            {
                addr.AccessArea = (uint)Ids.NativeObjects_theMArea_Rid;
                addr.AccessSubArea = (uint)Ids.ControllerArea_ValueActual;
            }
            else
            {
                throw new FormatException($"Unknown area: {area}");
            }

            // LID_OMS_STB_ClassicBlob = 3, then startOffset, then byteCount
            addr.LID.Add(3);
            addr.LID.Add((uint)byteOffset);
            addr.LID.Add((uint)byteCount);

            return addr;
        }

        /// <summary>
        /// Converts a raw byte array (returned by a ClassicBlob read) to a .NET value
        /// according to the expected data type and optional bit offset.
        /// </summary>
        private static object ExtractValueFromBlob(byte[] blob, string dataType, int bitOffset)
        {
            if (blob == null || blob.Length == 0)
                throw new InvalidOperationException("Blob is null or empty.");

            switch (dataType)
            {
                case "Bool":
                    if (bitOffset < 0 || bitOffset > 7)
                        throw new ArgumentOutOfRangeException(nameof(bitOffset));
                    return ((blob[0] >> bitOffset) & 1) != 0;

                case "Byte":
                    return blob[0];

                case "Word":
                    if (blob.Length < 2) throw new InvalidOperationException("Blob too short for Word.");
                    return (ushort)((blob[0] << 8) | blob[1]);

                case "Int":
                    if (blob.Length < 2) throw new InvalidOperationException("Blob too short for Int.");
                    return (short)((blob[0] << 8) | blob[1]);

                case "DWord":
                    if (blob.Length < 4) throw new InvalidOperationException("Blob too short for DWord.");
                    return (uint)((blob[0] << 24) | (blob[1] << 16) | (blob[2] << 8) | blob[3]);

                case "DInt":
                    if (blob.Length < 4) throw new InvalidOperationException("Blob too short for DInt.");
                    return (int)((blob[0] << 24) | (blob[1] << 16) | (blob[2] << 8) | blob[3]);

                case "Real":
                    if (blob.Length < 4) throw new InvalidOperationException("Blob too short for Real.");
                    // S7 stores Real in big-endian; BitConverter expects little-endian on x86
                    byte[] realBytes = new byte[] { blob[3], blob[2], blob[1], blob[0] };
                    return BitConverter.ToSingle(realBytes, 0);

                default:
                    // Return raw bytes as hex string for unknown types
                    return BitConverter.ToString(blob);
            }
        }

        /// <summary>
        /// Encodes a .NET value to a big-endian byte array for a ClassicBlob write.
        /// </summary>
        private static byte[] EncodeValueToBlob(object value, string dataType, int bitOffset, byte[] existingBlobForBit)
        {
            // Unwrap JToken if coming from JSON deserialization
            if (value is JToken jt)
                value = UnwrapJToken(jt);

            switch (dataType)
            {
                case "Bool":
                {
                    bool boolVal = Convert.ToBoolean(value);
                    // existingBlobForBit must be pre-read (1 byte) to do RMW on the bit
                    byte b = (existingBlobForBit != null && existingBlobForBit.Length > 0)
                        ? existingBlobForBit[0]
                        : (byte)0;
                    if (boolVal)
                        b = (byte)(b | (1 << bitOffset));
                    else
                        b = (byte)(b & ~(1 << bitOffset));
                    return new byte[] { b };
                }

                case "Byte":
                {
                    byte byteVal = Convert.ToByte(value);
                    return new byte[] { byteVal };
                }

                case "Word":
                {
                    ushort wordVal = Convert.ToUInt16(value);
                    return new byte[] { (byte)(wordVal >> 8), (byte)(wordVal & 0xFF) };
                }

                case "Int":
                {
                    short intVal = Convert.ToInt16(value);
                    ushort u = (ushort)intVal;
                    return new byte[] { (byte)(u >> 8), (byte)(u & 0xFF) };
                }

                case "DWord":
                {
                    uint dwordVal = Convert.ToUInt32(value);
                    return new byte[] {
                        (byte)((dwordVal >> 24) & 0xFF),
                        (byte)((dwordVal >> 16) & 0xFF),
                        (byte)((dwordVal >> 8)  & 0xFF),
                        (byte)( dwordVal        & 0xFF)
                    };
                }

                case "DInt":
                {
                    int dintVal = Convert.ToInt32(value);
                    uint u = (uint)dintVal;
                    return new byte[] {
                        (byte)((u >> 24) & 0xFF),
                        (byte)((u >> 16) & 0xFF),
                        (byte)((u >> 8)  & 0xFF),
                        (byte)( u        & 0xFF)
                    };
                }

                case "Real":
                {
                    float realVal = Convert.ToSingle(value);
                    byte[] le = BitConverter.GetBytes(realVal); // little-endian on x86
                    return new byte[] { le[3], le[2], le[1], le[0] }; // convert to big-endian
                }

                default:
                    throw new NotSupportedException($"Write for data type '{dataType}' is not supported.");
            }
        }

        /// <summary>
        /// Reads a single PLC variable by absolute S7 address (e.g. "IB0", "MW100", "DB1.DBW4").
        /// Does not require a prior Browse — uses direct ClassicBlob addressing.
        /// Returns (success, error, value, dataType).
        /// </summary>
        public async Task<(bool success, string? error, object? value, string? dataType)>
            ReadByAddressAsync(string deviceId, string address)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found or not connected.", null, null);

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.", null, null);

            string parsedArea, parsedDataType;
            int parsedDbNum, parsedByteOffset, parsedBitOffset, parsedByteCount;
            try
            {
                (parsedArea, parsedDbNum, parsedByteOffset, parsedBitOffset, parsedByteCount, parsedDataType)
                    = ParseS7Address(address);
            }
            catch (FormatException fex)
            {
                return (false, $"Address parse error: {fex.Message}", null, null);
            }

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy", null, null);
            try
            {
                var addr = BuildAbsoluteItemAddress(parsedArea, parsedDbNum, parsedByteOffset, parsedByteCount);
                var readList = new List<ItemAddress> { addr };

                // Use a tuple return from Task.Run to avoid out-param capture issues
                var (res, values, errors) = await Task.Run(() =>
                {
                    List<object> vals;
                    List<ulong> errs;
                    int r = state.Connection.ReadValues(readList, out vals, out errs);
                    return (r, vals, errs);
                });

                state.LastActivity = DateTime.UtcNow;

                if (res != 0)
                {
                    string err = $"ReadValues failed with code {res}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] ReadByAddressAsync: {err} for '{address}' on '{deviceId}'.");
                    return (false, err, null, null);
                }

                if (values == null || values.Count == 0 || values[0] == null)
                {
                    ulong readError = (errors != null && errors.Count > 0) ? errors[0] : 0;
                    string err = $"No value returned (error code 0x{readError:X})";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] ReadByAddressAsync: {err} for '{address}' on '{deviceId}'.");
                    return (false, err, null, null);
                }

                state.TotalReads++;

                // The result from a ClassicBlob read is a ValueBlob containing raw bytes
                object rawResult = values[0];
                byte[] blobBytes = null;

                if (rawResult is ValueBlob vb)
                    blobBytes = vb.GetValue();
                else
                {
                    // Unexpected type — return a string representation for diagnostics
                    Console.WriteLine($"[S7PlusService] ReadByAddressAsync: unexpected PValue type {rawResult.GetType().Name} for '{address}'.");
                    return (true, string.Empty, rawResult.ToString(), parsedDataType);
                }

                object value = ExtractValueFromBlob(blobBytes, parsedDataType, parsedBitOffset);
                Console.WriteLine($"[S7PlusService] ReadByAddress '{address}' = {value} [{parsedDataType}] on '{deviceId}'.");
                return (true, string.Empty, value, parsedDataType);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] ReadByAddressAsync exception for '{address}' on '{deviceId}': {ex.Message}");
                return (false, ex.Message, null, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        /// <summary>
        /// Writes a value to the PLC by absolute S7 address (e.g. "MW100", "Q0.0", "DB1.DBW4").
        /// For Bool addresses a read-modify-write is performed to preserve other bits in the byte.
        /// Returns (success, error).
        /// </summary>
        public async Task<(bool success, string error)>
            WriteByAddressAsync(string deviceId, string address, object value)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found or not connected.");

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.");

            string parsedArea, parsedDataType;
            int parsedDbNum, parsedByteOffset, parsedBitOffset, parsedByteCount;
            try
            {
                (parsedArea, parsedDbNum, parsedByteOffset, parsedBitOffset, parsedByteCount, parsedDataType)
                    = ParseS7Address(address);
            }
            catch (FormatException fex)
            {
                return (false, $"Address parse error: {fex.Message}");
            }

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy");
            try
            {
                var addr = BuildAbsoluteItemAddress(parsedArea, parsedDbNum, parsedByteOffset, parsedByteCount);

                byte[] existingBytes = null;

                // For Bool: read-modify-write to avoid clearing sibling bits in the same byte
                if (parsedDataType == "Bool")
                {
                    var readList2 = new List<ItemAddress> { addr };
                    var (rRes, rValues, _) = await Task.Run(() =>
                    {
                        List<object> rv;
                        List<ulong> re;
                        int r = state.Connection.ReadValues(readList2, out rv, out re);
                        return (r, rv, re);
                    });

                    if (rRes == 0 && rValues != null && rValues.Count > 0 && rValues[0] is ValueBlob rvb)
                        existingBytes = rvb.GetValue();
                    else
                        existingBytes = new byte[] { 0 }; // fallback: assume 0
                }

                byte[] blobData;
                try
                {
                    blobData = EncodeValueToBlob(value, parsedDataType, parsedBitOffset, existingBytes);
                }
                catch (Exception encEx)
                {
                    return (false, $"Encode error: {encEx.Message}");
                }

                // ValueBlob: BlobRootId = 0 for absolute access writes
                var pval = new ValueBlob(0, blobData);
                var writeList = new List<ItemAddress> { addr };
                var writeValues = new List<PValue> { pval };

                var (wRes, wErrors) = await Task.Run(() =>
                {
                    List<ulong> we;
                    int r = state.Connection.WriteValues(writeList, writeValues, out we);
                    return (r, we);
                });

                state.LastActivity = DateTime.UtcNow;

                if (wRes != 0)
                {
                    string err = $"WriteValues failed with code {wRes}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] WriteByAddressAsync: {err} for '{address}' on '{deviceId}'.");
                    return (false, err);
                }

                if (wErrors != null && wErrors.Count > 0 && wErrors[0] != 0)
                {
                    string err = $"Write error for '{address}': 0x{wErrors[0]:X}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] WriteByAddressAsync: {err} on '{deviceId}'.");
                    return (false, err);
                }

                state.TotalWrites++;
                Console.WriteLine($"[S7PlusService] WriteByAddress '{address}' = {value} OK on '{deviceId}'.");
                return (true, string.Empty);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] WriteByAddressAsync exception for '{address}' on '{deviceId}': {ex.Message}");
                return (false, ex.Message);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        /// <summary>
        /// Reads multiple PLC variables by absolute S7 address in a single driver call.
        /// Suitable for monitoring panels. Returns a list ordered as the input addresses.
        /// Each entry contains: { address, value, dataType, error }.
        /// </summary>
        public async Task<(bool success, string? error, List<object>? results)>
            ReadMultipleByAddressAsync(string deviceId, List<string> addresses)
        {
            if (addresses == null || addresses.Count == 0)
                return (false, "No addresses provided.", null);

            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found or not connected.", null);

            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' is not connected.", null);

            // Parse all addresses first — fail-fast on bad syntax
            var parsed = new List<(string area, int dbNum, int byteOffset, int bitOffset, int byteCount, string dataType)>();
            for (int i = 0; i < addresses.Count; i++)
            {
                try
                {
                    parsed.Add(ParseS7Address(addresses[i]));
                }
                catch (FormatException fex)
                {
                    return (false, $"Address parse error for '{addresses[i]}': {fex.Message}", null);
                }
            }

            if (!await state.PrioLock.WaitHighAsync(TimeSpan.FromSeconds(5)))
                return (false, "Lock timeout: PLC busy", null);
            try
            {
                // Build one ItemAddress per parsed entry
                var readList = new List<ItemAddress>();
                foreach (var p in parsed)
                    readList.Add(BuildAbsoluteItemAddress(p.area, p.dbNum, p.byteOffset, p.byteCount));

                var (res, values, errors) = await Task.Run(() =>
                {
                    List<object> vals;
                    List<ulong> errs;
                    int r = state.Connection.ReadValues(readList, out vals, out errs);
                    return (r, vals, errs);
                });

                state.LastActivity = DateTime.UtcNow;

                if (res != 0)
                {
                    string err = $"ReadValues failed with code {res}";
                    state.LastError = err;
                    state.TotalErrors++;
                    Console.WriteLine($"[S7PlusService] ReadMultipleByAddressAsync: {err} on '{deviceId}'.");
                    return (false, err, null);
                }

                state.TotalReads += addresses.Count;

                var results = new List<object>();
                for (int i = 0; i < addresses.Count; i++)
                {
                    object rawResult = (values != null && i < values.Count) ? values[i] : null;
                    ulong itemError = (errors != null && i < errors.Count) ? errors[i] : 0;

                    if (rawResult == null || itemError != 0)
                    {
                        results.Add(new
                        {
                            address = addresses[i],
                            value = (object)null,
                            dataType = parsed[i].dataType,
                            error = $"Read error 0x{itemError:X}",
                        });
                        continue;
                    }

                    if (rawResult is ValueBlob vb)
                    {
                        try
                        {
                            object extractedVal = ExtractValueFromBlob(vb.GetValue(), parsed[i].dataType, parsed[i].bitOffset);
                            results.Add(new
                            {
                                address = addresses[i],
                                value = extractedVal,
                                dataType = parsed[i].dataType,
                                error = (string)null,
                            });
                        }
                        catch (Exception extractEx)
                        {
                            results.Add(new
                            {
                                address = addresses[i],
                                value = (object)null,
                                dataType = parsed[i].dataType,
                                error = $"Extract error: {extractEx.Message}",
                            });
                        }
                    }
                    else
                    {
                        // Unexpected PValue type — return its string representation
                        results.Add(new
                        {
                            address = addresses[i],
                            value = (object)rawResult.ToString(),
                            dataType = parsed[i].dataType,
                            error = (string)null,
                        });
                    }
                }

                Console.WriteLine($"[S7PlusService] ReadMultipleByAddress: {addresses.Count} addresses read on '{deviceId}'.");
                return (true, string.Empty, results);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                Console.WriteLine($"[S7PlusService] ReadMultipleByAddressAsync exception on '{deviceId}': {ex.Message}");
                return (false, ex.Message, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // -------------------------------------------------------------------------
        // Helpers
        // -------------------------------------------------------------------------

        /// <summary>
        /// Converts a PlcTag's internal value to a JSON-serializable object.
        /// DateTime is returned as ISO-8601 string; arrays as plain object[].
        /// </summary>
        private static object SerializableValue(PlcTag tag)
        {
            switch (tag)
            {
                // Scalar types
                case PlcTagBool b:          return b.Value;
                case PlcTagByte by:         return by.Value;
                case PlcTagChar c:          return c.Value.ToString();
                case PlcTagWChar wc:        return wc.Value.ToString();
                case PlcTagWord w:          return w.Value;
                case PlcTagInt i:           return i.Value;
                case PlcTagDWord dw:        return dw.Value;
                case PlcTagDInt di:         return di.Value;
                case PlcTagReal r:          return r.Value;
                case PlcTagLReal lr:        return lr.Value;
                case PlcTagString s:        return s.Value;
                case PlcTagWString ws:      return ws.Value;
                case PlcTagSInt si:         return si.Value;
                case PlcTagUSInt us:        return us.Value;
                case PlcTagUInt ui:         return ui.Value;
                case PlcTagUDInt ud:        return ud.Value;
                case PlcTagLInt li:         return li.Value;
                case PlcTagULInt ul:        return ul.Value.ToString(); // ulong -> string (JS precision)
                case PlcTagLWord lw:        return lw.Value.ToString(); // ulong -> string
                case PlcTagTime t:          return t.Value;
                case PlcTagLTime lt:        return lt.Value;
                case PlcTagDate d:          return d.Value.ToString("yyyy-MM-dd");
                case PlcTagDateAndTime dt:  return dt.Value.ToString("o"); // ISO-8601
                case PlcTagDTL dtl:         return dtl.Value.ToString("o");
                // Array types — convert to plain object[] so JSON serializer handles them
                case PlcTagBoolArray ba:    return ba.Value;
                case PlcTagByteArray bya:   return bya.Value;
                case PlcTagWordArray wa:    return wa.Value;
                case PlcTagIntArray ia:     return ia.Value;
                case PlcTagDWordArray dwa:  return dwa.Value;
                case PlcTagDIntArray dia:   return dia.Value;
                case PlcTagRealArray ra:    return ra.Value;
                case PlcTagUSIntArray usa:  return usa.Value;
                case PlcTagUIntArray uia:   return uia.Value;
                case PlcTagUDIntArray uda:  return uda.Value;
                case PlcTagSIntArray sia:   return sia.Value;
                case PlcTagStringArray stra:
                    return stra.Value;
                default:
                    // Fallback: use ToString() which includes quality prefix
                    return tag.ToString();
            }
        }

        /// <summary>
        /// Sets the value of a PlcTag created by TagFactory before a write operation.
        /// <paramref name="rawValue"/> may come from JSON deserialization (JToken, string, bool, long, double…).
        /// </summary>
        private static void SetTagValue(PlcTag tag, object rawValue, uint softdatatype)
        {
            // Unwrap JToken if coming from Newtonsoft deserialization
            if (rawValue is JToken jt)
                rawValue = UnwrapJToken(jt);

            switch (tag)
            {
                case PlcTagBool b:         b.Value = Convert.ToBoolean(rawValue);                            break;
                case PlcTagByte by:        by.Value = Convert.ToByte(rawValue);                              break;
                case PlcTagChar c:         c.Value = SafeFirstChar(Convert.ToString(rawValue));              break;
                case PlcTagWChar wc:       wc.Value = SafeFirstChar(Convert.ToString(rawValue));             break;
                case PlcTagWord w:         w.Value = Convert.ToUInt16(rawValue);                             break;
                case PlcTagInt i:          i.Value = Convert.ToInt16(rawValue);                              break;
                case PlcTagDWord dw:       dw.Value = Convert.ToUInt32(rawValue);                            break;
                case PlcTagDInt di:        di.Value = Convert.ToInt32(rawValue);                             break;
                case PlcTagReal r:         r.Value = Convert.ToSingle(rawValue);                             break;
                case PlcTagLReal lr:       lr.Value = Convert.ToDouble(rawValue);                            break;
                case PlcTagString s:       s.Value = Convert.ToString(rawValue) ?? string.Empty;             break;
                case PlcTagWString ws:     ws.Value = Convert.ToString(rawValue) ?? string.Empty;            break;
                case PlcTagSInt si:        si.Value = Convert.ToSByte(rawValue);                             break;
                case PlcTagUSInt us:       us.Value = Convert.ToByte(rawValue);                              break;
                case PlcTagUInt ui:        ui.Value = Convert.ToUInt16(rawValue);                            break;
                case PlcTagUDInt ud:       ud.Value = Convert.ToUInt32(rawValue);                            break;
                case PlcTagLInt li:        li.Value = Convert.ToInt64(rawValue);                             break;
                case PlcTagULInt ul:       ul.Value = Convert.ToUInt64(rawValue);                            break;
                case PlcTagLWord lw:       lw.Value = Convert.ToUInt64(rawValue);                            break;
                default:
                    Console.WriteLine($"[S7PlusService] SetTagValue: unsupported tag type {tag.GetType().Name}.");
                    break;
            }
        }

        private static char SafeFirstChar(string s) =>
            string.IsNullOrEmpty(s) ? '\0' : s[0];

        private static object UnwrapJToken(JToken jt)
        {
            switch (jt.Type)
            {
                case JTokenType.Boolean: return jt.Value<bool>();
                case JTokenType.Integer: return jt.Value<long>();
                case JTokenType.Float:   return jt.Value<double>();
                case JTokenType.String:  return jt.Value<string>();
                default:                 return jt.ToString();
            }
        }

        // GetTypeName, ComputeS7Address, ComputeFullS7Address → moved to S7PlusBlockOperations

        // =====================================================================
        // GetBlocksAsync: discovers all blocks (DB, FB, FC, OB) from PLC
        // =====================================================================
        [HandleProcessCorruptedStateExceptions]
        public async Task<(bool success, string? error, List<object>? blocks)> GetBlocksAsync(string deviceId)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null);
            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' not connected.", null);

            Task<(bool success, string? error, List<object>? blocks)> blockTask;
            lock (state.BlockTaskSync)
            {
                if (state.PendingBlockTask != null && !state.PendingBlockTask.IsCompleted)
                {
                    Console.WriteLine($"[S7PlusService] GetBlocksAsync: join pending GetAllBlocks for '{deviceId}'.");
                    blockTask = state.PendingBlockTask;
                }
                else
                {
                    blockTask = _blockOperations.GetBlocksCoreAsync(deviceId, state);
                    state.PendingBlockTask = blockTask;
                }
            }

            try
            {
                return await blockTask;
            }
            finally
            {
                lock (state.BlockTaskSync)
                {
                    if (ReferenceEquals(state.PendingBlockTask, blockTask) && blockTask.IsCompleted)
                        state.PendingBlockTask = null;
                }
            }
        }

        // =====================================================================
        // GetBlockSchemaAsync: returns interface variables with Section info
        // Uses browse cache for DBs + InterfaceDescription XML for FB/FC/OB
        // =====================================================================
        public async Task<(bool success, string? error, object? schema)> GetBlockSchemaAsync(string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null);
            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' not connected.", null);

            // Find block info (refresh block list if not cached)
            var blockInfo = await _blockOperations.ResolveBlockInfoAsync(deviceId, blockName);
            if (blockInfo == null)
            {
                var (blocksOk, blocksErr, _) = await GetBlocksAsync(deviceId);
                if (!blocksOk)
                    Console.WriteLine($"[S7PlusService] GetBlocksAsync failed while loading schema for '{blockName}': {blocksErr}");
                blockInfo = await _blockOperations.ResolveBlockInfoAsync(deviceId, blockName);
            }

            // NEVER call BrowseAsync here — Browse can block PrioLock for up to 90s.
            // Use whatever is already in cache. Browse is only triggered by explicit API call.
            var blockVars = _blockOperations.FilterAndProjectBlockVariables(state, blockName);

            // For blocks with no browse vars, try InterfaceXml (with circuit breaker)
            var (interfaceXml, commentXml) = await _blockOperations.TryReadInterfaceXmlSafeAsync(
                deviceId, state, blockInfo, blockName, blockVars.Count > 0);

            return (true, null, S7PlusBlockOperations.BuildBlockSchemaResponse(
                blockName, blockInfo, blockVars, interfaceXml, commentXml));
        }

        public async Task<(bool success, string? error, object? body)> GetBlockBodyAsync(string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            // Find block RelID
            uint blockRelId = 0;
            var initialBlockInfo = await _blockOperations.ResolveBlockInfoAsync(deviceId, blockName);
            if (initialBlockInfo != null) blockRelId = initialBlockInfo.block_relid;

            if (blockRelId == 0)
            {
                var (blocksOk, blocksErr, blocksList) = await GetBlocksAsync(deviceId);
                if (blocksOk && state.CachedBlockList != null)
                {
                    var blockInfo = await _blockOperations.ResolveBlockInfoAsync(deviceId, blockName);
                    if (blockInfo != null) blockRelId = blockInfo.block_relid;
                }
            }

            if (blockRelId == 0)
                return (false, $"Block '{blockName}' not found or has no RelID", null);

            // GetBlockBody — LOW priority, reduced timeout (was 30s lock + 25s native)
            // Non dipendere dal browse simbolico: molti PLC riescono a restituire il body
            // anche quando il browse o InterfaceXml sono in circuit breaker.
            if (state.InterfaceXmlDisabled || (state.ConsecutiveBrowseFailures > 0 && !state.BrowseCacheValid))
            {
                Console.WriteLine($"[S7PlusService] GetBlockBodyAsync: bypass circuit breaker for '{blockName}' on '{deviceId}' (Browse failures: {state.ConsecutiveBrowseFailures}, InterfaceXml disabled: {state.InterfaceXmlDisabled})");
            }

            try
            {
                var (readOk, readErr, bodyXml, refDataXml, serverDiag) =
                    await _blockOperations.TryReadBodyXmlAsync(deviceId, state, blockRelId);

                state.LastActivity = DateTime.UtcNow;
                state.TotalReads++;

                if (!readOk)
                    return (false, readErr, null);

                var (networkCount, compileUnitCount, integrityWarning) =
                    _blockOperations.AnalyzeBodyIntegrity(bodyXml);

                if (integrityWarning != null)
                    Console.WriteLine($"[S7PlusService] GetBlockBodyAsync WARNING for '{blockName}': {integrityWarning}");

                return (true, null, S7PlusBlockOperations.BuildBlockBodyResponse(
                    blockName, blockRelId, bodyXml, refDataXml,
                    networkCount, compileUnitCount, integrityWarning, serverDiag));
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                return (false, $"Exception: {ex.Message}", null);
            }
        }

        /// <summary>
        /// ATTENZIONE: DiscoverBlockAttributes nel driver nativo puo' generare
        /// AccessViolationException che in .NET 4.8 termina il processo.
        /// HandleProcessCorruptedStateExceptions permette di intercettarla.
        /// </summary>
        [HandleProcessCorruptedStateExceptions]
        public async Task<(bool success, string? error, object? attributes)> DiscoverAttributesAsync(string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            // Find block RelID from cached block list
            uint blockRelId = 0;
            if (state.CachedBlockList != null)
            {
                var blockInfo = state.CachedBlockList.FirstOrDefault(b =>
                    b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
                if (blockInfo != null)
                {
                    blockRelId = blockInfo.block_relid;
                }
            }

            if (blockRelId == 0)
            {
                // Try to get blocks first
                var (blocksOk, blocksErr, blocksList) = await GetBlocksAsync(deviceId);
                if (blocksOk && state.CachedBlockList != null)
                {
                    var blockInfo = state.CachedBlockList.FirstOrDefault(b =>
                        b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
                    if (blockInfo != null) blockRelId = blockInfo.block_relid;
                }
            }

            if (blockRelId == 0)
                return (false, $"Block '{blockName}' not found or has no RelID", null);

            // DiscoverAttributes is HEAVY — LOW priority, 30s timeout
            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(30)))
                return (false, "Lock timeout: PLC busy with other operations", null);
            try
            {
                List<DiscoveredAttribute> discovered = null;
                int res = await SafeNativeCallAsync(() =>
                    state.Connection.DiscoverBlockAttributes(blockRelId, out discovered),
                    "DiscoverBlockAttributes", deviceId, 25000);

                state.LastActivity = DateTime.UtcNow;

                if (res <= -9997)
                    return (false, $"NATIVE CRASH/TIMEOUT in DiscoverBlockAttributes (code {res}). Operazione non supportata per questo blocco.", null);
                if (res < 0)
                    return (false, $"Driver error (code {res}) in DiscoverBlockAttributes", null);
                if (res != 0)
                    return (false, $"DiscoverBlockAttributes failed with code {res}", null);

                // Build response - truncate decompressed content for response size
                var result = discovered?.Select(a => new
                {
                    attributeId = a.AttributeId,
                    attributeIdHex = $"0x{a.AttributeId:X4}",
                    objectRelId = $"0x{a.ObjectRelId:X8}",
                    objectClassId = a.ObjectClassId,
                    valueType = a.ValueTypeName,
                    isBlob = a.IsBlob,
                    isNested = a.IsNested,
                    blobSize = a.BlobSize,
                    contentType = a.ContentType,
                    decompressedSize = a.DecompressedSize,
                    decompressError = a.DecompressError,
                    stringValue = a.StringValue,
                    numericValue = a.NumericValue > 0 ? (object)a.NumericValue : null,
                    // Include first 2000 chars of decompressed content as preview
                    contentPreview = a.DecompressedContent?.Length > 2000
                        ? a.DecompressedContent.Substring(0, 2000) + "..."
                        : a.DecompressedContent
                }).ToList();

                // Summary
                var bodyAttrs = discovered?.Where(a => a.ContentType != null && a.ContentType.StartsWith("BodyDescription")).ToList();

                return (true, null, new
                {
                    blockName = blockName,
                    blockRelId = $"0x{blockRelId:X8}",
                    totalAttributes = discovered?.Count ?? 0,
                    blobAttributes = discovered?.Count(a => a.IsBlob) ?? 0,
                    identifiedContent = discovered?.Count(a => a.ContentType != null) ?? 0,
                    bodyFound = bodyAttrs?.Any() == true,
                    bodyAttributeId = bodyAttrs?.FirstOrDefault()?.AttributeId,
                    bodyAttributeIdHex = bodyAttrs?.FirstOrDefault() != null ? $"0x{bodyAttrs.First().AttributeId:X4}" : null,
                    bodyContentType = bodyAttrs?.FirstOrDefault()?.ContentType,
                    attributes = result
                });
            }
            catch (AccessViolationException avEx)
            {
                state.TotalErrors++;
                state.LastError = $"NATIVE CRASH: {avEx.Message}";
                Serilog.Log.Error(avEx, "AccessViolationException (outer) in DiscoverAttributesAsync for {Block}", blockName);
                return (false, $"NATIVE CRASH: Il driver S7CommPlus ha generato un errore critico. Operazione non supportata.", null);
            }
            catch (Exception ex)
            {
                state.TotalErrors++;
                state.LastError = ex.Message;
                return (false, $"Exception: {ex.Message}", null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // SectionToName → moved to S7PlusBlockOperations

        // =====================================================================
        // S7 Raw Classic — Hybrid methods
        // These use the classic S7comm protocol in parallel with S7CommPlus
        // =====================================================================

        /// <summary>
        /// Ensures a parallel S7 Raw Classic connection exists for the given device.
        /// Creates it lazily on first use.
        /// </summary>
        private async Task<(bool success, string? error, S7RawClient? client, bool ephemeral)> EnsureRawClientAsync(string deviceId, string ip = null)
        {
            var state = string.IsNullOrWhiteSpace(deviceId) ? null : _manager.GetConnection(deviceId);

            if (state != null && state.IsConnected)
            {
                if (state.RawClient != null && state.RawClient.IsConnected)
                    return (true, null, state.RawClient, false);

                // Create and connect a new S7RawClient
                return await Task.Run(() =>
                {
                    try
                    {
                        var raw = new S7RawClient { Timeout = 10000 };
                        int res = raw.Connect(state.Config.Ip, 0, 0); // rack 0, slot 0 default
                        if (res == 0)
                        {
                            // Try rack 0, slot 1 (S7-1500 default) if slot 0 fails
                        }
                        else
                        {
                            // Retry with slot 1 (common for S7-1500)
                            Console.WriteLine($"[S7PlusService] S7Raw connect slot 0 failed ({res}), trying slot 1...");
                            res = raw.Connect(state.Config.Ip, 0, 1);
                        }

                        if (res != 0)
                        {
                            raw.Dispose();
                            return (false, $"S7 Raw Classic connection failed (code {res}). PUT/GET may not be enabled on PLC.", (S7RawClient)null, false);
                        }

                        state.RawClient = raw;
                        Console.WriteLine($"[S7PlusService] S7 Raw Classic connected to '{deviceId}' at {state.Config.Ip}");
                        return (true, (string)null, raw, false);
                    }
                    catch (Exception ex)
                    {
                        return (false, $"S7 Raw connect exception: {ex.Message}", (S7RawClient)null, false);
                    }
                });
            }

            if (string.IsNullOrWhiteSpace(ip))
            {
                if (state == null)
                    return (false, $"Device '{deviceId}' not found or not connected.", null, false);

                return (false, $"Device '{deviceId}' is not connected.", null, false);
            }

            // Fallback standalone raw connection by IP, used when classic PLC is connected but S7+ is not.
            return await Task.Run(() =>
            {
                try
                {
                    var raw = new S7RawClient { Timeout = 10000 };
                    int res = raw.Connect(ip, 0, 0); // rack 0, slot 0 default
                    if (res == 0)
                    {
                        // Try rack 0, slot 1 (S7-1500 default) if slot 0 fails
                    }
                    else
                    {
                        // Retry with slot 1 (common for S7-1500)
                        Console.WriteLine($"[S7PlusService] S7Raw connect slot 0 failed ({res}), trying slot 1...");
                        res = raw.Connect(ip, 0, 1);
                    }

                    if (res != 0)
                    {
                        raw.Dispose();
                        return (false, $"S7 Raw Classic connection failed (code {res}). PUT/GET may not be enabled on PLC.", (S7RawClient)null, true);
                    }

                    Console.WriteLine($"[S7PlusService] Standalone S7 Raw Classic connected to '{ip}'");
                    return (true, (string)null, raw, true);
                }
                catch (Exception ex)
                {
                    return (false, $"S7 Raw connect exception: {ex.Message}", (S7RawClient)null, true);
                }
            });
        }

        /// <summary>
        /// Reads SZL data from the PLC via classic S7 protocol.
        /// </summary>
        public async Task<(bool success, string? error, object? data)> ReadSzlAsync(string deviceId, ushort szlId, ushort szlIndex)
        {
            var (ok, err, raw, _) = await EnsureRawClientAsync(deviceId);
            if (!ok) return (false, err, null);

            return await Task.Run(() =>
            {
                try
                {
                    byte[] szlData = raw.ReadSzl(szlId, szlIndex);
                    if (szlData == null)
                        return (false, "SZL read returned null — function may not be supported.", (object)null);

                    return (true, (string)null, (object)new
                    {
                        szlId = $"0x{szlId:X4}",
                        szlIndex = $"0x{szlIndex:X4}",
                        dataLength = szlData.Length,
                        hexDump = S7RawClient.FormatHexDump(szlData, 16),
                        rawBase64 = Convert.ToBase64String(szlData)
                    });
                }
                catch (Exception ex)
                {
                    return (false, $"SZL read error: {ex.Message}", (object)null);
                }
            });
        }

        /// <summary>
        /// Gets CPU info via SZL 0x001C.
        /// </summary>
        public async Task<(bool success, string? error, object? cpuInfo)> GetCpuInfoRawAsync(string deviceId, string ip = null)
        {
            var (ok, err, raw, ephemeral) = await EnsureRawClientAsync(deviceId, ip);
            if (!ok) return (false, err, null);

            try
            {
                return await Task.Run(() =>
                {
                    try
                    {
                        var info = raw.ReadCpuInfo();
                        if (info == null)
                            return (false, "CPU info not available via classic S7.", (object)null);

                        return (true, (string)null, (object)new
                        {
                            moduleTypeName = info.ModuleTypeName,
                            serialNumber = info.SerialNumber,
                            plantId = info.PlantId,
                            copyright = info.Copyright,
                            moduleName = info.ModuleName,
                            orderCode = info.OrderCode,
                            memoryCardOrderCode = info.MemoryCardOrderCode,
                            firmwareVersion = info.FirmwareVersion,
                            bootLoaderVersion = info.BootLoaderVersion
                        });
                    }
                    catch (Exception ex)
                    {
                        return (false, $"CPU info error: {ex.Message}", (object)null);
                    }
                });
            }
            finally
            {
                if (ephemeral) raw?.Dispose();
            }
        }

        /// <summary>
        /// Lists block counts (how many OB, FB, FC, DB) via classic S7.
        /// </summary>
        public async Task<(bool success, string? error, object? blocks)> ListBlocksRawAsync(string deviceId, string ip = null)
        {
            var (ok, err, raw, ephemeral) = await EnsureRawClientAsync(deviceId, ip);
            if (!ok) return (false, err, null);

            try
            {
                return await Task.Run(() =>
                {
                    try
                    {
                        var counts = raw.ListBlocks();
                        if (counts == null)
                            return (false, "ListBlocks not available.", (object)null);

                        return (true, (string)null, (object)new
                        {
                            blockCounts = counts.Select(c => new
                            {
                                type = c.Type,
                                typeName = c.TypeName,
                                count = c.Count
                            }).ToList()
                        });
                    }
                    catch (Exception ex)
                    {
                        return (false, $"ListBlocks error: {ex.Message}", (object)null);
                    }
                });
            }
            finally
            {
                if (ephemeral) raw?.Dispose();
            }
        }

        /// <summary>
        /// Lists all block numbers of a given type via classic S7.
        /// </summary>
        public async Task<(bool success, string? error, object? entries)> ListBlocksOfTypeRawAsync(string deviceId, byte blockType, string ip = null)
        {
            var (ok, err, raw, ephemeral) = await EnsureRawClientAsync(deviceId, ip);
            if (!ok) return (false, err, null);

            try
            {
                return await Task.Run(() =>
                {
                    try
                    {
                        var entries = raw.ListBlocksOfType(blockType);
                        if (entries == null)
                            return (false, "ListBlocksOfType not available.", (object)null);

                        return (true, (string)null, (object)new
                        {
                            blockType = S7RawClient.BlockTypeName(blockType),
                            count = entries.Count,
                            blocks = entries.Select(e => new
                            {
                                number = e.Number,
                                flags = e.Flags,
                                language = e.Language
                            }).ToList()
                        });
                    }
                    catch (Exception ex)
                    {
                        return (false, $"ListBlocksOfType error: {ex.Message}", (object)null);
                    }
                });
            }
            finally
            {
                if (ephemeral) raw?.Dispose();
            }
        }

        /// <summary>
        /// Gets detailed block info (author, family, dates, size, language) via classic S7.
        /// </summary>
        public async Task<(bool success, string? error, object? info)> GetBlockInfoRawAsync(string deviceId, byte blockType, ushort blockNumber, string ip = null)
        {
            var (ok, err, raw, ephemeral) = await EnsureRawClientAsync(deviceId, ip);
            if (!ok) return (false, err, null);

            try
            {
                return await Task.Run(() =>
                {
                    try
                    {
                        var info = raw.GetBlockInfo(blockType, blockNumber);
                        if (info == null)
                            return (false, "Block info not available.", (object)null);

                        return (true, (string)null, (object)new
                        {
                            blockType = S7RawClient.BlockTypeName(info.BlockType),
                            blockNumber = info.BlockNumber,
                            language = info.BlockLang,
                            author = info.Author,
                            family = info.Family,
                            name = info.Name,
                            version = info.Version,
                            mcSize = info.McSize,
                            loadSize = info.LoadSize,
                            sbbLength = info.SbbLength,
                            checksum = $"0x{info.Checksum:X4}",
                            loadDate = info.LoadDate.ToString("yyyy-MM-dd HH:mm:ss"),
                            codeDate = info.CodeDate.ToString("yyyy-MM-dd HH:mm:ss")
                        });
                    }
                    catch (Exception ex)
                    {
                        return (false, $"Block info error: {ex.Message}", (object)null);
                    }
                });
            }
            finally
            {
                if (ephemeral) raw?.Dispose();
            }
        }

        /// <summary>
        /// Uploads a block's binary data (MC7/MC7+ bytecode) from the PLC via classic S7.
        /// </summary>
        public async Task<(bool success, string? error, object? blockData)> UploadBlockRawAsync(string deviceId, byte blockType, ushort blockNumber, string ip = null)
        {
            var (ok, err, raw, ephemeral) = await EnsureRawClientAsync(deviceId, ip);
            if (!ok) return (false, err, null);

            try
            {
                return await Task.Run(() =>
                {
                    try
                    {
                        var data = raw.UploadBlock(blockType, blockNumber);
                        if (data == null || data.RawData == null)
                            return (false, "Upload block returned null.", (object)null);

                        return (true, (string)null, (object)new
                        {
                            blockType = S7RawClient.BlockTypeName(blockType),
                            blockNumber = blockNumber,
                            dataSize = data.RawData.Length,
                            hexDump = data.HexDump,
                            rawBase64 = Convert.ToBase64String(data.RawData)
                        });
                    }
                    catch (Exception ex)
                    {
                        return (false, $"Upload block error: {ex.Message}", (object)null);
                    }
                });
            }
            finally
            {
                if (ephemeral) raw?.Dispose();
            }
        }

        /// <summary>
        /// Combined hybrid: returns block info (from S7 Raw) + schema (from S7CommPlus) + bytecode (from S7 Raw).
        /// </summary>
        public async Task<(bool success, string? error, object? hybrid)> GetBlockHybridAsync(string deviceId, string blockName, byte blockType, ushort blockNumber)
        {
            // Phase 1: S7CommPlus schema (browse + interface XML)
            var (schemaOk, schemaErr, schema) = await GetBlockSchemaAsync(deviceId, blockName);

            // Phase 2: S7 Raw block info + bytecode (parallel)
            object rawInfo = null;
            object rawData = null;
            string rawError = null;

            try
            {
                var (infoOk, infoErr, infoResult) = await GetBlockInfoRawAsync(deviceId, blockType, blockNumber);
                if (infoOk) rawInfo = infoResult;
                else rawError = infoErr;

                var (uploadOk, uploadErr, uploadResult) = await UploadBlockRawAsync(deviceId, blockType, blockNumber);
                if (uploadOk) rawData = uploadResult;
                else if (rawError == null) rawError = uploadErr;
            }
            catch (Exception ex)
            {
                rawError = ex.Message;
            }

            return (true, null, new
            {
                blockName,
                blockType = S7RawClient.BlockTypeName(blockType),
                blockNumber,
                // S7CommPlus data
                schema = schema,
                schemaAvailable = schemaOk,
                // S7 Raw Classic data
                rawInfo = rawInfo,
                rawData = rawData,
                rawAvailable = rawInfo != null || rawData != null,
                rawError = rawError
            });
        }
        // =====================================================================
        // #5 — Batch Block Body Reading
        // =====================================================================

        public async Task<(bool success, string? error, object? results)> GetBlockBodyBatchAsync(
            string deviceId, List<string> blockNames, int maxConcurrency = 2)
        {
            if (blockNames == null || blockNames.Count == 0)
                return (false, "blockNames list is empty", null);

            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            // Ensure block list is available
            await GetBlocksAsync(deviceId);

            var semaphore = new SemaphoreSlim(Math.Max(1, Math.Min(maxConcurrency, 4)));
            var tasks = blockNames.Select(async blockName =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var (ok, err, body) = await GetBlockBodyAsync(deviceId, blockName);
                    return new { blockName, success = ok, error = err, data = body };
                }
                catch (Exception ex)
                {
                    return new { blockName, success = false, error = (string?)ex.Message, data = (object)null };
                }
                finally
                {
                    semaphore.Release();
                }
            }).ToList();

            var results = await Task.WhenAll(tasks);
            int okCount = results.Count(r => r.success);

            return (true, null, new
            {
                totalRequested = blockNames.Count,
                totalSuccess = okCount,
                totalFailed = blockNames.Count - okCount,
                results = results.Select(r => new
                {
                    r.blockName,
                    r.success,
                    r.data,
                    r.error
                }).ToList()
            });
        }

        // =====================================================================
        // #4 — UDT/Struct Hierarchy Extraction
        // =====================================================================

        public async Task<(bool success, string? error, object? hierarchy)> GetTypeHierarchyAsync(
            string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            // Ensure browse cache is populated
            if (!state.BrowseCacheValid || state.CachedVarInfoList == null || state.CachedVarInfoList.Count == 0)
                await BrowseAsync(deviceId);

            var varList = state.CachedVarInfoList;
            if (varList == null || varList.Count == 0)
                return (false, "Browse cache is empty — cannot build type hierarchy", null);

            // Filter variables belonging to this block
            string prefix = blockName + ".";
            var blockVars = varList.Where(v =>
                v.Name.Equals(blockName, StringComparison.OrdinalIgnoreCase) ||
                v.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)).ToList();

            if (blockVars.Count == 0)
                return (false, $"No variables found for block '{blockName}'", null);

            // Check if it's a UDT
            bool isUDT = false;
            if (state.CachedBlockList != null)
            {
                var blockInfo = state.CachedBlockList.FirstOrDefault(b =>
                    b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
                if (blockInfo != null)
                    isUDT = blockInfo.block_type?.Equals("UDT", StringComparison.OrdinalIgnoreCase) == true;
            }

            // Build tree from flat dotted names
            var root = new Dictionary<string, object>
            {
                ["name"] = blockName,
                ["type"] = isUDT ? "UDT" : null,
                ["children"] = new List<object>()
            };

            foreach (var v in blockVars)
            {
                string relativeName = v.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                    ? v.Name.Substring(prefix.Length)
                    : "";

                if (string.IsNullOrEmpty(relativeName)) continue;

                var parts = relativeName.Split('.');
                var currentChildren = (List<object>)root["children"];

                for (int i = 0; i < parts.Length; i++)
                {
                    bool isLeaf = (i == parts.Length - 1);
                    string partName = parts[i];

                    // Find existing child node
                    var existing = currentChildren
                        .Cast<Dictionary<string, object>>()
                        .FirstOrDefault(c => (string)c["name"] == partName);

                    if (existing == null)
                    {
                        var node = new Dictionary<string, object> { ["name"] = partName };

                        if (isLeaf)
                        {
                            node["type"] = SoftdatatypeName(v.Softdatatype);
                            node["softdatatype"] = v.Softdatatype;
                            node["s7Address"] = S7PlusBlockOperations.ComputeFullS7Address(v);
                            node["optAddress"] = $"0x{v.OptAddress:X4}";
                            node["nonOptAddress"] = $"0x{v.NonOptAddress:X4}";
                            node["section"] = SectionName(v.Section);
                        }
                        else
                        {
                            node["type"] = "Struct";
                            node["children"] = new List<object>();
                        }

                        currentChildren.Add(node);
                        existing = node;
                    }

                    if (!isLeaf)
                    {
                        if (!existing.ContainsKey("children"))
                            existing["children"] = new List<object>();
                        currentChildren = (List<object>)existing["children"];
                    }
                }
            }

            return (true, null, new
            {
                blockName,
                isUDT,
                root,
                flatVariableCount = blockVars.Count
            });
        }

        private static string SectionName(int section)
        {
            switch (section)
            {
                case 1: return "VAR_INPUT";
                case 2: return "VAR_OUTPUT";
                case 3: return "VAR_IN_OUT";
                case 4: return "VAR_TEMP";
                case 5: return "VAR_STAT";
                case 6: return "CONSTANT";
                case 7: return "RETURN";
                default: return "VAR";
            }
        }

        private static string SoftdatatypeName(uint sdt)
        {
            switch (sdt)
            {
                case 0x01: return "Bool";
                case 0x02: return "Byte";
                case 0x03: return "Char";
                case 0x04: return "Word";
                case 0x05: return "Int";
                case 0x06: return "DWord";
                case 0x07: return "DInt";
                case 0x08: return "Real";
                case 0x09: return "Date";
                case 0x0A: return "Time_Of_Day";
                case 0x0B: return "Time";
                case 0x0C: return "S5Time";
                case 0x0E: return "Date_And_Time";
                case 0x13: return "String";
                case 0x30: return "LReal";
                case 0x31: return "ULInt";
                case 0x32: return "LInt";
                case 0x33: return "LWord";
                case 0x34: return "USInt";
                case 0x35: return "UInt";
                case 0x36: return "UDInt";
                case 0x37: return "SInt";
                case 0x48: return "WChar";
                case 0x49: return "WString";
                default: return $"Type_0x{sdt:X2}";
            }
        }

        // =====================================================================
        // #6 — FB ↔ Instance DB Correlation
        // =====================================================================

        public async Task<(bool success, string? error, object? instances)> GetInstanceDBsAsync(
            string deviceId, string fbName, bool readValues = false)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            // Ensure caches are populated
            await GetBlocksAsync(deviceId);
            if (!state.BrowseCacheValid || state.CachedVarInfoList == null)
                await BrowseAsync(deviceId);

            if (state.CachedBlockList == null)
                return (false, "Block list not available", null);

            // Find the FB
            var fbBlock = state.CachedBlockList.FirstOrDefault(b =>
                b.block_name.Equals(fbName, StringComparison.OrdinalIgnoreCase) &&
                (b.block_type?.Equals("FB", StringComparison.OrdinalIgnoreCase) == true));
            if (fbBlock == null)
                return (false, $"FB '{fbName}' not found in block list", null);

            // Get FB's variable names (interface)
            string fbPrefix = fbName + ".";
            var fbVarNames = state.CachedVarInfoList?
                .Where(v => v.Name.StartsWith(fbPrefix, StringComparison.OrdinalIgnoreCase))
                .Select(v => v.Name.Substring(fbPrefix.Length))
                .ToHashSet(StringComparer.OrdinalIgnoreCase) ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Scan all DBs to find instance DBs (DBs with matching variable structure)
            var dbBlocks = state.CachedBlockList
                .Where(b => b.block_type?.Equals("DB", StringComparison.OrdinalIgnoreCase) == true)
                .ToList();

            var instanceDBs = new List<object>();

            foreach (var db in dbBlocks)
            {
                string dbPrefix = db.block_name + ".";
                var dbVarNames = state.CachedVarInfoList?
                    .Where(v => v.Name.StartsWith(dbPrefix, StringComparison.OrdinalIgnoreCase))
                    .Select(v => v.Name.Substring(dbPrefix.Length))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase) ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (dbVarNames.Count == 0 || fbVarNames.Count == 0) continue;

                // Heuristic: if >=60% of FB variables appear in DB, it's likely an instance DB
                int matchCount = fbVarNames.Count(fn => dbVarNames.Contains(fn));
                double matchRatio = (double)matchCount / fbVarNames.Count;

                if (matchRatio >= 0.6)
                {
                    var dbResult = new Dictionary<string, object>
                    {
                        ["dbName"] = db.block_name,
                        ["dbNumber"] = db.block_number,
                        ["matchRatio"] = Math.Round(matchRatio, 2),
                        ["matchedVariables"] = matchCount,
                        ["totalFbVariables"] = fbVarNames.Count
                    };

                    if (readValues)
                    {
                        // Read first 20 variables from this instance DB
                        var dbVars = state.CachedVarInfoList
                            .Where(v => v.Name.StartsWith(dbPrefix, StringComparison.OrdinalIgnoreCase))
                            .Take(20)
                            .ToList();

                        var values = new List<object>();
                        foreach (var v in dbVars)
                        {
                            try
                            {
                                var (readOk, readErr, val, typeName, quality) = await ReadAsync(deviceId, v.Name);
                                values.Add(new
                                {
                                    name = v.Name,
                                    localName = v.Name.Substring(dbPrefix.Length),
                                    type = SoftdatatypeName(v.Softdatatype),
                                    value = readOk ? val : null,
                                    quality = readOk ? quality : "ERROR",
                                    error = readOk ? null : readErr
                                });
                            }
                            catch
                            {
                                values.Add(new { name = v.Name, localName = v.Name.Substring(dbPrefix.Length), type = SoftdatatypeName(v.Softdatatype), value = (object)null, quality = "ERROR", error = "Read exception" });
                            }
                        }
                        dbResult["variables"] = values;
                    }

                    instanceDBs.Add(dbResult);
                }
            }

            return (true, null, new
            {
                fbName,
                fbNumber = fbBlock.block_number,
                instanceDBs,
                instanceCount = instanceDBs.Count
            });
        }

        // =====================================================================
        // #3 — Cross-Reference Analysis
        // =====================================================================

        public async Task<(bool success, string? error, object? crossRefs)> GetCrossReferencesAsync(
            string deviceId, List<string> blockNames = null)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            await GetBlocksAsync(deviceId);
            if (state.CachedBlockList == null || state.CachedBlockList.Count == 0)
                return (false, "No blocks found on device", null);

            // Filter to code blocks (FB, FC, OB) — DBs have no executable code
            var codeBlocks = state.CachedBlockList
                .Where(b => b.block_type != null && (
                    b.block_type.Equals("FB", StringComparison.OrdinalIgnoreCase) ||
                    b.block_type.Equals("FC", StringComparison.OrdinalIgnoreCase) ||
                    b.block_type.Equals("OB", StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (blockNames != null && blockNames.Count > 0)
            {
                var nameSet = new HashSet<string>(blockNames, StringComparer.OrdinalIgnoreCase);
                codeBlocks = codeBlocks.Where(b => nameSet.Contains(b.block_name)).ToList();
            }

            var allBlockNames = new HashSet<string>(
                state.CachedBlockList.Select(b => b.block_name), StringComparer.OrdinalIgnoreCase);

            // Analyze each block's body XML
            var blockRefs = new System.Collections.Concurrent.ConcurrentDictionary<string, CrossRefEntry>(StringComparer.OrdinalIgnoreCase);
            var semaphore = new SemaphoreSlim(2);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            var tasks = codeBlocks.Select(async block =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var entry = new CrossRefEntry { Name = block.block_name, Type = block.block_type };
                    var (ok, err, bodyObj) = await GetBlockBodyAsync(deviceId, block.block_name);
                    if (ok && bodyObj != null)
                    {
                        // Extract bodyXml from anonymous object via reflection
                        string bodyXml = bodyObj.GetType().GetProperty("bodyXml")?.GetValue(bodyObj) as string;
                        string refDataXml = bodyObj.GetType().GetProperty("refDataXml")?.GetValue(bodyObj) as string;

                        if (!string.IsNullOrEmpty(bodyXml))
                        {
                            // Parse calls: look for block call references in XML
                            ParseCallReferences(bodyXml, entry, allBlockNames);
                            // Parse variable references: look for DB references
                            ParseVariableReferences(bodyXml, refDataXml, entry, allBlockNames);
                        }
                    }
                    blockRefs[block.block_name] = entry;
                }
                finally
                {
                    semaphore.Release();
                }
            }).ToList();

            await Task.WhenAll(tasks);
            sw.Stop();

            // Build calledBy / usedByBlocks inverse maps
            foreach (var kvp in blockRefs)
            {
                foreach (var calledBlock in kvp.Value.Calls)
                {
                    if (blockRefs.TryGetValue(calledBlock, out var target))
                        target.CalledBy.Add(kvp.Key);
                }
                foreach (var usedDb in kvp.Value.UsesDBs)
                {
                    if (blockRefs.TryGetValue(usedDb, out var target))
                        target.UsedByBlocks.Add(kvp.Key);
                }
            }

            // Find unreachable blocks (not called by anyone, not OB)
            var unreachable = blockRefs.Values
                .Where(e => e.CalledBy.Count == 0 &&
                            !e.Type.Equals("OB", StringComparison.OrdinalIgnoreCase))
                .Select(e => e.Name)
                .ToList();

            return (true, null, new
            {
                analyzed = blockRefs.Count,
                analysisTime = $"{sw.Elapsed.TotalSeconds:F1}s",
                blocks = blockRefs.Values.Select(e => new
                {
                    name = e.Name,
                    type = e.Type,
                    calls = e.Calls.ToList(),
                    calledBy = e.CalledBy.ToList(),
                    usesDBs = e.UsesDBs.ToList(),
                    usedByBlocks = e.UsedByBlocks.ToList()
                }).OrderBy(b => b.name).ToList(),
                unreachableBlocks = unreachable
            });
        }

        private class CrossRefEntry
        {
            public string Name { get; set; }
            public string Type { get; set; }
            public HashSet<string> Calls { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> CalledBy { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> UsesDBs { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> UsedByBlocks { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        private static void ParseCallReferences(string bodyXml, CrossRefEntry entry, HashSet<string> allBlockNames)
        {
            // Look for call patterns in LAD/FBD: <Part Name="Call" ... > with block name attributes
            // Pattern 1: CallBlock="FB_Motor" or similar
            var callBlockPattern = new System.Text.RegularExpressions.Regex(
                @"(?:CallBlock|BlockName|InstanceBlock)\s*=\s*""([^""]+)""",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            foreach (System.Text.RegularExpressions.Match m in callBlockPattern.Matches(bodyXml))
            {
                string name = m.Groups[1].Value;
                if (allBlockNames.Contains(name) && !name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                    entry.Calls.Add(name);
            }

            // Pattern 2: <CRef> elements
            var crefPattern = new System.Text.RegularExpressions.Regex(
                @"<CRef[^>]*?(?:CallType|BlockType)\s*=\s*""(\w+)""[^>]*?(?:BlockNumber|Number)\s*=\s*""(\d+)""",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            foreach (System.Text.RegularExpressions.Match m in crefPattern.Matches(bodyXml))
            {
                string refType = m.Groups[1].Value;
                string refNum = m.Groups[2].Value;
                string refName = $"{refType}{refNum}";
                if (allBlockNames.Contains(refName) && !refName.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                    entry.Calls.Add(refName);
            }

            // Pattern 3: SCL function calls — "BlockName"( in TokenText
            var sclCallPattern = new System.Text.RegularExpressions.Regex(
                @"""(\w+)""\s*\(",
                System.Text.RegularExpressions.RegexOptions.None);
            foreach (System.Text.RegularExpressions.Match m in sclCallPattern.Matches(bodyXml))
            {
                string name = m.Groups[1].Value;
                if (allBlockNames.Contains(name) && !name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                    entry.Calls.Add(name);
            }
        }

        private static void ParseVariableReferences(string bodyXml, string refDataXml, CrossRefEntry entry, HashSet<string> allBlockNames)
        {
            // Look for DB references in variable names: "DB_name.xxx"
            var displayNamePattern = new System.Text.RegularExpressions.Regex(
                @"DisplayName\s*=\s*""([^""\.]+)\.",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            foreach (System.Text.RegularExpressions.Match m in displayNamePattern.Matches(bodyXml))
            {
                string name = m.Groups[1].Value;
                if (allBlockNames.Contains(name) && !name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                    entry.UsesDBs.Add(name);
            }

            // Also check refDataXml for Ident references
            if (!string.IsNullOrEmpty(refDataXml))
            {
                var identPattern = new System.Text.RegularExpressions.Regex(
                    @"<Ident[^>]*?Name\s*=\s*""([^""\.]+)\.",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                foreach (System.Text.RegularExpressions.Match m in identPattern.Matches(refDataXml))
                {
                    string name = m.Groups[1].Value;
                    if (allBlockNames.Contains(name) && !name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                        entry.UsesDBs.Add(name);
                }
            }

            // SCL patterns: "DB_name".variable in TokenText
            var sclDbPattern = new System.Text.RegularExpressions.Regex(
                @"""(\w+)""\.",
                System.Text.RegularExpressions.RegexOptions.None);
            foreach (System.Text.RegularExpressions.Match m in sclDbPattern.Matches(bodyXml))
            {
                string name = m.Groups[1].Value;
                if (allBlockNames.Contains(name) && !name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase))
                    entry.UsesDBs.Add(name);
            }
        }

        // =====================================================================
        // #2 — LAD/FBD → SCL Decompiler (delegates to LadFbdDecompiler)
        // =====================================================================

        public async Task<(bool success, string? error, object? decompiled)> DecompileBlockAsync(
            string deviceId, string blockName, string providedBodyXml = null, string providedRefDataXml = null, string providedBlockType = null)
        {
            string bodyXml = providedBodyXml;
            string refDataXml = providedRefDataXml;
            string blockType = providedBlockType ?? "FC";

            if (string.IsNullOrEmpty(bodyXml))
            {
                // Fetch from PLC
                var (ok, err, bodyObj) = await GetBlockBodyAsync(deviceId, blockName);
                if (!ok) return (false, err, null);
                bodyXml = bodyObj?.GetType().GetProperty("bodyXml")?.GetValue(bodyObj) as string;
                refDataXml = bodyObj?.GetType().GetProperty("refDataXml")?.GetValue(bodyObj) as string;
            }

            if (string.IsNullOrEmpty(bodyXml))
                return (false, $"No body XML available for block '{blockName}'", null);

            // Determine block type from cached list if not provided
            if (providedBlockType == null)
            {
                var state = _manager.GetConnection(deviceId);
                if (state?.CachedBlockList != null)
                {
                    var bi = state.CachedBlockList.FirstOrDefault(b =>
                        b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
                    if (bi != null) blockType = bi.block_type ?? "FC";
                }
            }

            try
            {
                var result = LadFbdDecompiler.Decompile(bodyXml, refDataXml, blockName, blockType);
                return (true, null, result);
            }
            catch (Exception ex)
            {
                return (false, $"Decompilation failed: {ex.Message}", null);
            }
        }

        // =====================================================================
        // #8 — Block Snapshot & Diff
        // =====================================================================

        public async Task<(bool success, string? error, object? snapshot)> SaveBlockSnapshotAsync(
            string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);
            if (!state.IsConnected) return (false, $"Device '{deviceId}' not connected", null);

            var (ok, err, bodyObj) = await GetBlockBodyAsync(deviceId, blockName);
            if (!ok) return (false, err, null);

            string bodyXml = bodyObj?.GetType().GetProperty("bodyXml")?.GetValue(bodyObj) as string;
            string refDataXml = bodyObj?.GetType().GetProperty("refDataXml")?.GetValue(bodyObj) as string;

            int networkCount = 0;
            if (!string.IsNullOrEmpty(bodyXml))
            {
                int idx = 0;
                while ((idx = bodyXml.IndexOf("<Network", idx, StringComparison.Ordinal)) >= 0) { networkCount++; idx += 8; }
            }

            var snapshot = new BlockSnapshot
            {
                Id = Guid.NewGuid().ToString("N").Substring(0, 12),
                BlockName = blockName,
                Timestamp = DateTime.UtcNow,
                BodyXml = bodyXml,
                RefDataXml = refDataXml,
                NetworkCount = networkCount
            };

            var snapshots = state.BlockSnapshots.GetOrAdd(blockName,
                _ => new List<BlockSnapshot>());

            lock (snapshots)
            {
                snapshots.Add(snapshot);
                // Keep max 10 snapshots per block (FIFO)
                while (snapshots.Count > 10) snapshots.RemoveAt(0);
            }

            return (true, null, new
            {
                id = snapshot.Id,
                blockName = snapshot.BlockName,
                timestamp = snapshot.Timestamp.ToString("o"),
                networkCount = snapshot.NetworkCount,
                bodySize = snapshot.BodyXml?.Length ?? 0,
                refDataSize = snapshot.RefDataXml?.Length ?? 0,
                totalSnapshots = snapshots.Count
            });
        }

        public (bool success, string? error, object? snapshots) ListBlockSnapshotsSync(
            string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);

            if (!state.BlockSnapshots.TryGetValue(blockName, out var snapshots) || snapshots.Count == 0)
                return (true, null, new { blockName, count = 0, snapshots = new List<object>() });

            List<object> items;
            lock (snapshots)
            {
                items = snapshots.Select(s => (object)new
                {
                    id = s.Id,
                    blockName = s.BlockName,
                    timestamp = s.Timestamp.ToString("o"),
                    networkCount = s.NetworkCount,
                    bodySize = s.BodyXml?.Length ?? 0
                }).ToList();
            }

            return (true, null, new { blockName, count = items.Count, snapshots = items });
        }

        public async Task<(bool success, string? error, object? diff)> CompareBlockSnapshotsAsync(
            string deviceId, string blockName, string snapshotId1, string snapshotId2 = null)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null) return (false, $"Device '{deviceId}' not found", null);

            if (!state.BlockSnapshots.TryGetValue(blockName, out var snapshots))
                return (false, $"No snapshots for block '{blockName}'", null);

            BlockSnapshot snap1, snap2;
            lock (snapshots)
            {
                snap1 = snapshots.FirstOrDefault(s => s.Id == snapshotId1);
            }
            if (snap1 == null)
                return (false, $"Snapshot '{snapshotId1}' not found", null);

            if (string.IsNullOrEmpty(snapshotId2))
            {
                // Compare with live data
                var (ok, err, bodyObj) = await GetBlockBodyAsync(deviceId, blockName);
                if (!ok) return (false, err, null);
                string liveBody = bodyObj?.GetType().GetProperty("bodyXml")?.GetValue(bodyObj) as string;
                snap2 = new BlockSnapshot
                {
                    Id = "live",
                    BlockName = blockName,
                    Timestamp = DateTime.UtcNow,
                    BodyXml = liveBody,
                    NetworkCount = 0
                };
            }
            else
            {
                lock (snapshots) { snap2 = snapshots.FirstOrDefault(s => s.Id == snapshotId2); }
                if (snap2 == null) return (false, $"Snapshot '{snapshotId2}' not found", null);
            }

            // Compare network-by-network
            var networks1 = ExtractNetworks(snap1.BodyXml);
            var networks2 = ExtractNetworks(snap2.BodyXml);
            int maxNetworks = Math.Max(networks1.Count, networks2.Count);
            var changes = new List<object>();

            for (int i = 0; i < maxNetworks; i++)
            {
                string n1 = i < networks1.Count ? networks1[i] : null;
                string n2 = i < networks2.Count ? networks2[i] : null;

                string changeType;
                if (n1 == null) changeType = "added";
                else if (n2 == null) changeType = "removed";
                else if (n1 == n2) changeType = "unchanged";
                else changeType = "modified";

                if (changeType != "unchanged")
                {
                    changes.Add(new
                    {
                        networkIndex = i + 1,
                        changeType,
                        size1 = n1?.Length ?? 0,
                        size2 = n2?.Length ?? 0,
                        preview1 = n1 != null ? n1.Substring(0, Math.Min(n1.Length, 200)) : null,
                        preview2 = n2 != null ? n2.Substring(0, Math.Min(n2.Length, 200)) : null
                    });
                }
            }

            return (true, null, new
            {
                blockName,
                snapshot1 = new { id = snap1.Id, timestamp = snap1.Timestamp.ToString("o") },
                snapshot2 = new { id = snap2.Id, timestamp = snap2.Timestamp.ToString("o") },
                totalNetworks1 = networks1.Count,
                totalNetworks2 = networks2.Count,
                changedNetworks = changes.Count,
                identical = changes.Count == 0,
                changes
            });
        }

        private static List<string> ExtractNetworks(string bodyXml)
        {
            var networks = new List<string>();
            if (string.IsNullOrEmpty(bodyXml)) return networks;

            int start = 0;
            while ((start = bodyXml.IndexOf("<Network", start, StringComparison.Ordinal)) >= 0)
            {
                int end = bodyXml.IndexOf("</Network>", start, StringComparison.Ordinal);
                if (end < 0)
                {
                    // Try self-closing or end of string
                    end = bodyXml.IndexOf("/>", start, StringComparison.Ordinal);
                    if (end < 0) break;
                    networks.Add(bodyXml.Substring(start, end - start + 2));
                    start = end + 2;
                }
                else
                {
                    end += "</Network>".Length;
                    networks.Add(bodyXml.Substring(start, end - start));
                    start = end;
                }
            }
            return networks;
        }
    }

    // =====================================================================
    // #8 — BlockSnapshot data class
    // =====================================================================

    public class BlockSnapshot
    {
        public string Id { get; set; }
        public string BlockName { get; set; }
        public DateTime Timestamp { get; set; }
        public string BodyXml { get; set; }
        public string RefDataXml { get; set; }
        public int NetworkCount { get; set; }
    }
}
