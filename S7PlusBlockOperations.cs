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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using S7CommPlusDriver;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Block-centric operations extracted from S7PlusService.
    /// Centralizes block resolution, per-block XML reads, block listing with
    /// resilience patterns (cache, circuit breaker, deduplication), and
    /// response payload construction.
    /// </summary>
    internal sealed class S7PlusBlockOperations
    {
        private readonly S7PlusConnectionManager _manager;
        private readonly Func<Func<int>, string, string, int, Task<int>> _safeNativeCallAsync;

        public S7PlusBlockOperations(
            S7PlusConnectionManager manager,
            Func<Func<int>, string, string, int, Task<int>> safeNativeCallAsync)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _safeNativeCallAsync = safeNativeCallAsync ?? throw new ArgumentNullException(nameof(safeNativeCallAsync));
        }

        // =====================================================================
        // Block Resolution
        // =====================================================================

        public async Task<S7CommPlusConnection.BlockInfo?> ResolveBlockInfoAsync(string deviceId, string blockName)
        {
            var state = _manager.GetConnection(deviceId);
            if (state == null || !state.IsConnected || string.IsNullOrWhiteSpace(blockName))
                return null;

            var blockInfo = state.CachedBlockList?.FirstOrDefault(
                b => b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
            if (blockInfo != null)
                return blockInfo;

            await Task.CompletedTask;
            return state.CachedBlockList?.FirstOrDefault(
                b => b.block_name.Equals(blockName, StringComparison.OrdinalIgnoreCase));
        }

        // =====================================================================
        // Per-Block XML Reads (low-priority lock)
        // =====================================================================

        public async Task<(bool success, string? error, string? interfaceXml, string? commentXml)> TryReadInterfaceXmlAsync(
            string deviceId,
            S7PlusConnectionState state,
            S7CommPlusConnection.BlockInfo blockInfo)
        {
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null, null);
            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' not connected.", null, null);
            if (blockInfo == null)
                return (false, "Block info not available.", null, null);

            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(8)))
                return (false, "Lock timeout: PLC busy with other operations", null, null);

            try
            {
                string xmlIntf = null;
                string xmlComment = null;
                int res = await _safeNativeCallAsync(
                    () => state.Connection.GetBlockInterfaceXml(blockInfo.block_relid, out xmlIntf, out xmlComment),
                    "GetBlockInterfaceXml",
                    deviceId,
                    8000);

                if (res != 0 || string.IsNullOrEmpty(xmlIntf))
                    return (false, $"GetBlockInterfaceXml failed with code {res}", null, null);

                return (true, string.Empty, xmlIntf, xmlComment);
            }
            catch (Exception ex)
            {
                return (false, ex.Message, null, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        public async Task<(bool success, string? error, string? bodyXml, string? refDataXml, string? serverDiag)> TryReadBodyXmlAsync(
            string deviceId,
            S7PlusConnectionState state,
            uint blockRelId)
        {
            if (state == null)
                return (false, $"Device '{deviceId}' not found.", null, null, null);
            if (!state.IsConnected)
                return (false, $"Device '{deviceId}' not connected.", null, null, null);
            if (blockRelId == 0)
                return (false, "Block RelID not available.", null, null, null);

            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(10)))
                return (false, "Lock timeout: PLC busy with other operations", null, null, null);

            try
            {
                string bodyXml = null;
                string refDataXml = null;
                string serverDiag = null;
                int res = await _safeNativeCallAsync(
                    () => state.Connection.GetBlockBodyXml(blockRelId, out bodyXml, out refDataXml, out serverDiag),
                    "GetBlockBodyXml",
                    deviceId,
                    10000);

                if (res != 0)
                    return (false, $"GetBlockBodyXml failed with code {res}", null, null, null);

                return (true, string.Empty, bodyXml, refDataXml, serverDiag);
            }
            catch (Exception ex)
            {
                return (false, ex.Message, null, null, null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // =====================================================================
        // Body Integrity Analysis
        // =====================================================================

        public (int networkCount, int compileUnitCount, string? integrityWarning) AnalyzeBodyIntegrity(string? bodyXml)
        {
            int networkCount = 0;
            int compileUnitCount = 0;

            if (!string.IsNullOrEmpty(bodyXml))
            {
                int idx = 0;
                while ((idx = bodyXml.IndexOf("<Network", idx, StringComparison.Ordinal)) >= 0)
                {
                    networkCount++;
                    idx += 8;
                }

                idx = 0;
                while ((idx = bodyXml.IndexOf("<CompileUnit", idx, StringComparison.Ordinal)) >= 0)
                {
                    compileUnitCount++;
                    idx += 12;
                }
            }

            string? integrityWarning = null;
            if (compileUnitCount > 0 && networkCount < compileUnitCount)
                integrityWarning = $"Detected {networkCount} networks but {compileUnitCount} CompileUnits — some networks may be missing from child exploration";
            else if (networkCount == 0 && !string.IsNullOrEmpty(bodyXml) && bodyXml.Length > 100)
                integrityWarning = "Body XML present but no <Network> elements found";

            return (networkCount, compileUnitCount, integrityWarning);
        }

        // =====================================================================
        // InterfaceXml with Circuit Breaker (extracted from GetBlockSchemaAsync)
        // =====================================================================

        /// <summary>
        /// Attempts to read InterfaceXml for a block, respecting the circuit breaker
        /// and browse failure state. Updates state.InterfaceXmlFailures on failure.
        /// Returns (interfaceXml, commentXml) — both null if skipped or failed.
        /// </summary>
        public async Task<(string? interfaceXml, string? commentXml)> TryReadInterfaceXmlSafeAsync(
            string deviceId,
            S7PlusConnectionState state,
            S7CommPlusConnection.BlockInfo? blockInfo,
            string blockName,
            bool hasVariablesFromBrowse)
        {
            // Skip if browse already provided variables
            if (hasVariablesFromBrowse)
                return (null, null);

            // Skip if block info unavailable
            if (blockInfo == null)
                return (null, null);

            // Circuit breaker: skip if Browse failed on this PLC
            var browseFailed = state.ConsecutiveBrowseFailures > 0 && !state.BrowseCacheValid;
            if (browseFailed)
            {
                Console.WriteLine($"[S7PlusService] InterfaceXml skipped for '{blockName}' — Browse failed on this PLC (failures: {state.ConsecutiveBrowseFailures})");
                return (null, null);
            }

            // Circuit breaker: skip if InterfaceXml is disabled
            if (state.InterfaceXmlDisabled)
            {
                Console.WriteLine($"[S7PlusService] InterfaceXml skipped for '{blockName}' — circuit breaker open for '{deviceId}'");
                return (null, null);
            }

            try
            {
                var (readOk, readErr, xmlIntf, xmlComment) =
                    await TryReadInterfaceXmlAsync(deviceId, state, blockInfo);

                if (readOk && !string.IsNullOrEmpty(xmlIntf))
                {
                    // Reset circuit breaker on success
                    state.InterfaceXmlFailures = 0;
                    return (xmlIntf, xmlComment);
                }
                else
                {
                    state.InterfaceXmlFailures++;
                    state.InterfaceXmlLastFailUtc = DateTime.UtcNow;
                    Console.WriteLine($"[S7PlusService] GetBlockInterfaceXml failed for '{blockName}': {readErr} (failures: {state.InterfaceXmlFailures})");
                    if (state.InterfaceXmlDisabled)
                        Console.WriteLine($"[S7PlusService] InterfaceXml CIRCUIT BREAKER OPEN for '{deviceId}' — skipping for 10 min");
                    return (null, null);
                }
            }
            catch (Exception ex)
            {
                state.InterfaceXmlFailures++;
                state.InterfaceXmlLastFailUtc = DateTime.UtcNow;
                Console.WriteLine($"[S7PlusService] GetBlockInterfaceXml exception: {ex.Message} (failures: {state.InterfaceXmlFailures})");
                return (null, null);
            }
        }

        // =====================================================================
        // Browse Variable Filtering (extracted from GetBlockSchemaAsync)
        // =====================================================================

        /// <summary>
        /// Filters and projects browse-cached variables for a specific block.
        /// Returns a list of anonymous objects matching the schema API contract.
        /// </summary>
        public List<object> FilterAndProjectBlockVariables(S7PlusConnectionState state, string blockName)
        {
            return (state.CachedVarInfoList ?? new List<VarInfo>())
                .Where(v => v.Name.StartsWith(blockName + ".", StringComparison.OrdinalIgnoreCase)
                         || v.Name.Equals(blockName, StringComparison.OrdinalIgnoreCase))
                .Select(v => (object)new
                {
                    name = v.Name,
                    localName = v.Name.Contains(".") ? v.Name.Substring(v.Name.IndexOf('.') + 1) : v.Name,
                    type = GetTypeName(v.Softdatatype),
                    softdatatype = v.Softdatatype,
                    section = v.Section,
                    sectionName = SectionToName(v.Section),
                    accessSequence = v.AccessSequence,
                    s7Address = ComputeFullS7Address(v),
                    optAddress = v.OptAddress,
                    optBitoffset = v.OptBitoffset,
                    nonOptAddress = v.NonOptAddress,
                    nonOptBitoffset = v.NonOptBitoffset,
                })
                .ToList();
        }

        // =====================================================================
        // Response Payload Builders
        // =====================================================================

        /// <summary>
        /// Builds the response object for GetBlockSchemaAsync.
        /// </summary>
        public static object BuildBlockSchemaResponse(
            string blockName,
            S7CommPlusConnection.BlockInfo blockInfo,
            List<object> blockVars,
            string? interfaceXml,
            string? commentXml)
        {
            return new
            {
                blockName = blockName,
                blockType = blockInfo?.block_type ?? "DB",
                blockNumber = blockInfo?.block_number ?? 0,
                comment = blockInfo?.block_comment ?? commentXml,
                variableCount = blockVars.Count,
                variables = blockVars,
                interfaceXml = interfaceXml
            };
        }

        /// <summary>
        /// Builds the response object for GetBlockBodyAsync.
        /// </summary>
        public static object BuildBlockBodyResponse(
            string blockName, uint blockRelId,
            string? bodyXml, string? refDataXml,
            int networkCount, int compileUnitCount,
            string? integrityWarning, string? serverDiag)
        {
            return new
            {
                blockName = blockName,
                blockRelId = $"0x{blockRelId:X8}",
                bodyXml = bodyXml,
                refDataXml = refDataXml,
                hasBody = !string.IsNullOrEmpty(bodyXml),
                hasRefData = !string.IsNullOrEmpty(refDataXml),
                bodySize = bodyXml?.Length ?? 0,
                refDataSize = refDataXml?.Length ?? 0,
                networkIntegrity = new
                {
                    detected = networkCount,
                    compileUnits = compileUnitCount,
                    valid = networkCount >= compileUnitCount || compileUnitCount == 0,
                    warning = integrityWarning
                },
                _debug = new
                {
                    networkTagCount = networkCount,
                    bodyPreview = bodyXml?.Length > 0 ? bodyXml.Substring(0, Math.Min(bodyXml.Length, 800)) : "",
                    bodyTail = bodyXml?.Length > 800 ? bodyXml.Substring(bodyXml.Length - Math.Min(bodyXml.Length, 400)) : "",
                    hasXmlDecl = bodyXml?.Contains("<?xml") ?? false,
                    xmlDeclCount = bodyXml != null ? Regex.Matches(bodyXml, @"<\?xml").Count : 0,
                    serverDiag = serverDiag ?? ""
                }
            };
        }

        // =====================================================================
        // Block Listing with Resilience (moved from S7PlusService)
        // =====================================================================

        internal static Dictionary<string, int> BuildVarCounts(S7PlusConnectionState state)
        {
            var varCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            if (state?.CachedVarInfoList == null)
                return varCounts;

            foreach (var v in state.CachedVarInfoList)
            {
                var blockName = v.Name.Split('.')[0];
                if (!varCounts.ContainsKey(blockName))
                    varCounts[blockName] = 0;
                varCounts[blockName]++;
            }

            return varCounts;
        }

        internal static List<object> ToBlockResult(
            IEnumerable<S7CommPlusConnection.BlockInfo> blockList,
            Dictionary<string, int> varCounts)
        {
            return (blockList ?? Enumerable.Empty<S7CommPlusConnection.BlockInfo>())
                .Select(b => (object)new
                {
                    name = b.block_name,
                    number = b.block_number,
                    type = b.block_type,
                    relid = b.block_relid.ToString("X8"),
                    classId = b.class_id,
                    comment = b.block_comment,
                    varCount = varCounts != null && varCounts.ContainsKey(b.block_name) ? varCounts[b.block_name] : 0
                })
                .ToList();
        }

        internal static bool HasUsableBlockCache(S7PlusConnectionState state)
        {
            return state?.CachedBlockList != null && state.CachedBlockList.Count > 0;
        }

        internal static bool HasUsableBrowseCache(S7PlusConnectionState state)
        {
            return state?.CachedVarInfoList != null && state.CachedVarInfoList.Count > 0;
        }

        internal static bool IsBlockDiscoveryTimeoutLike(string error)
        {
            if (string.IsNullOrWhiteSpace(error))
                return false;

            string normalized = error.ToLowerInvariant();
            return normalized.Contains("code 5") ||
                   normalized.Contains("timeout") ||
                   normalized.Contains("did not respond") ||
                   normalized.Contains("native crash") ||
                   normalized.Contains("lock timeout");
        }

        /// <summary>
        /// Core block listing with resilience: cache, fast-fail, deduplication, stale fallback.
        /// </summary>
        public async Task<(bool success, string? error, List<object>? blocks)> GetBlocksCoreAsync(
            string deviceId,
            S7PlusConnectionState state)
        {
            var now = DateTime.UtcNow;
            var hasCachedBlocks = HasUsableBlockCache(state);
            var recentBlockCache = hasCachedBlocks &&
                state.LastBlockListSuccessUtc != default(DateTime) &&
                now - state.LastBlockListSuccessUtc < TimeSpan.FromMinutes(5);
            var varCounts = BuildVarCounts(state);

            if (recentBlockCache)
            {
                Console.WriteLine($"[S7PlusService] GetBlocksAsync: returning {state.CachedBlockList.Count} cached blocks for '{deviceId}'.");
                return (true, null, ToBlockResult(state.CachedBlockList, varCounts));
            }

            if (!hasCachedBlocks &&
                state.ConsecutiveBlockListFailures >= 2 &&
                state.LastBlockListFailureUtc != default(DateTime) &&
                now - state.LastBlockListFailureUtc < TimeSpan.FromSeconds(20))
            {
                string fastFailReason = string.IsNullOrWhiteSpace(state.LastBlockListError)
                    ? "recent repeated GetAllBlocks failures"
                    : state.LastBlockListError;
                Console.WriteLine($"[S7PlusService] GetBlocksAsync: fast-fail cooldown for '{deviceId}' after repeated failures: {fastFailReason}");
                return (false, $"GetAllBlocks temporarily cooled down after repeated failures: {fastFailReason}", null);
            }

            if (!await state.PrioLock.WaitLowAsync(TimeSpan.FromSeconds(18)))
            {
                if (hasCachedBlocks)
                {
                    Console.WriteLine($"[S7PlusService] GetBlocksAsync: lock timeout for '{deviceId}', returning stale cached blocks.");
                    return (true, "Using cached block list: PLC busy with other operations", ToBlockResult(state.CachedBlockList, varCounts));
                }

                state.LastBlockListFailureUtc = DateTime.UtcNow;
                state.ConsecutiveBlockListFailures++;
                state.LastBlockListError = "Lock timeout: PLC busy with other operations";
                return (false, state.LastBlockListError, null);
            }

            try
            {
                if (HasUsableBlockCache(state) &&
                    state.LastBlockListSuccessUtc != default(DateTime) &&
                    DateTime.UtcNow - state.LastBlockListSuccessUtc < TimeSpan.FromMinutes(5))
                {
                    return (true, null, ToBlockResult(state.CachedBlockList, BuildVarCounts(state)));
                }

                List<S7CommPlusConnection.BlockInfo> blockList = null;
                int res = await _safeNativeCallAsync(() => state.Connection.GetAllBlocks(out blockList), "GetAllBlocks", deviceId, 25000);
                if (res != 0)
                {
                    state.LastBlockListFailureUtc = DateTime.UtcNow;
                    state.ConsecutiveBlockListFailures++;
                    state.LastBlockListError = $"GetAllBlocks failed with code {res}";

                    if (hasCachedBlocks)
                    {
                        Console.WriteLine($"[S7PlusService] GetBlocksAsync: GetAllBlocks failed for '{deviceId}', serving stale cache.");
                        return (true, $"Using cached block list after failure: {state.LastBlockListError}", ToBlockResult(state.CachedBlockList, varCounts));
                    }

                    // If browse cache is available, let the caller fall back to symbolic browse instead of hammering GetAllBlocks.
                    if (HasUsableBrowseCache(state) && IsBlockDiscoveryTimeoutLike(state.LastBlockListError))
                    {
                        Console.WriteLine($"[S7PlusService] GetBlocksAsync: deferring to browse-derived fallback for '{deviceId}' after {state.LastBlockListError}.");
                    }

                    return (false, state.LastBlockListError, null);
                }

                state.CachedBlockList = blockList ?? new List<S7CommPlusConnection.BlockInfo>();
                state.LastBlockListSuccessUtc = DateTime.UtcNow;
                state.LastBlockListFailureUtc = default(DateTime);
                state.ConsecutiveBlockListFailures = 0;
                state.LastBlockListError = string.Empty;
                state.LastActivity = DateTime.UtcNow;
                _manager.RememberGoodBlockList(deviceId, state.CachedBlockList, state.LastBlockListSuccessUtc);
                return (true, null, ToBlockResult(state.CachedBlockList, BuildVarCounts(state)));
            }
            catch (Exception ex)
            {
                state.LastBlockListFailureUtc = DateTime.UtcNow;
                state.ConsecutiveBlockListFailures++;
                state.LastBlockListError = ex.Message;

                if (hasCachedBlocks)
                {
                    Console.WriteLine($"[S7PlusService] GetBlocksAsync exception for '{deviceId}', serving stale cache: {ex.Message}");
                    return (true, $"Using cached block list after exception: {ex.Message}", ToBlockResult(state.CachedBlockList, varCounts));
                }

                return (false, $"GetAllBlocks exception: {ex.Message}", null);
            }
            finally
            {
                state.PrioLock.Release();
            }
        }

        // =====================================================================
        // Type & Address Utilities (moved from S7PlusService)
        // =====================================================================

        internal static string GetTypeName(uint softdatatype)
        {
            if (Softdatatype.Types.TryGetValue(softdatatype, out string name))
                return name;
            return $"Unknown(0x{softdatatype:X})";
        }

        /// <summary>
        /// Calculates the S7 absolute address for a variable (e.g. "DBB0", "DBX0.7", "DBW4", "DBD8")
        /// using OptAddress (byte offset) and OptBitoffset, plus softdatatype to determine size prefix.
        /// </summary>
        internal static string ComputeS7Address(VarInfo v)
        {
            if (v == null) return null;
            uint byteOff = v.OptAddress;
            int bitOff = v.OptBitoffset;
            uint sdt = v.Softdatatype;

            // S7CommPlus Softdatatype → S7 address prefix
            // Reference: Softdatatype.cs constants
            string prefix;
            bool isBit = false;
            switch (sdt)
            {
                case 1:  // Bool
                    prefix = "DBX"; isBit = true; break;
                case 2:  // Byte
                case 3:  // Char
                case 40: // BBOOL (byte-sized bool)
                case 52: // USInt
                case 55: // SInt
                case 56: // BCD8
                    prefix = "DBB"; break;
                case 4:  // Word
                case 5:  // Int
                case 9:  // Date
                case 12: // S5Time
                case 13: // S5Count
                case 53: // UInt
                case 57: // BCD16
                    prefix = "DBW"; break;
                case 6:  // DWord
                case 7:  // DInt
                case 8:  // Real
                case 10: // Time_Of_Day
                case 11: // Time
                case 54: // UDInt
                case 58: // BCD32
                    prefix = "DBD"; break;
                case 48: // LReal
                case 49: // ULInt
                case 50: // LInt
                case 51: // LWord
                case 59: // BCD64
                case 64: // LTime
                case 65: // LTOD
                case 66: // LDT
                    prefix = "DBX"; isBit = false; break; // 8-byte types, show byte offset
                case 14: // Date_And_Time (8 bytes)
                case 67: // DTL (12 bytes)
                case 19: // String (variable)
                case 62: // WString (variable)
                    prefix = "DBB"; break; // variable/large size, show byte offset
                default:
                    prefix = "DBB"; break;
            }

            if (isBit)
                return $"{prefix}{byteOff}.{bitOff}";
            else
                return $"{prefix}{byteOff}";
        }

        /// <summary>
        /// Builds a full S7 address with DB prefix (e.g. "DB10.DBX0.7", "DB5.DBW4")
        /// </summary>
        internal static string ComputeFullS7Address(VarInfo v)
        {
            if (v == null) return null;
            string addr = ComputeS7Address(v);
            if (addr == null) return null;

            // Extract DB number from block name (e.g. "RICETTE" -> need blockNumber)
            // If blockName looks like "DB<n>", use it directly
            string bn = v.BlockName ?? "";
            if (bn.StartsWith("DB", StringComparison.OrdinalIgnoreCase) && int.TryParse(bn.Substring(2), out _))
                return $"{bn}.{addr}";

            // Otherwise just return the offset part (block name is symbolic)
            return addr;
        }

        internal static string SectionToName(int section)
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
    }
}
