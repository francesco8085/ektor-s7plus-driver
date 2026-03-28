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
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// HTTP request handler for the S7CommPlus integration endpoints.
    /// Each Handle* method parses a JSON request body, delegates to the
    /// appropriate service method, and returns an anonymous object that
    /// the caller serializes back to JSON.
    ///
    /// Error contract: every method returns { success=false, error="..." }
    /// on failure — exceptions are never propagated to the caller.
    /// </summary>
    public class S7PlusApiHandler
    {
        private readonly S7PlusConnectionManager _manager;
        private readonly S7PlusService _service;

        public S7PlusApiHandler(S7PlusConnectionManager manager, S7PlusService service)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _service = service ?? throw new ArgumentNullException(nameof(service));
        }

        // -------------------------------------------------------------------------
        // Connect
        // -------------------------------------------------------------------------

        /// <summary>
        /// Expected JSON body:
        /// {
        ///   "deviceId": "plc1",     // required
        ///   "name":     "My PLC",   // optional, defaults to deviceId
        ///   "ip":       "192.168.1.10",
        ///   "port":     102,         // optional, default 102
        ///   "password": "",          // optional
        ///   "useTls":   false,       // optional
        ///   "timeout":  10000        // optional, ms
        /// }
        /// </summary>
        public async Task<object> HandleConnect(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null)
                    return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                string ip = GetString(req, "ip");

                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");
                if (string.IsNullOrWhiteSpace(ip))
                    return ErrorResponse("Field 'ip' is required.");

                var config = new S7PlusConfig
                {
                    Id = deviceId,
                    Name = GetString(req, "name") ?? deviceId,
                    Ip = ip,
                    Port = GetInt(req, "port", 102),
                    Password = GetString(req, "password") ?? string.Empty,
                    UseTls = GetBool(req, "useTls", false),
                    Timeout = GetInt(req, "timeout", 10000),
                    Enabled = true,
                };

                Console.WriteLine($"[S7PlusApiHandler] HandleConnect: deviceId='{deviceId}' ip='{ip}'");

                var result = await _manager.ConnectAsync(config);
                if (!result.Success)
                    return ErrorResponse(result.Error);

                return new { success = true, deviceId = deviceId, message = "Connected successfully." };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleConnect exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Disconnect
        // -------------------------------------------------------------------------

        /// <summary>
        /// Expected JSON body: { "deviceId": "plc1" }
        /// </summary>
        public async Task<object> HandleDisconnect(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");

                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");

                Console.WriteLine($"[S7PlusApiHandler] HandleDisconnect: deviceId='{deviceId}'");

                var result = await _manager.DisconnectAsync(deviceId);
                if (!result.Success)
                    return ErrorResponse(result.Error);

                return new { success = true, deviceId = deviceId, message = "Disconnected." };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleDisconnect exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Read (single or batch)
        // -------------------------------------------------------------------------

        /// <summary>
        /// Single read:  { "deviceId": "plc1", "variable": "DB1.myTag" }
        /// Batch read:   { "deviceId": "plc1", "variables": ["DB1.tag1", "DB1.tag2"] }
        /// </summary>
        public async Task<object> HandleRead(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null)
                    return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");

                // Check if this is a batch request (variables[]) or single (variable)
                if (req.ContainsKey("variables") && req["variables"] is JArray variablesArray)
                {
                    var names = new List<string>();
                    foreach (var item in variablesArray)
                        names.Add(item.ToString());

                    Console.WriteLine($"[S7PlusApiHandler] HandleRead batch: {names.Count} variables on '{deviceId}'.");

                    var (success, error, results) = await _service.ReadBatchAsync(deviceId, names);
                    if (!success)
                        return ErrorResponse(error);

                    return new { success = true, deviceId = deviceId, results = results };
                }
                else
                {
                    string variable = GetString(req, "variable");
                    if (string.IsNullOrWhiteSpace(variable))
                        return ErrorResponse("Field 'variable' or 'variables' is required.");

                    Console.WriteLine($"[S7PlusApiHandler] HandleRead single: '{variable}' on '{deviceId}'.");

                    var (success, error, value, typeName, quality) = await _service.ReadAsync(deviceId, variable);
                    if (!success)
                        return ErrorResponse(error);

                    return new
                    {
                        success = true,
                        deviceId = deviceId,
                        variable = variable,
                        value = value,
                        typeName = typeName,
                        quality = quality,
                    };
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleRead exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Write
        // -------------------------------------------------------------------------

        /// <summary>
        /// Expected JSON body:
        /// {
        ///   "deviceId":  "plc1",
        ///   "variable":  "DB1.myTag",
        ///   "value":     42,
        ///   "dataType":  0      // optional softdatatype override (uint)
        /// }
        /// </summary>
        public async Task<object> HandleWrite(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null)
                    return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                string variable = GetString(req, "variable");

                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");
                if (string.IsNullOrWhiteSpace(variable))
                    return ErrorResponse("Field 'variable' is required.");
                if (!req.ContainsKey("value"))
                    return ErrorResponse("Field 'value' is required.");

                object value = req.ContainsKey("value") ? req["value"] : null;
                uint dataType = (uint)GetInt(req, "dataType", 0);

                Console.WriteLine($"[S7PlusApiHandler] HandleWrite: '{variable}'={value} on '{deviceId}'.");

                var (success, error) = await _service.WriteAsync(deviceId, variable, value, dataType);
                if (!success)
                    return ErrorResponse(error);

                return new { success = true, deviceId = deviceId, variable = variable, message = "Write successful." };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleWrite exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Read by absolute S7 address (I0.0, QW0, MW100, DB1.DBW0)
        // -------------------------------------------------------------------------

        public async Task<object> HandleReadAddress(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null) return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                if (string.IsNullOrWhiteSpace(deviceId)) return ErrorResponse("Field 'deviceId' is required.");

                // Single address or multiple addresses
                string address = GetString(req, "address");
                var addresses = req.ContainsKey("addresses") ? req["addresses"] as Newtonsoft.Json.Linq.JArray : null;

                if (addresses != null && addresses.Count > 0)
                {
                    // Batch read
                    var addrList = addresses.Select(a => a.ToString()).ToList();
                    Console.WriteLine($"[S7PlusApiHandler] HandleReadAddress batch: deviceId='{deviceId}' count={addrList.Count}");
                    var (success, error, results) = await _service.ReadMultipleByAddressAsync(deviceId, addrList);
                    if (!success) return ErrorResponse(error);
                    return new { success = true, deviceId, count = results.Count, results };
                }
                else if (!string.IsNullOrWhiteSpace(address))
                {
                    // Single read
                    Console.WriteLine($"[S7PlusApiHandler] HandleReadAddress: deviceId='{deviceId}' address='{address}'");
                    var (success, error, value, dataType) = await _service.ReadByAddressAsync(deviceId, address);
                    if (!success) return ErrorResponse(error);
                    return new { success = true, deviceId, address, value, dataType };
                }
                else
                {
                    return ErrorResponse("Field 'address' or 'addresses' is required.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleReadAddress exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Write by absolute S7 address
        // -------------------------------------------------------------------------

        public async Task<object> HandleWriteAddress(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null) return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                string address = GetString(req, "address");

                if (string.IsNullOrWhiteSpace(deviceId)) return ErrorResponse("Field 'deviceId' is required.");
                if (string.IsNullOrWhiteSpace(address)) return ErrorResponse("Field 'address' is required.");

                object value = req.ContainsKey("value") ? req["value"] : null;
                if (value is Newtonsoft.Json.Linq.JValue jv) value = jv.Value;

                Console.WriteLine($"[S7PlusApiHandler] HandleWriteAddress: deviceId='{deviceId}' address='{address}' value='{value}'");

                var (success, error) = await _service.WriteByAddressAsync(deviceId, address, value);
                if (!success) return ErrorResponse(error);

                return new { success = true, deviceId, address, message = "Write successful." };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleWriteAddress exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Browse
        // -------------------------------------------------------------------------

        /// <summary>
        /// Expected JSON body:
        /// { "deviceId": "plc1", "filter": "DB1" }  // filter is optional substring match on name
        /// </summary>
        public async Task<object> HandleBrowse(string body)
        {
            try
            {
                var req = ParseBody(body);
                if (req == null)
                    return ErrorResponse("Invalid or empty request body.");

                string deviceId = GetString(req, "deviceId");
                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");

                string filter = GetString(req, "filter");

                Console.WriteLine($"[S7PlusApiHandler] HandleBrowse: deviceId='{deviceId}' filter='{filter}'.");

                var (success, error, variables) = await _service.BrowseAsync(deviceId);
                if (!success)
                    return ErrorResponse(error);

                // Apply optional substring filter on variable name
                if (!string.IsNullOrWhiteSpace(filter))
                {
                    var filtered = new List<object>();
                    foreach (var v in variables)
                    {
                        // Anonymous objects: use dynamic to access name field
                        dynamic dv = v;
                        string varName = dv.name?.ToString() ?? string.Empty;
                        if (varName.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0)
                            filtered.Add(v);
                    }
                    variables = filtered;
                }

                return new { success = true, deviceId = deviceId, count = variables.Count, variables = variables };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleBrowse exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Status
        // -------------------------------------------------------------------------

        /// <summary>
        /// Returns the status of all registered connections. No body required.
        /// </summary>
        public object HandleStatus()
        {
            try
            {
                var statuses = _manager.GetAllStatuses();
                return new { success = true, count = statuses.Count, connections = statuses };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleStatus exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
        }

        // -------------------------------------------------------------------------
        // Test (connect → status-check → disconnect)
        // -------------------------------------------------------------------------

        /// <summary>
        /// Ephemeral connectivity test. Accepts the same body as HandleConnect.
        /// Connects, verifies the connection, then disconnects.
        /// Returns { success, connected, variables, message }.
        /// </summary>
        public async Task<object> HandleTest(string body)
        {
            string testDeviceId = $"__test_{Guid.NewGuid():N}";
            try
            {
                var req = ParseBody(body);
                if (req == null)
                    return ErrorResponse("Invalid or empty request body.");

                string ip = GetString(req, "ip");
                if (string.IsNullOrWhiteSpace(ip))
                    return ErrorResponse("Field 'ip' is required.");

                var config = new S7PlusConfig
                {
                    Id = testDeviceId,
                    Name = $"test-{ip}",
                    Ip = ip,
                    Port = GetInt(req, "port", 102),
                    Password = GetString(req, "password") ?? string.Empty,
                    UseTls = GetBool(req, "useTls", false),
                    Timeout = GetInt(req, "timeout", 10000),
                    Enabled = true,
                };

                Console.WriteLine($"[S7PlusApiHandler] HandleTest: ip='{ip}'...");

                // Connect
                var connectResult = await _manager.ConnectAsync(config);
                if (!connectResult.Success)
                {
                    return new
                    {
                        success = false,
                        connected = false,
                        error = connectResult.Error,
                        message = "Connection test failed.",
                    };
                }

                // Quick browse to check reachability
                var (browseOk, browseErr, variables) = await _service.BrowseAsync(testDeviceId);

                return new
                {
                    success = browseOk,
                    connected = true,
                    variableCount = browseOk ? variables?.Count : 0,
                    message = browseOk
                        ? $"Test successful: {variables?.Count ?? 0} variable(s) found."
                        : $"Connected but browse failed: {browseErr}",
                    error = browseOk ? null : browseErr,
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] HandleTest exception: {ex.Message}");
                return ErrorResponse(ex.Message);
            }
            finally
            {
                // Always clean up the ephemeral test connection
                try
                {
                    await _manager.DisconnectAsync(testDeviceId);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        // -------------------------------------------------------------------------
        // Private helpers
        // -------------------------------------------------------------------------

        private static object ErrorResponse(string message) =>
            new { success = false, error = message };

        /// <summary>
        /// Parses the request body as a JObject. Returns null if parsing fails.
        /// </summary>
        private static Dictionary<string, object> ParseBody(string body)
        {
            if (string.IsNullOrWhiteSpace(body))
                return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            try
            {
                var jObj = JObject.Parse(body);
                var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in jObj)
                    dict[kv.Key] = kv.Value;
                return dict;
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"[S7PlusApiHandler] ParseBody failed: {ex.Message}");
                return null;
            }
        }

        private static string GetString(Dictionary<string, object> d, string key)
        {
            if (d != null && d.TryGetValue(key, out var val) && val != null)
                return val.ToString();
            return null;
        }

        private static int GetInt(Dictionary<string, object> d, string key, int defaultValue = 0)
        {
            if (d != null && d.TryGetValue(key, out var val) && val is JToken jt)
            {
                if (jt.Type == JTokenType.Integer || jt.Type == JTokenType.Float)
                    return jt.Value<int>();
            }
            return defaultValue;
        }

        private static bool GetBool(Dictionary<string, object> d, string key, bool defaultValue = false)
        {
            if (d != null && d.TryGetValue(key, out var val) && val is JToken jt)
            {
                if (jt.Type == JTokenType.Boolean)
                    return jt.Value<bool>();
                if (jt.Type == JTokenType.String)
                    return string.Equals(jt.Value<string>(), "true", StringComparison.OrdinalIgnoreCase);
            }
            return defaultValue;
        }

        // =====================================================================
        // HandleGetBlocks: returns all blocks (DB, FB, FC, OB) from the PLC
        // POST { deviceId }
        // =====================================================================
        public async Task<object> HandleGetBlocks(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                if (string.IsNullOrWhiteSpace(deviceId))
                    return new { success = false, error = "Field 'deviceId' is required." };

                var (success, error, blocks) = await _service.GetBlocksAsync(deviceId);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, count = blocks.Count, blocks };
            }
            catch (Exception ex)
            {
                return new { success = false, error = ex.Message };
            }
        }

        // =====================================================================
        // HandleGetBlockSchema: returns interface/structure of a specific block
        // POST { deviceId, blockName }
        // =====================================================================
        public async Task<object> HandleGetBlockSchema(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string blockName = GetString(d, "blockName");
                if (string.IsNullOrWhiteSpace(deviceId) || string.IsNullOrWhiteSpace(blockName))
                    return new { success = false, error = "Fields 'deviceId' and 'blockName' are required." };

                var (success, error, schema) = await _service.GetBlockSchemaAsync(deviceId, blockName);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, schema };
            }
            catch (Exception ex)
            {
                return new { success = false, error = ex.Message };
            }
        }

        // =====================================================================
        // S7 Raw Classic — Hybrid endpoints
        // =====================================================================

        /// <summary>
        /// Read SZL data via classic S7 protocol.
        /// POST { deviceId, szlId (hex string or int), szlIndex (hex string or int) }
        /// </summary>
        public async Task<object> HandleReadSzl(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                if (string.IsNullOrWhiteSpace(deviceId))
                    return ErrorResponse("Field 'deviceId' is required.");

                ushort szlId = ParseHexOrInt(d, "szlId", 0x001C);
                ushort szlIndex = ParseHexOrInt(d, "szlIndex", 0x0000);

                var (success, error, data) = await _service.ReadSzlAsync(deviceId, szlId, szlIndex);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, data };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// Get CPU info via SZL 0x001C.
        /// POST { deviceId?, ip? }
        /// </summary>
        public async Task<object> HandleCpuInfo(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string ip = GetString(d, "ip");
                if (string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(ip))
                    return ErrorResponse("Field 'deviceId' or 'ip' is required.");

                var (success, error, cpuInfo) = await _service.GetCpuInfoRawAsync(deviceId, ip);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, ip, cpuInfo };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// List block counts via classic S7.
        /// POST { deviceId?, ip? }
        /// </summary>
        public async Task<object> HandleListBlocksRaw(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string ip = GetString(d, "ip");
                if (string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(ip))
                    return ErrorResponse("Field 'deviceId' or 'ip' is required.");

                var (success, error, blocks) = await _service.ListBlocksRawAsync(deviceId, ip);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, ip, data = blocks };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// List block numbers of a given type via classic S7.
        /// POST { deviceId?, ip?, blockType: "OB"|"FB"|"FC"|"DB" or byte value }
        /// </summary>
        public async Task<object> HandleListBlocksOfTypeRaw(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string ip = GetString(d, "ip");
                string typeStr = GetString(d, "blockType");
                if ((string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(ip)) || string.IsNullOrWhiteSpace(typeStr))
                    return ErrorResponse("Fields 'blockType' and one of 'deviceId' or 'ip' are required.");

                byte blockType = ParseBlockType(typeStr);
                var (success, error, entries) = await _service.ListBlocksOfTypeRawAsync(deviceId, blockType, ip);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, ip, data = entries };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// Get detailed block info (author, family, dates, language, size) via classic S7.
        /// POST { deviceId?, ip?, blockType, blockNumber }
        /// </summary>
        public async Task<object> HandleBlockInfoRaw(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string ip = GetString(d, "ip");
                string typeStr = GetString(d, "blockType");
                int blockNumber = GetInt(d, "blockNumber", 1);
                if ((string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(ip)) || string.IsNullOrWhiteSpace(typeStr))
                    return ErrorResponse("Fields 'blockType' and one of 'deviceId' or 'ip' are required.");

                byte blockType = ParseBlockType(typeStr);
                var (success, error, info) = await _service.GetBlockInfoRawAsync(deviceId, blockType, (ushort)blockNumber, ip);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, ip, info };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// Upload block binary (MC7/MC7+ bytecode) from PLC via classic S7.
        /// POST { deviceId?, ip?, blockType, blockNumber }
        /// </summary>
        public async Task<object> HandleUploadBlock(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string ip = GetString(d, "ip");
                string typeStr = GetString(d, "blockType");
                int blockNumber = GetInt(d, "blockNumber", 1);
                if ((string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(ip)) || string.IsNullOrWhiteSpace(typeStr))
                    return ErrorResponse("Fields 'blockType' and one of 'deviceId' or 'ip' are required.");

                byte blockType = ParseBlockType(typeStr);
                var (success, error, blockData) = await _service.UploadBlockRawAsync(deviceId, blockType, (ushort)blockNumber, ip);
                if (!success)
                    return new { success = false, error };

                // MC7 annotation + enhanced header parsing
                string stlAnnotation = null;
                Mc7PlusHeaderInfo parsedHeader = null;
                List<Mc7Constant> constants = null;
                try
                {
                    dynamic bd = blockData;
                    string b64 = bd.rawBase64;
                    if (!string.IsNullOrEmpty(b64))
                    {
                        byte[] rawBytes = Convert.FromBase64String(b64);
                        var annotation = Mc7Annotator.Annotate(rawBytes, typeStr, blockNumber);
                        stlAnnotation = annotation?.StlAnnotation;

                        // Enhanced MC7+ header decoder (#7)
                        parsedHeader = Mc7AnnotatorExtensions.ParseMc7PlusHeader(rawBytes);
                        constants = Mc7AnnotatorExtensions.ExtractConstants(rawBytes);
                    }
                }
                catch { }

                return new { success = true, deviceId, ip, blockData, stlAnnotation, parsedHeader, constants };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// Hybrid: combines S7CommPlus schema + S7 Raw info + bytecode for a block.
        /// POST { deviceId, blockName, blockType, blockNumber }
        /// </summary>
        public async Task<object> HandleBlockHybrid(string body)
        {
            try
            {
                var d = ParseBody(body);
                string deviceId = GetString(d, "deviceId");
                string blockName = GetString(d, "blockName");
                string typeStr = GetString(d, "blockType") ?? "DB";
                int blockNumber = GetInt(d, "blockNumber", 1);
                if (string.IsNullOrWhiteSpace(deviceId) || string.IsNullOrWhiteSpace(blockName))
                    return ErrorResponse("Fields 'deviceId' and 'blockName' are required.");

                byte blockType = ParseBlockType(typeStr);
                var (success, error, hybrid) = await _service.GetBlockHybridAsync(
                    deviceId, blockName, blockType, (ushort)blockNumber);
                if (!success)
                    return new { success = false, error };

                return new { success = true, deviceId, hybrid };
            }
            catch (Exception ex)
            {
                return ErrorResponse(ex.Message);
            }
        }

        public async Task<object> HandleBlockBody(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");

                if (string.IsNullOrEmpty(deviceId))
                    return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName))
                    return ErrorResponse("blockName richiesto");

                var (success, error, bodyData) = await _service.GetBlockBodyAsync(deviceId, blockName);
                if (!success) return ErrorResponse(error);

                return new { success = true, deviceId, blockName, data = bodyData };
            }
            catch (Exception ex)
            {
                return ErrorResponse($"Internal error: {ex.Message}");
            }
        }

        public async Task<object> HandleDiscoverAttributes(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");

                if (string.IsNullOrEmpty(deviceId))
                    return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName))
                    return ErrorResponse("blockName richiesto");

                var (success, error, attributes) = await _service.DiscoverAttributesAsync(deviceId, blockName);
                if (!success) return ErrorResponse(error);

                return new { success = true, deviceId, blockName, data = attributes };
            }
            catch (Exception ex)
            {
                return ErrorResponse($"Internal error: {ex.Message}");
            }
        }

        // -------------------------------------------------------------------------
        // #5 — Batch Block Body Reading
        // -------------------------------------------------------------------------

        public async Task<object> HandleBlockBodyBatch(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                if (string.IsNullOrEmpty(deviceId))
                    return ErrorResponse("deviceId richiesto");

                var blockNamesRaw = req.ContainsKey("blockNames") ? req["blockNames"] : null;
                var blockNames = new List<string>();
                if (blockNamesRaw is JArray arr)
                    blockNames = arr.Select(t => t.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s!).ToList();
                else if (blockNamesRaw is IEnumerable<object> list)
                    blockNames = list.Select(o => o?.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s!).ToList();

                if (blockNames.Count == 0)
                    return ErrorResponse("blockNames array richiesto (non vuoto)");

                int maxConcurrency = GetInt(req, "maxConcurrency", 2);

                var (success, error, results) = await _service.GetBlockBodyBatchAsync(deviceId, blockNames, maxConcurrency);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, data = results };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // #4 — UDT/Struct Hierarchy
        // -------------------------------------------------------------------------

        public async Task<object> HandleTypeHierarchy(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName)) return ErrorResponse("blockName richiesto");

                var (success, error, hierarchy) = await _service.GetTypeHierarchyAsync(deviceId, blockName);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, blockName, data = hierarchy };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // #6 — FB ↔ Instance DB Correlation
        // -------------------------------------------------------------------------

        public async Task<object> HandleInstanceDBs(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string fbName = GetString(req, "fbName");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(fbName)) return ErrorResponse("fbName richiesto");

                bool readValues = false;
                if (req.ContainsKey("readValues"))
                {
                    var rv = req["readValues"];
                    if (rv is bool b) readValues = b;
                    else bool.TryParse(rv?.ToString(), out readValues);
                }

                var (success, error, instances) = await _service.GetInstanceDBsAsync(deviceId, fbName, readValues);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, data = instances };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // #3 — Cross-Reference Analysis
        // -------------------------------------------------------------------------

        public async Task<object> HandleCrossReferences(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");

                List<string> blockNames = null;
                var blockNamesRaw = req.ContainsKey("blockNames") ? req["blockNames"] : null;
                if (blockNamesRaw is JArray arr)
                {
                    blockNames = arr.Select(t => t.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
                    if (blockNames.Count == 0) blockNames = null;
                }

                var (success, error, crossRefs) = await _service.GetCrossReferencesAsync(deviceId, blockNames);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, data = crossRefs };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // #2 — LAD/FBD → SCL Decompiler
        // -------------------------------------------------------------------------

        public async Task<object> HandleDecompile(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");
                if (string.IsNullOrEmpty(blockName)) return ErrorResponse("blockName richiesto");

                // Support offline mode: bodyXml + refDataXml provided directly
                string bodyXml = GetString(req, "bodyXml");
                string refDataXml = GetString(req, "refDataXml");
                string blockType = GetString(req, "blockType");

                if (string.IsNullOrEmpty(bodyXml) && string.IsNullOrEmpty(deviceId))
                    return ErrorResponse("deviceId richiesto (oppure fornire bodyXml per modalita' offline)");

                var (success, error, decompiled) = await _service.DecompileBlockAsync(
                    deviceId, blockName, bodyXml, refDataXml, blockType);
                if (!success) return ErrorResponse(error);
                return new { success = true, blockName, data = decompiled };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // #8 — Block Snapshot & Diff
        // -------------------------------------------------------------------------

        public async Task<object> HandleBlockSnapshot(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName)) return ErrorResponse("blockName richiesto");

                var (success, error, snapshot) = await _service.SaveBlockSnapshotAsync(deviceId, blockName);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, blockName, data = snapshot };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        public object HandleListSnapshots(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName)) return ErrorResponse("blockName richiesto");

                var (success, error, snapshots) = _service.ListBlockSnapshotsSync(deviceId, blockName);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, blockName, data = snapshots };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        public async Task<object> HandleBlockDiff(string body)
        {
            try
            {
                var req = ParseBody(body);
                string deviceId = GetString(req, "deviceId");
                string blockName = GetString(req, "blockName");
                string snapshotId1 = GetString(req, "snapshotId1");
                string snapshotId2 = GetString(req, "snapshotId2");
                if (string.IsNullOrEmpty(deviceId)) return ErrorResponse("deviceId richiesto");
                if (string.IsNullOrEmpty(blockName)) return ErrorResponse("blockName richiesto");
                if (string.IsNullOrEmpty(snapshotId1)) return ErrorResponse("snapshotId1 richiesto");

                var (success, error, diff) = await _service.CompareBlockSnapshotsAsync(
                    deviceId, blockName, snapshotId1, snapshotId2);
                if (!success) return ErrorResponse(error);
                return new { success = true, deviceId, blockName, data = diff };
            }
            catch (Exception ex) { return ErrorResponse($"Internal error: {ex.Message}"); }
        }

        // -------------------------------------------------------------------------
        // Raw helpers
        // -------------------------------------------------------------------------

        private static ushort ParseHexOrInt(Dictionary<string, object> d, string key, ushort defaultValue)
        {
            var str = GetString(d, key);
            if (string.IsNullOrWhiteSpace(str)) return defaultValue;
            str = str.Trim();

            if (str.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            {
                if (ushort.TryParse(str.Substring(2), System.Globalization.NumberStyles.HexNumber, null, out var hexVal))
                    return hexVal;
            }
            if (ushort.TryParse(str, out var intVal))
                return intVal;
            return defaultValue;
        }

        private static byte ParseBlockType(string typeStr)
        {
            if (string.IsNullOrWhiteSpace(typeStr)) return S7RawClient.BlockDB;
            switch (typeStr.Trim().ToUpperInvariant())
            {
                case "OB": return S7RawClient.BlockOB;
                case "DB": return S7RawClient.BlockDB;
                case "FC": return S7RawClient.BlockFC;
                case "FB": return S7RawClient.BlockFB;
                case "SDB": return S7RawClient.BlockSDB;
                case "SFC": return S7RawClient.BlockSFC;
                case "SFB": return S7RawClient.BlockSFB;
                default:
                    if (byte.TryParse(typeStr, out var b)) return b;
                    return S7RawClient.BlockDB;
            }
        }
    }
}
