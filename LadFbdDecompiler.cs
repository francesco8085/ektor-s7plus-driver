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
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Decompiles LAD/FBD/SCL network XML (from S7CommPlus GetBlockBodyXml) into
    /// readable pseudo-SCL code. Handles wire graph reconstruction, boolean logic,
    /// timers, counters, math operations, comparisons, and block calls.
    /// </summary>
    public static class LadFbdDecompiler
    {
        // =====================================================================
        // Public API
        // =====================================================================

        public static DecompileResult Decompile(string bodyXml, string refDataXml, string blockName, string blockType)
        {
            var result = new DecompileResult
            {
                BlockName = blockName,
                BlockType = blockType?.ToUpperInvariant() ?? "FC",
                Networks = new List<NetworkScl>()
            };

            if (string.IsNullOrEmpty(bodyXml))
            {
                result.FullScl = $"// No body XML available for {blockName}";
                return result;
            }

            // Parse refData for tag resolution
            var tagMap = ParseRefData(refDataXml);

            // Clean and wrap bodyXml for parsing
            string cleanXml = Regex.Replace(bodyXml, @"<\?xml[^?]*\?>", "").Trim();
            cleanXml = "<Root>" + cleanXml + "</Root>";

            XDocument doc;
            try
            {
                doc = XDocument.Parse(cleanXml);
            }
            catch (Exception ex)
            {
                result.FullScl = $"// XML parse error: {ex.Message}";
                return result;
            }

            var networks = doc.Descendants("Network").ToList();
            // Also try SW.Blocks.CompileUnit pattern
            if (networks.Count == 0)
                networks = doc.Descendants().Where(e => e.Name.LocalName == "Network" ||
                    e.Name.LocalName.Contains("CompileUnit")).ToList();

            var sb = new StringBuilder();
            string blockKeyword = result.BlockType == "FB" ? "FUNCTION_BLOCK" :
                                  result.BlockType == "OB" ? "ORGANIZATION_BLOCK" : "FUNCTION";

            sb.AppendLine($"{blockKeyword} \"{blockName}\"");
            sb.AppendLine("BEGIN");

            int netIdx = 0;
            foreach (var netEl in networks)
            {
                netIdx++;
                var netScl = DecompileNetwork(netEl, tagMap, netIdx);
                result.Networks.Add(netScl);

                sb.AppendLine();
                sb.AppendLine($"    // Network {netScl.Number}: {netScl.Title}");
                if (!string.IsNullOrEmpty(netScl.Scl))
                    sb.AppendLine(IndentBlock(netScl.Scl, "    "));
                else if (!string.IsNullOrEmpty(netScl.Error))
                    sb.AppendLine($"    // Error: {netScl.Error}");
            }

            sb.AppendLine();
            sb.AppendLine($"END_{blockKeyword}");

            result.FullScl = sb.ToString();
            result.TotalNetworks = networks.Count;
            result.SuccessfulNetworks = result.Networks.Count(n => n.Error == null);
            result.FailedNetworks = result.Networks.Count(n => n.Error != null);

            return result;
        }

        // =====================================================================
        // RefData Parsing
        // =====================================================================

        private static Dictionary<string, TagInfo> ParseRefData(string refDataXml)
        {
            var map = new Dictionary<string, TagInfo>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrEmpty(refDataXml)) return map;

            try
            {
                string cleanXml = Regex.Replace(refDataXml, @"<\?xml[^?]*\?>", "").Trim();
                if (!cleanXml.StartsWith("<")) return map;
                cleanXml = "<RefRoot>" + cleanXml + "</RefRoot>";
                var doc = XDocument.Parse(cleanXml);

                foreach (var ident in doc.Descendants())
                {
                    if (ident.Name.LocalName != "Ident" && ident.Name.LocalName != "IdentEntry")
                        continue;

                    string name = ident.Attribute("Name")?.Value;
                    string refId = ident.Attribute("RefId")?.Value ?? ident.Attribute("UId")?.Value;
                    if (string.IsNullOrEmpty(refId)) continue;

                    var access = ident.Descendants().FirstOrDefault(d => d.Name.LocalName == "Access");
                    var tag = new TagInfo
                    {
                        Name = name ?? refId,
                        Scope = ident.Attribute("Scope")?.Value ?? "Local",
                        TypeName = access?.Attribute("TypeName")?.Value ?? ident.Attribute("Type")?.Value,
                        Range = access?.Attribute("Range")?.Value,
                        Offset = access?.Attribute("AbsOffset")?.Value ?? access?.Attribute("Offset")?.Value,
                        BitOffset = access?.Attribute("BitOffset")?.Value
                    };

                    map[refId] = tag;
                }
            }
            catch { /* non-fatal: proceed without refData */ }

            return map;
        }

        // =====================================================================
        // Network Decompilation
        // =====================================================================

        private static NetworkScl DecompileNetwork(XElement netEl, Dictionary<string, TagInfo> tagMap, int defaultNumber)
        {
            var result = new NetworkScl
            {
                Number = int.TryParse(netEl.Attribute("Number")?.Value, out int n) ? n : defaultNumber,
                Title = netEl.Attribute("Title")?.Value ?? netEl.Descendants()
                    .FirstOrDefault(e => e.Name.LocalName == "Title")?.Value ?? "",
                Language = netEl.Attribute("Lang")?.Value ??
                           netEl.Attribute("Language")?.Value ??
                           netEl.Attribute("ProgrammingLanguage")?.Value ?? "unknown"
            };

            try
            {
                string lang = result.Language.ToUpperInvariant();

                if (lang.Contains("SCL") || lang.Contains("STL") || lang.Contains("STRUCTURED"))
                {
                    result.Scl = ExtractSclText(netEl);
                }
                else if (lang.Contains("LAD") || lang.Contains("FBD") || lang.Contains("LADDER") || lang.Contains("FUNCTION_BLOCK_DIAGRAM"))
                {
                    result.Scl = DecompileLadFbd(netEl, tagMap);
                }
                else
                {
                    // Try both approaches
                    string scl = ExtractSclText(netEl);
                    if (string.IsNullOrWhiteSpace(scl))
                        scl = DecompileLadFbd(netEl, tagMap);
                    result.Scl = string.IsNullOrWhiteSpace(scl) ? $"// Unsupported language: {result.Language}" : scl;
                }
            }
            catch (Exception ex)
            {
                result.Error = ex.Message;
                result.Scl = $"// Decompilation error: {ex.Message}";
            }

            return result;
        }

        // =====================================================================
        // SCL Text Extraction (passthrough for SCL networks)
        // =====================================================================

        private static string ExtractSclText(XElement netEl)
        {
            var sb = new StringBuilder();

            // Look for SCLSource or StructuredText elements
            var sclSource = netEl.Descendants().FirstOrDefault(e =>
                e.Name.LocalName == "SCLSource" || e.Name.LocalName == "StructuredText" ||
                e.Name.LocalName == "RootStatements" || e.Name.LocalName == "StatementList");

            if (sclSource != null)
            {
                ExtractSclStatements(sclSource, sb, 0);
            }
            else
            {
                // Try to extract from TokenText elements directly
                var tokens = netEl.Descendants().Where(e => e.Name.LocalName == "TokenText" || e.Name.LocalName == "Token");
                foreach (var t in tokens)
                {
                    string text = t.Attribute("Text")?.Value ?? t.Attribute("TE")?.Value ?? t.Value;
                    if (!string.IsNullOrEmpty(text)) sb.Append(text);
                }
            }

            return sb.ToString().Trim();
        }

        private static void ExtractSclStatements(XElement el, StringBuilder sb, int depth)
        {
            foreach (var child in el.Elements())
            {
                string localName = child.Name.LocalName;
                string indent = new string(' ', depth * 4);

                switch (localName)
                {
                    case "Statement":
                    case "AssignmentStatement":
                        string si = child.Attribute("SI")?.Value ?? "";
                        if (si == "STAss" || localName == "AssignmentStatement" || string.IsNullOrEmpty(si))
                        {
                            var target = ExtractExpression(child.Elements().FirstOrDefault());
                            var source = ExtractExpression(child.Elements().Skip(1).FirstOrDefault());
                            if (!string.IsNullOrEmpty(target) || !string.IsNullOrEmpty(source))
                                sb.AppendLine($"{indent}{target} := {source};");
                        }
                        else if (si == "STIf")
                        {
                            sb.AppendLine($"{indent}IF ... THEN");
                            ExtractSclStatements(child, sb, depth + 1);
                            sb.AppendLine($"{indent}END_IF;");
                        }
                        else if (si == "STFor")
                        {
                            sb.AppendLine($"{indent}FOR ... DO");
                            ExtractSclStatements(child, sb, depth + 1);
                            sb.AppendLine($"{indent}END_FOR;");
                        }
                        else if (si == "STWhile")
                        {
                            sb.AppendLine($"{indent}WHILE ... DO");
                            ExtractSclStatements(child, sb, depth + 1);
                            sb.AppendLine($"{indent}END_WHILE;");
                        }
                        else if (si == "STReturn")
                        {
                            sb.AppendLine($"{indent}RETURN;");
                        }
                        else
                        {
                            ExtractSclStatements(child, sb, depth);
                        }
                        break;

                    case "IfStatement":
                        string cond = ExtractExpression(child.Elements().FirstOrDefault());
                        sb.AppendLine($"{indent}IF {cond} THEN");
                        var thenPart = child.Elements().Where(e => e.Name.LocalName == "Then" || e.Name.LocalName == "ThenStatements").FirstOrDefault();
                        if (thenPart != null) ExtractSclStatements(thenPart, sb, depth + 1);
                        var elsePart = child.Elements().Where(e => e.Name.LocalName == "Else" || e.Name.LocalName == "ElseStatements").FirstOrDefault();
                        if (elsePart != null)
                        {
                            sb.AppendLine($"{indent}ELSE");
                            ExtractSclStatements(elsePart, sb, depth + 1);
                        }
                        sb.AppendLine($"{indent}END_IF;");
                        break;

                    case "ForStatement":
                        sb.AppendLine($"{indent}FOR ... DO");
                        ExtractSclStatements(child, sb, depth + 1);
                        sb.AppendLine($"{indent}END_FOR;");
                        break;

                    case "FctCa":
                    case "FunctionCall":
                        string funcName = child.Attribute("Name")?.Value ?? child.Attribute("ODN")?.Value ?? "FUNC";
                        sb.AppendLine($"{indent}{funcName}(...);");
                        break;

                    case "RootStatements":
                    case "StatementList":
                    case "Then":
                    case "ThenStatements":
                    case "Else":
                    case "ElseStatements":
                    case "Body":
                        ExtractSclStatements(child, sb, depth);
                        break;

                    default:
                        // Recurse for unknown containers
                        if (child.HasElements)
                            ExtractSclStatements(child, sb, depth);
                        else
                        {
                            string text = child.Attribute("Text")?.Value ?? child.Attribute("TE")?.Value;
                            if (!string.IsNullOrEmpty(text))
                                sb.Append(text);
                        }
                        break;
                }
            }
        }

        private static string ExtractExpression(XElement el)
        {
            if (el == null) return "";

            string localName = el.Name.LocalName;

            // Direct value elements
            if (localName == "SymVa" || localName == "SymbolVariable")
                return "\"" + (el.Attribute("ODN")?.Value ?? el.Attribute("Name")?.Value ?? el.Value) + "\"";

            if (localName == "Const" || localName == "Constant")
                return el.Attribute("TE")?.Value ?? el.Attribute("Value")?.Value ?? el.Value;

            if (localName == "TokenText" || localName == "Token")
                return el.Attribute("Text")?.Value ?? el.Attribute("TE")?.Value ?? el.Value;

            if (localName == "Target" || localName == "Expression" || localName == "Source")
                return ExtractExpression(el.Elements().FirstOrDefault());

            // Concatenate all children
            var parts = el.Elements().Select(ExtractExpression).Where(s => !string.IsNullOrEmpty(s));
            return string.Join(" ", parts);
        }

        // =====================================================================
        // LAD/FBD Decompilation (wire graph approach)
        // =====================================================================

        private static string DecompileLadFbd(XElement netEl, Dictionary<string, TagInfo> tagMap)
        {
            // Find the FlgNet (flag network) element
            var flgNet = netEl.Descendants().FirstOrDefault(e =>
                e.Name.LocalName == "FlgNet" || e.Name.LocalName == "ObjectList" ||
                e.Name.LocalName == "Parts");

            if (flgNet == null)
            {
                // Try flat structure where Parts and Wires are directly under Network
                flgNet = netEl;
            }

            // Build part map: UId -> PartInfo
            var partMap = new Dictionary<string, PartInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var partEl in flgNet.Descendants().Where(e =>
                e.Name.LocalName == "Part" || e.Name.LocalName == "Access" ||
                e.Name.LocalName == "Call"))
            {
                string uid = partEl.Attribute("UId")?.Value ?? partEl.Attribute("UID")?.Value;
                if (string.IsNullOrEmpty(uid)) continue;

                var pi = new PartInfo
                {
                    UId = uid,
                    Name = partEl.Attribute("Name")?.Value ?? partEl.Name.LocalName,
                    FullElement = partEl
                };

                // Resolve display name from RefId / ODN attributes
                string refId = partEl.Attribute("RefId")?.Value;
                if (refId != null && tagMap.TryGetValue(refId, out var tag))
                    pi.DisplayName = tag.Name;
                else
                    pi.DisplayName = partEl.Attribute("DisplayName")?.Value ??
                                     partEl.Attribute("ODN")?.Value ??
                                     pi.Name;

                // For Access elements (operands), extract the symbol
                if (partEl.Name.LocalName == "Access")
                {
                    pi.IsAccess = true;
                    var symbol = partEl.Descendants().FirstOrDefault(e => e.Name.LocalName == "Symbol" || e.Name.LocalName == "Ident");
                    if (symbol != null)
                    {
                        var components = symbol.Elements().Select(c => c.Attribute("Name")?.Value ?? c.Value).Where(s => !string.IsNullOrEmpty(s));
                        pi.DisplayName = "\"" + string.Join(".", components) + "\"";
                    }
                    var constant = partEl.Descendants().FirstOrDefault(e => e.Name.LocalName == "Constant" || e.Name.LocalName == "Const");
                    if (constant != null)
                    {
                        pi.DisplayName = constant.Attribute("Value")?.Value ??
                                         constant.Element("ConstantValue")?.Value ??
                                         constant.Value;
                        pi.IsConstant = true;
                    }
                }

                partMap[uid] = pi;
            }

            // Build wire graph: maps (targetUId, targetPin) -> list of (sourceUId, sourcePin)
            var wireGraph = new Dictionary<string, List<string>>();
            foreach (var wireEl in flgNet.Descendants().Where(e => e.Name.LocalName == "Wire"))
            {
                var connections = wireEl.Elements().ToList();
                // First element is usually the source, rest are targets
                string sourceKey = null;
                foreach (var conn in connections)
                {
                    string connUid = conn.Attribute("UId")?.Value ?? conn.Attribute("UID")?.Value;
                    string connName = conn.Attribute("Name")?.Value ?? "";
                    string connType = conn.Name.LocalName; // ICon, OCon, NameCon, PCon

                    string key = $"{connUid}.{connName}";

                    if (connType == "PCon" || connType == "Powerrail")
                    {
                        sourceKey = "POWER";
                        continue;
                    }

                    if (sourceKey == null)
                    {
                        // This is the source (output)
                        sourceKey = key;
                    }
                    else
                    {
                        // This is a target (input)
                        if (!wireGraph.ContainsKey(key))
                            wireGraph[key] = new List<string>();
                        wireGraph[key].Add(sourceKey);
                    }
                }
            }

            // Generate SCL from parts
            var sb = new StringBuilder();
            var processedCoils = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Find coils/outputs (parts that receive assignments)
            foreach (var kvp in partMap)
            {
                var part = kvp.Value;
                string partName = part.Name.ToUpperInvariant();

                if (IsOutputPart(partName))
                {
                    if (processedCoils.Contains(kvp.Key)) continue;
                    processedCoils.Add(kvp.Key);

                    string output = ResolveOutputTarget(part, wireGraph, partMap, tagMap);
                    string condition = BuildConditionExpression(kvp.Key, "in", wireGraph, partMap, tagMap, new HashSet<string>());

                    if (partName == "COIL" || partName == "ASSIGNMENT")
                    {
                        if (!string.IsNullOrEmpty(condition) && condition != "TRUE")
                            sb.AppendLine($"{output} := {condition};");
                        else
                            sb.AppendLine($"{output} := TRUE;");
                    }
                    else if (partName == "SCOIL" || partName == "SET" || partName == "S_ASSIGN")
                    {
                        if (!string.IsNullOrEmpty(condition) && condition != "TRUE")
                            sb.AppendLine($"IF {condition} THEN {output} := TRUE; END_IF;");
                        else
                            sb.AppendLine($"{output} := TRUE; // SET");
                    }
                    else if (partName == "RCOIL" || partName == "RESET" || partName == "R_ASSIGN")
                    {
                        if (!string.IsNullOrEmpty(condition) && condition != "TRUE")
                            sb.AppendLine($"IF {condition} THEN {output} := FALSE; END_IF;");
                        else
                            sb.AppendLine($"{output} := FALSE; // RESET");
                    }
                    else
                    {
                        sb.AppendLine($"// {partName}: {output} = {condition};");
                    }
                }
                else if (IsTimerCounterPart(partName))
                {
                    if (processedCoils.Contains(kvp.Key)) continue;
                    processedCoils.Add(kvp.Key);
                    string timerScl = DecompileTimerCounter(part, kvp.Key, wireGraph, partMap, tagMap);
                    if (!string.IsNullOrEmpty(timerScl)) sb.AppendLine(timerScl);
                }
                else if (IsMathPart(partName))
                {
                    if (processedCoils.Contains(kvp.Key)) continue;
                    processedCoils.Add(kvp.Key);
                    string mathScl = DecompileMathOp(part, kvp.Key, wireGraph, partMap, tagMap);
                    if (!string.IsNullOrEmpty(mathScl)) sb.AppendLine(mathScl);
                }
                else if (partName == "MOVE" || partName == "CONVERT")
                {
                    if (processedCoils.Contains(kvp.Key)) continue;
                    processedCoils.Add(kvp.Key);
                    string moveScl = DecompileMove(part, kvp.Key, wireGraph, partMap, tagMap);
                    if (!string.IsNullOrEmpty(moveScl)) sb.AppendLine(moveScl);
                }
                else if (partName.StartsWith("CALL") || partName.Contains("CALL"))
                {
                    if (processedCoils.Contains(kvp.Key)) continue;
                    processedCoils.Add(kvp.Key);
                    string callScl = DecompileCall(part, kvp.Key, wireGraph, partMap, tagMap);
                    if (!string.IsNullOrEmpty(callScl)) sb.AppendLine(callScl);
                }
            }

            string result = sb.ToString().Trim();
            if (string.IsNullOrEmpty(result))
                result = "// (empty network or unsupported elements)";

            return result;
        }

        // =====================================================================
        // Boolean Expression Builder (series=AND, parallel=OR)
        // =====================================================================

        private static string BuildConditionExpression(string targetUid, string targetPin,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap, HashSet<string> visited)
        {
            string key = $"{targetUid}.{targetPin}";
            if (visited.Contains(key)) return "/* circular */";
            visited.Add(key);

            if (!wireGraph.TryGetValue(key, out var sources) || sources.Count == 0)
            {
                // Try without pin name
                key = $"{targetUid}.";
                if (!wireGraph.TryGetValue(key, out sources) || sources.Count == 0)
                    return "";
            }

            // Multiple sources to same input = OR (parallel branches)
            var expressions = new List<string>();
            foreach (var sourceKey in sources)
            {
                if (sourceKey == "POWER")
                {
                    expressions.Add("TRUE");
                    continue;
                }

                var parts = sourceKey.Split(new[] { '.' }, 2);
                string srcUid = parts[0];
                string srcPin = parts.Length > 1 ? parts[1] : "out";

                if (!partMap.TryGetValue(srcUid, out var srcPart))
                {
                    expressions.Add($"#{srcUid}");
                    continue;
                }

                if (srcPart.IsAccess)
                {
                    expressions.Add(srcPart.DisplayName);
                    continue;
                }

                string partName = srcPart.Name.ToUpperInvariant();

                if (partName == "CONTACT" || partName == "PCONTACT" || partName == "NCONTACT")
                {
                    // Normally open / positive / negative contact
                    string operand = ResolveOperand(srcPart, wireGraph, partMap, tagMap);
                    string inputExpr = BuildConditionExpression(srcUid, "in", wireGraph, partMap, tagMap, visited);

                    string contactExpr;
                    if (partName == "NCONTACT")
                        contactExpr = $"NOT {operand}";
                    else
                        contactExpr = operand;

                    if (!string.IsNullOrEmpty(inputExpr) && inputExpr != "TRUE")
                        expressions.Add($"{inputExpr} AND {contactExpr}");
                    else
                        expressions.Add(contactExpr);
                }
                else if (IsComparisonPart(partName))
                {
                    string op = GetComparisonOperator(partName);
                    string val1 = ResolvePin(srcUid, "val1", wireGraph, partMap, tagMap, visited);
                    string val2 = ResolvePin(srcUid, "val2", wireGraph, partMap, tagMap, visited);
                    if (string.IsNullOrEmpty(val1)) val1 = ResolvePin(srcUid, "in1", wireGraph, partMap, tagMap, visited);
                    if (string.IsNullOrEmpty(val2)) val2 = ResolvePin(srcUid, "in2", wireGraph, partMap, tagMap, visited);
                    expressions.Add($"({val1} {op} {val2})");
                }
                else
                {
                    // Generic: recurse
                    string inner = BuildConditionExpression(srcUid, "in", wireGraph, partMap, tagMap, visited);
                    if (!string.IsNullOrEmpty(inner))
                        expressions.Add(inner);
                    else
                        expressions.Add($"#{srcPart.DisplayName}");
                }
            }

            if (expressions.Count == 0) return "";
            if (expressions.Count == 1) return expressions[0];
            return "(" + string.Join(" OR ", expressions) + ")";
        }

        // =====================================================================
        // Timer / Counter Decompilation
        // =====================================================================

        private static string DecompileTimerCounter(PartInfo part, string uid,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap)
        {
            string name = part.Name.ToUpperInvariant();
            string instanceName = part.DisplayName;

            string inExpr = ResolvePin(uid, "IN", wireGraph, partMap, tagMap, new HashSet<string>());
            if (string.IsNullOrEmpty(inExpr)) inExpr = ResolvePin(uid, "in", wireGraph, partMap, tagMap, new HashSet<string>());

            string ptExpr = ResolvePin(uid, "PT", wireGraph, partMap, tagMap, new HashSet<string>());
            if (string.IsNullOrEmpty(ptExpr)) ptExpr = ResolvePin(uid, "pt", wireGraph, partMap, tagMap, new HashSet<string>());

            string pvExpr = ResolvePin(uid, "PV", wireGraph, partMap, tagMap, new HashSet<string>());
            string cuExpr = ResolvePin(uid, "CU", wireGraph, partMap, tagMap, new HashSet<string>());
            string cdExpr = ResolvePin(uid, "CD", wireGraph, partMap, tagMap, new HashSet<string>());

            if (name.Contains("TON") || name.Contains("TOF") || name.Contains("TP") || name.Contains("TONR"))
            {
                return $"{instanceName}(IN := {inExpr ?? "FALSE"}, PT := {ptExpr ?? "T#0s"});";
            }
            else if (name.Contains("CTU"))
            {
                return $"{instanceName}(CU := {cuExpr ?? "FALSE"}, PV := {pvExpr ?? "0"});";
            }
            else if (name.Contains("CTD"))
            {
                return $"{instanceName}(CD := {cdExpr ?? "FALSE"}, PV := {pvExpr ?? "0"});";
            }
            else if (name.Contains("CTUD"))
            {
                return $"{instanceName}(CU := {cuExpr ?? "FALSE"}, CD := {cdExpr ?? "FALSE"}, PV := {pvExpr ?? "0"});";
            }

            return $"// Timer/Counter: {name}({instanceName});";
        }

        // =====================================================================
        // Math Operation Decompilation
        // =====================================================================

        private static string DecompileMathOp(PartInfo part, string uid,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap)
        {
            string name = part.Name.ToUpperInvariant();
            string op = GetMathOperator(name);

            string in1 = ResolvePin(uid, "in1", wireGraph, partMap, tagMap, new HashSet<string>());
            string in2 = ResolvePin(uid, "in2", wireGraph, partMap, tagMap, new HashSet<string>());
            string outPin = ResolveOutputPin(uid, "out", wireGraph, partMap, tagMap);

            if (!string.IsNullOrEmpty(outPin) && !string.IsNullOrEmpty(in1))
                return $"{outPin} := {in1} {op} {in2 ?? "0"};";

            return $"// Math: {name}({in1}, {in2}) -> {outPin};";
        }

        // =====================================================================
        // Move/Convert Decompilation
        // =====================================================================

        private static string DecompileMove(PartInfo part, string uid,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap)
        {
            string inVal = ResolvePin(uid, "in", wireGraph, partMap, tagMap, new HashSet<string>());
            string outVal = ResolveOutputPin(uid, "out1", wireGraph, partMap, tagMap);
            if (string.IsNullOrEmpty(outVal))
                outVal = ResolveOutputPin(uid, "out", wireGraph, partMap, tagMap);

            string name = part.Name.ToUpperInvariant();
            if (name == "CONVERT")
                return $"{outVal} := {name}({inVal});";

            return $"{outVal} := {inVal}; // MOVE";
        }

        // =====================================================================
        // Block Call Decompilation
        // =====================================================================

        private static string DecompileCall(PartInfo part, string uid,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap)
        {
            string blockName = part.DisplayName;
            // Collect all input pins
            var inputPins = wireGraph.Keys
                .Where(k => k.StartsWith(uid + ".", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var params_ = new List<string>();
            foreach (var pinKey in inputPins)
            {
                string pinName = pinKey.Substring(uid.Length + 1);
                if (string.IsNullOrEmpty(pinName) || pinName == "en" || pinName == "eno") continue;
                string pinVal = ResolvePin(uid, pinName, wireGraph, partMap, tagMap, new HashSet<string>());
                if (!string.IsNullOrEmpty(pinVal))
                    params_.Add($"{pinName} := {pinVal}");
            }

            if (params_.Count > 0)
                return $"\"{blockName}\"({string.Join(", ", params_)});";

            return $"\"{blockName}\"();";
        }

        // =====================================================================
        // Helper methods
        // =====================================================================

        private static string ResolveOperand(PartInfo part, Dictionary<string, List<string>> wireGraph,
            Dictionary<string, PartInfo> partMap, Dictionary<string, TagInfo> tagMap)
        {
            // Try to find operand from connected Access element
            string key = $"{part.UId}.operand";
            if (wireGraph.TryGetValue(key, out var sources) && sources.Count > 0)
            {
                var srcParts = sources[0].Split(new[] { '.' }, 2);
                if (partMap.TryGetValue(srcParts[0], out var accessPart) && accessPart.IsAccess)
                    return accessPart.DisplayName;
            }
            return part.DisplayName;
        }

        private static string ResolvePin(string uid, string pinName,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap, HashSet<string> visited)
        {
            string key = $"{uid}.{pinName}";
            if (!wireGraph.TryGetValue(key, out var sources) || sources.Count == 0)
                return null;

            var srcParts = sources[0].Split(new[] { '.' }, 2);
            string srcUid = srcParts[0];

            if (srcUid == "POWER") return "TRUE";
            if (partMap.TryGetValue(srcUid, out var part))
            {
                if (part.IsAccess) return part.DisplayName;
                return BuildConditionExpression(srcUid, "", wireGraph, partMap, tagMap, visited);
            }
            return null;
        }

        private static string ResolveOutputPin(string uid, string pinName,
            Dictionary<string, List<string>> wireGraph, Dictionary<string, PartInfo> partMap,
            Dictionary<string, TagInfo> tagMap)
        {
            // Output pins: look for wires where this uid.pin is the SOURCE
            string sourceKey = $"{uid}.{pinName}";
            foreach (var kvp in wireGraph)
            {
                if (kvp.Value.Contains(sourceKey, StringComparer.OrdinalIgnoreCase))
                {
                    var targetParts = kvp.Key.Split(new[] { '.' }, 2);
                    if (partMap.TryGetValue(targetParts[0], out var targetPart) && targetPart.IsAccess)
                        return targetPart.DisplayName;
                }
            }
            return null;
        }

        private static string ResolveOutputTarget(PartInfo part, Dictionary<string, List<string>> wireGraph,
            Dictionary<string, PartInfo> partMap, Dictionary<string, TagInfo> tagMap)
        {
            // For coils: the operand connected to the coil
            return ResolveOperand(part, wireGraph, partMap, tagMap);
        }

        private static bool IsOutputPart(string name)
        {
            return name == "COIL" || name == "SCOIL" || name == "RCOIL" ||
                   name == "ASSIGNMENT" || name == "SET" || name == "RESET" ||
                   name == "S_ASSIGN" || name == "R_ASSIGN";
        }

        private static bool IsTimerCounterPart(string name)
        {
            return name.Contains("TON") || name.Contains("TOF") || name.Contains("TP") || name.Contains("TONR") ||
                   name.Contains("CTU") || name.Contains("CTD") || name.Contains("CTUD");
        }

        private static bool IsMathPart(string name)
        {
            return name == "ADD" || name == "SUB" || name == "MUL" || name == "DIV" || name == "MOD" ||
                   name == "NEG" || name == "ABS" || name == "SQRT";
        }

        private static bool IsComparisonPart(string name)
        {
            return name == "EQ" || name == "NE" || name == "GT" || name == "GE" || name == "LT" || name == "LE" ||
                   name == "CMP_EQ" || name == "CMP_NE" || name == "CMP_GT" || name == "CMP_GE" || name == "CMP_LT" || name == "CMP_LE";
        }

        private static string GetComparisonOperator(string name)
        {
            if (name.Contains("EQ")) return "=";
            if (name.Contains("NE")) return "<>";
            if (name.Contains("GE")) return ">=";
            if (name.Contains("GT")) return ">";
            if (name.Contains("LE")) return "<=";
            if (name.Contains("LT")) return "<";
            return "=";
        }

        private static string GetMathOperator(string name)
        {
            switch (name)
            {
                case "ADD": return "+";
                case "SUB": return "-";
                case "MUL": return "*";
                case "DIV": return "/";
                case "MOD": return "MOD";
                default: return "+";
            }
        }

        private static string IndentBlock(string text, string indent)
        {
            if (string.IsNullOrEmpty(text)) return "";
            return string.Join(Environment.NewLine,
                text.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None)
                    .Select(line => indent + line));
        }
    }

    // =====================================================================
    // Data classes
    // =====================================================================

    public class DecompileResult
    {
        public string BlockName { get; set; }
        public string BlockType { get; set; }
        public List<NetworkScl> Networks { get; set; }
        public string FullScl { get; set; }
        public int TotalNetworks { get; set; }
        public int SuccessfulNetworks { get; set; }
        public int FailedNetworks { get; set; }
    }

    public class NetworkScl
    {
        public int Number { get; set; }
        public string Title { get; set; }
        public string Language { get; set; }
        public string Scl { get; set; }
        public string Error { get; set; }
    }

    public class TagInfo
    {
        public string Name { get; set; }
        public string Scope { get; set; }
        public string TypeName { get; set; }
        public string Range { get; set; }
        public string Offset { get; set; }
        public string BitOffset { get; set; }
    }

    internal class PartInfo
    {
        public string UId { get; set; }
        public string Name { get; set; }
        public string DisplayName { get; set; }
        public bool IsAccess { get; set; }
        public bool IsConstant { get; set; }
        public XElement FullElement { get; set; }
    }
}
