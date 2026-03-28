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

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Basic MC7/MC7+ bytecode annotator.
    /// Parses block headers and identifies common patterns in uploaded block data.
    /// Not a full disassembler - provides structured annotations for display.
    /// </summary>
    public static class Mc7Annotator
    {
        /// <summary>
        /// Annotate raw block data from S7 Classic upload
        /// </summary>
        public static Mc7Annotation Annotate(byte[] rawData, string blockType, int blockNumber)
        {
            var result = new Mc7Annotation
            {
                BlockType = blockType,
                BlockNumber = blockNumber,
                TotalSize = rawData?.Length ?? 0,
                Sections = new List<Mc7Section>()
            };

            if (rawData == null || rawData.Length < 36)
            {
                result.Error = "Block data too short for header parsing";
                return result;
            }

            try
            {
                // Parse block header (first 36+ bytes)
                ParseBlockHeader(rawData, result);

                // Parse interface description if present
                ParseInterface(rawData, result);

                // Parse code section - basic pattern recognition
                ParseCodeSection(rawData, result);

                // Generate STL-like annotation
                result.StlAnnotation = GenerateStlAnnotation(result);
            }
            catch (Exception ex)
            {
                result.Error = $"Annotation error: {ex.Message}";
            }

            return result;
        }

        private static void ParseBlockHeader(byte[] data, Mc7Annotation result)
        {
            // S7 block header structure (classic format):
            // Offset 0: PP (block type signature)
            // Offset 2: Block type byte
            // Offset 3-4: Block number (big endian)
            // Offset 5-8: Block length
            // The exact format varies between S7-300/400 (MC7) and S7-1200/1500 (MC7+)

            var section = new Mc7Section
            {
                Name = "Block Header",
                Offset = 0,
                Length = Math.Min(36, data.Length),
                Type = "header"
            };

            // Try to identify block signature
            if (data.Length >= 2)
            {
                ushort sig = (ushort)((data[0] << 8) | data[1]);
                switch (sig)
                {
                    case 0x7070: section.Description = "PP signature (standard block)"; break;
                    case 0x7071: section.Description = "PP signature (system block)"; break;
                    default: section.Description = $"Signature: 0x{sig:X4}"; break;
                }
            }

            // Extract visible strings from header area
            var strings = ExtractStrings(data, 0, Math.Min(80, data.Length), 3);
            if (strings.Count > 0)
            {
                section.Annotations = strings;
            }

            result.Sections.Add(section);

            // Try to find MC7 code size from header
            if (data.Length >= 40)
            {
                // Various offset attempts for MC7 code length
                int mc7Len = (data[34] << 8) | data[35];
                if (mc7Len > 0 && mc7Len < data.Length)
                {
                    result.Mc7CodeSize = mc7Len;
                }
            }
        }

        private static void ParseInterface(byte[] data, Mc7Annotation result)
        {
            // Look for interface markers in the data
            // Interface description typically follows the header
            // For MC7+, it may be compressed (zlib)

            // Look for common patterns:
            // - 0x00 0x01 followed by variable count
            // - Section markers for Input/Output/InOut/Static/Temp

            int searchStart = 36;
            int searchEnd = Math.Min(data.Length, 500); // Interface is usually in first 500 bytes

            var interfaceSection = new Mc7Section
            {
                Name = "Interface",
                Offset = searchStart,
                Type = "interface",
                Annotations = new List<string>()
            };

            // Look for zlib compressed data (starts with 0x78)
            for (int i = searchStart; i < searchEnd - 2; i++)
            {
                if (data[i] == 0x78 && (data[i + 1] == 0x9C || data[i + 1] == 0x01 || data[i + 1] == 0xDA || data[i + 1] == 0x5E))
                {
                    interfaceSection.Annotations.Add($"Compressed data (zlib) at offset 0x{i:X4}");
                    interfaceSection.Offset = i;
                    result.HasCompressedInterface = true;
                    break;
                }
            }

            // Look for section count markers
            // In MC7+, the interface has section headers: 0x00=Input, 0x01=Output, 0x02=InOut, etc.

            if (interfaceSection.Annotations.Count > 0)
            {
                interfaceSection.Length = 0; // Unknown
                result.Sections.Add(interfaceSection);
            }
        }

        private static void ParseCodeSection(byte[] data, Mc7Annotation result)
        {
            if (data.Length < 50) return;

            // The code section typically starts after header + interface
            int codeStart = 36;
            if (result.Mc7CodeSize > 0)
            {
                // Try to find the code section boundary
                codeStart = data.Length - result.Mc7CodeSize;
                if (codeStart < 36) codeStart = 36;
            }

            var codeSection = new Mc7Section
            {
                Name = "MC7+ Code",
                Offset = codeStart,
                Length = data.Length - codeStart,
                Type = "code",
                Annotations = new List<string>(),
                Instructions = new List<Mc7Instruction>()
            };

            // Basic MC7+ instruction recognition
            // MC7+ uses variable-length instructions
            // Common patterns we can identify:
            int pos = codeStart;
            int instrCount = 0;
            int maxInstr = 200; // Limit for performance

            while (pos < data.Length - 1 && instrCount < maxInstr)
            {
                var instr = TryDecodeInstruction(data, pos);
                if (instr != null)
                {
                    codeSection.Instructions.Add(instr);
                    pos += instr.Length;
                    instrCount++;
                }
                else
                {
                    pos++;
                }
            }

            codeSection.Annotations.Add($"{codeSection.Instructions.Count} instructions identified");
            result.Sections.Add(codeSection);
        }

        private static Mc7Instruction TryDecodeInstruction(byte[] data, int offset)
        {
            if (offset >= data.Length) return null;

            byte opcode = data[offset];

            // Classic MC7 opcodes (S7-300/400 style)
            // Even for MC7+, some basic opcodes are similar
            switch (opcode)
            {
                // Load/Transfer
                case 0x70: return MakeInstr(offset, 2, "L", "Load (byte)", data, offset);
                case 0x38: return MakeInstr(offset, 4, "L", "Load Word", data, offset);
                case 0x08: return MakeInstr(offset, 4, "T", "Transfer Word", data, offset);

                // Boolean logic
                case 0x30: return MakeInstr(offset, 2, "U", "AND", data, offset);
                case 0x31: return MakeInstr(offset, 2, "UN", "AND NOT", data, offset);
                case 0x20: return MakeInstr(offset, 2, "O", "OR", data, offset);
                case 0x21: return MakeInstr(offset, 2, "ON", "OR NOT", data, offset);

                // Assignment
                case 0x10: return MakeInstr(offset, 2, "=", "Assign output", data, offset);
                case 0x11: return MakeInstr(offset, 2, "S", "Set", data, offset);
                case 0x12: return MakeInstr(offset, 2, "R", "Reset", data, offset);

                // Jumps
                case 0x65:
                    if (offset + 1 < data.Length)
                        return MakeInstr(offset, 4, "JU", "Jump unconditional", data, offset);
                    break;
                case 0x66:
                    if (offset + 1 < data.Length)
                        return MakeInstr(offset, 4, "JC", "Jump conditional", data, offset);
                    break;
                case 0x67:
                    if (offset + 1 < data.Length)
                        return MakeInstr(offset, 4, "JCN", "Jump if not", data, offset);
                    break;

                // Call
                case 0x7E: return MakeInstr(offset, 4, "CALL", "Call block", data, offset);
                case 0xFB: return MakeInstr(offset, 4, "CALL FB", "Call FB", data, offset);
                case 0xFC: return MakeInstr(offset, 4, "CALL FC", "Call FC", data, offset);

                // Arithmetic
                case 0x58: return MakeInstr(offset, 2, "+I", "Add Integer", data, offset);
                case 0x48: return MakeInstr(offset, 2, "-I", "Sub Integer", data, offset);
                case 0x78: return MakeInstr(offset, 2, "*I", "Multiply Integer", data, offset);
                case 0x68: return MakeInstr(offset, 2, "/I", "Divide Integer", data, offset);

                // Comparison
                case 0x80: return MakeInstr(offset, 2, "==I", "Compare Equal Int", data, offset);
                case 0x82: return MakeInstr(offset, 2, "<>I", "Compare Not Equal Int", data, offset);
                case 0x84: return MakeInstr(offset, 2, ">I", "Compare Greater Int", data, offset);
                case 0x86: return MakeInstr(offset, 2, "<I", "Compare Less Int", data, offset);

                // Block end
                case 0xBE: return MakeInstr(offset, 2, "BLD", "Block end", data, offset);

                // NOP
                case 0x00:
                    if (offset + 1 < data.Length && data[offset + 1] == 0x00)
                        return MakeInstr(offset, 2, "NOP", "No operation", data, offset);
                    break;
            }

            return null;
        }

        private static Mc7Instruction MakeInstr(int offset, int len, string mnemonic, string description, byte[] data, int pos)
        {
            len = Math.Min(len, data.Length - pos);
            var bytes = new byte[len];
            Array.Copy(data, pos, bytes, 0, len);

            return new Mc7Instruction
            {
                Offset = offset,
                Length = len,
                Mnemonic = mnemonic,
                Description = description,
                RawBytes = bytes,
                Hex = BitConverter.ToString(bytes).Replace("-", " ")
            };
        }

        private static List<string> ExtractStrings(byte[] data, int start, int end, int minLength)
        {
            var strings = new List<string>();
            var sb = new StringBuilder();

            for (int i = start; i < end && i < data.Length; i++)
            {
                if (data[i] >= 0x20 && data[i] < 0x7F)
                {
                    sb.Append((char)data[i]);
                }
                else
                {
                    if (sb.Length >= minLength)
                    {
                        strings.Add($"@0x{(i - sb.Length):X4}: \"{sb}\"");
                    }
                    sb.Clear();
                }
            }
            if (sb.Length >= minLength)
                strings.Add($"@0x{(end - sb.Length):X4}: \"{sb}\"");

            return strings;
        }

        private static string GenerateStlAnnotation(Mc7Annotation result)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"// ========================================");
            sb.AppendLine($"// {result.BlockType} #{result.BlockNumber} — MC7+ Bytecode Analysis");
            sb.AppendLine($"// Total size: {result.TotalSize} bytes");
            if (result.Mc7CodeSize > 0)
                sb.AppendLine($"// MC7 code size: {result.Mc7CodeSize} bytes");
            if (result.HasCompressedInterface)
                sb.AppendLine($"// Interface: compressed (zlib)");
            sb.AppendLine($"// ========================================");
            sb.AppendLine();

            foreach (var section in result.Sections)
            {
                sb.AppendLine($"// --- {section.Name} (offset 0x{section.Offset:X4}, {section.Length} bytes) ---");

                if (section.Annotations != null)
                {
                    foreach (var ann in section.Annotations)
                        sb.AppendLine($"//   {ann}");
                }

                if (section.Instructions != null && section.Instructions.Count > 0)
                {
                    sb.AppendLine();
                    foreach (var instr in section.Instructions)
                    {
                        string addr = $"0x{instr.Offset:X4}";
                        string hex = instr.Hex.PadRight(12);
                        string mnem = instr.Mnemonic.PadRight(8);
                        sb.AppendLine($"  {addr}:  {hex}  {mnem}  // {instr.Description}");
                    }
                }
                sb.AppendLine();
            }

            if (!result.Sections.Exists(s => s.Instructions?.Count > 0))
            {
                sb.AppendLine("// MC7+ bytecode (S7-1200/1500) uses a different instruction encoding");
                sb.AppendLine("// than classic MC7 (S7-300/400). Full disassembly requires specialized tools.");
                sb.AppendLine("// Use the HEX tab for raw bytecode visualization.");
            }

            return sb.ToString();
        }
    }

    public class Mc7Annotation
    {
        public string BlockType { get; set; }
        public int BlockNumber { get; set; }
        public int TotalSize { get; set; }
        public int Mc7CodeSize { get; set; }
        public bool HasCompressedInterface { get; set; }
        public string Error { get; set; }
        public string StlAnnotation { get; set; }
        public List<Mc7Section> Sections { get; set; }
    }

    public class Mc7Section
    {
        public string Name { get; set; }
        public int Offset { get; set; }
        public int Length { get; set; }
        public string Type { get; set; }
        public string Description { get; set; }
        public List<string> Annotations { get; set; }
        public List<Mc7Instruction> Instructions { get; set; }
    }

    public class Mc7Instruction
    {
        public int Offset { get; set; }
        public int Length { get; set; }
        public string Mnemonic { get; set; }
        public string Description { get; set; }
        public byte[] RawBytes { get; set; }
        public string Hex { get; set; }
    }

    // =====================================================================
    // MC7+ Enhanced Header Decoder (#7)
    // =====================================================================

    public class Mc7PlusHeaderInfo
    {
        public string Signature { get; set; }
        public string SignatureHex { get; set; }
        public byte BlockTypeByte { get; set; }
        public string BlockTypeStr { get; set; }
        public ushort BlockNumber { get; set; }
        public int TotalBlockLength { get; set; }
        public int LoadMemorySize { get; set; }
        public int CodeSize { get; set; }
        public int LocalDataSize { get; set; }
        public string BlockLanguage { get; set; }
        public string Version { get; set; }
        public ushort Checksum { get; set; }
        public string ChecksumHex { get; set; }
        public bool IsCompressed { get; set; }
        public int HeaderSize { get; set; }
        public string Error { get; set; }
    }

    public class Mc7Constant
    {
        public int Offset { get; set; }
        public string Type { get; set; }
        public string Value { get; set; }
        public string Hex { get; set; }
    }

    public static partial class Mc7AnnotatorExtensions
    {
        /// <summary>
        /// Enhanced MC7+ header parsing for S7-1200/1500 block bytecode.
        /// Returns structured header information beyond what the basic Annotate() provides.
        /// </summary>
        public static Mc7PlusHeaderInfo ParseMc7PlusHeader(byte[] rawData)
        {
            var info = new Mc7PlusHeaderInfo { HeaderSize = 36 };

            if (rawData == null || rawData.Length < 36)
            {
                info.Error = "Data too short for MC7+ header (minimum 36 bytes)";
                return info;
            }

            try
            {
                // Bytes 0-1: Signature
                ushort sig = (ushort)((rawData[0] << 8) | rawData[1]);
                info.SignatureHex = $"0x{sig:X4}";
                switch (sig)
                {
                    case 0x7070: info.Signature = "PP (standard block)"; break;
                    case 0x7071: info.Signature = "PP (system block)"; break;
                    default: info.Signature = $"Unknown (0x{sig:X4})"; break;
                }

                // Byte 2: Block type
                info.BlockTypeByte = rawData[2];
                switch (rawData[2])
                {
                    case 0x08: info.BlockTypeStr = "OB"; break;
                    case 0x0A: info.BlockTypeStr = "DB"; break;
                    case 0x0B: info.BlockTypeStr = "SDB"; break;
                    case 0x0C: info.BlockTypeStr = "FC"; break;
                    case 0x0E: info.BlockTypeStr = "FB"; break;
                    default: info.BlockTypeStr = $"0x{rawData[2]:X2}"; break;
                }

                // Bytes 3-4: Block number (big-endian)
                info.BlockNumber = (ushort)((rawData[3] << 8) | rawData[4]);

                // Bytes 5-8: Total block length (big-endian, 4 bytes)
                if (rawData.Length >= 9)
                    info.TotalBlockLength = (rawData[5] << 24) | (rawData[6] << 16) | (rawData[7] << 8) | rawData[8];

                // Code and local data sizes vary by format
                // Classic MC7 (S7-300/400): code at bytes 30-31, local at 32-33
                // MC7+ (S7-1200/1500): may be at different offsets
                if (rawData.Length >= 34)
                {
                    info.CodeSize = (rawData[30] << 8) | rawData[31];
                    info.LocalDataSize = (rawData[32] << 8) | rawData[33];
                }

                // Load memory size: total block length is often the load memory
                info.LoadMemorySize = info.TotalBlockLength;

                // Language detection (varies by block version)
                info.BlockLanguage = DetectBlockLanguage(rawData);

                // Version detection
                if (rawData.Length >= 10)
                    info.Version = $"{rawData[9] >> 4}.{rawData[9] & 0x0F}";

                // Checksum (typically last 2 bytes of header area or block footer)
                if (rawData.Length >= 36)
                {
                    info.Checksum = (ushort)((rawData[34] << 8) | rawData[35]);
                    info.ChecksumHex = $"0x{info.Checksum:X4}";
                }

                // Compression: check for zlib header (0x789C) in data after header
                if (rawData.Length > 40)
                {
                    for (int i = 36; i < Math.Min(rawData.Length - 1, 100); i++)
                    {
                        if (rawData[i] == 0x78 && (rawData[i + 1] == 0x9C || rawData[i + 1] == 0x01 || rawData[i + 1] == 0xDA))
                        {
                            info.IsCompressed = true;
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                info.Error = $"Header parse error: {ex.Message}";
            }

            return info;
        }

        /// <summary>
        /// Attempts to extract constant values found in the block data.
        /// Looks for recognizable patterns in the constant area.
        /// </summary>
        public static List<Mc7Constant> ExtractConstants(byte[] rawData)
        {
            var constants = new List<Mc7Constant>();
            if (rawData == null || rawData.Length < 40) return constants;

            try
            {
                // Look for string constants (sequences of printable ASCII followed by null)
                for (int i = 36; i < rawData.Length - 4; i++)
                {
                    // Detect string patterns: length byte + ASCII chars
                    if (rawData[i] > 2 && rawData[i] < 128)
                    {
                        int strLen = rawData[i];
                        if (i + 1 + strLen <= rawData.Length)
                        {
                            bool isString = true;
                            for (int j = 0; j < Math.Min(strLen, 50); j++)
                            {
                                byte b = rawData[i + 1 + j];
                                if (b < 0x20 || b > 0x7E) { isString = false; break; }
                            }
                            if (isString && strLen >= 3 && strLen <= 50)
                            {
                                string str = System.Text.Encoding.ASCII.GetString(rawData, i + 1, strLen);
                                constants.Add(new Mc7Constant
                                {
                                    Offset = i,
                                    Type = "String",
                                    Value = str,
                                    Hex = BitConverter.ToString(rawData, i, Math.Min(strLen + 1, 20)).Replace("-", " ")
                                });
                                i += strLen; // Skip past this string
                            }
                        }
                    }

                    // Detect float constants (4 bytes that decode to reasonable Real values)
                    if (i + 4 <= rawData.Length)
                    {
                        byte[] floatBytes = new byte[4];
                        // S7 uses big-endian
                        floatBytes[0] = rawData[i + 3];
                        floatBytes[1] = rawData[i + 2];
                        floatBytes[2] = rawData[i + 1];
                        floatBytes[3] = rawData[i];

                        float val = BitConverter.ToSingle(floatBytes, 0);
                        // Only report "interesting" floats (not NaN, Inf, or very small exponents)
                        if (!float.IsNaN(val) && !float.IsInfinity(val) && Math.Abs(val) > 0.001f && Math.Abs(val) < 1e10f)
                        {
                            // Check if this looks like an intentional constant (not random bytes)
                            byte exp = (byte)((rawData[i] & 0x7F) >> 0);
                            if (exp > 0x20 && exp < 0x60) // Reasonable exponent range
                            {
                                constants.Add(new Mc7Constant
                                {
                                    Offset = i,
                                    Type = "Real",
                                    Value = val.ToString("G6"),
                                    Hex = BitConverter.ToString(rawData, i, 4).Replace("-", " ")
                                });
                            }
                        }
                    }
                }

                // Limit to first 50 constants to avoid noise
                if (constants.Count > 50)
                    constants = constants.Take(50).ToList();
            }
            catch { /* non-fatal */ }

            return constants;
        }

        private static string DetectBlockLanguage(byte[] data)
        {
            // The language byte location varies. Common positions:
            // Offset 36-40 in classic MC7, or encoded in attributes for MC7+
            // Try a few known positions
            if (data.Length > 40)
            {
                byte langByte = data[36];
                switch (langByte)
                {
                    case 0x01: return "AWL/STL";
                    case 0x02: return "KOP/LAD";
                    case 0x03: return "FUP/FBD";
                    case 0x04: return "SCL";
                    case 0x05: return "DB";
                    case 0x06: return "GRAPH";
                    case 0x07: return "SDB";
                    case 0x08: return "CPU_DB";
                }

                // Try alternative offset (byte 38 or 40)
                if (data.Length > 42)
                {
                    langByte = data[38];
                    switch (langByte)
                    {
                        case 0x01: return "AWL/STL";
                        case 0x02: return "KOP/LAD";
                        case 0x03: return "FUP/FBD";
                        case 0x04: return "SCL";
                        case 0x05: return "DB";
                        case 0x06: return "GRAPH";
                    }
                }
            }
            return "Unknown";
        }
    }
}
