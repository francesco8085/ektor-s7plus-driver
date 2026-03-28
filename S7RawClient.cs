// =============================================================================
// EktorS7PlusDriver — S7CommPlus Communication Driver for Siemens S7-1200/1500
// =============================================================================
// Copyright (c) 2025-2026 Francesco Cesarone <f.cesarone@entersrl.it>
// Azienda   : Enter SRL
// Progetto  : EKTOR Industrial IoT Platform
// Licenza   : Proprietaria — uso riservato Enter SRL
// =============================================================================
//
// S7RawClient.cs — S7comm classic protocol client for SZL reads and block upload
// Uses raw TCP on port 102 (ISO-on-TCP / COTP), no TLS
// Runs in parallel on a dedicated COTP session, independent from S7CommPlus
//
// Implements the publicly documented Siemens S7 communication protocol

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace EnterSrl.Ektor.S7Plus
{
    // =========================================================================
    // Data models
    // =========================================================================

    public class S7BlockCount
    {
        public byte Type { get; set; }
        public string TypeName { get; set; }
        public int Count { get; set; }
    }

    public class S7BlockListEntry
    {
        public ushort Number { get; set; }
        public byte Flags { get; set; }
        public byte Language { get; set; }
    }

    public class S7BlockInfoDetail
    {
        public byte BlockType { get; set; }
        public ushort BlockNumber { get; set; }
        public string BlockLang { get; set; }
        public string Author { get; set; }
        public string Family { get; set; }
        public string Name { get; set; }
        public int McSize { get; set; }     // MC7 code size
        public int LoadSize { get; set; }   // Load memory size
        public int SbbLength { get; set; }
        public string Version { get; set; }
        public DateTime LoadDate { get; set; }
        public DateTime CodeDate { get; set; }
        public ushort Checksum { get; set; }
    }

    public class S7CpuInfo
    {
        public string ModuleTypeName { get; set; }
        public string SerialNumber { get; set; }
        public string PlantId { get; set; }
        public string Copyright { get; set; }
        public string ModuleName { get; set; }
        public string OrderCode { get; set; }
        public string MemoryCardOrderCode { get; set; }
        public string FirmwareVersion { get; set; }
        public string BootLoaderVersion { get; set; }
    }

    public class S7RawBlockData
    {
        public byte BlockType { get; set; }
        public ushort BlockNumber { get; set; }
        public byte[] RawData { get; set; }
        public string HexDump { get; set; }
    }

    // =========================================================================
    // S7 Raw Client
    // =========================================================================

    public class S7RawClient : IDisposable
    {
        // --- Block type codes (S7 protocol) ---
        public const byte BlockOB  = 0x08;
        public const byte BlockDB  = 0x0A;
        public const byte BlockSDB = 0x0B;
        public const byte BlockFC  = 0x0C;
        public const byte BlockSFC = 0x0D;
        public const byte BlockFB  = 0x0E;
        public const byte BlockSFB = 0x0F;

        // --- Block type ASCII codes (for upload filename) ---
        private const byte FileOB  = 0x38; // '8'
        private const byte FileDB  = 0x41; // 'A'
        private const byte FileSDB = 0x42; // 'B'
        private const byte FileSFC = 0x43; // 'C' -- note: SFC not FC
        private const byte FileSFB = 0x44; // 'D' -- note: SFB not FB
        private const byte FileFB  = 0x45; // 'E'
        private const byte FileFC  = 0x46; // 'F'

        // --- Protocol constants ---
        private const int IsoHSize = 7;    // TPKT(4) + COTP DT(3)
        private const int MaxPdu = 960;
        private const int DefaultTimeout = 5000;

        // --- Connection state ---
        private TcpClient _tcp;
        private NetworkStream _stream;
        private readonly object _lock = new object();
        private ushort _pduRef;
        private int _negotiatedPdu = 480;
        private bool _connected;

        public bool IsConnected => _connected && _tcp != null && _tcp.Connected;
        public string IpAddress { get; private set; }
        public int Timeout { get; set; } = DefaultTimeout;

        // =====================================================================
        // Connection
        // =====================================================================

        public int Connect(string ip, int rack = 0, int slot = 0)
        {
            lock (_lock)
            {
                if (_connected) Disconnect();

                IpAddress = ip;
                _pduRef = 0;

                try
                {
                    // 1. TCP connect
                    _tcp = new TcpClient();
                    _tcp.NoDelay = true;
                    _tcp.SendTimeout = Timeout;
                    _tcp.ReceiveTimeout = Timeout;

                    var ar = _tcp.BeginConnect(ip, 102, null, null);
                    if (!ar.AsyncWaitHandle.WaitOne(Timeout))
                    {
                        _tcp.Close();
                        _tcp = null;
                        return -1; // Timeout
                    }
                    _tcp.EndConnect(ar);
                    _stream = _tcp.GetStream();

                    // 2. COTP Connection Request
                    int isoRes = IsoConnect(rack, slot);
                    if (isoRes != 0) return isoRes;

                    // 3. S7 Setup Communication
                    int setupRes = S7SetupCommunication();
                    if (setupRes != 0) return setupRes;

                    _connected = true;
                    Console.WriteLine($"[S7RawClient] Connected to {ip} (rack={rack}, slot={slot}), PDU={_negotiatedPdu}");
                    return 0;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[S7RawClient] Connect error: {ex.Message}");
                    Disconnect();
                    return -2;
                }
            }
        }

        public void Disconnect()
        {
            lock (_lock)
            {
                _connected = false;
                try { _stream?.Close(); } catch { }
                try { _tcp?.Close(); } catch { }
                _stream = null;
                _tcp = null;
            }
        }

        // =====================================================================
        // SZL Read (generic)
        // =====================================================================

        public byte[] ReadSzl(ushort szlId, ushort szlIndex)
        {
            lock (_lock)
            {
                if (!IsConnected) throw new InvalidOperationException("Not connected");

                var request = BuildSzlRequest(szlId, szlIndex);
                var response = SendReceive(request);
                if (response == null) return null;

                // Parse the userdata response
                // S7 header at offset 7 (after TPKT+COTP)
                // Parameter at offset 7+10, Data after parameter
                int paramLen = GetWordAt(response, 13);
                int dataOfs = 17 + paramLen; // 7(iso) + 10(s7hdr) + paramLen

                // Check return code in data section
                if (dataOfs >= response.Length) return null;
                byte retCode = response[dataOfs];

                if (retCode != 0xFF && retCode != 0x0A)
                    return null; // Error

                // Data starts at dataOfs+4 (skip retcode, transport, length)
                int dataLen = GetWordAt(response, dataOfs + 2);
                if (dataLen <= 0) return null;

                var result = new MemoryStream();

                // First chunk
                int chunkStart = dataOfs + 4;
                int chunkLen = Math.Min(dataLen, response.Length - chunkStart);
                if (chunkLen > 0)
                    result.Write(response, chunkStart, chunkLen);

                // Multi-PDU continuation
                byte seq = 1;
                while (retCode == 0x0A)
                {
                    var contReq = BuildSzlContinuation(szlId, szlIndex, seq);
                    response = SendReceive(contReq);
                    if (response == null) break;

                    paramLen = GetWordAt(response, 13);
                    dataOfs = 17 + paramLen;
                    if (dataOfs >= response.Length) break;

                    retCode = response[dataOfs];
                    dataLen = GetWordAt(response, dataOfs + 2);

                    chunkStart = dataOfs + 4;
                    chunkLen = Math.Min(dataLen, response.Length - chunkStart);
                    if (chunkLen > 0)
                        result.Write(response, chunkStart, chunkLen);

                    seq++;
                    if (seq > 100) break; // Safety limit
                }

                return result.ToArray();
            }
        }

        // =====================================================================
        // CPU Info (SZL 0x001C)
        // =====================================================================

        public S7CpuInfo ReadCpuInfo()
        {
            var info = new S7CpuInfo();

            var data = ReadSzl(0x001C, 0x0000);
            if (data != null && data.Length >= 36)
            {
                // SZL 0x001C header: szlId(2) + index(2) + recordLen(2) + recordCount(2) = 8 bytes
                // Then records of recordLen each
                int recordLen = GetWordAt(data, 4);
                int recordCount = GetWordAt(data, 6);
                int ofs = 8; // Skip SZL header

                if (recordLen > 0 && recordCount > 0)
                {
                    for (int i = 0; i < recordCount && ofs + recordLen <= data.Length; i++)
                    {
                        ushort idx = GetWordAt(data, ofs);
                        string val = ReadSzlString(data, ofs + 2, recordLen - 2);

                        switch (idx)
                        {
                            case 1: info.ModuleTypeName = val; break;
                            case 2: info.SerialNumber = val; break;
                            case 3: info.PlantId = val; break;
                            case 4: info.Copyright = val; break;
                            case 5: info.ModuleName = val; break;
                        }
                        ofs += recordLen;
                    }
                }
            }

            TryEnrichCpuInfoWithModuleIdentification(info);
            if (string.IsNullOrWhiteSpace(info.ModuleTypeName) &&
                string.IsNullOrWhiteSpace(info.SerialNumber) &&
                string.IsNullOrWhiteSpace(info.PlantId) &&
                string.IsNullOrWhiteSpace(info.Copyright) &&
                string.IsNullOrWhiteSpace(info.ModuleName) &&
                string.IsNullOrWhiteSpace(info.OrderCode) &&
                string.IsNullOrWhiteSpace(info.MemoryCardOrderCode) &&
                string.IsNullOrWhiteSpace(info.FirmwareVersion) &&
                string.IsNullOrWhiteSpace(info.BootLoaderVersion))
            {
                return null;
            }

            return info;
        }

        private void TryEnrichCpuInfoWithModuleIdentification(S7CpuInfo info)
        {
            if (info == null) return;

            try
            {
                var data = ReadSzl(0x0011, 0x0000);
                if (data == null || data.Length < 16) return;

                var orderCodes = ExtractOrderCodes(data);
                if (orderCodes.Count > 0 && string.IsNullOrWhiteSpace(info.OrderCode))
                    info.OrderCode = orderCodes[0];
                if (orderCodes.Count > 1 && string.IsNullOrWhiteSpace(info.MemoryCardOrderCode))
                    info.MemoryCardOrderCode = orderCodes[1];

                if (string.IsNullOrWhiteSpace(info.FirmwareVersion))
                    info.FirmwareVersion = ExtractFirmwareVersion(data);

                if (string.IsNullOrWhiteSpace(info.BootLoaderVersion))
                    info.BootLoaderVersion = ExtractBootLoaderVersion(data);
            }
            catch
            {
                // Metadata enrichment is best-effort only.
            }
        }

        // =====================================================================
        // List Blocks (Userdata func group 3, subfunc 1)
        // =====================================================================

        public List<S7BlockCount> ListBlocks()
        {
            lock (_lock)
            {
                if (!IsConnected) throw new InvalidOperationException("Not connected");

                var request = BuildListBlocksRequest();
                var response = SendReceive(request);
                if (response == null) return null;

                int paramLen = GetWordAt(response, 13);
                int dataOfs = 17 + paramLen;
                if (dataOfs >= response.Length) return null;

                byte retCode = response[dataOfs];
                if (retCode != 0xFF) return null;

                int dataLen = GetWordAt(response, dataOfs + 2);
                if (dataLen < 4) return null;

                var result = new List<S7BlockCount>();
                int pos = dataOfs + 4;

                // Each entry is 4 bytes: blockType(2) + count(2)
                while (pos + 4 <= dataOfs + 4 + dataLen)
                {
                    byte type = response[pos + 1]; // Second byte is the block type
                    ushort count = GetWordAt(response, pos + 2);
                    result.Add(new S7BlockCount
                    {
                        Type = type,
                        TypeName = BlockTypeName(type),
                        Count = count
                    });
                    pos += 4;
                }

                return result;
            }
        }

        // =====================================================================
        // List Blocks of Type (Userdata func group 3, subfunc 2)
        // =====================================================================

        public List<S7BlockListEntry> ListBlocksOfType(byte blockType)
        {
            lock (_lock)
            {
                if (!IsConnected) throw new InvalidOperationException("Not connected");

                var result = new List<S7BlockListEntry>();
                byte seq = 0;
                bool moreData = true;
                bool first = true;

                while (moreData)
                {
                    byte[] response;
                    if (first)
                    {
                        var request = BuildListBlocksOfTypeRequest(blockType);
                        response = SendReceive(request);
                        first = false;
                    }
                    else
                    {
                        var contReq = BuildListBlocksOfTypeContinuation(blockType, seq);
                        response = SendReceive(contReq);
                    }

                    if (response == null) break;

                    int paramLen = GetWordAt(response, 13);
                    int dataOfs = 17 + paramLen;
                    if (dataOfs >= response.Length) break;

                    byte retCode = response[dataOfs];
                    if (retCode != 0xFF && retCode != 0x0A) break;

                    int dataLen = GetWordAt(response, dataOfs + 2);
                    int pos = dataOfs + 4;

                    // Each entry: blockNumber(2) + flags(1) + language(1) = 4 bytes
                    while (pos + 4 <= dataOfs + 4 + dataLen)
                    {
                        result.Add(new S7BlockListEntry
                        {
                            Number = GetWordAt(response, pos),
                            Flags = response[pos + 2],
                            Language = response[pos + 3]
                        });
                        pos += 4;
                    }

                    moreData = (retCode == 0x0A);
                    seq++;
                    if (seq > 100) break;
                }

                return result;
            }
        }

        // =====================================================================
        // Get Block Info (Userdata func group 3, subfunc 3)
        // =====================================================================

        public S7BlockInfoDetail GetBlockInfo(byte blockType, ushort blockNumber)
        {
            lock (_lock)
            {
                if (!IsConnected) throw new InvalidOperationException("Not connected");

                var request = BuildGetBlockInfoRequest(blockType, blockNumber);
                var response = SendReceive(request);
                if (response == null) return null;

                int paramLen = GetWordAt(response, 13);
                int dataOfs = 17 + paramLen;
                if (dataOfs + 4 >= response.Length) return null;

                byte retCode = response[dataOfs];
                if (retCode != 0xFF) return null;

                int dataLen = GetWordAt(response, dataOfs + 2);
                if (dataLen < 64) return null;

                int d = dataOfs + 4; // Start of block info data

                var info = new S7BlockInfoDetail
                {
                    BlockType = response[d + 1],  // Subtype
                    BlockNumber = GetWordAt(response, d + 2),
                    BlockLang = LangName(response[d + 6]),
                    Author = ReadFixedString(response, d + 10, 8),
                    Family = ReadFixedString(response, d + 18, 8),
                    Name = ReadFixedString(response, d + 26, 8),
                    Version = $"{response[d + 34] >> 4}.{response[d + 34] & 0x0F}",
                    Checksum = GetWordAt(response, d + 38),
                    McSize = (int)GetDWordAt(response, d + 44),
                    LoadSize = (int)GetDWordAt(response, d + 48),
                    SbbLength = GetWordAt(response, d + 52),
                };

                // Load date (BCD encoded, 6 bytes at offset 54)
                try { info.LoadDate = DecodeBcdDate(response, d + 54); } catch { }
                // Code date (BCD encoded, 6 bytes at offset 60)
                try { info.CodeDate = DecodeBcdDate(response, d + 60); } catch { }

                return info;
            }
        }

        // =====================================================================
        // Upload Block (functions 0x1D Start, 0x1E Data, 0x1F End)
        // =====================================================================

        public S7RawBlockData UploadBlock(byte blockType, ushort blockNumber)
        {
            lock (_lock)
            {
                if (!IsConnected) throw new InvalidOperationException("Not connected");

                // --- Step 1: Start Upload ---
                byte fileBlockType = BlockTypeToFile(blockType);
                var startReq = BuildStartUploadRequest(fileBlockType, blockNumber);
                var startResp = SendReceive(startReq);
                if (startResp == null)
                    throw new IOException("No response to StartUpload");

                // Parse StartUpload response
                // S7 header at offset 7, function code at offset 17
                if (startResp.Length < 20) throw new IOException("StartUpload response too short");

                byte funcCode = startResp[17];
                byte funcStatus = startResp[18];
                if (funcCode != 0x1D)
                    throw new IOException($"Unexpected function code 0x{funcCode:X2} in StartUpload response");

                // Check for error (error class at offset 21, error code at offset 22 in Ack_Data)
                byte errClass = startResp[21];
                byte errCode = startResp[22];
                if (errClass != 0)
                    throw new IOException($"StartUpload error: class=0x{errClass:X2} code=0x{errCode:X2}");

                // Upload ID at offset 20 (after error fields)
                // The response format varies — parse parameter
                // Param: [funcCode][funcStatus][2 reserved][4 bytes uploadId][1 byte lenLen][len bytes blockLen]
                uint uploadId = GetDWordAt(startResp, 20);

                // --- Step 2: Upload data chunks ---
                var allData = new MemoryStream();
                bool moreData = true;

                while (moreData)
                {
                    var dataReq = BuildUploadDataRequest(uploadId);
                    var dataResp = SendReceive(dataReq);
                    if (dataResp == null) throw new IOException("No response to Upload");

                    // Parse response
                    if (dataResp.Length < 24) throw new IOException("Upload response too short");

                    funcCode = dataResp[17];
                    funcStatus = dataResp[18];

                    if (funcCode != 0x1E)
                        throw new IOException($"Unexpected function code 0x{funcCode:X2} in Upload response");

                    // Data length at param offset + data offset
                    int paramLen2 = GetWordAt(dataResp, 13);
                    int dataOfs2 = 17 + paramLen2;

                    if (dataOfs2 + 4 > dataResp.Length) break;

                    // Data: [returnCode][transportSize][dataLength(2)][data...]
                    int blockDataLen = GetWordAt(dataResp, dataOfs2 + 2);
                    int blockDataStart = dataOfs2 + 4;

                    if (blockDataStart + blockDataLen > dataResp.Length)
                        blockDataLen = dataResp.Length - blockDataStart;

                    if (blockDataLen > 0)
                        allData.Write(dataResp, blockDataStart, blockDataLen);

                    // Check if more data follows (bit 0 of funcStatus)
                    moreData = (funcStatus & 0x01) != 0;
                }

                // --- Step 3: End Upload ---
                try
                {
                    var endReq = BuildEndUploadRequest(uploadId);
                    SendReceive(endReq);
                }
                catch { /* End upload failure is non-fatal */ }

                byte[] raw = allData.ToArray();
                return new S7RawBlockData
                {
                    BlockType = blockType,
                    BlockNumber = blockNumber,
                    RawData = raw,
                    HexDump = FormatHexDump(raw, 32)
                };
            }
        }

        // =====================================================================
        // Request builders
        // =====================================================================

        private int IsoConnect(int rack, int slot)
        {
            // COTP Connection Request with 2-byte TSAPs
            byte[] cr = {
                0x03, 0x00, 0x00, 0x16, // TPKT: version 3, length 22
                0x11,                    // COTP length = 17
                0xE0,                    // CR = Connection Request
                0x00, 0x00,              // Dst Reference
                0x00, 0x01,              // Src Reference
                0x00,                    // Class 0
                0xC0, 0x01, 0x0A,       // PDU Max Length = 1024
                0xC1, 0x02, 0x01, 0x00, // Src TSAP: 0x0100
                0xC2, 0x02, 0x00, 0x00  // Dst TSAP: computed
            };

            // Dst TSAP = (connType << 8) | (rack * 0x20 + slot)
            // connType: 1=PG, 2=OP, 3=S7Basic
            cr[20] = 0x01; // PG connection
            cr[21] = (byte)(rack * 0x20 + slot);

            SendRaw(cr);
            var resp = RecvIso();
            if (resp == null || resp.Length < 6 || resp[5] != 0xD0)
            {
                Console.WriteLine("[S7RawClient] ISO Connect failed — CC not received");
                return -3;
            }
            return 0;
        }

        private int S7SetupCommunication()
        {
            byte[] setup = {
                0x03, 0x00, 0x00, 0x19, // TPKT: length 25
                0x02, 0xF0, 0x80,       // COTP DT
                0x32,                    // S7 Protocol ID
                0x01,                    // Message type: Job
                0x00, 0x00,              // Reserved
                0x00, 0x00,              // PDU Reference
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x00,              // Data length (0)
                // Parameter: Setup Communication
                0xF0,                    // Function
                0x00,                    // Reserved
                0x00, 0x01,              // Max AMQ calling
                0x00, 0x01,              // Max AMQ called
                0x03, 0xC0              // PDU Length (960)
            };

            SetWordAt(setup, 11, NextPduRef());
            SendRaw(setup);

            var resp = RecvIso();
            if (resp == null || resp.Length < 25)
            {
                Console.WriteLine("[S7RawClient] Setup Communication failed");
                return -4;
            }

            // Check S7 header: protocol ID must be 0x32, message type 0x03 (Ack_Data)
            if (resp[7] != 0x32 || resp[8] != 0x03) return -4;

            // Negotiated PDU at offset 25 (parameter offset 17 + 8)
            _negotiatedPdu = GetWordAt(resp, 23);
            if (_negotiatedPdu < 200) _negotiatedPdu = 480;

            return 0;
        }

        private byte[] BuildSzlRequest(ushort szlId, ushort szlIndex)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x21, // TPKT: length 33
                0x02, 0xF0, 0x80,       // COTP DT
                0x32,                    // S7 Protocol ID
                0x07,                    // Message type: Userdata
                0x00, 0x00,              // Reserved
                0x00, 0x00,              // PDU Reference (filled)
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x08,              // Data length (8)
                // Parameter (8 bytes):
                0x00, 0x01, 0x12,       // Head
                0x04,                    // Param data len (4)
                0x11,                    // Method: Request
                0x44,                    // Type(4=req) | FuncGroup(4=CPU)
                0x01,                    // Subfunction: Read SZL
                0x00,                    // Sequence
                // Data (8 bytes):
                0xFF,                    // Return code
                0x09,                    // Transport size: Octet string
                0x00, 0x04,              // Data length (4 bytes)
                0x00, 0x00,              // SZL ID (filled)
                0x00, 0x00               // SZL Index (filled)
            };

            SetWordAt(req, 11, NextPduRef());
            SetWordAt(req, 29, szlId);
            SetWordAt(req, 31, szlIndex);
            return req;
        }

        private byte[] BuildSzlContinuation(ushort szlId, ushort szlIndex, byte seq)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x25, // TPKT: length 37
                0x02, 0xF0, 0x80,       // COTP DT
                0x32,                    // S7 Protocol ID
                0x07,                    // Message type: Userdata
                0x00, 0x00,              // Reserved
                0x00, 0x00,              // PDU Reference
                0x00, 0x0C,              // Parameter length (12)
                0x00, 0x04,              // Data length (4)
                // Parameter (12 bytes):
                0x00, 0x01, 0x12,       // Head
                0x08,                    // Param data len (8)
                0x12,                    // Method: Follow-up
                0x44,                    // Type(4) | FuncGroup(4=CPU)
                0x01,                    // Subfunction: Read SZL
                seq,                     // Sequence
                0x00, 0x00, 0x00, 0x00, // Error (zeroed)
                // Data (4 bytes):
                0x0A,                    // Return code: continuation
                0x00,                    // Transport size
                0x00, 0x00               // Data length (0)
            };

            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildListBlocksRequest()
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x1D, // TPKT: length 29
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x07, 0x00, 0x00, // S7 Userdata
                0x00, 0x00,              // PDU Reference
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x04,              // Data length (4)
                // Parameter:
                0x00, 0x01, 0x12,
                0x04,                    // Param data len
                0x11,                    // Method: Request
                0x43,                    // Type(4=req) | FuncGroup(3=Block)
                0x01,                    // Subfunction: List blocks
                0x00,                    // Sequence
                // Data:
                0x0A,                    // Return code
                0x00,                    // Transport
                0x00, 0x00               // Data length 0
            };

            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildListBlocksOfTypeRequest(byte blockType)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x21, // TPKT: length 33
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x07, 0x00, 0x00, // S7 Userdata
                0x00, 0x00,              // PDU Reference
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x08,              // Data length (8)
                // Parameter:
                0x00, 0x01, 0x12,
                0x04,
                0x11,                    // Method: Request
                0x43,                    // FuncGroup 3 = Block
                0x02,                    // Subfunction: List blocks of type
                0x00,                    // Sequence
                // Data:
                0xFF,                    // Return code
                0x09,                    // Transport: Octet string
                0x00, 0x04,              // Data length (4)
                0x30, 0x00,              // ASCII prefix + block type
                0x00, 0x00               // Reserved
            };

            SetWordAt(req, 11, NextPduRef());
            req[29] = BlockTypeToFile(blockType); // Block type as file prefix
            return req;
        }

        private byte[] BuildListBlocksOfTypeContinuation(byte blockType, byte seq)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x25, // TPKT: length 37
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x07, 0x00, 0x00,
                0x00, 0x00,              // PDU Reference
                0x00, 0x0C,              // Parameter length (12)
                0x00, 0x04,              // Data length (4)
                // Parameter (12 bytes):
                0x00, 0x01, 0x12,
                0x08,
                0x12,                    // Follow-up
                0x43,                    // Block
                0x02,                    // List of type
                seq,
                0x00, 0x00, 0x00, 0x00,
                // Data:
                0x0A, 0x00, 0x00, 0x00
            };

            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildGetBlockInfoRequest(byte blockType, ushort blockNumber)
        {
            // Filename: "_0" + fileType + number(5 ascii) + "A"
            byte fileType = BlockTypeToFile(blockType);
            string numStr = blockNumber.ToString("D5");

            byte[] req = {
                0x03, 0x00, 0x00, 0x25, // TPKT: length 37
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x07, 0x00, 0x00,
                0x00, 0x00,              // PDU Reference
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x0C,              // Data length (12)
                // Parameter:
                0x00, 0x01, 0x12,
                0x04,
                0x11,                    // Request
                0x43,                    // Block
                0x03,                    // Subfunction: Get block info
                0x00,                    // Sequence
                // Data (12 bytes):
                0xFF,                    // Return code
                0x09,                    // Transport
                0x00, 0x08,              // Data length (8 = filename)
                0x30, fileType,          // "0" + file type
                (byte)numStr[0], (byte)numStr[1],
                (byte)numStr[2], (byte)numStr[3],
                (byte)numStr[4],
                0x41                     // 'A' = active
            };

            // Fix TPKT length
            SetWordAt(req, 2, (ushort)req.Length);
            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildStartUploadRequest(byte fileType, ushort blockNumber)
        {
            string numStr = blockNumber.ToString("D5");

            byte[] req = {
                0x03, 0x00, 0x00, 0x00, // TPKT: length (filled)
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x01, 0x00, 0x00, // S7 Job
                0x00, 0x00,              // PDU Reference
                0x00, 0x12,              // Parameter length (18)
                0x00, 0x00,              // Data length (0)
                // Parameter (18 bytes):
                0x1D,                    // Function: Start Upload
                0x00,                    // Reserved
                0x00, 0x00, 0x00, 0x00, // Error status
                0x00, 0x00, 0x00, 0x01, // Upload ID + reserved
                0x09,                    // Filename length (9)
                0x5F, 0x30,              // "_0"
                fileType,                // Block type
                (byte)numStr[0], (byte)numStr[1],
                (byte)numStr[2], (byte)numStr[3],
                (byte)numStr[4],
                0x41                     // 'A' = Active
            };

            SetWordAt(req, 2, (ushort)req.Length);
            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildUploadDataRequest(uint uploadId)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x00, // TPKT: length (filled)
                0x02, 0xF0, 0x80,       // COTP DT
                0x32, 0x01, 0x00, 0x00, // S7 Job
                0x00, 0x00,              // PDU Reference
                0x00, 0x08,              // Parameter length (8)
                0x00, 0x00,              // Data length (0)
                // Parameter:
                0x1E,                    // Function: Upload
                0x00,                    // Reserved
                0x00, 0x00, 0x00, 0x00, // Status
                0x00, 0x00, 0x00, 0x00  // Upload ID (filled)
            };

            SetDWordAt(req, 21, uploadId);
            SetWordAt(req, 2, (ushort)req.Length);
            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        private byte[] BuildEndUploadRequest(uint uploadId)
        {
            byte[] req = {
                0x03, 0x00, 0x00, 0x00, // TPKT
                0x02, 0xF0, 0x80,       // COTP
                0x32, 0x01, 0x00, 0x00, // S7 Job
                0x00, 0x00,              // PDU Ref
                0x00, 0x08,              // Param len
                0x00, 0x00,              // Data len
                0x1F,                    // Function: End Upload
                0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00  // Upload ID
            };

            SetDWordAt(req, 21, uploadId);
            SetWordAt(req, 2, (ushort)req.Length);
            SetWordAt(req, 11, NextPduRef());
            return req;
        }

        // =====================================================================
        // Low-level I/O
        // =====================================================================

        private void SendRaw(byte[] data)
        {
            _stream.Write(data, 0, data.Length);
            _stream.Flush();
        }

        private byte[] RecvIso()
        {
            // Read TPKT header (4 bytes)
            byte[] hdr = new byte[4];
            int read = ReadFull(hdr, 0, 4);
            if (read < 4) return null;

            if (hdr[0] != 0x03) return null;
            int pduLen = (hdr[2] << 8) | hdr[3];
            if (pduLen < 7 || pduLen > 65535) return null;

            // Read rest of the PDU
            byte[] pdu = new byte[pduLen];
            Array.Copy(hdr, pdu, 4);
            read = ReadFull(pdu, 4, pduLen - 4);
            if (read < pduLen - 4) return null;

            return pdu;
        }

        private byte[] SendReceive(byte[] request)
        {
            try
            {
                SendRaw(request);
                return RecvIso();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[S7RawClient] SendReceive error: {ex.Message}");
                _connected = false;
                return null;
            }
        }

        private int ReadFull(byte[] buffer, int offset, int count)
        {
            int total = 0;
            var deadline = DateTime.UtcNow.AddMilliseconds(Timeout);

            while (total < count)
            {
                if (DateTime.UtcNow > deadline) break;

                try
                {
                    int n = _stream.Read(buffer, offset + total, count - total);
                    if (n <= 0) break;
                    total += n;
                }
                catch (IOException)
                {
                    break;
                }
            }
            return total;
        }

        // =====================================================================
        // Helpers
        // =====================================================================

        private ushort NextPduRef()
        {
            _pduRef++;
            if (_pduRef == 0) _pduRef = 1;
            return _pduRef;
        }

        private static ushort GetWordAt(byte[] buf, int pos)
        {
            return (ushort)((buf[pos] << 8) | buf[pos + 1]);
        }

        private static void SetWordAt(byte[] buf, int pos, ushort val)
        {
            buf[pos] = (byte)(val >> 8);
            buf[pos + 1] = (byte)(val & 0xFF);
        }

        private static uint GetDWordAt(byte[] buf, int pos)
        {
            return (uint)((buf[pos] << 24) | (buf[pos + 1] << 16) | (buf[pos + 2] << 8) | buf[pos + 3]);
        }

        private static void SetDWordAt(byte[] buf, int pos, uint val)
        {
            buf[pos]     = (byte)(val >> 24);
            buf[pos + 1] = (byte)(val >> 16);
            buf[pos + 2] = (byte)(val >> 8);
            buf[pos + 3] = (byte)(val & 0xFF);
        }

        private static string ReadFixedString(byte[] buf, int pos, int maxLen)
        {
            if (pos + maxLen > buf.Length) maxLen = buf.Length - pos;
            int len = 0;
            while (len < maxLen && buf[pos + len] != 0) len++;
            return Encoding.ASCII.GetString(buf, pos, len).Trim();
        }

        private static string ReadSzlString(byte[] buf, int pos, int maxLen)
        {
            if (pos + maxLen > buf.Length) maxLen = buf.Length - pos;
            int len = 0;
            while (len < maxLen && buf[pos + len] != 0) len++;
            return Encoding.ASCII.GetString(buf, pos, len).Trim();
        }

        private static List<string> ExtractOrderCodes(byte[] data)
        {
            var result = new List<string>();
            if (data == null || data.Length == 0) return result;

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string ascii = Encoding.ASCII.GetString(data);
            foreach (Match match in Regex.Matches(ascii, @"6ES7[0-9A-Z \-]{8,24}"))
            {
                string value = Regex.Replace(match.Value ?? string.Empty, @"\s{2,}", " ").Trim();
                if (string.IsNullOrWhiteSpace(value)) continue;
                if (seen.Add(value))
                    result.Add(value);
            }

            return result;
        }

        private static string ExtractFirmwareVersion(byte[] data)
        {
            if (data == null || data.Length < 10) return null;

            for (int i = 0; i <= data.Length - 10; i++)
            {
                if (data[i] == 0x56 &&
                    data[i + 5] == 0x80 &&
                    data[i + 6] == 0x36 &&
                    data[i + 7] == 0x45 &&
                    data[i + 8] == 0x53 &&
                    data[i + 9] == 0x37)
                {
                    return $"V{data[i + 1]}.{data[i + 2]}.{data[i + 3]}";
                }
            }

            return null;
        }

        private static string ExtractBootLoaderVersion(byte[] data)
        {
            if (data == null || data.Length < 4) return null;

            int bootLoaderPos = FindAscii(data, "Boot Loader");
            if (bootLoaderPos < 0) return null;

            for (int i = bootLoaderPos; i <= data.Length - 4; i++)
            {
                if (data[i] == 0x56)
                    return $"V{data[i + 1]}.{data[i + 2]}.{data[i + 3]}";
            }

            return null;
        }

        private static int FindAscii(byte[] data, string text)
        {
            if (data == null || string.IsNullOrWhiteSpace(text)) return -1;

            byte[] needle = Encoding.ASCII.GetBytes(text);
            if (needle.Length == 0 || data.Length < needle.Length) return -1;

            for (int i = 0; i <= data.Length - needle.Length; i++)
            {
                bool match = true;
                for (int j = 0; j < needle.Length; j++)
                {
                    if (data[i + j] != needle[j])
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                    return i;
            }

            return -1;
        }

        private static DateTime DecodeBcdDate(byte[] buf, int pos)
        {
            // BCD date: YY-MM-DD-HH-MM-SS (6 bytes)
            int yy = BcdToByte(buf[pos]);
            int mm = BcdToByte(buf[pos + 1]);
            int dd = BcdToByte(buf[pos + 2]);
            int hh = BcdToByte(buf[pos + 3]);
            int mi = BcdToByte(buf[pos + 4]);
            int ss = BcdToByte(buf[pos + 5]);
            int year = yy < 90 ? 2000 + yy : 1900 + yy;
            if (mm < 1 || mm > 12) mm = 1;
            if (dd < 1 || dd > 31) dd = 1;
            return new DateTime(year, mm, dd, hh, mi, ss);
        }

        private static int BcdToByte(byte b)
        {
            return ((b >> 4) * 10) + (b & 0x0F);
        }

        private static byte BlockTypeToFile(byte blockType)
        {
            switch (blockType)
            {
                case BlockOB:  return FileOB;
                case BlockDB:  return FileDB;
                case BlockSDB: return FileSDB;
                case BlockFC:  return FileFC;
                case BlockSFC: return FileSFC;
                case BlockFB:  return FileFB;
                case BlockSFB: return FileSFB;
                default: return FileDB;
            }
        }

        public static string BlockTypeName(byte type)
        {
            switch (type)
            {
                case BlockOB:  return "OB";
                case BlockDB:  return "DB";
                case BlockSDB: return "SDB";
                case BlockFC:  return "FC";
                case BlockSFC: return "SFC";
                case BlockFB:  return "FB";
                case BlockSFB: return "SFB";
                default: return $"0x{type:X2}";
            }
        }

        private static string LangName(byte lang)
        {
            switch (lang)
            {
                case 0x00: return "Not defined";
                case 0x01: return "AWL/STL";
                case 0x02: return "KOP/LAD";
                case 0x03: return "FUP/FBD";
                case 0x04: return "SCL";
                case 0x05: return "DB";
                case 0x06: return "GRAPH";
                case 0x07: return "SDB";
                case 0x08: return "CPU-DB";
                case 0x11: return "SDB (V2)";
                case 0x12: return "SDB (V3)";
                case 0x29: return "Encrypted";
                default: return $"Lang(0x{lang:X2})";
            }
        }

        public static string FormatHexDump(byte[] data, int bytesPerLine = 16)
        {
            if (data == null || data.Length == 0) return "(empty)";

            var sb = new StringBuilder();
            for (int i = 0; i < data.Length; i += bytesPerLine)
            {
                sb.Append($"{i:X6}: ");

                // Hex part
                int lineLen = Math.Min(bytesPerLine, data.Length - i);
                for (int j = 0; j < bytesPerLine; j++)
                {
                    if (j < lineLen)
                        sb.Append($"{data[i + j]:X2} ");
                    else
                        sb.Append("   ");

                    if (j == 7) sb.Append(" "); // Mid separator
                }

                sb.Append(" | ");

                // ASCII part
                for (int j = 0; j < lineLen; j++)
                {
                    byte b = data[i + j];
                    sb.Append(b >= 0x20 && b < 0x7F ? (char)b : '.');
                }

                sb.AppendLine();
            }
            return sb.ToString();
        }

        // =====================================================================
        // IDisposable
        // =====================================================================

        public void Dispose()
        {
            Disconnect();
        }
    }
}
