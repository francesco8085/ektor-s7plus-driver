// =============================================================================
// EktorS7PlusDriver — S7CommPlus Communication Driver for Siemens S7-1200/1500
// =============================================================================
// Copyright (c) 2025-2026 Francesco Cesarone <f.cesarone@entersrl.it>
// Azienda   : Enter SRL
// Progetto  : EKTOR Industrial IoT Platform
// Licenza   : Proprietaria — uso riservato Enter SRL
// =============================================================================

using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Configuration POCO for a single S7CommPlus PLC device.
    /// Serializable to/from JSON via Newtonsoft.Json.
    /// </summary>
    public class S7PlusConfig
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("ip")]
        public string Ip { get; set; }

        [JsonProperty("port")]
        public int Port { get; set; } = 102;

        [JsonProperty("password")]
        public string Password { get; set; } = "";

        [JsonProperty("useTls")]
        public bool UseTls { get; set; } = false;

        [JsonProperty("timeout")]
        public int Timeout { get; set; } = 30000;

        [JsonProperty("enabled")]
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Forza un profilo di connessione specifico, bypassando il rilevamento automatico.
        /// Valori supportati: "et200sp-1512-fw3x", "et200sp-1511sp", "s7-1500-1511-fw29", "generic-s7plus"
        /// </summary>
        [JsonProperty("deviceProfileKey")]
        public string DeviceProfileKey { get; set; } = "";
    }
}
