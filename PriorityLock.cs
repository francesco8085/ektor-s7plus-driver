// =============================================================================
// EktorS7PlusDriver — S7CommPlus Communication Driver for Siemens S7-1200/1500
// =============================================================================
// Copyright (c) 2025-2026 Francesco Cesarone <f.cesarone@entersrl.it>
// Azienda   : Enter SRL
// Progetto  : EKTOR Industrial IoT Platform
// Licenza   : Proprietaria — uso riservato Enter SRL
// =============================================================================

using System;
using System.Threading;
using System.Threading.Tasks;

namespace EnterSrl.Ektor.S7Plus
{
    /// <summary>
    /// Priority-aware mutual-exclusion lock for S7CommPlus PLC connections.
    ///
    /// S7CommPlusConnection is NOT thread-safe: only one operation may hold the
    /// lock at any time. This class prevents heavy exploration operations (browse,
    /// get-blocks, block-body, discover-attributes) from starving lightweight
    /// read/write operations that need a fast turnaround.
    ///
    /// Two priority levels:
    ///   HIGH  - reads, writes, status checks       (recommended timeout: ~5 s)
    ///   LOW   - browse, get-blocks, block-body     (recommended timeout: ~30 s)
    ///
    /// Fairness guarantee:
    ///   When the lock is released and both HIGH and LOW waiters are queued,
    ///   the next HIGH waiter always wins. LOW waiters also yield to any HIGH
    ///   waiter that arrives while they are still waiting.
    ///
    /// Compatible with .NET Framework 4.8 (no nullable reference types,
    /// no C# 10+ features).
    /// </summary>
    public sealed class PriorityLock
    {
        // The single mutex — only one holder at a time regardless of priority.
        private readonly SemaphoreSlim _mutex = new SemaphoreSlim(1, 1);

        // Counts of threads actively waiting for each priority tier.
        private int _highWaiters;
        private int _lowWaiters;

        // Guards _highWaiters / _lowWaiters counter updates.
        private readonly object _counterLock = new object();

        // ------------------------------------------------------------------ //
        //  Public API                                                          //
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Acquires the lock at HIGH priority (reads, writes, status checks).
        /// Returns <c>true</c> when the lock has been acquired, <c>false</c> if
        /// the <paramref name="timeout"/> elapses before acquisition.
        /// </summary>
        public async Task<bool> WaitHighAsync(TimeSpan timeout)
        {
            // Register as a high-priority waiter so that any LOW waiter that
            // is looping will see us and yield its turn.
            lock (_counterLock)
            {
                _highWaiters++;
            }

            bool acquired = false;
            try
            {
                acquired = await _mutex.WaitAsync(timeout).ConfigureAwait(false);
                return acquired;
            }
            finally
            {
                lock (_counterLock)
                {
                    _highWaiters--;
                }

                // If we timed out we never own the mutex, nothing to release.
            }
        }

        /// <summary>
        /// Acquires the lock at LOW priority (browse, get-blocks, block-body,
        /// discover-attributes).
        /// Returns <c>true</c> when the lock has been acquired, <c>false</c> if
        /// the <paramref name="timeout"/> elapses before acquisition.
        ///
        /// While waiting, this method polls in short slices and yields to any
        /// HIGH priority waiter that arrives, resetting its own wait-slice so
        /// that the HIGH waiter can proceed first.
        /// </summary>
        public async Task<bool> WaitLowAsync(TimeSpan timeout)
        {
            lock (_counterLock)
            {
                _lowWaiters++;
            }

            bool acquired = false;
            try
            {
                DateTime deadline = DateTime.UtcNow.Add(timeout);

                // Polling slice: short enough to react quickly to a HIGH waiter,
                // long enough to avoid busy-spinning.
                TimeSpan slice = TimeSpan.FromMilliseconds(50);

                while (true)
                {
                    TimeSpan remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        // Timed out.
                        return false;
                    }

                    // If a HIGH priority waiter is queued, back off for one
                    // slice to let it win the next WaitAsync on the mutex.
                    bool highPending;
                    lock (_counterLock)
                    {
                        highPending = _highWaiters > 0;
                    }

                    if (highPending)
                    {
                        // Yield one slice — the HIGH waiter will attempt
                        // WaitAsync(timeout) without any deliberate delay and
                        // will therefore beat us to the semaphore.
                        TimeSpan yieldDelay = slice < remaining ? slice : remaining;
                        await Task.Delay(yieldDelay).ConfigureAwait(false);
                        continue;
                    }

                    // No HIGH waiters visible — try to acquire for one slice.
                    TimeSpan tryFor = slice < remaining ? slice : remaining;
                    acquired = await _mutex.WaitAsync(tryFor).ConfigureAwait(false);

                    if (acquired)
                    {
                        return true;
                    }

                    // Did not acquire within the slice — loop and re-check for
                    // HIGH waiters before trying again.
                }
            }
            finally
            {
                lock (_counterLock)
                {
                    _lowWaiters--;
                }

                // If we timed out we never own the mutex, nothing to release.
            }
        }

        /// <summary>
        /// Releases the lock. Must be called exactly once after a successful
        /// <see cref="WaitHighAsync"/> or <see cref="WaitLowAsync"/>.
        /// </summary>
        public void Release()
        {
            _mutex.Release();
        }
    }
}
