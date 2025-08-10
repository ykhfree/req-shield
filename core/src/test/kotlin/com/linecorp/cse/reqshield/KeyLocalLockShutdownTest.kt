/*
 *  Copyright 2024 LY Corporation
 *
 *  LY Corporation licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.linecorp.cse.reqshield

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

class KeyLocalLockShutdownTest {
    @Test
    fun testShutdownPreventsMemoryLeak() {
        val keyLock = KeyLocalLock(5000L) // 5 second timeout

        // Create multiple locks to verify monitoring works
        val key1 = "testKey1"
        val key2 = "testKey2"

        keyLock.tryLock(key1, LockType.CREATE)
        keyLock.tryLock(key2, LockType.CREATE)

        // Call shutdown
        keyLock.shutdown()

        // Executor should be shutdown after shutdown() call
        assertTrue(
            keyLock.javaClass.getDeclaredField("scheduledExecutor").let { field ->
                field.isAccessible = true
                val executor = field.get(keyLock) as java.util.concurrent.ScheduledExecutorService
                executor.isShutdown
            },
            "ScheduledExecutor should be shutdown",
        )
    }

    @Test
    fun testScheduledCleanupRemovesExpiredLocks() {
        val shortTimeout = 100L // Fast test with 100ms timeout
        val keyLock = KeyLocalLock(shortTimeout)

        val key = "expireTestKey"
        val lockType = LockType.CREATE

        // Acquire lock
        assertTrue(keyLock.tryLock(key, lockType), "Should acquire lock initially")

        // Should fail to acquire same lock again (already acquired)
        assertTrue(!keyLock.tryLock(key, lockType), "Should not acquire same lock again")

        // Should be automatically cleaned up after timeout
        await().atMost(Duration.ofMillis(shortTimeout + 200L)).untilAsserted {
            // Should be able to acquire new lock after timeout
            assertTrue(keyLock.tryLock(key, lockType), "Should be able to acquire lock after timeout")
            keyLock.unLock(key, lockType) // Cleanup
        }

        keyLock.shutdown()
    }

    @Test
    fun testScheduledExecutorIntervalWorksCorrectly() {
        val lockTimeoutMillis = 500L
        val keyLock = KeyLocalLock(lockTimeoutMillis)

        // Create multiple expired locks
        for (i in 1..5) {
            keyLock.tryLock("key$i", LockType.CREATE)
        }

        // Verify cleanup after certain time
        await().atMost(Duration.ofMillis(lockTimeoutMillis + 500L)).untilAsserted {
            // Should be able to acquire locks again after all keys are cleaned up
            var availableCount = 0
            for (i in 1..5) {
                if (keyLock.tryLock("key$i", LockType.CREATE)) {
                    availableCount++
                    keyLock.unLock("key$i", LockType.CREATE)
                }
            }
            assertEquals(5, availableCount, "All expired locks should be cleaned up")
        }

        keyLock.shutdown()
    }

    @Test
    fun testShutdownWithTimeout() {
        val keyLock = KeyLocalLock(1000L)

        val startTime = System.currentTimeMillis()
        keyLock.shutdown()
        val endTime = System.currentTimeMillis()

        // Shutdown should complete within 5 seconds
        assertTrue(
            endTime - startTime < 6000,
            "Shutdown should complete within timeout period",
        )
    }

    @Test
    fun testInterruptedShutdown() {
        val keyLock = KeyLocalLock(1000L)

        // Call shutdown with current thread in interrupt state
        Thread.currentThread().interrupt()

        keyLock.shutdown()

        // Shutdown should complete normally (regardless of interrupt state)
        assertTrue(
            keyLock.javaClass.getDeclaredField("scheduledExecutor").let { field ->
                field.isAccessible = true
                val executor = field.get(keyLock) as java.util.concurrent.ScheduledExecutorService
                executor.isShutdown
            },
            "ScheduledExecutor should be shutdown even when interrupted",
        )

        // Clear interrupt state
        Thread.interrupted()
    }
}
