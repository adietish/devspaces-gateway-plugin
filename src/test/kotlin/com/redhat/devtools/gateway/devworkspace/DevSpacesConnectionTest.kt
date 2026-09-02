/*
 * Copyright (c) 2026 Red Hat, Inc.
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Red Hat, Inc. - initial API and implementation
 */
package com.redhat.devtools.gateway.devworkspace

import com.jetbrains.gateway.thinClientLink.ThinClientHandle
import com.redhat.devtools.gateway.ConnectWaitTimeoutException
import com.redhat.devtools.gateway.DevSpacesConnection
import com.redhat.devtools.gateway.DevSpacesContext
import com.jetbrains.gateway.thinClientLink.LinkedClientManager
import com.jetbrains.rd.util.lifetime.Lifetime
import io.mockk.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean

class DevSpacesConnectionTest {

    private lateinit var devSpacesContext: DevSpacesContext
    private lateinit var thinClient: ThinClientHandle

    private val namespace = "test-namespace"
    private val workspaceName = "test-workspace"

    private lateinit var connection: DevSpacesConnection

    @BeforeEach
    fun beforeEach() {
        devSpacesContext = mockk(relaxed = true) {
            every { devWorkspace.namespace } returns namespace
            every { devWorkspace.name } returns workspaceName
        }
        thinClient = mockk(relaxed = true) {
            every { clientPresent } returns false
            every { lifetime } returns mockk(relaxed = true)
        }

        connection = DevSpacesConnection(devSpacesContext)

        // Mock LinkedClientManager.getInstance() to avoid NPE in tests
        mockkObject(LinkedClientManager)
        every { LinkedClientManager.getInstance() } returns mockk(relaxed = true)

        // Mock DevWorkspaces.get() for tearDownConnection's DevWorkspacePatch
        mockkConstructor(DevWorkspaces::class)
        every { anyConstructed<DevWorkspaces>().get(any<String>(), any<String>()) } returns mockk(relaxed = true) {
            every { annotations } returns emptyMap()
        }
    }

    @Test
    fun `waitForThinClientConnect succeeds when client is present`() = runTest {
        // given
        val connectFailed = AtomicBoolean(false)
        every { thinClient.clientPresent } returns true

        // when
        connection.waitForThinClientConnect(thinClient, connectFailed, null)

        // then — no exception means success
    }

    @Test
    fun `waitForThinClientConnect times out when client is never present`() = runTest {
        // given
        val connectFailed = AtomicBoolean(false)
        every { thinClient.clientPresent } returns false

        // when/then — short timeout so the test completes quickly
        var thrown: Throwable? = null
        try {
            connection.waitForThinClientConnect(thinClient, connectFailed, null, timeoutMs = 500L)
        } catch (e: Throwable) {
            thrown = e
        }
        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        assertThat(thrown?.message).contains("Could not connect")
    }

    @Test
    fun `waitForThinClientConnect tolerates transient presence absence`() = runTest {
        // given — simulate transient absence: absent for first N polls, then present
        val connectFailed = AtomicBoolean(false)
        var callCount = 0
        every { thinClient.clientPresent } answers {
            callCount++
            callCount > 3
        }

        // when — generous timeout so the loop can recover
        connection.waitForThinClientConnect(thinClient, connectFailed, null, timeoutMs = 10_000L)

        // then — loop polled at least 4 times (3 absent + 1 present)
        assertThat(callCount).isGreaterThan(3)
    }

    @Test
    fun `waitForThinClientConnect fails when connectFailed is set`() = runTest {
        // given — connectFailed already set before calling waitForThinClientConnect
        val connectFailed = AtomicBoolean(true)
        every { thinClient.clientPresent } returns false

        // when/then
        var thrown: Throwable? = null
        try {
            connection.waitForThinClientConnect(thinClient, connectFailed, null, timeoutMs = 1_000L)
        } catch (e: Throwable) {
            thrown = e
        }
        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        assertThat(thrown?.message).contains("Could not connect")
    }

    // -- onThinClientClosed tests (covers: onThinClientClosed) --

    @Test
    fun `onThinClientClosed sets connectFailed and tears down when live`() {
        // given
        val connectFailed = AtomicBoolean(false)
        val connectionLive = AtomicBoolean(true)

        // when
        connection.onThinClientClosed(
            connectFailed,
            connectionLive,
            thinClient,
            devSpacesContext.devWorkspace,
            mockk<() -> Unit>(relaxed = true),
            {},
            null,
            null
        )

        // then
        assertThat(connectFailed.get()).isTrue()
        verify { devSpacesContext.removeWorkspace(devSpacesContext.devWorkspace) }
    }

    @Test
    fun `onThinClientClosed does not tear down when not live`() {
        // given
        val connectFailed = AtomicBoolean(false)
        val connectionLive = AtomicBoolean(false)

        // when
        connection.onThinClientClosed(
            connectFailed,
            connectionLive,
            thinClient,
            devSpacesContext.devWorkspace,
            mockk<() -> Unit>(relaxed = true),
            {},
            null,
            null
        )

        // then
        assertThat(connectFailed.get()).isTrue()
        verify(exactly = 0) { devSpacesContext.removeWorkspace(any()) }
    }

    @Test
    fun `connect retries on connection failure and succeeds within timeout`() = runTest {
        val spied = spyk(connection)
        coEvery { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns thinClient

        val cfSlot = slot<AtomicBoolean>()
        coEvery {
            spied.waitForThinClientConnect(any(), capture(cfSlot), any(), any(), any())
        } coAnswers {
            cfSlot.captured.set(true)
            throw IllegalStateException("Connection failed")
        } andThen {
            // second call succeeds (no-op)
        }

        spied.retryThinClientConnection(
            "https://link", devSpacesContext.devWorkspace, {}, {}, {}, null, null, null
        )

        coVerify(exactly = 2) { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
        coVerify(exactly = 2) { spied.waitForThinClientConnect(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `retry on ConnectWaitTimeoutException when clientPresent never true and connectFailed false`() = runTest {
        val spied = spyk(connection)
        every { thinClient.clientPresent } returns false
        coEvery { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns thinClient

        coEvery {
            spied.waitForThinClientConnect(any(), any(), any(), any(), any())
        } coAnswers {
            throw ConnectWaitTimeoutException("timeout")
        } andThen {
            // second call succeeds
        }

        spied.retryThinClientConnection(
            "https://link", devSpacesContext.devWorkspace, {}, {}, {}, null, null, null
        )

        coVerify(exactly = 2) { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
    }

    @Test
    fun `no outer retry when clientPresent is true`() = runTest {
        val spied = spyk(connection)
        every { thinClient.clientPresent } returns true
        coEvery { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns thinClient
        coEvery {
            spied.waitForThinClientConnect(any(), any(), any(), any(), any())
        } throws IllegalStateException("Connection failed")

        var thrown: Throwable? = null
        try {
            spied.retryThinClientConnection(
                "https://link", devSpacesContext.devWorkspace, {}, {}, {}, null, null, null
            )
        } catch (e: Throwable) {
            thrown = e
        }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        coVerify(exactly = 1) { spied.startThinClient(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
    }

    @Test
    fun `waitForThinClientConnect exits immediately when launchFailed completes`() = runTest {
        // given — client never present, but launchFailed triggers event-driven exit
        val connectFailed = AtomicBoolean(false)
        val launchFailed = CompletableDeferred<Unit>()
        var callCount = 0
        every { thinClient.clientPresent } answers {
            callCount++
            false
        }

        val startNanos = System.nanoTime()

        // when — launchFailed completes after a short delay, should break out of poll loop
        launch {
            delay(100)
            launchFailed.complete(Unit)
        }

        var thrown: Throwable? = null
        try {
            connection.waitForThinClientConnect(
                thinClient,
                connectFailed,
                null,
                timeoutMs = 60_000L,
                launchFailed = launchFailed
            )
        } catch (e: Throwable) {
            thrown = e
        }

        val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

        // then — should exit quickly (within ~500ms) instead of waiting full timeout
        assertThat(elapsedMs).isLessThan(500)
        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        assertThat(thrown?.message).contains("Could not connect")
    }
}
