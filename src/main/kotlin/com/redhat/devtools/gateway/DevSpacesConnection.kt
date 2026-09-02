/*
 * Copyright (c) 2024-2026 Red Hat, Inc.
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Red Hat, Inc. - initial API and implementation
 */
package com.redhat.devtools.gateway

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.application.ModalityState
import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.project.ex.ProjectManagerEx
import com.jetbrains.gateway.thinClientLink.LinkedClientManager
import com.jetbrains.gateway.thinClientLink.ThinClientHandle
import com.jetbrains.rd.util.lifetime.Lifetime
import com.redhat.devtools.gateway.devworkspace.DevWorkspace
import com.redhat.devtools.gateway.devworkspace.DevWorkspacePatch
import com.redhat.devtools.gateway.devworkspace.DevWorkspaceRestart
import com.redhat.devtools.gateway.devworkspace.DevWorkspaces
import com.redhat.devtools.gateway.devworkspace.RestartDevWorkspaceAnnotationWatch
import com.redhat.devtools.gateway.openshift.DevWorkspacePods
import com.redhat.devtools.gateway.server.RemoteIDEServer
import com.redhat.devtools.gateway.server.RemoteIDEServerStatus
import com.redhat.devtools.gateway.util.ProgressCountdown
import com.redhat.devtools.gateway.util.isCancellationException
import com.redhat.devtools.gateway.util.isServerContainerNotFound
import com.redhat.devtools.gateway.view.ui.Dialogs
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.models.V1Pod
import kotlinx.coroutines.*
import java.io.Closeable
import java.io.IOException
import java.net.ServerSocket
import java.net.URI
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Thin-client connection lifecycle.
 *
 * Thin-client close always ends the connect wait; [tearDownConnection] runs only if the
 * connection is already live ([connectionLive]). Failures during connect are cleaned up by
 * [connect]'s catch path.
 *
 * Connect enablement in the wizard is based on workspace Running state (not
 * [DevSpacesContext.activeWorkspaces]), because IDEA often keeps the connector view
 * after Guest close and thin-client signals are unreliable for UI gating.
 *
 * Still clear [DevSpacesContext.activeWorkspaces] when the connection ends (before optional
 * remote stop) for tooltips / bookkeeping.
 */
class ConnectWaitTimeoutException(message: String) : IllegalStateException(message)

class DevSpacesConnection(private val devSpacesContext: DevSpacesContext) {

    companion object {
        const val CONNECT_TIMEOUT: Long = 4 * 60 * 1000 // millis
        private const val CONNECT_POLL: Long = 200 // millis
        private const val RETRY_BACKOFF_MS = 2_000L
    }

    /** Ensures [tearDownConnection] runs at most once for this connect attempt. */
    private val tearDownStarted = AtomicBoolean(false)

    @Throws(Exception::class)
    @Suppress("UnstableApiUsage")
    suspend fun connect(
        onConnected: () -> Unit,
        onConnectionEnded: () -> Unit,
        onDevWorkspaceStopped: () -> Unit,
        onProgress: ((value: ProgressCountdown.ProgressEvent) -> Unit)? = null,
        checkCancelled: (() -> Unit)? = null,
        modalityState: ModalityState? = null,
        registerRestartWatcher: Boolean? = true
    ): ThinClientHandle {
        val workspace = devSpacesContext.devWorkspace
        tearDownStarted.set(false)
        devSpacesContext.addWorkspace(workspace)

        var remoteIdeServer: RemoteIDEServer? = null
        var forwarder: Closeable? = null
        var client: ThinClientHandle? = null
        val connectionLive = AtomicBoolean(false)

        return try {
            remoteIdeServer = waitUntilServerReady(checkCancelled, onProgress, modalityState)

            checkCancelled?.invoke()
            val joinLink = remoteIdeServer.getStatus(checkCancelled).joinLink
                ?: throw IOException("Could not connect, workspace IDE is not ready. No join link present.")

            checkCancelled?.invoke()
            onProgress?.invoke(ProgressCountdown.ProgressEvent(
                message = "Waiting for the workspace IDE client to start (first-time download may take several minutes)..."))

            val (fwd, localPort) = setupPortForwarding(remoteIdeServer.pod)
            forwarder = fwd

            val effectiveJoinLink = joinLink.replace(":5990", ":$localPort")

            checkCancelled?.invoke()

            client = retryThinClientConnection(
                effectiveJoinLink, workspace, onConnected, onConnectionEnded, onDevWorkspaceStopped,
                remoteIdeServer, forwarder, checkCancelled
            )!!

            if (registerRestartWatcher == true) {
                watchRestartAnnotation(
                    workspace.namespace,
                    workspace.name,
                    devSpacesContext.client,
                    client
                )
            }

            connectionLive.set(true)
            onConnected()
            client
        } catch (e: Exception) {
            if (e !is ConnectWaitTimeoutException) {
                runCatching { client?.close() }
            }
            tearDownConnection(
                client, workspace, onConnectionEnded, onDevWorkspaceStopped, remoteIdeServer, forwarder
            )
            throw e
        }
    }

    /**
     * Retry loop for thin-client startup within the CONNECT_TIMEOUT budget.
     * Returns the connected client, or throws on timeout.
     *
     * On failure: preserves partial download when clientPresent is false,
     * retries while timeout budget remains with backoff.
     * On success: breaks and returns the client.
     */
    @Suppress("UnstableApiUsage")
    internal suspend fun retryThinClientConnection(
        effectiveJoinLink: String,
        workspace: DevWorkspace,
        onConnected: () -> Unit,
        onConnectionEnded: () -> Unit,
        onDevWorkspaceStopped: () -> Unit,
        remoteIdeServer: RemoteIDEServer?,
        forwarder: Closeable?,
        checkCancelled: (() -> Unit)?,
    ): ThinClientHandle? {
        var connectFailed = AtomicBoolean(false)
        var currentClient: ThinClientHandle? = null
        val connectionLive = AtomicBoolean(false)
        var attempt = 0

        val connectStart = System.currentTimeMillis()
        while (true) {
            attempt++
            checkCancelled?.invoke()
            if (connectFailed.get()) {
                if (currentClient?.clientPresent == true) {
                    runCatching { currentClient?.close() }
                }
                connectFailed.set(false)
                currentClient = null
            }

            val elapsed = System.currentTimeMillis() - connectStart
            val remaining = CONNECT_TIMEOUT - elapsed
            if (remaining <= 0) {
                throw ConnectWaitTimeoutException("Could not connect, workspace IDE is not ready.")
            }

            val launchFailed = CompletableDeferred<Unit>()
            currentClient = startThinClient(
                URI(effectiveJoinLink), workspace, onConnected, onConnectionEnded, onDevWorkspaceStopped,
                remoteIdeServer, forwarder, connectFailed, connectionLive, launchFailed
            ) ?: throw IOException("Could not start thin client.")

            try {
                waitForThinClientConnect(currentClient!!, connectFailed, checkCancelled, timeoutMs = remaining, launchFailed = launchFailed)
                break
            } catch (e: Exception) {
                val currentClientRef = currentClient
                if (currentClientRef?.clientPresent != true) {
                    val elapsed2 = System.currentTimeMillis() - connectStart
                    val remaining2 = CONNECT_TIMEOUT - elapsed2
                    if (remaining2 > RETRY_BACKOFF_MS) {
                        currentClient = null
                        thisLogger().warn("Thin client launch failed (attempt $attempt), retrying in ${RETRY_BACKOFF_MS}ms: ${e.message}")
                        delay(RETRY_BACKOFF_MS)
                        continue
                    }
                }
                throw e
            }
        }
        return currentClient
    }

    /**
     * Thin client closed or failed to open.
     * Always ends the connect-wait loop. Calls [tearDownConnection] only if the connection is
     * already live; failures during connect are cleaned up by [connect]'s catch.
     */
    @Suppress("UnstableApiUsage")
    internal fun onThinClientClosed(
        connectFailed: AtomicBoolean,
        connectionLive: AtomicBoolean,
        thinClient: ThinClientHandle,
        workspace: DevWorkspace,
        onConnectionEnded: () -> Unit,
        onDevWorkspaceStopped: () -> Unit,
        remoteIdeServer: RemoteIDEServer?,
        forwarder: Closeable?,
    ) {
        connectFailed.set(true)
        if (connectionLive.get()) {
            tearDownConnection(
                thinClient,
                workspace,
                onConnectionEnded,
                onDevWorkspaceStopped,
                remoteIdeServer,
                forwarder
            )
        }
    }

    @Suppress("UnstableApiUsage")
    private fun watchRestartAnnotation(
        namespace: String,
        workspaceName: String,
        kubeClient: ApiClient,
        thinClient: ThinClientHandle
    ) {
        val restartWatchScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        RestartDevWorkspaceAnnotationWatch(
            onRestartAnnotated(thinClient),
            kubeClient,
            namespace,
            workspaceName
        ).start(restartWatchScope)

        thinClient.lifetime.onTermination {
            restartWatchScope.cancel()
        }
    }

    @Suppress("UnstableApiUsage")
    private fun onRestartAnnotated(
        thinClient: ThinClientHandle
    ): () -> Job {
        return {
            CoroutineScope(Dispatchers.IO).launch {
                DevWorkspaceRestart(devSpacesContext).execute(thinClient)
            }
        }
    }

    /**
     * Ends the local connection: clear "already connected" tracking, notify the UI, then
     * optionally stop the DevWorkspace (restart annotation / Close and Stop).
     *
     * Uses the [workspace] captured at [connect] start so teardown stays correct if the wizard
     * selection changes while the Guest is open.
     *
     * Idempotent across thin-client close, connect failure, and duplicate signals.
     */
    @Suppress("UnstableApiUsage")
    private fun tearDownConnection(
        client: ThinClientHandle? = null,
        workspace: DevWorkspace,
        onConnectionEnded: () -> Unit,
        onDevWorkspaceStopped: () -> Unit,
        remoteIdeServer: RemoteIDEServer?,
        forwarder: Closeable?
    ) {
        if (!tearDownStarted.compareAndSet(false, true)) {
            return
        }
        // Clear tracking + refresh UI before any remote wait so Connect is not stuck
        // behind waitServerTerminated (up to 10s) when the wizard stays open in IDEA.
        devSpacesContext.removeWorkspace(workspace)
        runCatching { onConnectionEnded() }

        CoroutineScope(Dispatchers.IO).launch {
            runCatching { client?.close() }
            val workspacePatch = DevWorkspacePatch(
                workspace.namespace,
                workspace.name,
                devSpacesContext.client
            ) {
                DevWorkspaces(devSpacesContext.client).get(workspace.namespace, workspace.name)
            }
            try {
                if (workspacePatch.hasRestartAnnotation()) {
                    closeAllProjects()
                } else if (true == remoteIdeServer?.waitServerTerminated()) {
                    DevWorkspaces(devSpacesContext.client)
                        .stop(workspace.namespace, workspace.name)
                        .also { onDevWorkspaceStopped() }
                }
            } finally {
                runCatching { forwarder?.close() }
                    .onFailure { e -> thisLogger().debug("Failed to close port forwarder", e) }
            }
        }
    }

    private fun closeAllProjects() {
        ApplicationManager.getApplication().invokeLater(
            {
                val pm = ProjectManagerEx.getInstanceEx()
                for (project in pm.openProjects.toList()) {
                    if (!project.isDisposed) {
                        pm.closeAndDispose(project)
                    }
                }
            },
            ModalityState.nonModal()
        )
    }

    private fun findFreePort(): Int {
        ServerSocket(0).use { socket ->
            socket.reuseAddress = true
            return socket.localPort
        }
    }

    /**
     * Handles the case where the remote IDE server is not ready after starting the workspace.
     * Shows a dialog and either restarts the workspace (return true) or indicates cancellation (return false).
     */
    private fun handleServerNotReady(
        checkCancelled: (() -> Unit)?,
        modalityState: ModalityState?
    ): Boolean {
        val restartWorkspace = Dialogs.ideNotResponding(modalityState)
        if (restartWorkspace) {
            DevWorkspaces(devSpacesContext.client).stopAndWait(
                devSpacesContext.devWorkspace.namespace,
                devSpacesContext.devWorkspace.name,
                checkCancelled = checkCancelled
            )
        }
        return restartWorkspace
    }

    private suspend fun waitUntilServerReady(
        checkCancelled: (() -> Unit)?,
        onProgress: ((ProgressCountdown.ProgressEvent) -> Unit)?,
        modalityState: ModalityState?
    ): RemoteIDEServer {
        var remoteIdeServerStatus: RemoteIDEServerStatus = RemoteIDEServerStatus.empty()
        var remoteIdeServer: RemoteIDEServer? = null

        while (!remoteIdeServerStatus.isReady) {
            checkCancelled?.invoke()
            onProgress?.invoke(ProgressCountdown.ProgressEvent(
                message = "Waiting for the workspace to get started...",
                countdownSeconds = DevWorkspaces.RUNNING_TIMEOUT))

            DevWorkspaces(devSpacesContext.client)
                .startAndWait(
                    devSpacesContext.devWorkspace.namespace,
                    devSpacesContext.devWorkspace.name,
                    checkCancelled = checkCancelled)

            checkCancelled?.invoke()
            onProgress?.invoke(ProgressCountdown.ProgressEvent(
                message = "Waiting for the workspace to get ready...",
                countdownSeconds = RemoteIDEServer.readyTimeout))

            remoteIdeServer = RemoteIDEServer(devSpacesContext)
            remoteIdeServerStatus = runCatching {
                remoteIdeServer.apply { waitServerReady(checkCancelled) }.getStatus(checkCancelled)
            }.getOrElse { e ->
                if (e.isCancellationException()) throw e
                // no idea-server container, don't offer "restart pod" (CRW-11897).
                if (e.isServerContainerNotFound()) throw e
                RemoteIDEServerStatus.empty()
            }

            checkCancelled?.invoke()
            if (!remoteIdeServerStatus.isReady) {
                if (handleServerNotReady(checkCancelled, modalityState)) {
                    continue
                } else {
                    throw CancellationException("User cancelled the operation")
                }
            }
        }
        return remoteIdeServer!!
    }

    private fun setupPortForwarding(pod: V1Pod): Pair<Closeable, Int> {
        val pods = DevWorkspacePods(devSpacesContext.client)
        val localPort = findFreePort()
        val forwarder = pods.forward(pod, localPort, 5990)
        pods.waitForForwardReady(localPort)
        pods.waitForForwardAcceptingConnections(localPort)
        return forwarder to localPort
    }

    @Suppress("UnstableApiUsage")
    internal fun startThinClient(
        effectiveJoinLink: URI,
        workspace: DevWorkspace,
        onConnected: () -> Unit,
        onConnectionEnded: () -> Unit,
        onDevWorkspaceStopped: () -> Unit,
        remoteIdeServer: RemoteIDEServer?,
        forwarder: Closeable?,
        connectFailed: AtomicBoolean,
        connectionLive: AtomicBoolean,
        launchFailed: CompletableDeferred<Unit>? = null,
    ): ThinClientHandle? {
        val thinClient = LinkedClientManager
            .getInstance()
            .startNewClient(
                Lifetime.Eternal,
                effectiveJoinLink,
                "",
                onConnected,
                false
            )

        fun notifyThinClientClosed() {
            launchFailed?.takeIf { !it.isCompleted }?.complete(Unit)
            onThinClientClosed(
                connectFailed,
                connectionLive,
                thinClient,
                workspace,
                onConnectionEnded,
                onDevWorkspaceStopped,
                remoteIdeServer,
                forwarder
            )
        }
        thinClient.clientClosed.advise(thinClient.lifetime) { notifyThinClientClosed() }
        thinClient.clientFailedToOpenProject.advise(thinClient.lifetime) { notifyThinClientClosed() }

        return thinClient
    }

    @Suppress("UnstableApiUsage")
    internal suspend fun waitForThinClientConnect(
        thinClient: ThinClientHandle,
        connectFailed: AtomicBoolean,
        checkCancelled: (() -> Unit)?,
        timeoutMs: Long = CONNECT_TIMEOUT,
        launchFailed: CompletableDeferred<Unit>? = null,
    ) {
        @Suppress("ConvertLongToDuration")
        val connected: Boolean? = withTimeoutOrNull(timeoutMs) {
            while (true) {
                if (thinClient.clientPresent && !connectFailed.get()) return@withTimeoutOrNull true
                if (connectFailed.get()) return@withTimeoutOrNull false
                checkCancelled?.invoke()
                if (launchFailed != null) {
                    try {
                        withTimeout(CONNECT_POLL) { launchFailed.await() }
                        return@withTimeoutOrNull false
                    } catch (_: TimeoutCancellationException) {
                        /* continue poll */
                    }
                    if (launchFailed.isCompleted) return@withTimeoutOrNull false
                } else {
                    delay(CONNECT_POLL)
                }
            }
            false
        }
        if (connected == null) {
            throw ConnectWaitTimeoutException("Could not connect, workspace IDE is not ready.")
        }
        check(connected) { "Could not connect, workspace IDE is not ready." }
    }
}
