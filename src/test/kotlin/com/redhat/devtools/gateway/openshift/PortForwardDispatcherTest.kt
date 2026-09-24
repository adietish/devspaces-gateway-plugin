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
package com.redhat.devtools.gateway.openshift

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PortForwardDispatcherTest {

    @Test
    fun `dispatcher is not Dispatchers IO`() {
        assertThat(PortForwardDispatcher.dispatcher).isNotSameAs(Dispatchers.IO)
    }

    @Test
    fun `dispatcher runs work on named threads`() {
        runBlocking {
            val threadName = withContext(PortForwardDispatcher.dispatcher) {
                Thread.currentThread().name
            }

            assertThat(threadName).contains("devspaces-port-forward")
        }
    }
}
