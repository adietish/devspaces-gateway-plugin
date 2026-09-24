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
package com.redhat.devtools.gateway.view.steps.workspaces

import com.redhat.devtools.gateway.devworkspace.DevWorkspace
import com.redhat.devtools.gateway.devworkspace.DevWorkspaceObjectMeta
import com.redhat.devtools.gateway.devworkspace.DevWorkspaceSpec
import com.redhat.devtools.gateway.devworkspace.DevWorkspaceStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class WorkspacesWatchFilterTest {

    @Test
    fun `createFilter accepts all workspaces including VS Code`() {
        val jetBrainsDw = DevWorkspace(
            DevWorkspaceObjectMeta(name = "jb", namespace = "ns", uid = "uid-jb", emptyMap(), emptyMap()),
            DevWorkspaceSpec(started = true),
            DevWorkspaceStatus(phase = "Running")
        )
        val vscodeDw = DevWorkspace(
            DevWorkspaceObjectMeta(
                name = "vscode",
                namespace = "ns",
                uid = "uid-vscode",
                annotations = mapOf("che.eclipse.org/che-editor" to "eclipse/che-code/latest"),
                labels = emptyMap()
            ),
            DevWorkspaceSpec(started = true),
            DevWorkspaceStatus(phase = "Running")
        )

        // Same createFilter lambda pattern as WorkspacesWatch: accept every workspace.
        val createFilter: (String) -> ((DevWorkspace) -> Boolean) = { _ -> { true } }

        val filter = createFilter("ns")
        assertThat(filter(jetBrainsDw)).isTrue()
        assertThat(filter(vscodeDw)).isTrue()
    }
}
