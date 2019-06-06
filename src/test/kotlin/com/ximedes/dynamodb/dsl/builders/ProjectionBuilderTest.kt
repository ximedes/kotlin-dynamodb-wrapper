/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ximedes.dynamodb.dsl.builders

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.Projection
import software.amazon.awssdk.services.dynamodb.model.ProjectionType

internal class ProjectionBuilderTest {

    private fun dslProjection(
            type: ProjectionType = ProjectionType.ALL,
            init: ProjectionBuilder.() -> Unit
    ): Projection {
        return ProjectionBuilder(type).apply(init).build()
    }

    @Test
    fun type() {
        val sdkProjection = Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()
        val dslProjection = dslProjection(ProjectionType.KEYS_ONLY) {}

        assertEquals(sdkProjection.projectionType(), dslProjection.projectionType())
    }

    @Test
    fun attributes() {
        val sdkProjection = Projection.builder().nonKeyAttributes("a", "b", "c").build()
        val dslProjection = dslProjection {
            nonKeyAttributes("a", "b", "c")
        }
        assertEquals(sdkProjection.nonKeyAttributes(), dslProjection.nonKeyAttributes())
    }


}