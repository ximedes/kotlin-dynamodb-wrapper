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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

internal class AttributeDefinitionsBuilderTest {

    @Test
    fun mix() {
        val sdkAttributes = listOf(
                AttributeDefinition.builder().attributeName("s1").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("s2").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("b").attributeType(ScalarAttributeType.B).build(),
                AttributeDefinition.builder().attributeName("n").attributeType(ScalarAttributeType.N).build()
        )
        val dslAttributes = AttributeDefinitionsBuilder().apply {
            string("s1", "s2")
            binary("b")
            number("n")
        }.build()

        assertEquals(sdkAttributes, dslAttributes)
    }

    @Test
    fun empty() {
        val dslAttributes = AttributeDefinitionsBuilder().build()
        assertEquals(emptyList<AttributeDefinition>(), dslAttributes)
    }
}