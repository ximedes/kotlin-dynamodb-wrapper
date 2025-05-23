/*
 * Copyright 2021 the original author or authors.
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

package com.ximedes.dynamodb.dsl

import com.ximedes.dynamodb.dsl.builders.ItemBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

internal class ExtensionFunctionTest {

    @Test
    fun testTakeOrNull() {
        val item = ItemBuilder().apply {
            "null" from null as String?
        }.build()

        assertNull(item.takeOrNull<String>("null"))

    }

    @Test
    fun testTake() {
        val item = ItemBuilder().apply {
            "key" from "value"
        }.build()

        assertEquals("value", item.take<String>("key"))
    }

    @Test
    fun testTakeAllStrings() {
        val item = ItemBuilder().apply {
            "listKey" from listOf("a", "b")
        }.build()

        assertEquals(listOf("a", "b"), item.takeAll<String>("listKey"))
    }
}