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

import com.ximedes.dynamodb.dsl.Item
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

internal class ItemBuilderTest {

    private fun dslItem(init: ItemBuilder.() -> Unit): Item = ItemBuilder().apply(init).build()
    private val bytes = byteArrayOf(1, 2, 3)
    private val listOfByteArrays = listOf(byteArrayOf(4, 5, 6), byteArrayOf(7, 8, 9))

    @Test
    fun item() {

        val dslItem = dslItem {
            "string" from "a"
            "list_of_strings" from listOf("x", "y", "z")
            "int" from 1
            "list_of_ints" from listOf(1, 2, 3)
            "long" from 2L
            "list_of_longs" from listOf(9L, 8L)
            "list_of_numbers" from listOf<Number>(1, 2L, 3.1.toFloat(), 4.2, 8.toByte(), 12.toShort())
            "bool" from true
            "byteArray" from bytes
            "byteBuffer" from ByteBuffer.wrap(bytes)
            "inputStream" from ByteArrayInputStream(bytes)
            "list_of_byteArrays" from listOfByteArrays
            "list_of_byteBuffers" from listOfByteArrays.map { ByteBuffer.wrap(it) }
            "list_of_inputStreams" from listOfByteArrays.map { ByteArrayInputStream(it) }
            "item" from {
                "string" from "b"
            }
            "items" fromList {
                item {
                    "string" from "a"
                    "int" from 1
                }
                item {
                    "string" from "b"
                    "int" from 2
                }
            }
        }

        assertAll(
                { assertEquals(AttributeValue.builder().s("a").build(), dslItem["string"]) },
                { assertEquals(AttributeValue.builder().ss("x", "y", "z").build(), dslItem["list_of_strings"]) },
                { assertEquals(AttributeValue.builder().n("1").build(), dslItem["int"]) },
                { assertEquals(AttributeValue.builder().ns(listOf("1", "2", "3")).build(), dslItem["list_of_ints"]) },
                { assertEquals(AttributeValue.builder().n("2").build(), dslItem["long"]) },
                { assertEquals(AttributeValue.builder().ns(listOf("9", "8")).build(), dslItem["list_of_longs"]) },
                {
                    assertEquals(
                            AttributeValue.builder().ns(listOf("1", "2", "3.1", "4.2", "8", "12")).build(),
                            dslItem["list_of_numbers"]
                    )
                },
                { assertEquals(AttributeValue.builder().bool(true).build(), dslItem["bool"]) },
                { assertEquals(AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(), dslItem["byteArray"]) },
                { assertEquals(AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(), dslItem["byteBuffer"]) },
                { assertEquals(AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(), dslItem["inputStream"]) },
                {
                    assertEquals(
                            AttributeValue.builder().bs(listOfByteArrays.map { SdkBytes.fromByteArray(it) }).build(),
                            dslItem["list_of_byteArrays"]
                    )
                },
                {
                    assertEquals(
                            AttributeValue.builder().bs(listOfByteArrays.map { SdkBytes.fromByteArray(it) }).build(),
                            dslItem["list_of_byteBuffers"]
                    )
                },
                {
                    assertEquals(
                            AttributeValue.builder().bs(listOfByteArrays.map { SdkBytes.fromByteArray(it) }).build(),
                            dslItem["list_of_inputStreams"]
                    )
                },
                { assertEquals(AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(), dslItem["byteBuffer"]) },
                { assertEquals(AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(), dslItem["inputStream"]) },
                {
                    assertEquals(
                            AttributeValue.builder().m(mapOf("string" to AttributeValue.builder().s("b").build())).build(),
                            dslItem["item"]
                    )
                },
                {
                    assertEquals(
                            AttributeValue.builder().l(listOf(
                                AttributeValue.builder().m(mapOf("string" to AttributeValue.builder().s("a").build(), "int" to AttributeValue.builder().n("1").build())).build(),
                                AttributeValue.builder().m(mapOf("string" to AttributeValue.builder().s("b").build(), "int" to AttributeValue.builder().n("2").build())).build()
                            )).build(),
                            dslItem["items"]
                    )
                }
        )

    }
}