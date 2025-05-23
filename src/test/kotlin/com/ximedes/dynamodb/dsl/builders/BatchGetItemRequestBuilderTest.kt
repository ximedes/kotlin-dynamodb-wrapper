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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

internal class BatchGetItemRequestBuilderTest {
    private fun dslRequest(init: BatchGetItemRequestBuilder.() -> Unit) =
            BatchGetItemRequestBuilder().apply(init).build()

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = BatchGetItemRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(ReturnConsumedCapacity.INDEXES, sdkRequest.returnConsumedCapacity())
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
    }

    @Test
    fun items() {
        val sdkRequest = BatchGetItemRequest.builder().requestItems(
                mapOf(
                        "table1" to KeysAndAttributes.builder().keys(
                                mapOf("a" to AttributeValue.builder().s("b").build()),
                                mapOf("c" to AttributeValue.builder().s("d").build())
                        ).build(),
                        "table2" to KeysAndAttributes.builder().keys(
                                mapOf("y" to AttributeValue.builder().s("z").build())
                        ).consistentRead(true).build()
                )
        ).build()
        val dslRequest = dslRequest {
            items("table1") {
                // TODO: simplify key specification API
                keys({ "a" from "b" },
                        { "c" from "d" })
            }
            items("table2") {
                consistentRead(true)
                keys({
                    "y" from "z"
                })
            }
        }
        assertEquals(sdkRequest.requestItems(), dslRequest.requestItems())
    }
}