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
import software.amazon.awssdk.services.dynamodb.model.*

internal class TransactGetItemsRequestBuilderTest {

    private fun dslRequest(init: TransactGetItemsRequestBuilder.() -> Unit) =
            TransactGetItemsRequestBuilder().apply(init).build()

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest =
                TransactGetItemsRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(ReturnConsumedCapacity.INDEXES, sdkRequest.returnConsumedCapacity())
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
    }

    @Test
    fun singleGet() {
        val sdkRequest = TransactGetItemsRequest.builder().transactItems(
                TransactGetItem.builder().get(
                        Get.builder().tableName("foo").key(
                                mapOf("a" to AttributeValue.builder().s("z").build())
                        ).build()
                ).build()
        ).build()
        val dslRequest = dslRequest {
            get("foo") {
                key {
                    "a" from "z"
                }
            }
        }
        assertEquals(sdkRequest.transactItems(), dslRequest.transactItems())
    }


}