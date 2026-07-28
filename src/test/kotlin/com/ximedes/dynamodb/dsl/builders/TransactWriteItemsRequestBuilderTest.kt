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

internal class TransactWriteItemsRequestBuilderTest {
    private fun dslRequest(init: TransactWriteItemsRequestBuilder.() -> Unit) =
            TransactWriteItemsRequestBuilder().apply(init).build()

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = TransactWriteItemsRequest.builder()
            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
            .build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
        assertEquals(ReturnConsumedCapacity.INDEXES, dslRequest.returnConsumedCapacity())
    }


    @Test
    fun returnItemCollectionMetrics() {
        val sdkRequest = TransactWriteItemsRequest.builder()
            .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
            .build()
        val dslRequest = dslRequest {
            returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
        }
        assertEquals(sdkRequest.returnItemCollectionMetrics(), dslRequest.returnItemCollectionMetrics())
        assertEquals(ReturnItemCollectionMetrics.SIZE, dslRequest.returnItemCollectionMetrics())
    }

    @Test
    fun clientRequestToken() {
        val sdkRequest = TransactWriteItemsRequest.builder().clientRequestToken("token").build()
        val dslRequest = dslRequest {
            clientRequestToken("token")
        }
        assertEquals(sdkRequest.clientRequestToken(), dslRequest.clientRequestToken())
    }

    @Test
    fun putItem() {
        val sdkRequest = TransactWriteItemsRequest.builder().transactItems(
                TransactWriteItem.builder().put(
                        Put.builder().tableName("foo").item(
                                mapOf("a" to AttributeValue.builder().s("z").build())
                        ).build()
                ).build()
        ).build()
        val dslRequest = dslRequest {
            put("foo") {
                item {
                    "a" from "z"
                }
            }
        }
        assertEquals(sdkRequest.transactItems(), dslRequest.transactItems())
    }

    @Test
    fun updateItem() {
        val sdkRequest = TransactWriteItemsRequest.builder().transactItems(
                TransactWriteItem.builder().update(
                        Update.builder()
                                .tableName("foo")
                                .key(
                                        mapOf("a" to AttributeValue.builder().s("z").build())
                                )
                                .updateExpression("SET x = :b")
                                .expressionAttributeValues(mapOf("b" to AttributeValue.builder().s("x").build()))
                                .build()
                ).build()
        ).build()
        val dslRequest = dslRequest {
            update("foo") {
                key {
                    "a" from "z"
                }
                update("SET x = :b")
                attributeValues {
                    "b" from "x"
                }
            }
        }
        assertEquals(sdkRequest.transactItems(), dslRequest.transactItems())
    }

    @Test
    fun deleteItem() {
        val sdkRequest = TransactWriteItemsRequest.builder().transactItems(
                TransactWriteItem.builder().delete(
                        Delete.builder()
                                .tableName("foo")
                                .key(
                                        mapOf("a" to AttributeValue.builder().s("z").build())
                                )
                                .expressionAttributeValues(mapOf("b" to AttributeValue.builder().s("x").build()))
                                .build()
                ).build()
        ).build()
        val dslRequest = dslRequest {
            delete("foo") {
                key {
                    "a" from "z"
                }
                attributeValues {
                    "b" from "x"
                }
            }
        }
        assertEquals(sdkRequest.transactItems(), dslRequest.transactItems())
    }

    /**
     * Each item in a transaction renders its expressions independently, so placeholders generated
     * for one item must never leak into or be overwritten by another item's.
     */
    @Test
    fun transactionItemsKeepTheirOwnExpressions() {
        val dslRequest = dslRequest {
            put("users") {
                item { "id" from "new-user" }
                condition { attributeNotExists("id") }
            }
            update("counters") {
                key { "id" from "user-count" }
                update { set("count") { increment(1) } }
                condition { "locked" eq false }
            }
            delete("sessions") {
                key { "id" from "old-session" }
                condition { "expired" eq true }
            }
        }

        val put = dslRequest.transactItems()[0].put()
        assertEquals(
                "attribute_not_exists(id)",
                resolve(put.conditionExpression(), put.expressionAttributeNames(), put.expressionAttributeValues())
        )

        val update = dslRequest.transactItems()[1].update()
        assertEquals(
                "SET count = count + 1",
                resolve(update.updateExpression(), update.expressionAttributeNames(), update.expressionAttributeValues())
        )
        assertEquals(
                "locked = false",
                resolve(update.conditionExpression(), update.expressionAttributeNames(), update.expressionAttributeValues())
        )

        val delete = dslRequest.transactItems()[2].delete()
        assertEquals(
                "expired = true",
                resolve(delete.conditionExpression(), delete.expressionAttributeNames(), delete.expressionAttributeValues())
        )
    }
}