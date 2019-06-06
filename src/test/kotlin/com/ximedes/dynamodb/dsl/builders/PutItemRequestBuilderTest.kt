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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ReturnItemCollectionMetrics

internal class PutItemRequestBuilderTest {

    private fun dslRequest(
            tableName: String = "foo",
            init: PutItemRequestBuilder.() -> Unit
    ): PutItemRequest {
        return PutItemRequestBuilder(tableName).apply(init).build()
    }

    @Test
    fun tableName() {
        val sdkRequest = PutItemRequest.builder().tableName("table").build()
        val dslRequest = dslRequest("table") {}
        assertEquals(sdkRequest.tableName(), dslRequest.tableName())
    }

    @Test
    fun condition() {
        val sdkRequest = PutItemRequest.builder().conditionExpression("condition").build()
        val dslRequest = dslRequest {
            condition("condition")
        }
        assertEquals(sdkRequest.conditionExpression(), dslRequest.conditionExpression())
    }

    @Test
    fun item() {
        val item = ItemBuilder().apply {
            "string" from "a"
            "int" from 2
        }.build()
        val sdkRequest = PutItemRequest.builder().item(item).build()
        val dslRequest = dslRequest {
            item {
                "string" from "a"
                "int" from 2
            }
        }
        assertEquals(sdkRequest.item(), dslRequest.item())
    }

    @Test
    fun attributeValues() {
        val sdkRequest =
                PutItemRequest.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build()))
                        .build()
        val dslRequest = dslRequest {
            attributeValues {
                "a" from "z"
            }
        }
        assertEquals(sdkRequest.expressionAttributeValues(), dslRequest.expressionAttributeValues())
    }

    @Test
    fun attributeNames() {
        val sdkRequest = PutItemRequest.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslRequest = dslRequest {
            attributeNames("a" to "z")
        }
        assertEquals(sdkRequest.expressionAttributeNames(), dslRequest.expressionAttributeNames())
    }


    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = PutItemRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
        assertEquals(ReturnConsumedCapacity.INDEXES, dslRequest.returnConsumedCapacity())
    }


    @Test
    fun returnItemCollectionMetrics() {
        val sdkRequest = PutItemRequest.builder()
                .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
                .build()
        val dslRequest = dslRequest {
            returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
        }
        assertEquals(sdkRequest.returnItemCollectionMetrics(), dslRequest.returnItemCollectionMetrics())
        assertEquals(ReturnItemCollectionMetrics.SIZE, dslRequest.returnItemCollectionMetrics())
    }


}