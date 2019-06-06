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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.Select

internal class QueryRequestBuilderTest {

    private fun dslRequest(
            tableName: String = "foo",
            init: QueryRequestBuilder.() -> Unit
    ): QueryRequest {
        return QueryRequestBuilder(tableName).apply(init).build()
    }

    @Test
    fun tableName() {
        val sdkRequest = QueryRequest.builder().tableName("table").build()
        val dslRequest = dslRequest("table") { }
        assertEquals("table", dslRequest.tableName())
        assertEquals(sdkRequest.tableName(), dslRequest.tableName())
    }

    @Test
    fun useIndex() {
        val sdkRequest = QueryRequest.builder().indexName("index").build()
        val dslRequest = dslRequest { indexName("index") }
        assertEquals("index", dslRequest.indexName())
        assertEquals(sdkRequest.indexName(), dslRequest.indexName())
    }

    @Test
    fun keyCondition() {
        val sdkRequest = QueryRequest.builder().keyConditionExpression("kce").build()
        val dslRequest = dslRequest { keyCondition("kce") }
        assertEquals("kce", dslRequest.keyConditionExpression())
        assertEquals(sdkRequest.keyConditionExpression(), dslRequest.keyConditionExpression())
    }

    @Test
    fun attributeValues() {
        val sdkRequest =
                QueryRequest.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build()))
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
        val sdkRequest = QueryRequest.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslRequest = dslRequest {
            attributeNames("a" to "z")
        }
        assertEquals(sdkRequest.expressionAttributeNames(), dslRequest.expressionAttributeNames())
    }

    @Test
    fun projection() {
        val sdkRequest = QueryRequest.builder().projectionExpression("a, b, c").build()
        val dslRequest = dslRequest { projection("a, b, c") }
        assertEquals(sdkRequest.projectionExpression(), dslRequest.projectionExpression())
    }


    @Test
    fun consistentRead() {
        val sdkRequest = QueryRequest.builder().consistentRead(true).build()
        val dslRequest = dslRequest {
            consistentRead(true)
        }
        assertTrue(sdkRequest.consistentRead())
        assertEquals(sdkRequest.consistentRead(), dslRequest.consistentRead())
    }


    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = QueryRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(ReturnConsumedCapacity.INDEXES, sdkRequest.returnConsumedCapacity())
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
    }


    @Test
    fun exclusiveStartKey() {
        val sdkRequest =
                QueryRequest.builder().exclusiveStartKey(mapOf("a" to AttributeValue.builder().s("z").build()))
                        .build()
        val dslRequest = dslRequest {
            exclusiveStartKey {
                "a" from "z"
            }
        }
        assertEquals(sdkRequest.exclusiveStartKey(), dslRequest.exclusiveStartKey())
    }

    @Test
    fun filter() {
        val sdkRequest = QueryRequest.builder().filterExpression("abc").build()
        val dslRequest = dslRequest { filter("abc") }
        assertEquals("abc", dslRequest.filterExpression())
        assertEquals(sdkRequest.filterExpression(), dslRequest.filterExpression())
    }

    @Test
    fun limit() {
        val sdkRequest = QueryRequest.builder().limit(999).build()
        val dslRequest = dslRequest { limit(999) }
        assertEquals(sdkRequest.limit(), dslRequest.limit())
    }

    @Test
    fun scanIndexForward() {
        val sdkRequest = QueryRequest.builder().scanIndexForward(false).build()
        val dslRequest = dslRequest {
            scanIndexForward(false)
        }
        assertEquals(sdkRequest.scanIndexForward(), dslRequest.scanIndexForward())
    }

    @Test
    fun select() {
        val sdkRequest = QueryRequest.builder().select(Select.COUNT).build()
        val dslRequest = dslRequest { select(Select.COUNT) }
        assertEquals(sdkRequest.select(), dslRequest.select())
    }
}