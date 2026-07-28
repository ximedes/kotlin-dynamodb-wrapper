/*
 * Copyright 2019-2025 the original author or authors.
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
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.Select

internal class ScanRequestBuilderTest {

    private fun dslRequest(
        tableName: String = "foo",
        init: ScanRequestBuilder.() -> Unit
    ): ScanRequest {
        return ScanRequestBuilder(tableName).apply(init).build()
    }

    @Test
    fun tableName() {
        val sdkRequest = ScanRequest.builder().tableName("table").build()
        val dslRequest = dslRequest("table") { }
        assertEquals("table", dslRequest.tableName())
        assertEquals(sdkRequest.tableName(), dslRequest.tableName())
    }

    @Test
    fun useIndex() {
        val sdkRequest = ScanRequest.builder().indexName("index").build()
        val dslRequest = dslRequest { indexName("index") }
        assertEquals("index", dslRequest.indexName())
        assertEquals(sdkRequest.indexName(), dslRequest.indexName())
    }

    @Test
    fun attributeValues() {
        val sdkRequest =
            ScanRequest.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build()))
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
        val sdkRequest = ScanRequest.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslRequest = dslRequest {
            attributeNames("a" to "z")
        }
        assertEquals(sdkRequest.expressionAttributeNames(), dslRequest.expressionAttributeNames())
    }

    @Test
    fun projection() {
        val sdkRequest = ScanRequest.builder().projectionExpression("a, b, c").build()
        val dslRequest = dslRequest { projection("a, b, c") }
        assertEquals(sdkRequest.projectionExpression(), dslRequest.projectionExpression())
    }

    @Test
    fun consistentRead() {
        val sdkRequest = ScanRequest.builder().consistentRead(true).build()
        val dslRequest = dslRequest {
            consistentRead(true)
        }
        assertTrue(sdkRequest.consistentRead())
        assertEquals(sdkRequest.consistentRead(), dslRequest.consistentRead())
    }

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = ScanRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(ReturnConsumedCapacity.INDEXES, sdkRequest.returnConsumedCapacity())
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
    }

    @Test
    fun exclusiveStartKey() {
        val sdkRequest =
            ScanRequest.builder().exclusiveStartKey(mapOf("a" to AttributeValue.builder().s("z").build()))
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
        val sdkRequest = ScanRequest.builder().filterExpression("abc").build()
        val dslRequest = dslRequest { filter("abc") }
        assertEquals("abc", dslRequest.filterExpression())
        assertEquals(sdkRequest.filterExpression(), dslRequest.filterExpression())
    }

    private fun ScanRequest.resolved(expression: String?) =
        resolve(expression, expressionAttributeNames(), expressionAttributeValues())

    @Test
    fun filterDsl() {
        val dslRequest = dslRequest { filter { ("price" lt 50) and attributeExists("discount") } }
        assertEquals(
            "(price < 50) AND (attribute_exists(discount))",
            dslRequest.resolved(dslRequest.filterExpression())
        )
    }

    @Test
    fun filterDslCombinedWithManualAttributes() {
        val dslRequest = dslRequest {
            filter { "status" eq "ACTIVE" }
            attributeValues { ":extra" from "manual-value" }
            attributeNames("#custom" to "customField")
        }

        assertEquals("status = \"ACTIVE\"", dslRequest.resolved(dslRequest.filterExpression()))
        assertEquals("customField", dslRequest.expressionAttributeNames()["#custom"])
        assertEquals(
            AttributeValue.builder().s("manual-value").build(),
            dslRequest.expressionAttributeValues()[":extra"]
        )
    }

    @Test
    fun limit() {
        val sdkRequest = ScanRequest.builder().limit(999).build()
        val dslRequest = dslRequest { limit(999) }
        assertEquals(sdkRequest.limit(), dslRequest.limit())
    }

    @Test
    fun select() {
        val sdkRequest = ScanRequest.builder().select(Select.COUNT).build()
        val dslRequest = dslRequest { select(Select.COUNT) }
        assertEquals(sdkRequest.select(), dslRequest.select())
    }

    @Test
    fun segment() {
        val sdkRequest = ScanRequest.builder().segment(2).build()
        val dslRequest = dslRequest { segment(2) }
        assertEquals(sdkRequest.segment(), dslRequest.segment())
    }

    @Test
    fun totalSegments() {
        val sdkRequest = ScanRequest.builder().totalSegments(4).build()
        val dslRequest = dslRequest { totalSegments(4) }
        assertEquals(sdkRequest.totalSegments(), dslRequest.totalSegments())
    }
}
