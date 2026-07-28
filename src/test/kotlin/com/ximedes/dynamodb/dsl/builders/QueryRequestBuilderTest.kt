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
import org.junit.jupiter.api.Assertions.assertThrows
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

    private fun QueryRequest.resolved(expression: String?) =
            resolve(expression, expressionAttributeNames(), expressionAttributeValues())

    @Test
    fun keyConditionDsl() {
        val dslRequest = dslRequest { keyCondition { ("pk" eq "USER#123") and ("sk" ge "ORDER#2024") } }
        assertEquals(
                """(pk = "USER#123") AND (sk >= "ORDER#2024")""",
                dslRequest.resolved(dslRequest.keyConditionExpression())
        )
    }

    @Test
    fun filterDsl() {
        val dslRequest = dslRequest { filter { ("total" gt 100) and ("status" ne "CANCELLED") } }
        assertEquals(
                """(total > 100) AND (status <> "CANCELLED")""",
                dslRequest.resolved(dslRequest.filterExpression())
        )
    }

    /**
     * A key condition and a filter on the same request each get their own placeholders; neither may
     * overwrite the other's attribute names or values. This is the Query example from the README.
     */
    @Test
    fun keyConditionAndFilterDslDoNotSharePlaceholders() {
        val dslRequest = dslRequest("orders") {
            keyCondition { ("pk" eq "USER#123") and ("sk" ge "ORDER#2024") }
            filter { ("total" gt 100) and ("status" ne "CANCELLED") }
        }

        assertEquals(
                """(pk = "USER#123") AND (sk >= "ORDER#2024")""",
                dslRequest.resolved(dslRequest.keyConditionExpression())
        )
        assertEquals(
                """(total > 100) AND (status <> "CANCELLED")""",
                dslRequest.resolved(dslRequest.filterExpression())
        )
    }

    @Test
    fun keyConditionAndFilterDslKeepAllAttributeNamesAndValues() {
        val dslRequest = dslRequest("orders") {
            keyCondition { "pk" eq "USER#123" }
            filter { "status" ne "CANCELLED" }
        }

        assertEquals(
                setOf("pk", "status"),
                dslRequest.expressionAttributeNames().values.toSet()
        )
        assertEquals(
                setOf(
                        AttributeValue.builder().s("USER#123").build(),
                        AttributeValue.builder().s("CANCELLED").build()
                ),
                dslRequest.expressionAttributeValues().values.toSet()
        )
    }

    /** Mixing the DSL with manually supplied placeholders is documented in the README. */
    @Test
    fun filterDslCombinedWithManualAttributes() {
        val dslRequest = dslRequest {
            filter { "status" eq "ACTIVE" }
            attributeValues { ":extra" from "manual-value" }
            attributeNames("#custom" to "customField")
        }

        assertEquals(
                "status = \"ACTIVE\"",
                dslRequest.resolved(dslRequest.filterExpression())
        )
        assertEquals("customField", dslRequest.expressionAttributeNames()["#custom"])
        assertEquals(
                AttributeValue.builder().s("manual-value").build(),
                dslRequest.expressionAttributeValues()[":extra"]
        )
    }

    /** An attribute used by two expressions is registered once and shares a single placeholder. */
    @Test
    fun attributeUsedByTwoExpressionsIsRegisteredOnce() {
        val dslRequest = dslRequest {
            keyCondition { "pk" eq "A" }
            filter { "pk" ne "B" }
        }

        assertEquals(mapOf("pk" to 1), dslRequest.expressionAttributeNames().values.groupingBy { it }.eachCount())
        assertEquals("pk = \"A\"", dslRequest.resolved(dslRequest.keyConditionExpression()))
        assertEquals("pk <> \"B\"", dslRequest.resolved(dslRequest.filterExpression()))
    }

    @Test
    fun manualPlaceholderClashingWithGeneratedOneIsRejected() {
        val values = assertThrows(IllegalArgumentException::class.java) {
            dslRequest {
                filter { "status" eq "ACTIVE" }
                attributeValues { ":v1" from "manual" }
            }
        }
        assertTrue(values.message!!.contains(":v1"), values.message)

        val names = assertThrows(IllegalArgumentException::class.java) {
            dslRequest {
                filter { "status" eq "ACTIVE" }
                attributeNames("#n1" to "manual")
            }
        }
        assertTrue(names.message!!.contains("#n1"), names.message)
    }

    @Test
    fun generatedPlaceholderClashingWithManualOneIsRejected() {
        val thrown = assertThrows(IllegalArgumentException::class.java) {
            dslRequest {
                attributeValues { ":v1" from "manual" }
                filter { "status" eq "ACTIVE" }
            }
        }
        assertTrue(thrown.message!!.contains(":v1"), thrown.message)
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