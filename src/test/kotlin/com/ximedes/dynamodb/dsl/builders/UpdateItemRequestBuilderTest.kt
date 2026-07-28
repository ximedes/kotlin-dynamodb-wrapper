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

internal class UpdateItemRequestBuilderTest {

    private fun dslRequest(tableName: String = "foo", init: UpdateItemRequestBuilder.() -> Unit) =
            UpdateItemRequestBuilder(tableName).apply(init).build()

    @Test
    fun update() {
        val sdkRequest = UpdateItemRequest.builder().updateExpression("update").build()
        val dslRequest = dslRequest { update("update") }
        assertEquals(sdkRequest.updateExpression(), dslRequest.updateExpression())
    }

    @Test
    fun condition() {
        val sdkRequest = UpdateItemRequest.builder().conditionExpression("condition").build()
        val dslRequest = dslRequest { condition("condition") }
        assertEquals(sdkRequest.conditionExpression(), dslRequest.conditionExpression())
    }

    @Test
    fun key() {
        val sdkRequest = UpdateItemRequest.builder().key(mapOf("a" to AttributeValue.builder().s("b").build())).build()
        val dslRequest = dslRequest { key { "a" from "b" } }
        assertEquals(sdkRequest.key(), dslRequest.key())
    }

    private fun UpdateItemRequest.resolved(expression: String?) =
            resolve(expression, expressionAttributeNames(), expressionAttributeValues())

    @Test
    fun updateDsl() {
        val dslRequest = dslRequest {
            update {
                set("name", "NewName")
                set("loginCount") { increment(1) }
                remove("tempField")
            }
        }
        assertEquals(
                "SET name = \"NewName\", loginCount = loginCount + 1 REMOVE tempField",
                dslRequest.resolved(dslRequest.updateExpression())
        )
    }

    @Test
    fun conditionDsl() {
        val dslRequest = dslRequest { condition { "status" eq "ACTIVE" } }
        assertEquals(
                "status = \"ACTIVE\"",
                dslRequest.resolved(dslRequest.conditionExpression())
        )
    }

    /**
     * An update expression and a condition expression on the same request each get their own
     * placeholders. If they collide the request stays well-formed, so DynamoDB accepts it and
     * silently writes the wrong attribute instead of reporting an error.
     */
    @Test
    fun updateAndConditionDslDoNotSharePlaceholders() {
        val dslRequest = dslRequest("users") {
            key { "id" from "123" }
            update { set("name", "NewName") }
            condition { "status" eq "ACTIVE" }
        }

        assertEquals(
                "SET name = \"NewName\"",
                dslRequest.resolved(dslRequest.updateExpression())
        )
        assertEquals(
                "status = \"ACTIVE\"",
                dslRequest.resolved(dslRequest.conditionExpression())
        )
    }

    @Test
    fun updateAndConditionDslWithNestedPaths() {
        val dslRequest = dslRequest("users") {
            key { "id" from "123" }
            update { set(AttributePath.of("address", "city"), "Amsterdam") }
            condition { path("address", "country") eq "NL" }
        }

        assertEquals(
                "SET address.city = \"Amsterdam\"",
                dslRequest.resolved(dslRequest.updateExpression())
        )
        assertEquals(
                "address.country = \"NL\"",
                dslRequest.resolved(dslRequest.conditionExpression())
        )
    }

    /** The counter increment from the README, guarded by a condition. */
    @Test
    fun incrementGuardedByConditionKeepsBothExpressions() {
        val dslRequest = dslRequest("counters") {
            key { "id" from "page-views" }
            update { set("count") { increment(1) } }
            condition { attributeExists("count") }
        }

        assertEquals("SET count = count + 1", dslRequest.resolved(dslRequest.updateExpression()))
        assertEquals("attribute_exists(count)", dslRequest.resolved(dslRequest.conditionExpression()))
    }


    @Test
    fun attributeValues() {
        val sdkRequest =
                UpdateItemRequest.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build()))
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
        val sdkRequest = UpdateItemRequest.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslRequest = dslRequest {
            attributeNames("a" to "z")
        }
        assertEquals(sdkRequest.expressionAttributeNames(), dslRequest.expressionAttributeNames())
    }

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = UpdateItemRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
        assertEquals(ReturnConsumedCapacity.INDEXES, dslRequest.returnConsumedCapacity())
    }


    @Test
    fun returnItemCollectionMetrics() {
        val sdkRequest = UpdateItemRequest.builder()
                .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
                .build()
        val dslRequest = dslRequest {
            returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
        }
        assertEquals(sdkRequest.returnItemCollectionMetrics(), dslRequest.returnItemCollectionMetrics())
        assertEquals(ReturnItemCollectionMetrics.SIZE, dslRequest.returnItemCollectionMetrics())
    }

    @Test
    fun returnValues() {
        val sdkRequest = UpdateItemRequest.builder()
            .returnValues(ReturnValue.ALL_NEW)
            .build()
        val dslRequest = dslRequest {
            returnValues(ReturnValue.ALL_NEW)
        }
        assertEquals(sdkRequest.returnValues(), dslRequest.returnValues())
        assertEquals(ReturnValue.ALL_NEW, dslRequest.returnValues())
    }
}