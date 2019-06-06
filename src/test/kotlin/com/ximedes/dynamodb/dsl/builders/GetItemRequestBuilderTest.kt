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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

internal class GetItemRequestBuilderTest {
    private fun dslRequest(tableName: String = "foo", init: GetItemRequestBuilder.() -> Unit) =
            GetItemRequestBuilder(tableName).apply(init).build()

    @Test
    fun consistentRead() {
        val sdkRequest = GetItemRequest.builder().consistentRead(true).build()
        val dslRequest = dslRequest {
            consistentRead(true)
        }
        assertTrue(sdkRequest.consistentRead())
        assertEquals(sdkRequest.consistentRead(), dslRequest.consistentRead())
    }

    @Test
    fun attributeNames() {
        val sdkRequest = GetItemRequest.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslRequest = dslRequest {
            attributeNames("a" to "z")
        }
        assertEquals(sdkRequest.expressionAttributeNames(), dslRequest.expressionAttributeNames())
    }

    @Test
    fun returnConsumedCapacity() {
        val sdkRequest = GetItemRequest.builder().returnConsumedCapacity(ReturnConsumedCapacity.INDEXES).build()
        val dslRequest = dslRequest {
            returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        }
        assertEquals(ReturnConsumedCapacity.INDEXES, sdkRequest.returnConsumedCapacity())
        assertEquals(sdkRequest.returnConsumedCapacity(), dslRequest.returnConsumedCapacity())
    }

    @Test
    fun projection() {
        val sdkRequest = GetItemRequest.builder().projectionExpression("a, b, c").build()
        val dslRequest = dslRequest { projection("a, b, c") }
        assertEquals(sdkRequest.projectionExpression(), dslRequest.projectionExpression())
    }

    @Test
    fun key() {
        val sdkRequest = GetItemRequest.builder().key(mapOf("a" to AttributeValue.builder().s("b").build())).build()
        val dslRequest = dslRequest { key { "a" from "b" } }
        assertEquals(sdkRequest.key(), dslRequest.key())
    }
}