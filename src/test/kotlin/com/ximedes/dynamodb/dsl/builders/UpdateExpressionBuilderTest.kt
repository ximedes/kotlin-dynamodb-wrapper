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

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class UpdateExpressionBuilderTest {

    // ========== SET Value Operations ==========

    @Test
    fun setStringValue() {
        val result = buildUpdate { set("name", "John") }
        assertEquals("SET #n1 = :v1", result.expression)
        assertEquals("name", result.attributeNames["#n1"])
        assertEquals("John", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun setNumberValue() {
        val result = buildUpdate { set("count", 42) }
        assertEquals("SET #n1 = :v1", result.expression)
        assertEquals("42", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun setBooleanValue() {
        val result = buildUpdate { set("active", true) }
        assertEquals("SET #n1 = :v1", result.expression)
        assertEquals(true, result.attributeValues[":v1"]?.bool())
    }

    @Test
    fun setMultipleValues() {
        val result = buildUpdate {
            set("name", "John")
            set("age", 30)
            set("active", true)
        }
        assertEquals("SET #n1 = :v1, #n2 = :v2, #n3 = :v3", result.expression)
        assertEquals(3, result.attributeNames.size)
        assertEquals(3, result.attributeValues.size)
    }

    // ========== SET with Nested Paths ==========

    @Test
    fun setNestedPath() {
        val result = buildUpdate { set(AttributePath.of("address", "city"), "Amsterdam") }
        assertEquals("SET #n1.#n2 = :v1", result.expression)
        assertEquals("address", result.attributeNames["#n1"])
        assertEquals("city", result.attributeNames["#n2"])
    }

    @Test
    fun setArrayIndex() {
        val result = buildUpdate { set(AttributePath.of("items", 0, "quantity"), 5) }
        assertEquals("SET #n1[0].#n2 = :v1", result.expression)
    }

    // ========== SET Increment/Decrement ==========

    @Test
    fun increment() {
        val result = buildUpdate { set("count") { increment(1) } }
        assertEquals("SET #n1 = #n1 + :v1", result.expression)
        assertEquals("1", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun incrementByLargeNumber() {
        val result = buildUpdate { set("views") { increment(1000) } }
        assertEquals("SET #n1 = #n1 + :v1", result.expression)
        assertEquals("1000", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun decrement() {
        val result = buildUpdate { set("stock") { decrement(5) } }
        assertEquals("SET #n1 = #n1 - :v1", result.expression)
        assertEquals("5", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun incrementWithInfixOperator() {
        val result = buildUpdate { set("count") { "count" plus 1 } }
        assertEquals("SET #n1 = #n1 + :v1", result.expression)
    }

    @Test
    fun decrementWithInfixOperator() {
        val result = buildUpdate { set("stock") { "stock" minus 5 } }
        assertEquals("SET #n1 = #n1 - :v1", result.expression)
    }

    // ========== SET if_not_exists ==========

    @Test
    fun ifNotExists() {
        val result = buildUpdate { set("version") { ifNotExists(1) } }
        assertEquals("SET #n1 = if_not_exists(#n1, :v1)", result.expression)
        assertEquals("1", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun ifNotExistsWithString() {
        val result = buildUpdate { set("status") { ifNotExists("PENDING") } }
        assertEquals("SET #n1 = if_not_exists(#n1, :v1)", result.expression)
        assertEquals("PENDING", result.attributeValues[":v1"]?.s())
    }

    // ========== SET list_append ==========

    @Test
    fun listAppend() {
        val result = buildUpdate { set("tags") { listAppend(listOf("new", "tags")) } }
        assertEquals("SET #n1 = list_append(#n1, :v1)", result.expression)
        val listValue = result.attributeValues[":v1"]?.l()
        assertNotNull(listValue)
        assertEquals(2, listValue?.size)
        assertEquals("new", listValue?.get(0)?.s())
    }

    @Test
    fun listPrepend() {
        val result = buildUpdate { set("history") { listPrepend(listOf("first")) } }
        assertEquals("SET #n1 = list_append(:v1, #n1)", result.expression)
    }

    @Test
    fun listAppendSingleItem() {
        val result = buildUpdate { set("items") { listAppend(listOf("single")) } }
        assertEquals("SET #n1 = list_append(#n1, :v1)", result.expression)
    }

    // ========== REMOVE Operations ==========

    @Test
    fun removeSingleAttribute() {
        val result = buildUpdate { remove("tempField") }
        assertEquals("REMOVE #n1", result.expression)
        assertEquals("tempField", result.attributeNames["#n1"])
        assertTrue(result.attributeValues.isEmpty())
    }

    @Test
    fun removeMultipleAttributes() {
        val result = buildUpdate { remove("field1", "field2", "field3") }
        assertEquals("REMOVE #n1, #n2, #n3", result.expression)
        assertEquals(3, result.attributeNames.size)
    }

    @Test
    fun removeNestedPath() {
        val result = buildUpdate { remove(AttributePath.of("metadata", "temp")) }
        assertEquals("REMOVE #n1.#n2", result.expression)
    }

    // ========== ADD Operations ==========

    @Test
    fun addToNumber() {
        val result = buildUpdate { add("counter", 10) }
        assertEquals("ADD #n1 :v1", result.expression)
        assertEquals("10", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun addToSet() {
        val result = buildUpdate { add("tags", setOf("new", "tags")) }
        assertEquals("ADD #n1 :v1", result.expression)
        val ssValue = result.attributeValues[":v1"]?.ss()
        assertNotNull(ssValue)
        assertTrue(ssValue!!.contains("new"))
        assertTrue(ssValue.contains("tags"))
    }

    // ========== DELETE Operations ==========

    @Test
    fun deleteFromSet() {
        val result = buildUpdate { delete("tags", setOf("old", "deprecated")) }
        assertEquals("DELETE #n1 :v1", result.expression)
        val ssValue = result.attributeValues[":v1"]?.ss()
        assertNotNull(ssValue)
        assertTrue(ssValue!!.contains("old"))
    }

    // ========== Combined Operations ==========

    @Test
    fun setAndRemove() {
        val result = buildUpdate {
            set("status", "ACTIVE")
            remove("tempData")
        }
        assertEquals("SET #n1 = :v1 REMOVE #n2", result.expression)
    }

    @Test
    fun allOperationTypes() {
        val result = buildUpdate {
            set("name", "Updated")
            remove("old")
            add("counter", 1)
            delete("tags", setOf("deprecated"))
        }
        assertTrue(result.expression.contains("SET"))
        assertTrue(result.expression.contains("REMOVE"))
        assertTrue(result.expression.contains("ADD"))
        assertTrue(result.expression.contains("DELETE"))
    }

    @Test
    fun multipleSetWithRemove() {
        val result = buildUpdate {
            set("a", 1)
            set("b", 2)
            remove("c", "d")
        }
        assertEquals("SET #n1 = :v1, #n2 = :v2 REMOVE #n3, #n4", result.expression)
    }

    // ========== Integration with Request Builders ==========

    @Test
    fun updateItemWithTypeSafeUpdate() {
        val dslRequest = UpdateItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            update {
                set("count") { increment(1) }
                set("updatedAt", 1234567890L)
            }
        }.build()

        assertTrue(dslRequest.updateExpression().contains("SET"))
        assertTrue(dslRequest.updateExpression().contains("#n1 = #n1 + :v1"))
        assertNotNull(dslRequest.expressionAttributeValues()[":v1"])
        assertNotNull(dslRequest.expressionAttributeValues()[":v2"])
    }

    @Test
    fun updateItemWithConditionAndUpdate() {
        val dslRequest = UpdateItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            update {
                set("status", "PROCESSED")
            }
            condition { "status" eq "PENDING" }
        }.build()

        assertNotNull(dslRequest.updateExpression())
        assertNotNull(dslRequest.conditionExpression())
        // Both update and condition contribute to attribute values/names
        // Each uses a separate renderer so names may repeat, but values are merged
        assertTrue(dslRequest.expressionAttributeValues().isNotEmpty())
        assertTrue(dslRequest.expressionAttributeNames().isNotEmpty())
    }

    // ========== Edge Cases ==========

    @Test
    fun setEmptyString() {
        val result = buildUpdate { set("field", "") }
        assertEquals("", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun setZero() {
        val result = buildUpdate { set("count", 0) }
        assertEquals("0", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun setNegativeNumber() {
        val result = buildUpdate { set("balance", -100) }
        assertEquals("-100", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun setFloatingPoint() {
        val result = buildUpdate { set("price", 19.99) }
        assertEquals("19.99", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun incrementByZero() {
        val result = buildUpdate { set("count") { increment(0) } }
        assertEquals("SET #n1 = #n1 + :v1", result.expression)
        assertEquals("0", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun removeWithNestedArrayIndex() {
        val result = buildUpdate { remove(AttributePath.of("items", 0)) }
        assertEquals("REMOVE #n1[0]", result.expression)
    }

    @Test
    fun listAppendEmpty() {
        val result = buildUpdate { set("list") { listAppend(emptyList()) } }
        assertEquals("SET #n1 = list_append(#n1, :v1)", result.expression)
        val listValue = result.attributeValues[":v1"]?.l()
        assertNotNull(listValue)
        assertTrue(listValue!!.isEmpty())
    }

    @Test
    fun addNegativeNumber() {
        // ADD with negative number is effectively a subtraction
        val result = buildUpdate { add("counter", -5) }
        assertEquals("ADD #n1 :v1", result.expression)
        assertEquals("-5", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun mixManualAndTypeSafeUpdate() {
        val dslRequest = UpdateItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            update { set("auto", "value") }
            attributeValues { ":manual" from "extra" }
            attributeNames("#manual" to "manualAttr")
        }.build()

        // Both should be present
        assertNotNull(dslRequest.expressionAttributeValues()[":v1"])
        assertNotNull(dslRequest.expressionAttributeValues()[":manual"])
        assertNotNull(dslRequest.expressionAttributeNames()["#n1"])
        assertNotNull(dslRequest.expressionAttributeNames()["#manual"])
    }
}
