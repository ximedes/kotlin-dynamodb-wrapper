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
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

internal class ConditionBuilderTest {

    // ========== Comparison Operators ==========

    @Test
    fun equalsString() {
        val result = buildCondition { "status" eq "ACTIVE" }
        assertEquals("#n1 = :v1", result.expression)
        assertEquals("status", result.attributeNames["#n1"])
        assertEquals("ACTIVE", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun equalsNumber() {
        val result = buildCondition { "count" eq 42 }
        assertEquals("#n1 = :v1", result.expression)
        assertEquals("count", result.attributeNames["#n1"])
        assertEquals("42", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun equalsBoolean() {
        val result = buildCondition { "active" eq true }
        assertEquals("#n1 = :v1", result.expression)
        assertEquals(true, result.attributeValues[":v1"]?.bool())
    }

    @Test
    fun notEquals() {
        val result = buildCondition { "status" ne "DELETED" }
        assertEquals("#n1 <> :v1", result.expression)
    }

    @Test
    fun lessThan() {
        val result = buildCondition { "age" lt 18 }
        assertEquals("#n1 < :v1", result.expression)
        assertEquals("18", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun lessThanOrEquals() {
        val result = buildCondition { "score" le 100 }
        assertEquals("#n1 <= :v1", result.expression)
    }

    @Test
    fun greaterThan() {
        val result = buildCondition { "price" gt 9.99 }
        assertEquals("#n1 > :v1", result.expression)
        assertEquals("9.99", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun greaterThanOrEquals() {
        val result = buildCondition { "quantity" ge 1 }
        assertEquals("#n1 >= :v1", result.expression)
    }

    // ========== Between Operator ==========

    @Test
    fun betweenWithRange() {
        val result = buildCondition { "age" between 18..65 }
        assertEquals("#n1 BETWEEN :v1 AND :v2", result.expression)
        assertEquals("18", result.attributeValues[":v1"]?.n())
        assertEquals("65", result.attributeValues[":v2"]?.n())
    }

    @Test
    fun betweenWithStringRange() {
        val result = buildCondition { "date" between "2024-01-01".."2024-12-31" }
        assertEquals("#n1 BETWEEN :v1 AND :v2", result.expression)
        assertEquals("2024-01-01", result.attributeValues[":v1"]?.s())
        assertEquals("2024-12-31", result.attributeValues[":v2"]?.s())
    }

    // ========== IN Operator ==========

    @Test
    fun inOperator() {
        val result = buildCondition { "status" isIn listOf("ACTIVE", "PENDING", "REVIEW") }
        assertEquals("#n1 IN (:v1, :v2, :v3)", result.expression)
        assertEquals("ACTIVE", result.attributeValues[":v1"]?.s())
        assertEquals("PENDING", result.attributeValues[":v2"]?.s())
        assertEquals("REVIEW", result.attributeValues[":v3"]?.s())
    }

    @Test
    fun inOperatorWithNumbers() {
        val result = buildCondition { "priority" isIn listOf(1, 2, 3) }
        assertEquals("#n1 IN (:v1, :v2, :v3)", result.expression)
    }

    // ========== Existence Checks ==========

    @Test
    fun attributeExists() {
        val result = buildCondition { attributeExists("email") }
        assertEquals("attribute_exists(#n1)", result.expression)
        assertEquals("email", result.attributeNames["#n1"])
        assertTrue(result.attributeValues.isEmpty())
    }

    @Test
    fun attributeNotExists() {
        val result = buildCondition { attributeNotExists("deletedAt") }
        assertEquals("attribute_not_exists(#n1)", result.expression)
    }

    // ========== Type Check ==========

    @Test
    fun attributeTypeString() {
        val result = buildCondition { attributeType("data", DynamoAttributeType.STRING) }
        assertEquals("attribute_type(#n1, :v1)", result.expression)
        assertEquals("S", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeMap() {
        val result = buildCondition { attributeType("metadata", DynamoAttributeType.MAP) }
        assertEquals("attribute_type(#n1, :v1)", result.expression)
        assertEquals("M", result.attributeValues[":v1"]?.s())
    }

    // ========== String Functions ==========

    @Test
    fun beginsWith() {
        val result = buildCondition { beginsWith("email", "admin@") }
        assertEquals("begins_with(#n1, :v1)", result.expression)
        assertEquals("admin@", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun contains() {
        val result = buildCondition { contains("description", "important") }
        assertEquals("contains(#n1, :v1)", result.expression)
        assertEquals("important", result.attributeValues[":v1"]?.s())
    }

    // ========== Size Function ==========

    @Test
    fun sizeEquals() {
        val result = buildCondition { size("tags") eq 5 }
        assertEquals("size(#n1) = :v1", result.expression)
        assertEquals("5", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun sizeLessThan() {
        val result = buildCondition { size("items") lt 100 }
        assertEquals("size(#n1) < :v1", result.expression)
    }

    @Test
    fun sizeGreaterThanOrEquals() {
        val result = buildCondition { size("comments") ge 1 }
        assertEquals("size(#n1) >= :v1", result.expression)
    }

    // ========== Logical Operators ==========

    @Test
    fun andOperator() {
        val result = buildCondition { ("status" eq "ACTIVE") and ("count" gt 0) }
        assertEquals("(#n1 = :v1) AND (#n2 > :v2)", result.expression)
        assertEquals("status", result.attributeNames["#n1"])
        assertEquals("count", result.attributeNames["#n2"])
    }

    @Test
    fun orOperator() {
        val result = buildCondition { ("status" eq "ACTIVE") or ("status" eq "PENDING") }
        assertEquals("(#n1 = :v1) OR (#n1 = :v2)", result.expression)
        // Same attribute should reuse the same name placeholder
        assertEquals(1, result.attributeNames.size)
    }

    @Test
    fun notOperator() {
        val result = buildCondition { not("deleted" eq true) }
        assertEquals("NOT (#n1 = :v1)", result.expression)
    }

    @Test
    fun complexNestedCondition() {
        val result = buildCondition {
            (("status" eq "ACTIVE") or ("status" eq "PENDING")) and ("count" lt 100)
        }
        assertEquals("((#n1 = :v1) OR (#n1 = :v2)) AND (#n2 < :v3)", result.expression)
    }

    @Test
    fun tripleAndCondition() {
        val result = buildCondition {
            ("a" eq 1) and ("b" eq 2) and ("c" eq 3)
        }
        assertEquals("((#n1 = :v1) AND (#n2 = :v2)) AND (#n3 = :v3)", result.expression)
    }

    // ========== Nested Attribute Paths ==========

    @Test
    fun nestedPathWithDot() {
        val result = buildCondition { path("address.city") eq "Amsterdam" }
        assertEquals("#n1.#n2 = :v1", result.expression)
        assertEquals("address", result.attributeNames["#n1"])
        assertEquals("city", result.attributeNames["#n2"])
    }

    @Test
    fun nestedPathWithVararg() {
        val result = buildCondition { path("user", "profile", "name") eq "John" }
        assertEquals("#n1.#n2.#n3 = :v1", result.expression)
    }

    @Test
    fun arrayIndexPath() {
        val result = buildCondition { path("items", 0, "quantity") gt 5 }
        assertEquals("#n1[0].#n2 > :v1", result.expression)
        assertEquals("items", result.attributeNames["#n1"])
        assertEquals("quantity", result.attributeNames["#n2"])
    }

    @Test
    fun arrayIndexPathWithExtension() {
        val result = buildCondition { path("items", 0, "name") eq "Widget" }
        assertEquals("#n1[0].#n2 = :v1", result.expression)
    }

    @Test
    fun deeplyNestedPath() {
        val result = buildCondition { path("level1", "level2", 0, "level3", 1, "value") eq 42 }
        assertEquals("#n1.#n2[0].#n3[1].#n4 = :v1", result.expression)
        assertEquals(4, result.attributeNames.size)
    }

    // ========== Integration with Request Builders ==========

    @Test
    fun deleteItemWithTypeSafeCondition() {
        val dslRequest = DeleteItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            condition { attributeExists("version") }
        }.build()

        assertEquals("attribute_exists(#n1)", dslRequest.conditionExpression())
        assertEquals("version", dslRequest.expressionAttributeNames()["#n1"])
    }

    @Test
    fun deleteItemWithComplexCondition() {
        val dslRequest = DeleteItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            condition {
                ("status" eq "INACTIVE") and (attributeNotExists("protected"))
            }
        }.build()

        assertEquals("(#n1 = :v1) AND (attribute_not_exists(#n2))", dslRequest.conditionExpression())
        assertEquals("INACTIVE", dslRequest.expressionAttributeValues()[":v1"]?.s())
    }

    @Test
    fun queryWithTypeSafeKeyConditionAndFilter() {
        val dslRequest = QueryRequestBuilder("orders").apply {
            keyCondition { ("pk" eq "USER#123") and ("sk" ge "ORDER#2024") }
            filter { "status" ne "CANCELLED" }
        }.build()

        assertEquals("(#n1 = :v1) AND (#n2 >= :v2)", dslRequest.keyConditionExpression())
        // filter uses its own renderer, so counters start fresh
        assertEquals("#n1 <> :v1", dslRequest.filterExpression())
        // All attribute values/names should be merged
        assertNotNull(dslRequest.expressionAttributeValues())
        assertNotNull(dslRequest.expressionAttributeNames())
    }

    // ========== Edge Cases and Corner Cases ==========

    @Test
    fun emptyStringValue() {
        val result = buildCondition { "field" eq "" }
        assertEquals("", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun zeroValue() {
        val result = buildCondition { "count" eq 0 }
        assertEquals("0", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun negativeNumber() {
        val result = buildCondition { "balance" lt -100 }
        assertEquals("-100", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun floatingPointNumber() {
        val result = buildCondition { "price" eq 19.99 }
        assertEquals("19.99", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun veryLongNumber() {
        val result = buildCondition { "bigNumber" eq Long.MAX_VALUE }
        assertEquals(Long.MAX_VALUE.toString(), result.attributeValues[":v1"]?.n())
    }

    @Test
    fun singleElementInList() {
        val result = buildCondition { "status" isIn listOf("ONLY") }
        assertEquals("#n1 IN (:v1)", result.expression)
    }

    @Test
    fun multipleConditionsOnSameAttribute() {
        val result = buildCondition { ("age" ge 18) and ("age" le 65) }
        // Should reuse the same attribute name placeholder
        assertEquals("(#n1 >= :v1) AND (#n1 <= :v2)", result.expression)
        assertEquals(1, result.attributeNames.size)
    }

    @Test
    fun reservedWordAttribute() {
        // DynamoDB has reserved words like "status", "name", etc.
        // The DSL should always use placeholders to avoid conflicts
        val result = buildCondition { "name" eq "test" }
        assertTrue(result.expression.contains("#n1"))
        assertEquals("name", result.attributeNames["#n1"])
    }

    @Test
    fun attributeWithSpecialCharacters() {
        val result = buildCondition { path("my-attr_1") eq "value" }
        assertEquals("#n1 = :v1", result.expression)
        assertEquals("my-attr_1", result.attributeNames["#n1"])
    }

    @Test
    fun mixStringConditionWithTypeSafe() {
        // User might want to use string condition alongside type-safe one
        val dslRequest = DeleteItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            condition { "status" eq "ACTIVE" }
            // Also adding manual attribute values should merge
            attributeValues { ":extra" from "value" }
        }.build()

        // Both should be present
        assertNotNull(dslRequest.expressionAttributeValues()[":v1"])
        assertNotNull(dslRequest.expressionAttributeValues()[":extra"])
    }

    @Test
    fun mixStringConditionWithManualNames() {
        val dslRequest = DeleteItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            condition { "status" eq "ACTIVE" }
            attributeNames("#manual" to "manualAttr")
        }.build()

        // Both should be present
        assertNotNull(dslRequest.expressionAttributeNames()["#n1"])
        assertNotNull(dslRequest.expressionAttributeNames()["#manual"])
    }

    // ========== Empty isIn Validation ==========

    @Test
    fun isInWithEmptyListThrows() {
        assertThrows<IllegalArgumentException> {
            buildCondition { "status" isIn emptyList() }
        }
    }

    @Test
    fun isInWithEmptyListOnPathThrows() {
        assertThrows<IllegalArgumentException> {
            buildCondition { path("data", "status") isIn emptyList() }
        }
    }

    // ========== AttributePath Overloads ==========

    @Test
    fun attributePathEquals() {
        val result = buildCondition { AttributePath.of("user", "name") eq "John" }
        assertEquals("#n1.#n2 = :v1", result.expression)
        assertEquals("user", result.attributeNames["#n1"])
        assertEquals("name", result.attributeNames["#n2"])
    }

    @Test
    fun attributePathNotEquals() {
        val result = buildCondition { AttributePath.of("data", "status") ne "DELETED" }
        assertEquals("#n1.#n2 <> :v1", result.expression)
    }

    @Test
    fun attributePathLessThan() {
        val result = buildCondition { AttributePath.of("metrics", "count") lt 100 }
        assertEquals("#n1.#n2 < :v1", result.expression)
    }

    @Test
    fun attributePathLessThanOrEquals() {
        val result = buildCondition { AttributePath.of("score", "value") le 50 }
        assertEquals("#n1.#n2 <= :v1", result.expression)
    }

    @Test
    fun attributePathGreaterThan() {
        val result = buildCondition { AttributePath.of("price", "amount") gt 9.99 }
        assertEquals("#n1.#n2 > :v1", result.expression)
    }

    @Test
    fun attributePathGreaterThanOrEquals() {
        val result = buildCondition { AttributePath.of("items", 0, "qty") ge 1 }
        assertEquals("#n1[0].#n2 >= :v1", result.expression)
    }

    @Test
    fun attributePathBetweenRange() {
        val result = buildCondition { AttributePath.of("data", "value") between 1..100 }
        assertEquals("#n1.#n2 BETWEEN :v1 AND :v2", result.expression)
    }

    @Test
    fun attributePathBetweenExplicit() {
        val result = buildCondition { AttributePath.of("data", "date").between("2024-01-01", "2024-12-31") }
        assertEquals("#n1.#n2 BETWEEN :v1 AND :v2", result.expression)
        assertEquals("2024-01-01", result.attributeValues[":v1"]?.s())
        assertEquals("2024-12-31", result.attributeValues[":v2"]?.s())
    }

    @Test
    fun attributePathIsIn() {
        val result = buildCondition { AttributePath.of("user", "role") isIn listOf("ADMIN", "MODERATOR") }
        assertEquals("#n1.#n2 IN (:v1, :v2)", result.expression)
    }

    @Test
    fun attributeExistsWithPath() {
        val result = buildCondition { attributeExists(AttributePath.of("data", "field")) }
        assertEquals("attribute_exists(#n1.#n2)", result.expression)
    }

    @Test
    fun attributeNotExistsWithPath() {
        val result = buildCondition { attributeNotExists(AttributePath.of("temp", "cache")) }
        assertEquals("attribute_not_exists(#n1.#n2)", result.expression)
    }

    @Test
    fun attributeTypeWithPath() {
        val result = buildCondition { attributeType(AttributePath.of("data", "payload"), DynamoAttributeType.BINARY) }
        assertEquals("attribute_type(#n1.#n2, :v1)", result.expression)
        assertEquals("B", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun beginsWithPath() {
        val result = buildCondition { beginsWith(AttributePath.of("user", "email"), "admin@") }
        assertEquals("begins_with(#n1.#n2, :v1)", result.expression)
    }

    @Test
    fun containsWithPath() {
        val result = buildCondition { contains(AttributePath.of("item", "description"), "sale") }
        assertEquals("contains(#n1.#n2, :v1)", result.expression)
    }

    @Test
    fun sizeWithPath() {
        val result = buildCondition { size(AttributePath.of("data", "items")) eq 10 }
        assertEquals("size(#n1.#n2) = :v1", result.expression)
    }

    // ========== SizeExpression Additional Operators ==========

    @Test
    fun sizeLessThanOrEquals() {
        val result = buildCondition { size("items") le 50 }
        assertEquals("size(#n1) <= :v1", result.expression)
        assertEquals("50", result.attributeValues[":v1"]?.n())
    }

    @Test
    fun sizeGreaterThan() {
        val result = buildCondition { size("comments") gt 0 }
        assertEquals("size(#n1) > :v1", result.expression)
        assertEquals("0", result.attributeValues[":v1"]?.n())
    }

    // ========== DynamoAttributeType Coverage ==========

    @Test
    fun attributeTypeNumber() {
        val result = buildCondition { attributeType("data", DynamoAttributeType.NUMBER) }
        assertEquals("N", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeBinary() {
        val result = buildCondition { attributeType("data", DynamoAttributeType.BINARY) }
        assertEquals("B", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeStringSet() {
        val result = buildCondition { attributeType("tags", DynamoAttributeType.STRING_SET) }
        assertEquals("SS", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeNumberSet() {
        val result = buildCondition { attributeType("scores", DynamoAttributeType.NUMBER_SET) }
        assertEquals("NS", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeBinarySet() {
        val result = buildCondition { attributeType("files", DynamoAttributeType.BINARY_SET) }
        assertEquals("BS", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeList() {
        val result = buildCondition { attributeType("items", DynamoAttributeType.LIST) }
        assertEquals("L", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeNull() {
        val result = buildCondition { attributeType("optional", DynamoAttributeType.NULL) }
        assertEquals("NULL", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun attributeTypeBoolean() {
        val result = buildCondition { attributeType("flag", DynamoAttributeType.BOOLEAN) }
        assertEquals("BOOL", result.attributeValues[":v1"]?.s())
    }

    // ========== ExpressionRenderer Value Types ==========

    @Test
    fun byteArrayValue() {
        val bytes = byteArrayOf(1, 2, 3, 4)
        val result = buildCondition { "data" eq bytes }
        assertEquals(SdkBytes.fromByteArray(bytes), result.attributeValues[":v1"]?.b())
    }

    @Test
    fun byteBufferValue() {
        val buffer = ByteBuffer.wrap(byteArrayOf(5, 6, 7, 8))
        val result = buildCondition { "data" eq buffer }
        assertNotNull(result.attributeValues[":v1"]?.b())
    }

    @Test
    fun inputStreamValue() {
        val stream = ByteArrayInputStream(byteArrayOf(9, 10, 11))
        val result = buildCondition { "data" eq stream }
        assertNotNull(result.attributeValues[":v1"]?.b())
    }

    @Test
    fun numberSetValue() {
        val numbers = setOf(1, 2, 3)
        val result = buildCondition { "scores" eq numbers }
        val ns = result.attributeValues[":v1"]?.ns()
        assertNotNull(ns)
        assertEquals(3, ns?.size)
        assertTrue(ns!!.contains("1"))
        assertTrue(ns.contains("2"))
        assertTrue(ns.contains("3"))
    }

    @Test
    fun emptyStringSetValue() {
        val emptySet = emptySet<String>()
        val result = buildCondition { "tags" eq emptySet }
        val ss = result.attributeValues[":v1"]?.ss()
        assertNotNull(ss)
        assertTrue(ss!!.isEmpty())
    }

    @Test
    fun mapValue() {
        val map = mapOf("key1" to "value1", "key2" to 42)
        val result = buildCondition { "metadata" eq map }
        val m = result.attributeValues[":v1"]?.m()
        assertNotNull(m)
        assertEquals("value1", m!!["key1"]?.s())
        assertEquals("42", m["key2"]?.n())
    }

    @Test
    fun nestedListValue() {
        val nestedList = listOf("a", 1, true)
        val result = buildCondition { "items" eq nestedList }
        val l = result.attributeValues[":v1"]?.l()
        assertNotNull(l)
        assertEquals(3, l?.size)
        assertEquals("a", l!![0].s())
        assertEquals("1", l[1].n())
        assertEquals(true, l[2].bool())
    }

    @Test
    fun attributeValuePassthrough() {
        val av = AttributeValue.builder().s("pre-built").build()
        val result = buildCondition { "field" eq av }
        assertEquals("pre-built", result.attributeValues[":v1"]?.s())
    }

    @Test
    fun unsupportedValueTypeThrows() {
        class CustomClass
        assertThrows<IllegalArgumentException> {
            buildCondition { "field" eq CustomClass() }
        }
    }

    // ========== Builder Integration Tests ==========

    @Test
    fun putItemRequestBuilderWithCondition() {
        val request = PutItemRequestBuilder("myTable").apply {
            item { "id" from "123"; "name" from "test" }
            condition { attributeNotExists("id") }
        }.build()

        assertEquals("attribute_not_exists(#n1)", request.conditionExpression())
        assertEquals("id", request.expressionAttributeNames()["#n1"])
    }

    @Test
    fun updateItemRequestBuilderWithCondition() {
        val request = UpdateItemRequestBuilder("myTable").apply {
            key { "id" from "123" }
            update { set("count") { increment(1) } }
            condition { "status" eq "ACTIVE" }
        }.build()

        assertEquals("#n1 = :v1", request.conditionExpression())
        assertNotNull(request.updateExpression())
    }

    @Test
    fun scanRequestBuilderWithFilter() {
        val request = ScanRequestBuilder("myTable").apply {
            filter { ("status" eq "ACTIVE") and ("price" gt 100) }
        }.build()

        assertEquals("(#n1 = :v1) AND (#n2 > :v2)", request.filterExpression())
        assertEquals("ACTIVE", request.expressionAttributeValues()[":v1"]?.s())
        assertEquals("100", request.expressionAttributeValues()[":v2"]?.n())
    }

    @Test
    fun deleteBuilderWithCondition() {
        val delete = DeleteBuilder("myTable").apply {
            key { "id" from "123" }
            condition { "version" eq 5 }
        }.build()

        assertEquals("#n1 = :v1", delete.conditionExpression())
        assertEquals("5", delete.expressionAttributeValues()[":v1"]?.n())
    }

    @Test
    fun putBuilderWithCondition() {
        val put = PutBuilder("myTable").apply {
            item { "id" from "456"; "data" from "test" }
            condition { attributeNotExists("id") or ("version" lt 10) }
        }.build()

        assertEquals("(attribute_not_exists(#n1)) OR (#n2 < :v1)", put.conditionExpression())
    }

    @Test
    fun updateBuilderWithConditionAndUpdate() {
        val update = UpdateBuilder("myTable").apply {
            key { "id" from "789" }
            update {
                set("status", "PROCESSED")
                set("count") { increment(1) }
            }
            condition { "status" eq "PENDING" }
        }.build()

        assertNotNull(update.conditionExpression())
        assertNotNull(update.updateExpression())
        assertTrue(update.updateExpression().contains("SET"))
    }

    // ========== Between with Explicit Parameters ==========

    @Test
    fun stringBetweenExplicitValues() {
        val result = buildCondition { "date".between("2024-01-01", "2024-06-30") }
        assertEquals("#n1 BETWEEN :v1 AND :v2", result.expression)
    }
}
