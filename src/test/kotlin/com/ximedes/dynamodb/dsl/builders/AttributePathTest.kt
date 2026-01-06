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

internal class AttributePathTest {

    // ========== Simple Paths ==========

    @Test
    fun simpleAttribute() {
        val path = AttributePath.of("name")
        assertEquals(listOf("name"), path.attributeNames())
        assertEquals("name", path.render(emptyMap<String, String>()))
    }

    @Test
    fun simpleAttributeWithMapping() {
        val path = AttributePath.of("name")
        assertEquals("#n1", path.render(mapOf("name" to "#n1")))
    }

    // ========== Dot Notation ==========

    @Test
    fun dotNotationInString() {
        val path = AttributePath.of("address.city")
        assertEquals(listOf("address", "city"), path.attributeNames())
        assertEquals("address.city", path.render(emptyMap<String, String>()))
    }

    @Test
    fun dotNotationWithMapping() {
        val path = AttributePath.of("address.city")
        val mapping = mapOf("address" to "#n1", "city" to "#n2")
        assertEquals("#n1.#n2", path.render(mapping))
    }

    @Test
    fun deeplyNestedDotNotation() {
        val path = AttributePath.of("a.b.c.d.e")
        assertEquals(listOf("a", "b", "c", "d", "e"), path.attributeNames())
    }

    // ========== Vararg Paths ==========

    @Test
    fun varargStringPath() {
        val path = AttributePath.of("user", "profile", "name")
        assertEquals(listOf("user", "profile", "name"), path.attributeNames())
        assertEquals("user.profile.name", path.render(emptyMap<String, String>()))
    }

    @Test
    fun varargWithArrayIndex() {
        val path = AttributePath.of("items", 0, "quantity")
        assertEquals(listOf("items", "quantity"), path.attributeNames())
        assertEquals("items[0].quantity", path.render(emptyMap<String, String>()))
    }

    @Test
    fun varargWithMultipleIndices() {
        val path = AttributePath.of("matrix", 0, 1)
        assertEquals(listOf("matrix"), path.attributeNames())
        assertEquals("matrix[0][1]", path.render(emptyMap<String, String>()))
    }

    @Test
    fun varargMixedPath() {
        val path = AttributePath.of("data", "items", 0, "values", 2, "name")
        assertEquals(listOf("data", "items", "values", "name"), path.attributeNames())
        assertEquals("data.items[0].values[2].name", path.render(emptyMap<String, String>()))
    }

    // ========== Array Index Operations ==========

    @Test
    fun indexOperator() {
        val path = AttributePath.of("items")[0]
        assertEquals("items[0]", path.render(emptyMap<String, String>()))
    }

    @Test
    fun chainedIndexOperators() {
        val path = AttributePath.of("matrix")[0][1][2]
        assertEquals("matrix[0][1][2]", path.render(emptyMap<String, String>()))
    }

    @Test
    fun indexThenAttribute() {
        val path = AttributePath.of("items")[0]["name"]
        assertEquals("items[0].name", path.render(emptyMap<String, String>()))
    }

    @Test
    fun attributeThenIndex() {
        val path = AttributePath.of("container")["items"][0]
        assertEquals("container.items[0]", path.render(emptyMap<String, String>()))
    }

    // ========== Chained Path Operations ==========

    @Test
    fun chainedPathWithName() {
        // Use AttributePath.of() then chain operators
        val path = AttributePath.of("user")["profile"]["name"]
        assertEquals("user.profile.name", path.render(emptyMap<String, String>()))
    }

    @Test
    fun chainedPathWithIndex() {
        val path = AttributePath.of("items")[0]["quantity"]
        assertEquals("items[0].quantity", path.render(emptyMap<String, String>()))
    }

    @Test
    fun complexChainedPath() {
        val path = AttributePath.of("data")["items"][0]["product"]["variants"][1]["sku"]
        assertEquals("data.items[0].product.variants[1].sku", path.render(emptyMap<String, String>()))
    }

    // ========== Edge Cases ==========

    @Test
    fun singleCharacterAttribute() {
        val path = AttributePath.of("a")
        assertEquals(listOf("a"), path.attributeNames())
    }

    @Test
    fun indexZero() {
        val path = AttributePath.of("arr", 0)
        assertEquals("arr[0]", path.render(emptyMap<String, String>()))
    }

    @Test
    fun largeIndex() {
        val path = AttributePath.of("arr", 999999)
        assertEquals("arr[999999]", path.render(emptyMap<String, String>()))
    }

    @Test
    fun attributeWithNumbers() {
        val path = AttributePath.of("field123")
        assertEquals(listOf("field123"), path.attributeNames())
    }

    @Test
    fun attributeWithHyphens() {
        val path = AttributePath.of("my-field")
        assertEquals(listOf("my-field"), path.attributeNames())
    }

    @Test
    fun attributeWithUnderscores() {
        val path = AttributePath.of("my_field_name")
        assertEquals(listOf("my_field_name"), path.attributeNames())
    }

    @Test
    fun consecutiveIndices() {
        val path = AttributePath.of("a")[0][1][2][3][4]
        assertEquals("a[0][1][2][3][4]", path.render(emptyMap<String, String>()))
        assertEquals(listOf("a"), path.attributeNames())
    }

    // ========== Equality and HashCode ==========

    @Test
    fun equalPaths() {
        val path1 = AttributePath.of("a", "b", 0)
        val path2 = AttributePath.of("a", "b", 0)
        assertEquals(path1, path2)
        assertEquals(path1.hashCode(), path2.hashCode())
    }

    @Test
    fun unequalPaths() {
        val path1 = AttributePath.of("a", "b")
        val path2 = AttributePath.of("a", "c")
        assertNotEquals(path1, path2)
    }

    @Test
    fun pathsWithDifferentIndices() {
        val path1 = AttributePath.of("a", 0)
        val path2 = AttributePath.of("a", 1)
        assertNotEquals(path1, path2)
    }

    // ========== Invalid Input ==========

    @Test
    fun invalidPartType() {
        assertThrows<IllegalArgumentException> {
            AttributePath.of("name", 1.5) // Double is not valid
        }
    }

    @Test
    fun invalidPartTypeList() {
        assertThrows<IllegalArgumentException> {
            AttributePath.of("name", listOf("invalid"))
        }
    }

    // ========== Rendering with Partial Mappings ==========

    @Test
    fun partialMappingRendering() {
        val path = AttributePath.of("a", "b", "c")
        val mapping = mapOf("a" to "#n1", "c" to "#n3")
        // "b" has no mapping, should render as-is
        assertEquals("#n1.b.#n3", path.render(mapping))
    }

    @Test
    fun noMappingsRendering() {
        val path = AttributePath.of("deep", "nested", "path")
        assertEquals("deep.nested.path", path.render(emptyMap<String, String>()))
    }
}
