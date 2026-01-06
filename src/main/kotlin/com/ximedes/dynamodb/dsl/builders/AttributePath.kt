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

/**
 * Represents a path to a DynamoDB attribute, supporting nested attributes and array indices.
 *
 * Examples:
 * - Simple: `AttributePath("name")` -> `name`
 * - Nested: `AttributePath("address", "city")` -> `address.city`
 * - Array: `AttributePath("items", 0, "quantity")` -> `items[0].quantity`
 */
class AttributePath private constructor(private val parts: List<PathPart>) {

    sealed class PathPart {
        data class Attribute(val name: String) : PathPart()
        data class Index(val index: Int) : PathPart()
    }

    /**
     * Appends a nested attribute to this path.
     */
    operator fun get(name: String): AttributePath {
        return AttributePath(parts + PathPart.Attribute(name))
    }

    /**
     * Appends an array index to this path.
     */
    operator fun get(index: Int): AttributePath {
        return AttributePath(parts + PathPart.Index(index))
    }

    /**
     * Returns all attribute names in this path (excluding indices).
     */
    fun attributeNames(): List<String> {
        return parts.filterIsInstance<PathPart.Attribute>().map { it.name }
    }

    /**
     * Renders the path using the provided name mappings.
     * Each attribute name is replaced with its placeholder (e.g., #n1).
     */
    fun render(nameMappings: Map<String, String>): String {
        return buildString {
            var first = true
            for (part in parts) {
                when (part) {
                    is PathPart.Attribute -> {
                        if (!first) append(".")
                        append(nameMappings[part.name] ?: part.name)
                        first = false
                    }
                    is PathPart.Index -> {
                        append("[${part.index}]")
                    }
                }
            }
        }
    }

    override fun toString(): String = render(emptyMap())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AttributePath) return false
        return parts == other.parts
    }

    override fun hashCode(): Int = parts.hashCode()

    companion object {
        /**
         * Creates an AttributePath from a simple attribute name.
         */
        fun of(name: String): AttributePath {
            // Handle dot notation in string
            val segments = name.split(".")
            val pathParts = segments.map { PathPart.Attribute(it) }
            return AttributePath(pathParts)
        }

        /**
         * Creates an AttributePath from vararg parts.
         * Strings are treated as attribute names, Ints as array indices.
         */
        fun of(vararg parts: Any): AttributePath {
            val pathParts = parts.map { part ->
                when (part) {
                    is String -> {
                        // Handle dot notation in string parts
                        part.split(".").map { PathPart.Attribute(it) }
                    }
                    is Int -> listOf(PathPart.Index(part))
                    else -> throw IllegalArgumentException("Path parts must be String or Int, got: ${part::class}")
                }
            }.flatten()
            return AttributePath(pathParts)
        }
    }
}
