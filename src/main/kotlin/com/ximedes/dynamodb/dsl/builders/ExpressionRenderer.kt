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

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.io.InputStream
import java.nio.ByteBuffer

/**
 * Renders conditions and update expressions to DynamoDB expression strings.
 * Automatically generates placeholder names and values.
 */
class ExpressionRenderer {

    private var valueCounter = 0
    private var nameCounter = 0

    private val _attributeValues = mutableMapOf<String, AttributeValue>()
    private val _attributeNames = mutableMapOf<String, String>()

    val attributeValues: Map<String, AttributeValue> get() = _attributeValues
    val attributeNames: Map<String, String> get() = _attributeNames

    /**
     * Renders a condition to a condition expression string.
     */
    fun renderCondition(condition: Condition): String {
        return when (condition) {
            is Condition.Equals -> "${renderPath(condition.path)} = ${renderValue(condition.value)}"
            is Condition.NotEquals -> "${renderPath(condition.path)} <> ${renderValue(condition.value)}"
            is Condition.LessThan -> "${renderPath(condition.path)} < ${renderValue(condition.value)}"
            is Condition.LessThanOrEquals -> "${renderPath(condition.path)} <= ${renderValue(condition.value)}"
            is Condition.GreaterThan -> "${renderPath(condition.path)} > ${renderValue(condition.value)}"
            is Condition.GreaterThanOrEquals -> "${renderPath(condition.path)} >= ${renderValue(condition.value)}"

            is Condition.Between -> {
                val low = renderValue(condition.low)
                val high = renderValue(condition.high)
                "${renderPath(condition.path)} BETWEEN $low AND $high"
            }

            is Condition.In -> {
                val values = condition.values.joinToString(", ") { renderValue(it) }
                "${renderPath(condition.path)} IN ($values)"
            }

            is Condition.AttributeExists -> "attribute_exists(${renderPath(condition.path)})"
            is Condition.AttributeNotExists -> "attribute_not_exists(${renderPath(condition.path)})"

            is Condition.AttributeType -> {
                val typeValue = renderValue(condition.type.code)
                "attribute_type(${renderPath(condition.path)}, $typeValue)"
            }

            is Condition.BeginsWith -> {
                val prefix = renderValue(condition.prefix)
                "begins_with(${renderPath(condition.path)}, $prefix)"
            }

            is Condition.Contains -> {
                val operand = renderValue(condition.operand)
                "contains(${renderPath(condition.path)}, $operand)"
            }

            is Condition.SizeEquals -> "size(${renderPath(condition.path)}) = ${renderValue(condition.value)}"
            is Condition.SizeLessThan -> "size(${renderPath(condition.path)}) < ${renderValue(condition.value)}"
            is Condition.SizeLessThanOrEquals -> "size(${renderPath(condition.path)}) <= ${renderValue(condition.value)}"
            is Condition.SizeGreaterThan -> "size(${renderPath(condition.path)}) > ${renderValue(condition.value)}"
            is Condition.SizeGreaterThanOrEquals -> "size(${renderPath(condition.path)}) >= ${renderValue(condition.value)}"

            is Condition.And -> "(${renderCondition(condition.left)}) AND (${renderCondition(condition.right)})"
            is Condition.Or -> "(${renderCondition(condition.left)}) OR (${renderCondition(condition.right)})"
            is Condition.Not -> "NOT (${renderCondition(condition.condition)})"
        }
    }

    /**
     * Renders update operations to an update expression string.
     */
    fun renderUpdate(operations: List<UpdateOperation>): String {
        val setOps = mutableListOf<String>()
        val removeOps = mutableListOf<String>()
        val addOps = mutableListOf<String>()
        val deleteOps = mutableListOf<String>()

        for (op in operations) {
            when (op) {
                is UpdateOperation.Set.Value -> {
                    setOps.add("${renderPath(op.path)} = ${renderValue(op.value)}")
                }
                is UpdateOperation.Set.Increment -> {
                    val path = renderPath(op.path)
                    setOps.add("$path = $path + ${renderValue(op.amount)}")
                }
                is UpdateOperation.Set.Decrement -> {
                    val path = renderPath(op.path)
                    setOps.add("$path = $path - ${renderValue(op.amount)}")
                }
                is UpdateOperation.Set.IfNotExists -> {
                    val path = renderPath(op.path)
                    setOps.add("$path = if_not_exists($path, ${renderValue(op.defaultValue)})")
                }
                is UpdateOperation.Set.ListAppend -> {
                    val path = renderPath(op.path)
                    val listValue = renderValue(op.values)
                    if (op.prepend) {
                        setOps.add("$path = list_append($listValue, $path)")
                    } else {
                        setOps.add("$path = list_append($path, $listValue)")
                    }
                }
                is UpdateOperation.Remove -> {
                    removeOps.addAll(op.paths.map { renderPath(it) })
                }
                is UpdateOperation.Add -> {
                    addOps.add("${renderPath(op.path)} ${renderValue(op.value)}")
                }
                is UpdateOperation.Delete -> {
                    deleteOps.add("${renderPath(op.path)} ${renderValue(op.values)}")
                }
            }
        }

        val parts = mutableListOf<String>()
        if (setOps.isNotEmpty()) parts.add("SET ${setOps.joinToString(", ")}")
        if (removeOps.isNotEmpty()) parts.add("REMOVE ${removeOps.joinToString(", ")}")
        if (addOps.isNotEmpty()) parts.add("ADD ${addOps.joinToString(", ")}")
        if (deleteOps.isNotEmpty()) parts.add("DELETE ${deleteOps.joinToString(", ")}")

        return parts.joinToString(" ")
    }

    private fun renderPath(path: AttributePath): String {
        // Register all attribute names in the path
        for (name in path.attributeNames()) {
            if (!_attributeNames.containsValue(name)) {
                val placeholder = "#n${++nameCounter}"
                _attributeNames[placeholder] = name
            }
        }

        // Create reverse lookup for rendering
        val nameMappings = _attributeNames.entries.associate { it.value to it.key }
        return path.render(nameMappings)
    }

    private fun renderValue(value: Any): String {
        val placeholder = ":v${++valueCounter}"
        _attributeValues[placeholder] = toAttributeValue(value)
        return placeholder
    }

    private fun toAttributeValue(value: Any): AttributeValue {
        return when (value) {
            is String -> AttributeValue.builder().s(value).build()
            is Number -> AttributeValue.builder().n(value.toString()).build()
            is Boolean -> AttributeValue.builder().bool(value).build()
            is ByteArray -> AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build()
            is ByteBuffer -> AttributeValue.builder().b(SdkBytes.fromByteBuffer(value)).build()
            is InputStream -> AttributeValue.builder().b(SdkBytes.fromInputStream(value)).build()
            is Set<*> -> {
                @Suppress("UNCHECKED_CAST")
                when (val first = value.firstOrNull()) {
                    is String -> AttributeValue.builder().ss(value as Set<String>).build()
                    is Number -> AttributeValue.builder().ns((value as Set<Number>).map { it.toString() }).build()
                    is ByteArray -> AttributeValue.builder().bs((value as Set<ByteArray>).map { SdkBytes.fromByteArray(it) }).build()
                    null -> AttributeValue.builder().ss(emptyList()).build()
                    else -> throw IllegalArgumentException("Unsupported set element type: ${first::class}")
                }
            }
            is List<*> -> {
                @Suppress("UNCHECKED_CAST")
                val items = (value as List<Any>).map { toAttributeValue(it) }
                AttributeValue.builder().l(items).build()
            }
            is Map<*, *> -> {
                @Suppress("UNCHECKED_CAST")
                val items = (value as Map<String, Any>).mapValues { toAttributeValue(it.value) }
                AttributeValue.builder().m(items).build()
            }
            is AttributeValue -> value
            else -> throw IllegalArgumentException("Unsupported value type: ${value::class}")
        }
    }
}
