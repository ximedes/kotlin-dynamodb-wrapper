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

import com.ximedes.dynamodb.dsl.DynamoDbDSL

/**
 * DSL builder for creating type-safe DynamoDB update expressions.
 *
 * Example usage:
 * ```kotlin
 * update {
 *     set("count") { increment(1) }
 *     set("name", "newValue")
 *     remove("tempField")
 * }
 * ```
 */
@DynamoDbDSL
class UpdateExpressionBuilder {

    private val operations = mutableListOf<UpdateOperation>()

    // ========== SET operations ==========

    /**
     * SET attribute = value
     */
    fun set(attr: String, value: Any) {
        operations.add(UpdateOperation.Set.Value(AttributePath.of(attr), value))
    }

    /**
     * SET path = value (for nested attributes)
     */
    fun set(path: AttributePath, value: Any) {
        operations.add(UpdateOperation.Set.Value(path, value))
    }

    /**
     * SET with expression builder for increment, decrement, if_not_exists, list_append
     */
    fun set(attr: String, block: SetExpressionBuilder.() -> SetExpressionBuilder.SetResult) {
        val result = SetExpressionBuilder(attr).block()
        operations.add(result.operation)
    }

    /**
     * SET with expression builder for nested paths
     */
    fun set(path: AttributePath, block: SetExpressionBuilder.() -> SetExpressionBuilder.SetResult) {
        val builder = SetExpressionBuilder(path)
        val result = builder.block()
        operations.add(result.operation)
    }

    // ========== REMOVE operations ==========

    /**
     * REMOVE one or more attributes
     */
    fun remove(vararg attrs: String) {
        operations.add(UpdateOperation.Remove(attrs.map { AttributePath.of(it) }))
    }

    /**
     * REMOVE one or more paths
     */
    fun remove(vararg paths: AttributePath) {
        operations.add(UpdateOperation.Remove(paths.toList()))
    }

    // ========== ADD operations ==========

    /**
     * ADD to a number or set
     */
    fun add(attr: String, value: Any) {
        operations.add(UpdateOperation.Add(AttributePath.of(attr), value))
    }

    /**
     * ADD to a number or set (nested path)
     */
    fun add(path: AttributePath, value: Any) {
        operations.add(UpdateOperation.Add(path, value))
    }

    // ========== DELETE operations ==========

    /**
     * DELETE from a set
     */
    fun delete(attr: String, values: Set<Any>) {
        operations.add(UpdateOperation.Delete(AttributePath.of(attr), values))
    }

    /**
     * DELETE from a set (nested path)
     */
    fun delete(path: AttributePath, values: Set<Any>) {
        operations.add(UpdateOperation.Delete(path, values))
    }

    fun build(): List<UpdateOperation> = operations.toList()
}

/**
 * Builder for SET expression operations like increment, if_not_exists, list_append
 */
@DynamoDbDSL
class SetExpressionBuilder(private val path: AttributePath) {

    constructor(attr: String) : this(AttributePath.of(attr))

    data class SetResult(val operation: UpdateOperation.Set)

    /**
     * SET attr = attr + amount
     */
    fun increment(amount: Number): SetResult =
        SetResult(UpdateOperation.Set.Increment(path, amount))

    /**
     * SET attr = attr - amount
     */
    fun decrement(amount: Number): SetResult =
        SetResult(UpdateOperation.Set.Decrement(path, amount))

    /**
     * SET attr = if_not_exists(attr, defaultValue)
     */
    fun ifNotExists(defaultValue: Any): SetResult =
        SetResult(UpdateOperation.Set.IfNotExists(path, defaultValue))

    /**
     * SET attr = list_append(attr, values)
     */
    fun listAppend(values: List<Any>): SetResult =
        SetResult(UpdateOperation.Set.ListAppend(path, values))

    /**
     * SET attr = list_append(values, attr) - prepend to list
     */
    fun listPrepend(values: List<Any>): SetResult =
        SetResult(UpdateOperation.Set.ListAppend(path, values, prepend = true))

    // Infix operators for arithmetic
    infix fun String.plus(amount: Number): SetResult =
        SetResult(UpdateOperation.Set.Increment(AttributePath.of(this), amount))

    infix fun String.minus(amount: Number): SetResult =
        SetResult(UpdateOperation.Set.Decrement(AttributePath.of(this), amount))
}

/**
 * Result of building an update expression, containing the expression string
 * and the attribute values/names to be passed to DynamoDB.
 */
data class UpdateExpressionResult(
    val expression: String,
    val attributeValues: Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>,
    val attributeNames: Map<String, String>
)

/**
 * Builds an update expression using the DSL and renders it to a result.
 */
fun buildUpdate(block: UpdateExpressionBuilder.() -> Unit): UpdateExpressionResult {
    val builder = UpdateExpressionBuilder()
    builder.block()
    val operations = builder.build()
    val renderer = ExpressionRenderer()
    val expression = renderer.renderUpdate(operations)
    return UpdateExpressionResult(
        expression = expression,
        attributeValues = renderer.attributeValues,
        attributeNames = renderer.attributeNames
    )
}
