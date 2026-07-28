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
 * DSL builder for creating type-safe DynamoDB condition expressions.
 *
 * Example usage:
 * ```kotlin
 * condition {
 *     ("status" eq "ACTIVE") and ("count" gt 10)
 * }
 * ```
 */
@DynamoDbDSL
class ConditionBuilder {

    // ========== Comparison operators for String (attribute name) ==========

    infix fun String.eq(value: Any): Condition = Condition.Equals(AttributePath.of(this), value)
    infix fun String.ne(value: Any): Condition = Condition.NotEquals(AttributePath.of(this), value)
    infix fun String.lt(value: Any): Condition = Condition.LessThan(AttributePath.of(this), value)
    infix fun String.le(value: Any): Condition = Condition.LessThanOrEquals(AttributePath.of(this), value)
    infix fun String.gt(value: Any): Condition = Condition.GreaterThan(AttributePath.of(this), value)
    infix fun String.ge(value: Any): Condition = Condition.GreaterThanOrEquals(AttributePath.of(this), value)

    // ========== Comparison operators for AttributePath ==========

    infix fun AttributePath.eq(value: Any): Condition = Condition.Equals(this, value)
    infix fun AttributePath.ne(value: Any): Condition = Condition.NotEquals(this, value)
    infix fun AttributePath.lt(value: Any): Condition = Condition.LessThan(this, value)
    infix fun AttributePath.le(value: Any): Condition = Condition.LessThanOrEquals(this, value)
    infix fun AttributePath.gt(value: Any): Condition = Condition.GreaterThan(this, value)
    infix fun AttributePath.ge(value: Any): Condition = Condition.GreaterThanOrEquals(this, value)

    // ========== Range operators ==========

    infix fun String.between(range: ClosedRange<*>): Condition =
        Condition.Between(AttributePath.of(this), range.start as Any, range.endInclusive as Any)

    infix fun AttributePath.between(range: ClosedRange<*>): Condition =
        Condition.Between(this, range.start as Any, range.endInclusive as Any)

    fun String.between(low: Any, high: Any): Condition =
        Condition.Between(AttributePath.of(this), low, high)

    fun AttributePath.between(low: Any, high: Any): Condition =
        Condition.Between(this, low, high)

    // ========== Set membership ==========

    infix fun String.isIn(values: Collection<Any>): Condition {
        require(values.isNotEmpty()) { "isIn requires at least one value" }
        return Condition.In(AttributePath.of(this), values.toList())
    }

    infix fun AttributePath.isIn(values: Collection<Any>): Condition {
        require(values.isNotEmpty()) { "isIn requires at least one value" }
        return Condition.In(this, values.toList())
    }

    // ========== Existence checks ==========

    fun attributeExists(attr: String): Condition = Condition.AttributeExists(AttributePath.of(attr))
    fun attributeExists(path: AttributePath): Condition = Condition.AttributeExists(path)

    fun attributeNotExists(attr: String): Condition = Condition.AttributeNotExists(AttributePath.of(attr))
    fun attributeNotExists(path: AttributePath): Condition = Condition.AttributeNotExists(path)

    // ========== Type check ==========

    fun attributeType(attr: String, type: DynamoAttributeType): Condition =
        Condition.AttributeType(AttributePath.of(attr), type)

    fun attributeType(path: AttributePath, type: DynamoAttributeType): Condition =
        Condition.AttributeType(path, type)

    // ========== String functions ==========

    fun beginsWith(attr: String, prefix: String): Condition =
        Condition.BeginsWith(AttributePath.of(attr), prefix)

    fun beginsWith(path: AttributePath, prefix: String): Condition =
        Condition.BeginsWith(path, prefix)

    fun contains(attr: String, value: Any): Condition =
        Condition.Contains(AttributePath.of(attr), value)

    fun contains(path: AttributePath, value: Any): Condition =
        Condition.Contains(path, value)

    // ========== Size function ==========

    fun size(attr: String): SizeExpression = SizeExpression(AttributePath.of(attr))
    fun size(path: AttributePath): SizeExpression = SizeExpression(path)

    // ========== Logical operators ==========

    infix fun Condition.and(other: Condition): Condition = Condition.And(this, other)
    infix fun Condition.or(other: Condition): Condition = Condition.Or(this, other)
    fun not(condition: Condition): Condition = Condition.Not(condition)

    // ========== Path builder ==========

    fun path(attr: String): AttributePath = AttributePath.of(attr)
    fun path(vararg parts: Any): AttributePath = AttributePath.of(*parts)
}

/**
 * Result of building a condition expression, containing the expression string
 * and the attribute values/names to be passed to DynamoDB.
 */
data class ConditionExpressionResult(
    val expression: String,
    val attributeValues: Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>,
    val attributeNames: Map<String, String>
)

/**
 * Builds a condition expression using the DSL and renders it to a result.
 *
 * The returned placeholders start at `#n1` / `:v1`. Do not merge the results of two separate calls
 * into one request: the second set overwrites the first. To put more than one expression on a
 * request — a key condition and a filter, or an update and a condition — use the request builder's
 * `condition` / `filter` / `update` functions, which render every expression with a shared renderer.
 */
fun buildCondition(block: ConditionBuilder.() -> Condition): ConditionExpressionResult {
    val condition = ConditionBuilder().block()
    val renderer = ExpressionRenderer()
    val expression = renderer.renderCondition(condition)
    return ConditionExpressionResult(
        expression = expression,
        attributeValues = renderer.attributeValues,
        attributeNames = renderer.attributeNames
    )
}
