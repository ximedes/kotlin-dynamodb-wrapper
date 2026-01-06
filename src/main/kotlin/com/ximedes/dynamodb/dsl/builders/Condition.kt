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
 * Sealed class hierarchy representing DynamoDB condition expressions.
 * These are rendered to condition expression strings by [ExpressionRenderer].
 */
sealed class Condition {

    // Comparison conditions
    data class Equals(val path: AttributePath, val value: Any) : Condition()
    data class NotEquals(val path: AttributePath, val value: Any) : Condition()
    data class LessThan(val path: AttributePath, val value: Any) : Condition()
    data class LessThanOrEquals(val path: AttributePath, val value: Any) : Condition()
    data class GreaterThan(val path: AttributePath, val value: Any) : Condition()
    data class GreaterThanOrEquals(val path: AttributePath, val value: Any) : Condition()

    // Range condition
    data class Between(val path: AttributePath, val low: Any, val high: Any) : Condition()

    // Set membership
    data class In(val path: AttributePath, val values: List<Any>) : Condition()

    // Existence checks
    data class AttributeExists(val path: AttributePath) : Condition()
    data class AttributeNotExists(val path: AttributePath) : Condition()

    // Type check
    data class AttributeType(val path: AttributePath, val type: DynamoAttributeType) : Condition()

    // String functions
    data class BeginsWith(val path: AttributePath, val prefix: String) : Condition()
    data class Contains(val path: AttributePath, val operand: Any) : Condition()

    // Size comparisons
    data class SizeEquals(val path: AttributePath, val value: Int) : Condition()
    data class SizeLessThan(val path: AttributePath, val value: Int) : Condition()
    data class SizeLessThanOrEquals(val path: AttributePath, val value: Int) : Condition()
    data class SizeGreaterThan(val path: AttributePath, val value: Int) : Condition()
    data class SizeGreaterThanOrEquals(val path: AttributePath, val value: Int) : Condition()

    // Logical operators
    data class And(val left: Condition, val right: Condition) : Condition()
    data class Or(val left: Condition, val right: Condition) : Condition()
    data class Not(val condition: Condition) : Condition()
}

/**
 * DynamoDB attribute types for attribute_type() function.
 */
enum class DynamoAttributeType(val code: String) {
    STRING("S"),
    NUMBER("N"),
    BINARY("B"),
    STRING_SET("SS"),
    NUMBER_SET("NS"),
    BINARY_SET("BS"),
    MAP("M"),
    LIST("L"),
    NULL("NULL"),
    BOOLEAN("BOOL")
}

/**
 * Represents a size() expression that can be compared with operators.
 */
class SizeExpression(val path: AttributePath) {
    infix fun eq(value: Int): Condition = Condition.SizeEquals(path, value)
    infix fun lt(value: Int): Condition = Condition.SizeLessThan(path, value)
    infix fun le(value: Int): Condition = Condition.SizeLessThanOrEquals(path, value)
    infix fun gt(value: Int): Condition = Condition.SizeGreaterThan(path, value)
    infix fun ge(value: Int): Condition = Condition.SizeGreaterThanOrEquals(path, value)
}
