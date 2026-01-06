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
 * Sealed class hierarchy representing DynamoDB update expression operations.
 * These are rendered to update expression strings by [ExpressionRenderer].
 */
sealed class UpdateOperation {

    // SET operations
    sealed class Set : UpdateOperation() {
        data class Value(val path: AttributePath, val value: Any) : Set()
        data class Increment(val path: AttributePath, val amount: Number) : Set()
        data class Decrement(val path: AttributePath, val amount: Number) : Set()
        data class IfNotExists(val path: AttributePath, val defaultValue: Any) : Set()
        data class ListAppend(val path: AttributePath, val values: List<Any>, val prepend: Boolean = false) : Set()
    }

    // REMOVE operation
    data class Remove(val paths: List<AttributePath>) : UpdateOperation()

    // ADD operation (for numbers and sets)
    data class Add(val path: AttributePath, val value: Any) : UpdateOperation()

    // DELETE operation (for sets)
    data class Delete(val path: AttributePath, val values: kotlin.collections.Set<Any>) : UpdateOperation()
}

/**
 * Container for a complete update expression consisting of multiple operations.
 */
data class UpdateExpressionSet(val operations: List<UpdateOperation>)
