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

import com.ximedes.dynamodb.dsl.DynamoDbDSL
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.Select

@DynamoDbDSL
class QueryRequestBuilder(tableName: String) {
    private val _builder = QueryRequest.builder().tableName(tableName)

    private val expressions = ExpressionAccumulator(
        { _builder.expressionAttributeValues(it) },
        { _builder.expressionAttributeNames(it) }
    )

    fun build(): QueryRequest = _builder.build()

    fun indexName(indexName: String) {
        _builder.indexName(indexName)
    }

    fun consistentRead(consistentRead: Boolean) {
        _builder.consistentRead(consistentRead)
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun keyCondition(expression: String) {
        _builder.keyConditionExpression(expression)
    }

    /**
     * Type-safe key condition expression builder.
     * Automatically generates attribute values and names.
     */
    fun keyCondition(init: ConditionBuilder.() -> Condition) {
        _builder.keyConditionExpression(expressions.condition(init))
    }

    fun filter(expression: String) {
        _builder.filterExpression(expression)
    }

    /**
     * Type-safe filter expression builder.
     * Automatically generates attribute values and names.
     */
    fun filter(init: ConditionBuilder.() -> Condition) {
        _builder.filterExpression(expressions.condition(init))
    }

    fun exclusiveStartKey(init: ItemBuilder.() -> Unit) {
        _builder.exclusiveStartKey(ItemBuilder().apply(init).build())
    }

    fun attributeValues(init: ItemBuilder.() -> Unit) {
        expressions.attributeValues(ItemBuilder().apply(init).build())
    }

    fun attributeNames(vararg names: Pair<String, String>) {
        expressions.attributeNames(mapOf(*names))
    }

    fun projection(expression: String) {
        _builder.projectionExpression(expression)
    }

    fun limit(limit: Int) {
        _builder.limit(limit)
    }

    fun scanIndexForward(b: Boolean) {
        _builder.scanIndexForward(b)
    }

    fun select(select: Select) {
        _builder.select(select)
    }
}