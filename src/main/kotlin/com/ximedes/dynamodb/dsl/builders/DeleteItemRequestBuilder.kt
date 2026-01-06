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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ReturnItemCollectionMetrics
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

@DynamoDbDSL
class DeleteItemRequestBuilder(tableName: String) {
    private val _builder = DeleteItemRequest.builder().tableName(tableName)

    fun build(): DeleteItemRequest = _builder.build()


    fun key(init: ItemBuilder.() -> Unit) {
        _builder.key(ItemBuilder().apply(init).build())
    }

    fun condition(expression: String) {
        _builder.conditionExpression(expression)
    }

    /**
     * Type-safe condition expression builder.
     * Automatically generates attribute values and names.
     */
    fun condition(init: ConditionBuilder.() -> Condition) {
        val result = buildCondition(init)
        _builder.conditionExpression(result.expression)
        mergeAttributeValues(result.attributeValues)
        mergeAttributeNames(result.attributeNames)
    }

    private var _attributeValues: MutableMap<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>? = null
    private var _attributeNames: MutableMap<String, String>? = null

    private fun mergeAttributeValues(values: Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>) {
        if (values.isEmpty()) return
        if (_attributeValues == null) _attributeValues = mutableMapOf()
        _attributeValues!!.putAll(values)
        _builder.expressionAttributeValues(_attributeValues)
    }

    private fun mergeAttributeNames(names: Map<String, String>) {
        if (names.isEmpty()) return
        if (_attributeNames == null) _attributeNames = mutableMapOf()
        _attributeNames!!.putAll(names)
        _builder.expressionAttributeNames(_attributeNames)
    }

    fun attributeValues(init: ItemBuilder.() -> Unit) {
        mergeAttributeValues(ItemBuilder().apply(init).build())
    }

    fun attributeNames(vararg names: Pair<String, String>) {
        mergeAttributeNames(mapOf(*names))
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun returnItemCollectionMetrics(returnItemCollectionMetrics: ReturnItemCollectionMetrics) {
        _builder.returnItemCollectionMetrics(returnItemCollectionMetrics)
    }

    fun returnValues(returnValue: ReturnValue) {
        _builder.returnValues(returnValue)
    }

}