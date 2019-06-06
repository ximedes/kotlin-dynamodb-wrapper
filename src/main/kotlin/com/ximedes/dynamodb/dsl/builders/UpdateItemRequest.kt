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
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ReturnItemCollectionMetrics
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

@DynamoDbDSL
class UpdateItemRequestBuilder(tableName: String) {
    private val _builder = UpdateItemRequest.builder().tableName(tableName)

    fun build(): UpdateItemRequest = _builder.build()

    fun update(expression: String) {
        _builder.updateExpression(expression)
    }

    fun condition(expression: String) {
        _builder.conditionExpression(expression)
    }

    fun key(init: ItemBuilder.() -> Unit) {
        _builder.key(ItemBuilder().apply(init).build())
    }

    fun attributeValues(init: ItemBuilder.() -> Unit) {
        _builder.expressionAttributeValues(ItemBuilder().apply(init).build())
    }

    fun attributeNames(vararg names: Pair<String, String>) {
        _builder.expressionAttributeNames(mapOf(*names))
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun returnItemCollectionMetrics(returnItemCollectionMetrics: ReturnItemCollectionMetrics) {
        _builder.returnItemCollectionMetrics(returnItemCollectionMetrics)
    }

}