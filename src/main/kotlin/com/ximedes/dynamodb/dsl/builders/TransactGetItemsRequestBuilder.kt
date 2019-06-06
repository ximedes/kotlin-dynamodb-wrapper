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
import software.amazon.awssdk.services.dynamodb.model.TransactGetItem
import software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest

@DynamoDbDSL
class TransactGetItemsRequestBuilder {
    private val _builder = TransactGetItemsRequest.builder()
    private val getItems = mutableListOf<TransactGetItem>()

    fun build(): TransactGetItemsRequest {
        return _builder.transactItems(getItems).build()
    }

    fun get(tableName: String, init: GetBuilder.() -> Unit) {
        val get = GetBuilder(tableName).apply(init).build()
        getItems.add(TransactGetItem.builder().get(get).build())
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

}