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
import software.amazon.awssdk.services.dynamodb.model.*

@DynamoDbDSL
class BatchWriteItemRequestBuilder {
    private val _builder = BatchWriteItemRequest.builder()
    private val writeRequests = mutableMapOf<String, MutableList<WriteRequest>>()

    fun build(): BatchWriteItemRequest = _builder.requestItems(writeRequests).build()

    fun delete(tableName: String, vararg inits: ItemBuilder.() -> Unit) {
        writeRequests.putIfAbsent(tableName, mutableListOf())
        writeRequests[tableName]?.addAll(inits.map {
            WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(ItemBuilder().apply(it).build()).build())
                    .build()
        })
    }

    fun put(tableName: String, vararg inits: ItemBuilder.() -> Unit) {
        writeRequests.putIfAbsent(tableName, mutableListOf())
        writeRequests[tableName]?.addAll(inits.map {
            WriteRequest.builder().putRequest(PutRequest.builder().item(ItemBuilder().apply(it).build()).build())
                    .build()
        })
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun returnItemCollectionMetrics(returnItemCollectionMetrics: ReturnItemCollectionMetrics) {
        _builder.returnItemCollectionMetrics(returnItemCollectionMetrics)
    }
}