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
import com.ximedes.dynamodb.dsl.Item
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ReturnItemCollectionMetrics
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

@DynamoDbDSL
class TransactWriteItemsRequestBuilder() {
    private val _builder: TransactWriteItemsRequest.Builder =
            TransactWriteItemsRequest.builder()
    private val writeItems = mutableListOf<TransactWriteItem>()

    fun build(): TransactWriteItemsRequest = _builder.transactItems(writeItems).build()

    fun update(tableName: String, block: UpdateBuilder.() -> Unit) {
        val update = UpdateBuilder(tableName).apply(block).build()
        writeItems.add(TransactWriteItem.builder().update(update).build())
    }

    fun put(tableName: String, block: PutBuilder.() -> Unit) {
        val put = PutBuilder(tableName).apply(block).build()
        writeItems.add(TransactWriteItem.builder().put(put).build())
    }

    fun put(tableName: String, item: Item, block: PutBuilder.() -> Unit = {}) {
        val put = PutBuilder(tableName, item).apply(block).build()
        writeItems.add(TransactWriteItem.builder().put(put).build())
    }

    fun delete(tableName: String, block: DeleteBuilder.() -> Unit) {
        val delete = DeleteBuilder(tableName).apply(block).build()
        writeItems.add(TransactWriteItem.builder().delete(delete).build())
    }

    fun clientRequestToken(clientRequestToken: String) {
        _builder.clientRequestToken(clientRequestToken)
    }

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun returnItemCollectionMetrics(returnItemCollectionMetrics: ReturnItemCollectionMetrics) {
        _builder.returnItemCollectionMetrics(returnItemCollectionMetrics)
    }

}