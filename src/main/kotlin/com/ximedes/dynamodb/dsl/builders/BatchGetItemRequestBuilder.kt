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
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity

@DynamoDbDSL
class BatchGetItemRequestBuilder {
    private val _builder = BatchGetItemRequest.builder()
    private val requestItems = mutableMapOf<String, KeysAndAttributes>()

    fun build(): BatchGetItemRequest = _builder.requestItems(requestItems).build()

    fun returnConsumedCapacity(returnConsumedCapacity: ReturnConsumedCapacity) {
        _builder.returnConsumedCapacity(returnConsumedCapacity)
    }

    fun items(tableName: String, init: KeysAndAttributesBuilder.() -> Unit) {
        requestItems[tableName] = KeysAndAttributesBuilder().apply(init).build()
    }


}