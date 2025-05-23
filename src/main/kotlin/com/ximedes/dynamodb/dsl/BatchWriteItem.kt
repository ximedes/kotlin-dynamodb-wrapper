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

package com.ximedes.dynamodb.dsl

import com.ximedes.dynamodb.dsl.builders.BatchWriteItemRequestBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

fun DynamoDbClient.batchWriteItem(init: BatchWriteItemRequestBuilder.() -> Unit): BatchWriteItemResponse {
    return BatchWriteItemRequestBuilder().apply(init).build().logAndRun(::batchWriteItem)
}

suspend fun DynamoDbAsyncClient.batchWriteItem(init: BatchWriteItemRequestBuilder.() -> Unit): BatchWriteItemResponse {
    return BatchWriteItemRequestBuilder().apply(init).build().logAndRunAsync(::batchWriteItem)
}