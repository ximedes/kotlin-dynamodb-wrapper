/*
 * Copyright 2021 the original author or authors.
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
class UpdateTableRequestBuilder(tableName: String) {
    private var _builder = UpdateTableRequest.builder().tableName(tableName)
    private val globalSecondaryIndexUpdates = mutableListOf<GlobalSecondaryIndexUpdate>()
    private var provisionedThroughput: ProvisionedThroughput? = null

    fun attributes(init: AttributeDefinitionsBuilder.() -> Unit) {
        _builder.attributeDefinitions(AttributeDefinitionsBuilder().apply(init).build())
    }

    fun billingMode(mode: BillingMode) {
        _builder.billingMode(mode)
    }

    fun provisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long) {
        provisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build()
    }

    fun createGlobalSecondaryIndex(name: String, create: GlobalSecondaryIndexBuilder.() -> Unit) {
        val index = GlobalSecondaryIndexBuilder(name).apply(create).build()

        globalSecondaryIndexUpdates.add(GlobalSecondaryIndexUpdate.builder().create(
            CreateGlobalSecondaryIndexAction.builder()
                .indexName(name)
                .keySchema(index.keySchema())
                .projection(index.projection())
                .provisionedThroughput(index.provisionedThroughput())
                .build()
        ).build())
    }

    fun updateGlobalSecondaryIndex(name: String, update: UpdateGlobalSecondaryIndexActionBuilder.() -> Unit) {
        globalSecondaryIndexUpdates.add(GlobalSecondaryIndexUpdate.builder().update(
            UpdateGlobalSecondaryIndexActionBuilder(name).apply(update).build()
        ).build())
    }

    fun deleteGlobalSecondaryIndex(name: String) {
        globalSecondaryIndexUpdates.add(GlobalSecondaryIndexUpdate.builder().delete(
            DeleteGlobalSecondaryIndexAction.builder().indexName(name).build()
        ).build())
    }

    fun build(): UpdateTableRequest {
        provisionedThroughput?.let {
            _builder.billingMode(BillingMode.PROVISIONED).provisionedThroughput(it)
        }

        if (globalSecondaryIndexUpdates.isNotEmpty()) {
            _builder.globalSecondaryIndexUpdates(*globalSecondaryIndexUpdates.toTypedArray())
        }

        return _builder.build()
    }

}