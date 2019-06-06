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
class CreateTableRequestBuilder(tableName: String) {
    private var _builder = CreateTableRequest.builder().tableName(tableName).billingMode(BillingMode.PAY_PER_REQUEST)
    private val keySchemaElements = mutableListOf<KeySchemaElement>()
    private val globalSecondaryIndexBuilders = mutableListOf<GlobalSecondaryIndexBuilder>()
    private var provisionedThroughput: ProvisionedThroughput? = null

    fun attributes(init: AttributeDefinitionsBuilder.() -> Unit) {
        _builder.attributeDefinitions(AttributeDefinitionsBuilder().apply(init).build())
    }

    fun partitionKey(name: String) {
        keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                        KeyType.HASH
                ).build()
        )
    }

    fun sortKey(name: String) {
        keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                        KeyType.RANGE
                ).build()
        )
    }

    fun provisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long) {
        provisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build()
    }

    fun globalSecondaryIndex(name: String, init: GlobalSecondaryIndexBuilder.() -> Unit) {
        val index = GlobalSecondaryIndexBuilder(name).apply(init)
        globalSecondaryIndexBuilders.add(index)
    }


    fun build(): CreateTableRequest {
        _builder.keySchema(*keySchemaElements.toTypedArray())

        provisionedThroughput?.let {
            _builder.billingMode(BillingMode.PROVISIONED).provisionedThroughput(it)
        }

        if (globalSecondaryIndexBuilders.isNotEmpty()) {
            _builder.globalSecondaryIndexes(*globalSecondaryIndexBuilders.map { it.build(provisionedThroughput) }.toTypedArray())
        }

        return _builder.build()
    }

}