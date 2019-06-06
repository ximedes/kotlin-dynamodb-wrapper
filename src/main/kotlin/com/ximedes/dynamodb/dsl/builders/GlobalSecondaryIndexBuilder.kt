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
class GlobalSecondaryIndexBuilder(name: String) {
    private val _builder = GlobalSecondaryIndex.builder().indexName(name)
    private val keySchemaElements = mutableListOf<KeySchemaElement>()
    private var provisionedThroughput: ProvisionedThroughput? = null


    fun partitionKey(name: String) {
        keySchemaElements.add(KeySchemaElement.builder().attributeName(name).keyType(KeyType.HASH).build())
    }

    fun sortKey(name: String) {
        keySchemaElements.add(KeySchemaElement.builder().attributeName(name).keyType(KeyType.RANGE).build())
    }

    fun provisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long) {
        provisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build()
    }

    fun projection(type: ProjectionType, block: (ProjectionBuilder.() -> Unit) = {}) {
        val projection = ProjectionBuilder(type).apply(block).build()
        _builder.projection(projection)
    }


    fun build(parentThroughput: ProvisionedThroughput?): GlobalSecondaryIndex {
        _builder.keySchema(*keySchemaElements.toTypedArray())

        (provisionedThroughput ?: parentThroughput)?.let {
            _builder.provisionedThroughput(it)
        }

        return _builder.build()
    }

}