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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.*

internal class CreateTableRequestBuilderTest {

    private fun dslRequest(tableName: String = "foo", init: CreateTableRequestBuilder.() -> Unit): CreateTableRequest {
        return CreateTableRequestBuilder(tableName).apply(init).build()
    }

    @Test
    fun `tableName matches`() {
        val sdkRequest = CreateTableRequest.builder()
                .tableName("foo")
                .build()
        val dslRequest = dslRequest("foo") {}

        assertEquals(sdkRequest.tableName(), dslRequest.tableName())

    }

    @Test
    fun `default billing mode is pay per request`() {
        assertEquals(BillingMode.PAY_PER_REQUEST, dslRequest("foo") {}.billingMode())
    }

    @Test
    fun `attribute definitions match`() {
        val sdkRequest = CreateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("s1").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("s2").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("n1").attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("b1").attributeType(ScalarAttributeType.B).build()
                ).build()
        val dslRequest = dslRequest {
            attributes {
                string("s1", "s2")
                number("n1")
                binary("b1")
            }
        }

        assertEquals(sdkRequest.attributeDefinitions(), dslRequest.attributeDefinitions())
    }

    @Test
    fun `partition keys match`() {
        val sdkRequest = CreateTableRequest.builder()
                .keySchema(
                        KeySchemaElement.builder().attributeName("k1").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("s1").keyType(KeyType.RANGE).build()
                )
                .build()
        val dslRequest = dslRequest {
            partitionKey("k1")
            sortKey("s1")
        }

        assertEquals(sdkRequest.keySchema(), dslRequest.keySchema())
    }


    @Test
    fun `provisioned throughput`() {
        val sdkRequest = CreateTableRequest.builder()
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
                )
                .build()
        val dslRequest = dslRequest {
            provisionedThroughput(1L, 2L)
        }

        assertEquals(BillingMode.PROVISIONED, dslRequest.billingMode())
        assertEquals(sdkRequest.provisionedThroughput(), dslRequest.provisionedThroughput())
    }

    @Test
    fun `global secondary index`() {
        val sdkRequest = CreateTableRequest.builder()
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName("gsi1")
                                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                .keySchema(KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("k1").build())
                                .build(),
                        GlobalSecondaryIndex.builder()
                                .indexName("gsi2")
                                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                                .keySchema(KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("k2").build())
                                .build()
                )
                .build()
        val dslRequest = dslRequest {
            globalSecondaryIndex("gsi1") {
                projection(ProjectionType.ALL)
                partitionKey("k1")
            }
            globalSecondaryIndex("gsi2") {
                projection(ProjectionType.KEYS_ONLY)
                sortKey("k2")
            }

        }

        assertEquals(sdkRequest.globalSecondaryIndexes(), dslRequest.globalSecondaryIndexes())
    }

    @Test
    fun `full CreateTable test`() {

        val sdkRequest = CreateTableRequest.builder()
                .tableName("foo")
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("myString1").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("myString2").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("myBool").attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("myNumber").attributeType(ScalarAttributeType.N).build()
                )
                .keySchema(
                        KeySchemaElement.builder().attributeName("myString1").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("myNumber").keyType(KeyType.RANGE).build()
                )
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName("globalIndex1")
                                .keySchema(KeySchemaElement.builder().attributeName("myString2").keyType(KeyType.HASH).build())
                                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build()


        val dslRequest = dslRequest("foo") {
            attributes {
                string("myString1", "myString2")
                binary("myBool")
                number("myNumber")
            }
            partitionKey("myString1")
            sortKey("myNumber")
            globalSecondaryIndex("globalIndex1") {
                partitionKey("myString2")
                projection(ProjectionType.ALL)
            }
        }

        assertEquals(sdkRequest, dslRequest)

    }
}