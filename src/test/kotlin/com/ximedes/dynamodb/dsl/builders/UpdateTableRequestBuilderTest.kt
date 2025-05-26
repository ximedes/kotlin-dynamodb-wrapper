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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.*

internal class UpdateTableRequestBuilderTest {

    private fun updateTableRequest(tableName: String = "foo", init: UpdateTableRequestBuilder.() -> Unit): UpdateTableRequest {
        return UpdateTableRequestBuilder(tableName).apply(init).build()
    }

    @Test
    fun `tableName matches`() {
        val sdkRequest = UpdateTableRequest.builder()
                .tableName("foo")
                .build()

        val dslRequest = updateTableRequest("foo") {}

        assertEquals(sdkRequest.tableName(), dslRequest.tableName())
    }

    @Test
    fun `default billing mode is unspecified`() {
        assertNull(updateTableRequest("foo") {}.billingMode())
    }

    @Test
    fun `billing mode can be changed`() {
        val dslRequest = updateTableRequest("foo") {
            billingMode(BillingMode.PAY_PER_REQUEST)
        }
        assertEquals(BillingMode.PAY_PER_REQUEST, dslRequest.billingMode())
    }

    @Test
    fun `attribute definitions match`() {
        val sdkRequest = UpdateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("s1").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("s2").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("n1").attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("b1").attributeType(ScalarAttributeType.B).build()
                ).build()

        val dslRequest = updateTableRequest {
            attributes {
                string("s1", "s2")
                number("n1")
                binary("b1")
            }
        }

        assertEquals(sdkRequest.attributeDefinitions(), dslRequest.attributeDefinitions())
    }

    @Test
    fun `provisioned throughput`() {
        val sdkRequest = UpdateTableRequest.builder()
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
                )
                .build()

        val dslRequest = updateTableRequest {
            provisionedThroughput(1L, 2L)
        }

        assertEquals(BillingMode.PROVISIONED, dslRequest.billingMode())
        assertEquals(sdkRequest.provisionedThroughput(), dslRequest.provisionedThroughput())
    }

    @Test
    fun `global secondary index`() {
        val sdkRequest = UpdateTableRequest.builder()
            .globalSecondaryIndexUpdates(
                GlobalSecondaryIndexUpdate.builder().create(
                    CreateGlobalSecondaryIndexAction.builder()
                        .indexName("gsi1")
                        .keySchema(
                            KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("pk").build(),
                            KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("sk").build()
                        )
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build())
                        .build()
                ).build(),
                GlobalSecondaryIndexUpdate.builder().update(
                    UpdateGlobalSecondaryIndexAction.builder()
                        .indexName("gsi2")
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build())
                        .build()
                ).build(),
                GlobalSecondaryIndexUpdate.builder().delete(
                    DeleteGlobalSecondaryIndexAction.builder()
                        .indexName("gsi3")
                        .build()
                ).build()
            )
            .build()

        val dslRequest = updateTableRequest {
            createGlobalSecondaryIndex("gsi1") {
                partitionKey("pk")
                sortKey("sk")
                projection(ProjectionType.ALL)
                provisionedThroughput(1L, 2L)
            }
            updateGlobalSecondaryIndex("gsi2") {
                provisionedThroughput(1L, 2L)
            }
            deleteGlobalSecondaryIndex("gsi3")
        }

        assertEquals(sdkRequest.globalSecondaryIndexUpdates(), dslRequest.globalSecondaryIndexUpdates())
    }

    @Test
    fun `full UpdateTable test`() {
        val sdkRequest = UpdateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("s1").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("s2").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("n1").attributeType(ScalarAttributeType.N).build(),
                AttributeDefinition.builder().attributeName("b1").attributeType(ScalarAttributeType.B).build()
            )
            .provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
            )
            .globalSecondaryIndexUpdates(
                GlobalSecondaryIndexUpdate.builder().create(
                    CreateGlobalSecondaryIndexAction.builder()
                        .indexName("gsi1")
                        .keySchema(
                            KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("pk").build(),
                            KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("sk").build()
                        )
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build())
                        .build()
                ).build(),
                GlobalSecondaryIndexUpdate.builder().update(
                    UpdateGlobalSecondaryIndexAction.builder()
                        .indexName("gsi2")
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build())
                        .build()
                ).build(),
                GlobalSecondaryIndexUpdate.builder().delete(
                    DeleteGlobalSecondaryIndexAction.builder()
                        .indexName("gsi3")
                        .build()
                ).build()
            )
            .build()

        val dslRequest = updateTableRequest {
            attributes {
                string("s1", "s2")
                number("n1")
                binary("b1")
            }
            billingMode(BillingMode.PAY_PER_REQUEST)
            provisionedThroughput(1L, 2L)  // overrides the pay-per-request billing mode
            createGlobalSecondaryIndex("gsi1") {
                partitionKey("pk")
                sortKey("sk")
                projection(ProjectionType.ALL)
                provisionedThroughput(1L, 2L)
            }
            updateGlobalSecondaryIndex("gsi2") {
                provisionedThroughput(1L, 2L)
            }
            deleteGlobalSecondaryIndex("gsi3")
        }

        assertEquals(sdkRequest.globalSecondaryIndexUpdates(), dslRequest.globalSecondaryIndexUpdates())
    }
}