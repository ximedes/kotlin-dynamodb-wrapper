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
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.*

internal class GlobalSecondaryIndexBuilderTest {

    private fun dslGSI(
            name: String = "gsi",
            throughput: ProvisionedThroughput? = null,
            init: GlobalSecondaryIndexBuilder.() -> Unit
    ): GlobalSecondaryIndex {
        return GlobalSecondaryIndexBuilder(name).apply(init).build(throughput)
    }

    @Test
    fun name() {
        val sdkGSI = GlobalSecondaryIndex.builder().indexName("foobar").build()
        val dslGSI = dslGSI("foobar") {}
        assertEquals(sdkGSI.indexName(), dslGSI.indexName())
    }

    @Nested
    inner class KeySchema {
        @Test
        fun singlePartitionKey() {
            val sdkGSI = GlobalSecondaryIndex.builder().keySchema(
                    KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
            ).build()
            val dslGSI = dslGSI("foobar") {
                partitionKey("pk")
            }
            assertEquals(sdkGSI.keySchema(), dslGSI.keySchema())
        }

        @Test
        fun partitionAndSortKey() {
            val sdkGSI = GlobalSecondaryIndex.builder().keySchema(
                    KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build()
            ).build()
            val dslGSI = dslGSI("foobar") {
                partitionKey("pk")
                sortKey("sk")
            }
            assertEquals(sdkGSI.keySchema(), dslGSI.keySchema())
        }
    }

    @Nested
    inner class Throughput {

        @Test
        fun explicitThroughputNoParent() {
            val sdkGSI = GlobalSecondaryIndex.builder()
                    .provisionedThroughput(
                            ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
                    ).build()
            val dslGSI = dslGSI {
                provisionedThroughput(1L, 2L)
            }
            assertEquals(sdkGSI.provisionedThroughput(), dslGSI.provisionedThroughput())
        }

        @Test
        fun `parent throughput is used when none explicitly set`() {
            val parentThroughput = ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
            val dslGSI = dslGSI(throughput = parentThroughput) {

            }
            assertEquals(parentThroughput, dslGSI.provisionedThroughput())
        }

        @Test
        fun `parent throughput is ignored when it is also explicitly set`() {
            val parentThroughput = ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(2L).build()
            val expectedThroughput =
                    ProvisionedThroughput.builder().readCapacityUnits(9L).writeCapacityUnits(8L).build()
            val dslGSI = dslGSI(throughput = parentThroughput) {
                provisionedThroughput(9L, 8L)
            }
            assertEquals(expectedThroughput, dslGSI.provisionedThroughput())
        }

    }

    @Nested
    inner class Projections {

        @Test
        fun include() {
            val sdkGSI = GlobalSecondaryIndex.builder()
                    .projection(
                            Projection.builder().projectionType(ProjectionType.INCLUDE).nonKeyAttributes("nk1", "nk2").build()
                    ).build()
            val dslGSI = dslGSI {
                projection(ProjectionType.INCLUDE) {
                    nonKeyAttributes("nk1", "nk2")
                }
            }
            assertEquals(sdkGSI.projection(), dslGSI.projection())
        }

        @Test
        fun keysOnly() {
            val sdkGSI = GlobalSecondaryIndex.builder()
                    .projection(
                            Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()
                    ).build()
            val dslGSI = dslGSI {
                projection(ProjectionType.KEYS_ONLY)
            }
            assertEquals(sdkGSI.projection(), dslGSI.projection())
        }


    }


}