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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.Get

internal class GetBuilderTest {

    private fun dslGet(tableName: String = "foo", init: GetBuilder.() -> Unit): Get {
        return GetBuilder(tableName).apply(init).build()
    }

    @Test
    fun tableName() {
        val sdkGet = Get.builder().tableName("ledger").build()
        val dslGet = dslGet("ledger") {}
        assertEquals(sdkGet.tableName(), dslGet.tableName())
    }

    @Test
    fun key() {
        val sdkGet = Get.builder().key(
                mapOf(
                        "a" to AttributeValue.builder().n("1").build(),
                        "b" to AttributeValue.builder().s("a").build()
                )
        ).build()
        val dslGet = dslGet {
            key {
                "a" from 1
                "b" from "a"
            }
        }
        assertEquals(sdkGet.key(), dslGet.key())
    }

    @Test
    fun projection() {
        val sdkGet = Get.builder().projectionExpression("a, b, c").build()
        val dslGet = dslGet { projection("a, b, c") }
        assertEquals(sdkGet.projectionExpression(), dslGet.projectionExpression())
    }

    @Test
    fun attributeNames() {
        val sdkGet = Get.builder().expressionAttributeNames(mapOf("a" to "1", "b" to "2")).build()
        val dslGet = dslGet { attributeNames("a" to "1", "b" to "2") }
        assertEquals(sdkGet.expressionAttributeNames(), dslGet.expressionAttributeNames())
    }
}