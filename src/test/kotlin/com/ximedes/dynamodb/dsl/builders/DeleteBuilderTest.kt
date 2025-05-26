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
import software.amazon.awssdk.services.dynamodb.model.Delete
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure

internal class DeleteBuilderTest {

    private fun dslDelete(tableName: String = "foo", init: DeleteBuilder.() -> Unit) =
            DeleteBuilder(tableName).apply(init).build()

    @Test
    fun tableName() {
        val sdkDelete = Delete.builder().tableName("table").build()
        val dslDelete = dslDelete("table") {}
        assertEquals(sdkDelete.tableName(), dslDelete.tableName())
    }

    @Test
    fun condition() {
        val sdkDelete = Delete.builder().conditionExpression("condition").build()
        val dslDelete = dslDelete { condition("condition") }
        assertEquals(sdkDelete.conditionExpression(), dslDelete.conditionExpression())
    }

    @Test
    fun key() {
        val sdkDelete = Delete.builder().key(mapOf("a" to AttributeValue.builder().s("b").build())).build()
        val dslDelete = dslDelete { key { "a" from "b" } }
        assertEquals(sdkDelete.key(), dslDelete.key())
    }


    @Test
    fun attributeValues() {
        val sdkDelete =
                Delete.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build()))
                        .build()
        val dslDelete = dslDelete {
            attributeValues {
                "a" from "z"
            }
        }
        assertEquals(sdkDelete.expressionAttributeValues(), dslDelete.expressionAttributeValues())
    }

    @Test
    fun attributeNames() {
        val sdkDelete = Delete.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslDelete = dslDelete {
            attributeNames("a" to "z")
        }
        assertEquals(sdkDelete.expressionAttributeNames(), dslDelete.expressionAttributeNames())
    }

    @Test
    fun returnValuesOnConditionCheckFailure() {
        val sdkDelete =
                Delete.builder().returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD).build()
        val dslDelete = dslDelete {
            returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
        }
        assertEquals(sdkDelete.returnValuesOnConditionCheckFailure(), dslDelete.returnValuesOnConditionCheckFailure())
    }


}