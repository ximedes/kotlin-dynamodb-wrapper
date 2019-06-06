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
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure

internal class PutBuilderTest {
    private fun dslPut(tableName: String = "foo", init: PutBuilder.() -> Unit) =
            PutBuilder(tableName).apply(init).build()

    @Test
    fun item() {
        val sdkPut = Put.builder().item(mapOf("a" to AttributeValue.builder().s("z").build())).build()
        val dslPut = dslPut {
            item {
                "a" from "z"
            }
        }
        assertEquals(sdkPut.item(), dslPut.item())
    }

    @Test
    fun condition() {
        val sdkPut = Put.builder().conditionExpression("condition").build()
        val dslPut = dslPut {
            condition("condition")
        }
        assertEquals(sdkPut.conditionExpression(), dslPut.conditionExpression())
    }

    @Test
    fun attributeValues() {
        val sdkPut =
                Put.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build())).build()
        val dslPut = dslPut {
            attributeValues {
                "a" from "z"
            }
        }
        assertEquals(sdkPut.expressionAttributeValues(), dslPut.expressionAttributeValues())
    }

    @Test
    fun attributeNames() {
        val sdkPut = Put.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslPut = dslPut {
            attributeNames("a" to "z")
        }
        assertEquals(sdkPut.expressionAttributeNames(), dslPut.expressionAttributeNames())
    }


    @Test
    fun returnValuesOnConditionCheckFailure() {
        val sdkPut =
                Put.builder().returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD).build()
        val dslPut = dslPut {
            returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
        }
        assertEquals(sdkPut.returnValuesOnConditionCheckFailure(), dslPut.returnValuesOnConditionCheckFailure())
    }

}