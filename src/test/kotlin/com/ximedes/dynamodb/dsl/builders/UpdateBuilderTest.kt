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
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.Update

internal class UpdateBuilderTest {

    private fun dslUpdate(tableName: String = "foo", init: UpdateBuilder.() -> Unit) =
            UpdateBuilder(tableName).apply(init).build()

    @Test
    fun condition() {
        val sdkUpdate = Update.builder().conditionExpression("condition").build()
        val dslUpdate = dslUpdate {
            condition("condition")
        }
        assertEquals(sdkUpdate.conditionExpression(), dslUpdate.conditionExpression())
    }

    @Test
    fun key() {
        val sdkUpdate = Update.builder().key(
                mapOf(
                        "a" to AttributeValue.builder().n("1").build(),
                        "b" to AttributeValue.builder().s("a").build()
                )
        ).build()
        val dslUpdate = dslUpdate {
            key {
                "a" from 1
                "b" from "a"
            }
        }
        assertEquals(sdkUpdate.key(), dslUpdate.key())
    }

    private fun Update.resolved(expression: String?) =
            resolve(expression, expressionAttributeNames(), expressionAttributeValues())

    @Test
    fun updateDsl() {
        val dslUpdate = dslUpdate { update { set("count") { increment(1) } } }
        assertEquals("SET count = count + 1", dslUpdate.resolved(dslUpdate.updateExpression()))
    }

    @Test
    fun conditionDsl() {
        val dslUpdate = dslUpdate { condition { attributeNotExists("deletedAt") } }
        assertEquals(
                "attribute_not_exists(deletedAt)",
                dslUpdate.resolved(dslUpdate.conditionExpression())
        )
    }

    /**
     * Update and condition expressions inside a transaction each get their own placeholders.
     * Neither may overwrite the other's attribute names or values.
     */
    @Test
    fun updateAndConditionDslDoNotSharePlaceholders() {
        val dslUpdate = dslUpdate("counters") {
            key { "id" from "user-count" }
            update { set("count") { increment(1) } }
            condition { "locked" eq false }
        }

        assertEquals("SET count = count + 1", dslUpdate.resolved(dslUpdate.updateExpression()))
        assertEquals("locked = false", dslUpdate.resolved(dslUpdate.conditionExpression()))
    }

    @Test
    fun attributeValues() {
        val sdkUpdate =
                Update.builder().expressionAttributeValues(mapOf("a" to AttributeValue.builder().s("z").build())).build()
        val dslUpdate = dslUpdate {
            attributeValues {
                "a" from "z"
            }
        }
        assertEquals(sdkUpdate.expressionAttributeValues(), dslUpdate.expressionAttributeValues())
    }

    @Test
    fun attributeNames() {
        val sdkUpdate = Update.builder().expressionAttributeNames(mapOf("a" to "z")).build()
        val dslUpdate = dslUpdate {
            attributeNames("a" to "z")
        }
        assertEquals(sdkUpdate.expressionAttributeNames(), dslUpdate.expressionAttributeNames())
    }


    @Test
    fun returnValuesOnConditionCheckFailure() {
        val sdkUpdate =
                Update.builder().returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD).build()
        val dslUpdate = dslUpdate {
            returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
        }
        assertEquals(sdkUpdate.returnValuesOnConditionCheckFailure(), dslUpdate.returnValuesOnConditionCheckFailure())
    }


}