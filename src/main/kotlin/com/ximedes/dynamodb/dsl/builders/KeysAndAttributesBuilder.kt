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
import com.ximedes.dynamodb.dsl.Item
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes

@DynamoDbDSL
class KeysAndAttributesBuilder {
    private val _builder = KeysAndAttributes.builder()
    private val keys = mutableListOf<Item>()

    fun build(): KeysAndAttributes = _builder.keys(keys).build()

    fun consistentRead(consistentRead: Boolean) {
        _builder.consistentRead(consistentRead)
    }

    fun attributeNames(vararg names: Pair<String, String>) {
        _builder.expressionAttributeNames(mapOf(*names))
    }

    fun projection(expression: String) {
        _builder.projectionExpression(expression)
    }

    fun keys(vararg inits: ItemBuilder.() -> Unit) {
        keys.addAll(inits.map { ItemBuilder().apply(it).build() })
    }


}