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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

@DynamoDbDSL
class AttributeDefinitionsBuilder {

    private val definitions = mutableListOf<AttributeDefinition>()

    private fun attributes(type: ScalarAttributeType, vararg names: String) {
        definitions.addAll(names.map { AttributeDefinition.builder().attributeName(it).attributeType(type).build() })
    }

    fun string(vararg names: String) = attributes(ScalarAttributeType.S, *names)

    fun number(vararg names: String) = attributes(ScalarAttributeType.N, *names)

    fun binary(vararg names: String) = attributes(ScalarAttributeType.B, *names)

    fun build(): List<AttributeDefinition> = definitions

}