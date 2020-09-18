/*
 * Copyright 2019-2020 the original author or authors.
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

package com.ximedes.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

typealias Item = Map<String, AttributeValue>
typealias MutableItem = MutableMap<String, AttributeValue>

@DslMarker
annotation class DynamoDbDSL

inline fun <reified T : Any> Item.take(key: String): T = takeOrNull(key)
        ?: error("Property '$key' is null, expected a ${T::class}")

inline fun <reified T : Any?> Item.takeOrNull(key: String): T = when (T::class) {
    String::class -> get(key)?.s()
    Int::class -> get(key)?.n()?.toInt()
    Long::class -> get(key)?.n()?.toLong()
    Double::class -> get(key)?.n()?.toDouble()
    Boolean::class -> get(key)?.bool()
    ByteArray::class -> get(key)?.b()?.asByteArray()
    else -> throw IllegalArgumentException("Unsupported type ${T::class}")
} as T

inline fun <reified T : Any> Item.takeAll(key: String): List<T> = when (T::class) {
    String::class -> get(key)?.ss()?.toList() ?: emptyList<T>()
    else -> throw IllegalArgumentException("Unsupported type ${T::class}")
} as List<T>