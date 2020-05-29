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
import com.ximedes.dynamodb.dsl.MutableItem
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.io.InputStream
import java.nio.ByteBuffer


@DynamoDbDSL
class ItemBuilder {

    private val item: MutableItem = mutableMapOf()

    infix fun String.from(value: String?) {
        value?.let { item[this] = AttributeValue.builder().s(value).build() }
    }

    infix fun String.from(block: ItemBuilder.() -> Unit) {
        item[this] = AttributeValue.builder().m(ItemBuilder().apply(block).build()).build()
    }

    @JvmName("fromStrings")
    infix fun String.from(value: Collection<String>?) {
        value?.let { item[this] = AttributeValue.builder().ss(value).build() }
    }

    infix fun String.from(value: Number?) {
        value?.let { item[this] = AttributeValue.builder().n(value.toString()).build() }
    }

    infix fun String.from(value: Boolean?) {
        value?.let { item[this] = AttributeValue.builder().bool(value).build() }
    }

    @JvmName("fromNumbers")
    infix fun String.from(value: Collection<Number>?) {
        value?.let { item[this] = AttributeValue.builder().ns(value.map { it.toString() }).build() }
    }

    infix fun String.from(value: ByteArray?) {
        value?.let { item[this] = AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build() }
    }

    @JvmName("fromByteArrays")
    infix fun String.from(value: Collection<ByteArray>?) {
        value?.let { item[this] = AttributeValue.builder().bs(value.map { SdkBytes.fromByteArray(it) }).build() }
    }

    infix fun String.from(value: ByteBuffer?) {
        value?.let { item[this] = AttributeValue.builder().b(SdkBytes.fromByteBuffer(value)).build() }
    }

    @JvmName("fromByteBuffers")
    infix fun String.from(value: Collection<ByteBuffer>?) {
        value?.let { item[this] = AttributeValue.builder().bs(value.map { SdkBytes.fromByteBuffer(it) }).build() }
    }

    infix fun String.from(value: InputStream?) {
        value?.let { item[this] = AttributeValue.builder().b(SdkBytes.fromInputStream(value)).build() }
    }

    @JvmName("fromInputStreams")
    infix fun String.from(value: Collection<InputStream>?) {
        value?.let { item[this] = AttributeValue.builder().bs(value.map { SdkBytes.fromInputStream(it) }).build() }
    }

    fun build(): Map<String, AttributeValue> = item

}