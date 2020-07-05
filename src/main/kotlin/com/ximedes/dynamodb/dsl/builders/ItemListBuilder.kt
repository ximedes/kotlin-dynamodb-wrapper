package com.ximedes.dynamodb.dsl.builders

import com.ximedes.dynamodb.dsl.DynamoDbDSL
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

@DynamoDbDSL
class ItemListBuilder {
    private val items = mutableListOf<AttributeValue>()

    fun item(init: ItemBuilder.() -> Unit) {
        val map = AttributeValue.builder().m(ItemBuilder().apply(init).build()).build()
        items.add(map)
    }

    fun build() = items.toList()
}
