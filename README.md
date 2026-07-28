[![Maven Central](https://img.shields.io/maven-central/v/com.ximedes/kotlin-dynamodb-wrapper.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.ximedes%22)

# kotlin-dynamodb-wrapper

A Kotlin DSL wrapper for the AWS DynamoDB SDK v2. Write clean, type-safe DynamoDB operations with minimal boilerplate.

> **Upgrade from 0.7.** In 0.7, combining two expression DSL blocks on one request — an `update { }`
> with a `condition { }`, or a `keyCondition { }` with a `filter { }` — generated colliding
> placeholders, which could silently write the wrong attribute. Fixed in 0.7.5. See the
> [changelog](CHANGELOG.md) for details.

## Installation

### Maven

```xml
<dependency>
    <groupId>com.ximedes</groupId>
    <artifactId>kotlin-dynamodb-wrapper</artifactId>
    <version>0.7.5</version>
</dependency>
```

### Gradle

```kotlin
implementation("com.ximedes:kotlin-dynamodb-wrapper:0.7.5")
```

## Quick Start

```kotlin
val client = DynamoDbClient.create()

// Put an item
client.putItem("users") {
    item {
        "id" from "user-123"
        "name" from "Alice"
        "age" from 30
    }
}

// Query with type-safe conditions
client.query("users") {
    keyCondition { ("pk" eq "USER#123") and ("sk" ge "ORDER#2024") }
    filter { "status" ne "CANCELLED" }
}

// Update with type-safe expressions
client.updateItem("counters") {
    key { "id" from "page-views" }
    update {
        set("count") { increment(1) }
        set("lastUpdated", System.currentTimeMillis())
    }
}
```

## Supported Operations

| Operation | Sync | Async |
|-----------|------|-------|
| PutItem | Yes | Yes |
| GetItem | Yes | Yes |
| UpdateItem | Yes | Yes |
| DeleteItem | Yes | Yes |
| Query | Yes | Yes |
| Scan | Yes | Yes |
| BatchGetItem | Yes | Yes |
| BatchWriteItem | Yes | Yes |
| TransactGetItems | Yes | Yes |
| TransactWriteItems | Yes | Yes |
| CreateTable | Yes | Yes |
| DeleteTable | Yes | Yes |
| UpdateTable | Yes | Yes |
| DescribeTable | Yes | Yes |

## Building Items

Use the `item` DSL to build DynamoDB items:

```kotlin
client.putItem("products") {
    item {
        "id" from "prod-001"
        "name" from "Widget"
        "price" from 29.99
        "inStock" from true
        "tags" from listOf("electronics", "gadgets")
        "metadata" from mapOf("color" to "blue", "weight" to 150)
    }
}
```

## Type-Safe Condition Expressions

Build condition expressions with compile-time safety and IDE autocompletion:

### Comparison Operators

```kotlin
client.deleteItem("users") {
    key { "id" from "123" }
    condition {
        ("status" eq "INACTIVE") and ("lastLogin" lt "2023-01-01")
    }
}
```

Available operators: `eq`, `ne`, `lt`, `le`, `gt`, `ge`

### Range and Set Operations

```kotlin
// BETWEEN
condition { "age" between 18..65 }
condition { "date" between "2024-01-01".."2024-12-31" }

// IN
condition { "status" isIn listOf("ACTIVE", "PENDING", "REVIEW") }
```

### Attribute Functions

```kotlin
// Existence checks
condition { attributeExists("email") }
condition { attributeNotExists("deletedAt") }

// Type check
condition { attributeType("data", DynamoAttributeType.MAP) }

// String functions
condition { beginsWith("email", "admin@") }
condition { contains("description", "important") }

// Size function
condition { size("tags") gt 0 }
condition { size("items") le 100 }
```

### Logical Operators

```kotlin
condition {
    (("status" eq "ACTIVE") or ("status" eq "PENDING")) and ("count" lt 100)
}

condition { not("deleted" eq true) }
```

### Nested Attribute Paths

```kotlin
// Dot notation
condition { path("address.city") eq "Amsterdam" }

// Array indices
condition { path("items", 0, "quantity") gt 5 }

// Deep nesting
condition { path("data", "items", 0, "product", "name") eq "Widget" }
```

## Type-Safe Update Expressions

Build update expressions without manual string concatenation:

### SET Operations

```kotlin
client.updateItem("users") {
    key { "id" from "123" }
    update {
        // Simple value assignment
        set("name", "New Name")
        set("active", true)

        // Increment/decrement
        set("loginCount") { increment(1) }
        set("credits") { decrement(10) }

        // Set if not exists
        set("version") { ifNotExists(1) }

        // List operations
        set("tags") { listAppend(listOf("new-tag")) }
        set("history") { listPrepend(listOf("first")) }
    }
}
```

### REMOVE, ADD, DELETE Operations

```kotlin
update {
    // Remove attributes
    remove("tempField", "obsoleteData")

    // Add to number or set
    add("viewCount", 1)
    add("tags", setOf("premium", "featured"))

    // Delete from set
    delete("oldTags", setOf("deprecated", "legacy"))
}
```

### Nested Path Updates

```kotlin
update {
    set(AttributePath.of("address", "city"), "New York")
    set(AttributePath.of("items", 0, "quantity"), 10)
    remove(AttributePath.of("metadata", "temp"))
}
```

## Query and Scan

```kotlin
// Query with key condition and filter
val response = client.query("orders") {
    indexName("status-index")
    keyCondition { ("pk" eq "USER#123") and ("sk" ge "ORDER#2024") }
    filter { ("total" gt 100) and ("status" ne "CANCELLED") }
    limit(50)
    scanIndexForward(false)
}

// Scan with filter
val scanResponse = client.scan("products") {
    filter { ("price" lt 50) and attributeExists("discount") }
    limit(100)
}
```

## Transactions

```kotlin
// Write transaction
client.transactWriteItems {
    put("users") {
        item { "id" from "new-user"; "name" from "Bob" }
        condition { attributeNotExists("id") }
    }
    update("counters") {
        key { "id" from "user-count" }
        update { set("count") { increment(1) } }
    }
    delete("sessions") {
        key { "id" from "old-session" }
    }
}

// Read transaction
client.transactGetItems {
    get("users") { key { "id" from "user-1" } }
    get("users") { key { "id" from "user-2" } }
}
```

## Batch Operations

```kotlin
// Batch write
client.batchWriteItem {
    table("users") {
        put { "id" from "user-1"; "name" from "Alice" }
        put { "id" from "user-2"; "name" from "Bob" }
        delete { "id" from "user-old" }
    }
}

// Batch get
client.batchGetItem {
    table("users") {
        keys(
            { "id" from "user-1" },
            { "id" from "user-2" }
        )
    }
}
```

## Async Support

All operations support Kotlin coroutines via `DynamoDbAsyncClient`:

```kotlin
val asyncClient = DynamoDbAsyncClient.create()

suspend fun example() {
    asyncClient.putItem("users") {
        item { "id" from "123"; "name" from "Alice" }
    }

    val response = asyncClient.query("orders") {
        keyCondition { "pk" eq "USER#123" }
    }
}
```

## Mixing DSL with Manual Expressions

You can combine type-safe DSL with manual attribute names/values when needed:

```kotlin
client.updateItem("items") {
    key { "id" from "123" }
    condition { "status" eq "ACTIVE" }
    // Add manual attributes alongside DSL-generated ones
    attributeValues { ":extra" from "manual-value" }
    attributeNames("#custom" to "customField")
}
```

The DSL generates placeholders of the form `#n1`, `#n2`, … and `:v1`, `:v2`, …. Pick manual
placeholders outside that pattern; reusing a generated one throws `IllegalArgumentException` rather
than silently overwriting it.

## Reading Items

Read attributes off a returned item with `take`, `takeOrNull` and `takeAll`:

```kotlin
val item = client.getItem("users") { key { "id" from "123" } }.item()

val name: String = item.take("name")            // throws if absent
val age: Int? = item.takeOrNull("age")          // null if absent
val tags: List<String> = item.takeAll("tags")   // empty list if absent
```

`take` and `takeOrNull` support `String`, `Int`, `Long`, `Double`, `Boolean` and `ByteArray`.
`takeAll` reads string, number and binary sets as `String`, `Int`, `Long`, `Double` or `ByteArray`.

## Logging

Request logging is available via SLF4J. Enable debug logging for `com.ximedes.dynamodb.dsl` to see all requests.

## License

Apache License 2.0
