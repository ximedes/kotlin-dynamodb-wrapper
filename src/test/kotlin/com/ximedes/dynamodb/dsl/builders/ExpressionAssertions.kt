/*
 * Copyright 2019-2025 the original author or authors.
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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/**
 * Test support for asserting on generated DynamoDB expressions.
 *
 * Expressions are asserted by substituting the generated `#name` / `:value` placeholders back in,
 * so a test states the *meaning* DynamoDB will evaluate rather than the exact placeholder
 * numbering. That keeps assertions readable and independent of how placeholders are allocated,
 * while still catching the case where two expressions on one request claim the same placeholder.
 */
private val PLACEHOLDER = Regex("[#:][A-Za-z_][A-Za-z0-9_]*")

/**
 * Substitutes attribute name and value placeholders into [expression].
 *
 * A placeholder with no entry in [names] / [values] is rendered as `<UNRESOLVED ...>` so that a
 * missing or overwritten mapping shows up in the assertion diff instead of passing silently.
 */
internal fun resolve(
    expression: String?,
    names: Map<String, String>?,
    values: Map<String, AttributeValue>?
): String? {
    if (expression == null) return null
    return PLACEHOLDER.replace(expression) { match ->
        val token = match.value
        if (token.startsWith("#")) {
            names?.get(token) ?: "<UNRESOLVED $token>"
        } else {
            values?.get(token)?.let(::literal) ?: "<UNRESOLVED $token>"
        }
    }
}

private fun literal(value: AttributeValue): String = when {
    value.s() != null -> "\"${value.s()}\""
    value.n() != null -> value.n()
    value.bool() != null -> value.bool().toString()
    value.nul() != null -> "null"
    value.b() != null -> "b[${value.b().asByteArray().size}]"
    value.hasSs() -> value.ss().joinToString(", ", "{", "}") { "\"$it\"" }
    value.hasNs() -> value.ns().joinToString(", ", "{", "}")
    value.hasBs() -> value.bs().joinToString(", ", "{", "}") { "b[${it.asByteArray().size}]" }
    value.hasL() -> value.l().joinToString(", ", "[", "]") { literal(it) }
    value.hasM() -> value.m().entries.joinToString(", ", "{", "}") { "${it.key}=${literal(it.value)}" }
    else -> value.toString()
}
