/*
 * Copyright 2021 the original author or authors.
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

import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest
import java.util.concurrent.CompletableFuture

private val logger = LoggerFactory.getLogger("com.ximedes.dynamodb.dsl")

fun <T : DynamoDbRequest, U> T.logAndRun(block: (T) -> U): U {
    if (logger.isDebugEnabled) {
        logger.debug("Executing $this")
    }
    try {
        return block(this)
    } catch (e: Exception) {
        logger.error("Caught exception when executing request $this")
        throw e
    }
}

suspend fun <T : DynamoDbRequest, U> T.logAndRunAsync(block: (T) -> CompletableFuture<U>): U {
    if (logger.isDebugEnabled) {
        logger.debug("Executing $this")
    }
    try {
        return block(this).await()
    } catch (e: Exception) {
        logger.error("Caught exception when executing request $this")
        throw e
    }
}


