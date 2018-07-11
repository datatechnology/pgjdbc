package org.postgresql.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class VertxHelper {

    public static <TResult> CompletableFuture<TResult> vertxTCompletableFuture(Consumer<Handler<AsyncResult<TResult>>> handlerConsumer) {
        CompletableFuture<TResult> completableFuture = new CompletableFuture<>();
        handlerConsumer.accept(ar -> {
            if (ar.succeeded()) {
                completableFuture.complete(ar.result());
            } else {
                completableFuture.completeExceptionally(ar.cause());
            }
        });
        return completableFuture;
    }
}
