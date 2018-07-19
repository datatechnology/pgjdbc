package org.postgresql.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class VertxHelper {

    private static Vertx vertx;
    private static Object singletonLock = new Object();

    public static void updateVertx(Vertx instance) {
        if (vertx != null) {
            throw new RuntimeException("vertx already assigned, cannot be changed anymore");
        }

        vertx = instance;
    }

    public static Vertx getVertx() {
        if (vertx == null) {
            synchronized (singletonLock) {
                if (vertx == null) {
                    vertx = Vertx.vertx();
                }
            }
        }

        return vertx;
    }

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
