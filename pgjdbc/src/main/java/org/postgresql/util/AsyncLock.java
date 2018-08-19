package org.postgresql.util;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A Nonblocking lock for java.
 * monitors will block the thread until the lock is available, however,
 * AsyncLock will not block the thread if the lock is not available.
 * Usage:
 *
 * UUID key = null;
 * try {
 *     key = await(lock.lock());
 *     // do your work here
 * } finally {
 *     lock.release(key);
 * }
 */
public class AsyncLock {

    private Queue<WaitTask> waitTasks = new ArrayDeque<>();
    private boolean acquired = false;
    private String owner;
    private UUID ownerKey;

    public synchronized CompletableFuture<UUID> lock() {
        if (!this.acquired) {
            this.acquired = true;
            this.owner = Thread.currentThread().getName();
            this.ownerKey = UUID.randomUUID();
            return CompletableFuture.completedFuture(this.ownerKey);
        } else {
            CompletableFuture<UUID> completableFuture = new CompletableFuture<>();
            waitTasks.add(new WaitTask(Thread.currentThread().getName(), completableFuture));
            return completableFuture;
        }
    }

    public synchronized boolean release(UUID ownerKey) {
        if (this.ownerKey == null || !this.ownerKey.equals(ownerKey)) {
            return false;
        }

        WaitTask waitTask = this.waitTasks.poll();
        if (waitTask == null) {
            this.acquired = false;
            this.owner = null;
            this.ownerKey = null;
        } else {
            this.owner = waitTask.getThreadName();
            this.ownerKey = UUID.randomUUID();
            waitTask.getCompletableFuture().complete(this.ownerKey);
        }

        return true;
    }

    public String getOwner() {
        return this.owner;
    }

    private static class WaitTask {
        private String threadName;
        private CompletableFuture<UUID> completableFuture;

        public WaitTask(String threadName, CompletableFuture<UUID> completableFuture) {
            this.threadName = threadName;
            this.completableFuture = completableFuture;
        }

        public String getThreadName() {
            return this.threadName;
        }

        public CompletableFuture<UUID> getCompletableFuture() {
            return this.completableFuture;
        }
    }
}
