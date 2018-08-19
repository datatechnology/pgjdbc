package org.postgresql.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.*;

public class AsyncLockTest {

    private ExecutorService executorService;
    private Random random;
    private int testValue = 0;

    @Before
    public void initialze() {
        this.executorService = new ScheduledThreadPoolExecutor(10);
        this.random = new Random(Calendar.getInstance().getTimeInMillis());
    }

    @Test
    public void testFirstLocker() throws Exception {
        AsyncLock asyncLock = new AsyncLock();
        CompletableFuture<UUID> result = asyncLock.lock();
        assertTrue("first locker should proceed without any wait", result.isDone());
        assertEquals("owner should be the current thread", Thread.currentThread().getName(), asyncLock.getOwner());
        assertTrue("release should be fine", asyncLock.release(result.get()));
    }

    @Test
    public void testConcurrentLockers() throws Exception {
        AsyncLock asyncLock = new AsyncLock();
        CompletableFuture<Void> work1 = this.executeDelayWork(asyncLock);
        CompletableFuture<Void> work2 = this.executeDelayWork(asyncLock);
        CompletableFuture<Void> work3 = this.executeDelayWork(asyncLock);
        CompletableFuture.allOf(work1, work2, work3).get();
        assertNull("There must no owner", asyncLock.getOwner());
        assertEquals("test value must be 3", 3, this.testValue);
    }

    @Test
    public void testReleaseWithBadKey() {
        AsyncLock asyncLock = new AsyncLock();
        asyncLock.lock();
        assertFalse("Release must failed because keys are not matched", asyncLock.release(null));
    }

    @Test
    public void testReleaseWithoutLock() {
        AsyncLock asyncLock = new AsyncLock();
        assertFalse("Release must failed because keys are not matched", asyncLock.release(UUID.randomUUID()));
    }

    private CompletableFuture<Void> executeDelayWork(AsyncLock asyncLock) {
        return asyncLock.lock()
                .thenCompose(key -> this.delay(100 + this.random.nextInt(100))
                        .exceptionally(ignored -> null)
                        .thenApply(ignored -> asyncLock.release(key)))
                .thenApply(ignored -> null);
    }

    private CompletableFuture<Void> delay(int timeInMilliseconds) {
        this.testValue += 1;
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.executorService.submit(() -> {
            try {
                Thread.sleep(timeInMilliseconds + this.random.nextInt(100));
                result.complete(null);
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }
}
