package org.postgresql.core;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class NetSocketStream {

    private Buffer readBuffer = null;
    private int readPos = -1;
    private Queue<Buffer> readableBuffers = new ArrayDeque<>();
    private Buffer writeBuffer = null;
    private Queue<CompletableFuture<Void>> readerTasks = new ArrayDeque<>();
    private NetSocket netSocket;
    private Throwable error;

    public NetSocketStream(NetSocket netSocket) {
        this.netSocket = netSocket;
        this.netSocket.handler(this::onDataAvaialble);
        this.netSocket.exceptionHandler(this::onChannelFaulted);
    }

    public synchronized CompletableFuture<Byte> read() throws Throwable {
        if (this.error != null) {
            throw this.error;
        }

        CompletableFuture<Byte> result = new CompletableFuture<>();
        this.handleReadByteReady(result, null);
        return result;
    }

    public synchronized CompletableFuture<Void> read(byte[] buf) throws Throwable {
        if (this.error != null) {
            throw this.error;
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        this.handleReadBufferReady(result, buf, 0, buf.length, null);
        return result;
    }

    public synchronized Buffer getWriteBuffer() {
        return this.writeBuffer;
    }

    public synchronized void flush() throws Throwable {
        if (this.error != null) {
            throw this.error;
        }

        if (this.writeBuffer != null) {
            this.netSocket.write(this.writeBuffer);
            this.writeBuffer = Buffer.buffer();
        }
    }

    private synchronized void handleReadByteReady(CompletableFuture<Byte> result, Throwable error) {
        if (error != null) {
            this.error = error;
            result.completeExceptionally(error);
            return;
        }

        if (this.checkOrResetReadBuffer()) {
            result.complete(this.readBuffer.getByte(this.readPos++));
            CompletableFuture<Void> nextTask = this.readerTasks.poll();
            if (nextTask != null) {
                nextTask.complete(null);
            }
        } else {
            CompletableFuture<Void> task = new CompletableFuture<>();
            task.whenComplete((ignored, err) -> this.handleReadByteReady(result, err));
            this.readerTasks.add(task);
        }
    }

    private synchronized void handleReadBufferReady(
            CompletableFuture<Void> result,
            byte[] buf,
            int offset,
            int bytesToRead,
            Throwable error) {
        if (error != null) {
            this.error = error;
            result.completeExceptionally(error);
            return;
        }

        if (this.checkOrResetReadBuffer()) {
            int bytesRead = Math.min(bytesToRead, this.readBuffer.length() - this.readPos);
            this.readBuffer.getBytes(this.readPos, this.readPos + bytesRead, buf);
            offset += bytesRead;
            bytesToRead -= bytesRead;
            this.readPos += bytesRead;
        }

        if (bytesToRead <= 0) {
            result.complete(null);
            CompletableFuture<Void> nextTask = this.readerTasks.poll();
            if (nextTask != null) {
                nextTask.complete(null);
            }
        } else {
            int newOffset = offset;
            int newBytesToRead = bytesToRead;
            CompletableFuture<Void> task = new CompletableFuture<>();
            task.whenComplete((ignored, err) -> this.handleReadBufferReady(result, buf, newOffset, newBytesToRead, err));
            this.readerTasks.add(task);
        }
    }

    private boolean checkOrResetReadBuffer() {
        if (this.readBuffer == null || this.readBuffer.length() == this.readPos) {
            this.readBuffer = this.readableBuffers.poll();
            this.readPos = 0;
            return this.readBuffer != null;
        }

        return true;
    }

    private synchronized void onDataAvaialble(Buffer buffer) {
        this.readableBuffers.add(buffer);
        CompletableFuture<Void> task = this.readerTasks.poll();
        if (task != null) {
            task.complete(null);
        }
    }

    private synchronized void onChannelFaulted(Throwable error) {
        this.error = error;

        // notify all pending readers about the error
        CompletableFuture<Void> task = this.readerTasks.poll();
        while (task != null) {
            task.complete(null);
            task = this.readerTasks.poll();
        }
    }
}
