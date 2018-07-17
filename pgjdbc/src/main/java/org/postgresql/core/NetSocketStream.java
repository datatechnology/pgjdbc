package org.postgresql.core;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class NetSocketStream {

    private Buffer readBuffer = null;
    private int readPos = -1;
    private Queue<Buffer> readableBuffers = new ArrayDeque<>();
    private Buffer writeBuffer = Buffer.buffer();
    private Queue<CompletableFuture<Void>> readerTasks = new ArrayDeque<>();
    private NetSocket netSocket;
    private Throwable error;
    private boolean closed;

    public NetSocketStream(NetSocket netSocket) {
        this.netSocket = netSocket;
        this.netSocket.handler(this::onDataAvaialble);
        this.netSocket.exceptionHandler(this::onChannelFaulted);
        this.netSocket.closeHandler(ignored -> {
            this.closed = true;
            CompletableFuture<Void> pendingRead = this.readerTasks.poll();
            while (pendingRead != null) {
                pendingRead.completeExceptionally(new IOException("socket closed"));
                pendingRead = this.readerTasks.poll();
            }
        });
    }

    public synchronized boolean moreToRead() {
        return this.checkOrResetReadBuffer();
    }

    public synchronized CompletableFuture<Integer> read() {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        this.handleReadByteReady(result, null, true);
        return result;
    }

    public synchronized CompletableFuture<Integer> peek() {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        this.handleReadByteReady(result, null, false);
        return result;
    }

    public synchronized CompletableFuture<Void> read(byte[] buf) {
        return read(buf, 0, buf.length);
    }

    public synchronized CompletableFuture<Void> read(byte[] buf, int offset, int size) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.handleReadBufferReady(result, buf, offset, size, null);
        return result;
    }

    public synchronized CompletableFuture<Void> skip(int size) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.handleReadSkip(result, size, null);
        return result;
    }

    public synchronized CompletableFuture<byte[]> readUntil(byte value) {
        CompletableFuture<Buffer> result = new CompletableFuture<>();
        this.handleReadUntil(result, Buffer.buffer(), value, null);
        return result.thenApply(Buffer::getBytes);
    }

    public synchronized Buffer getWriteBuffer() {
        return this.writeBuffer;
    }

    public synchronized void flush() throws Throwable {
        if (this.closed) {
            throw new IOException("write after closed");
        }

        if (this.error != null) {
            throw this.error;
        }

        if (this.writeBuffer != null) {
            this.netSocket.write(this.writeBuffer);
            this.writeBuffer = Buffer.buffer();
        }
    }

    private synchronized void handleReadUntil(CompletableFuture<Buffer> result, Buffer buffer, byte value, Throwable error) {
        if (error != null) {
            this.error = error;
            result.completeExceptionally(error);
            return;
        }

        boolean endRead = false;
        while (this.checkOrResetReadBuffer() && !endRead) {
            byte b = this.readBuffer.getByte(this.readPos++);
            buffer.appendByte(b);
            endRead = value == b;
        }

        if (endRead) {
            result.complete(buffer);
            CompletableFuture<Void> nextTask = this.readerTasks.poll();
            if (nextTask != null) {
                nextTask.complete(null);
            }
        } else {
            if (this.closed) {
                result.completeExceptionally(new IOException("read after closed"));
                return;
            }

            CompletableFuture<Void> task = new CompletableFuture<>();
            task.whenComplete((ignored, err) -> this.handleReadUntil(result, buffer, value, err));
            this.readerTasks.add(task);
        }
    }

    private synchronized void handleReadSkip(CompletableFuture<Void> result, int size, Throwable error) {
        if (error != null) {
            this.error = error;
            result.completeExceptionally(error);
            return;
        }

        while (this.checkOrResetReadBuffer() && size > 0) {
            int bytesToSkip = Math.min(size, this.readBuffer.length() - this.readPos);
            this.readPos += bytesToSkip;
            size -= bytesToSkip;
        }

        if (size <= 0) {
            // should never be less than zero
            result.complete(null);
            CompletableFuture<Void> nextTask = this.readerTasks.poll();
            if (nextTask != null) {
                nextTask.complete(null);
            }
        } else {
            int sizeLeft = size;
            if (this.closed) {
                result.completeExceptionally(new IOException("skip after closed"));
                return;
            }

            CompletableFuture<Void> task = new CompletableFuture<>();
            task.whenComplete((ignored, err) -> this.handleReadSkip(result, sizeLeft, err));
            this.readerTasks.add(task);
        }
    }

    private synchronized void handleReadByteReady(CompletableFuture<Integer> result, Throwable error, boolean advancePos) {
        if (error != null) {
            this.error = error;
            result.completeExceptionally(error);
            return;
        }

        if (this.checkOrResetReadBuffer()) {
            result.complete(this.readBuffer.getByte(this.readPos) & 0xFF);
            if (advancePos) {
                this.readPos += 1;
            }

            CompletableFuture<Void> nextTask = this.readerTasks.poll();
            if (nextTask != null) {
                nextTask.complete(null);
            }
        } else {
            if (this.closed) {
                result.complete(-1);
                return;
            }

            CompletableFuture<Void> task = new CompletableFuture<>();
            task.whenComplete((ignored, err) -> this.handleReadByteReady(result, err, advancePos));
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

        while (this.checkOrResetReadBuffer() && bytesToRead > 0) {
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
            if (this.closed) {
                result.completeExceptionally(new IOException("read after socket closed"));
                return;
            }

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
        if (buffer == null || buffer.length() == 0) {
            // empty buffer, skip it
            return;
        }

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
            task.completeExceptionally(error);
            task = this.readerTasks.poll();
        }
    }
}
