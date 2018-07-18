/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

/**
 * Wrapper around the raw connection to the server that implements some basic primitives
 * (reading/writing formatted data, doing string encoding, etc).
 * <p>
 * In general, instances of PGStream are not threadsafe; the caller must ensure that only one thread
 * at a time is accessing a particular PGStream instance.
 */
public class PGStream implements Closeable, Flushable {

    private final HostSpec hostSpec;

    private final byte[] _int4buf;
    private final byte[] _int2buf;

    private int timeout;
    private NetSocket netSocket;
    private NetClient netClient;
    private NetSocketStream stream;
    private byte[] streamBuffer;

    private Encoding encoding;

    /**
     * Constructor: Connect to the PostgreSQL back end and return a stream connection.
     *
     * @param netClient net client to use when creating sockets
     * @param hostSpec  the host and port to connect to
     * @throws IOException if an IOException occurs below it.
     */
    public PGStream(NetClient netClient, NetSocket netSocket, HostSpec hostSpec, int timeout) throws IOException {
        this.hostSpec = hostSpec;
        this.netClient = netClient;
        this.netSocket = netSocket;
        this.timeout = timeout;
        this.stream = new NetSocketStream(this.netSocket);
        setEncoding(Encoding.getJVMEncoding("UTF-8"));

        _int2buf = new byte[2];
        _int4buf = new byte[4];
    }

    public HostSpec getHostSpec() {
        return hostSpec;
    }

    public NetSocket getNetSocket() {
        return this.netSocket;
    }

    public NetClient getNetClient() {
        return this.netClient;
    }

    /**
     * Check for pending backend messages without blocking. Might return false when there actually are
     * messages waiting, depending on the characteristics of the underlying socket. This is used to
     * detect asynchronous notifies from the backend, when available.
     *
     * @return true if there is a pending backend message
     * @throws IOException if something wrong happens
     */
    public boolean hasMessagePending() throws IOException {
        return this.stream.moreToRead();
    }

    /**
     * Switch this stream to using a new socket. Any existing socket is <em>not</em> closed; it's
     * assumed that we are changing to a new socket that delegates to the original socket (e.g. SSL).
     *
     * @param socket the new socket to change to
     * @throws IOException if something goes wrong
     */
    public void changeSocket(Socket socket) throws IOException {
        throw new IOException("Not supported");
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public int getTimeout() {
        return this.timeout;
    }

    /**
     * Change the encoding used by this connection.
     *
     * @param encoding the new encoding to use
     * @throws IOException if something goes wrong
     */
    public void setEncoding(Encoding encoding) throws IOException {
        if (this.encoding != null && this.encoding.name().equals(encoding.name())) {
            return;
        }

        this.encoding = encoding;
    }

    /**
     * Sends a single character to the back end
     *
     * @param val the character to be sent
     * @throws IOException if an I/O error occurs
     */
    public void sendChar(int val) throws IOException {
        this.stream.getWriteBuffer().appendByte((byte) val);
    }

    /**
     * Sends a 4-byte integer to the back end
     *
     * @param val the integer to be sent
     * @throws IOException if an I/O error occurs
     */
    public void sendInteger4(int val) throws IOException {
        _int4buf[0] = (byte) (val >>> 24);
        _int4buf[1] = (byte) (val >>> 16);
        _int4buf[2] = (byte) (val >>> 8);
        _int4buf[3] = (byte) (val);
        this.stream.getWriteBuffer().appendBytes(_int4buf);
    }

    /**
     * Sends a 2-byte integer (short) to the back end
     *
     * @param val the integer to be sent
     * @throws IOException if an I/O error occurs or {@code val} cannot be encoded in 2 bytes
     */
    public void sendInteger2(int val) throws IOException {
        if (val < Short.MIN_VALUE || val > Short.MAX_VALUE) {
            throw new IOException("Tried to send an out-of-range integer as a 2-byte value: " + val);
        }

        _int2buf[0] = (byte) (val >>> 8);
        _int2buf[1] = (byte) val;
        this.stream.getWriteBuffer().appendBytes(_int2buf);
    }

    /**
     * Send an array of bytes to the backend
     *
     * @param buf The array of bytes to be sent
     * @throws IOException if an I/O error occurs
     */
    public void send(byte[] buf) throws IOException {
        this.stream.getWriteBuffer().appendBytes(buf);
    }

    /**
     * Send a fixed-size array of bytes to the backend. If {@code buf.length < siz}, pad with zeros.
     * If {@code buf.lengh > siz}, truncate the array.
     *
     * @param buf the array of bytes to be sent
     * @param siz the number of bytes to be sent
     * @throws IOException if an I/O error occurs
     */
    public void send(byte[] buf, int siz) throws IOException {
        send(buf, 0, siz);
    }

    /**
     * Send a fixed-size array of bytes to the backend. If {@code length < siz}, pad with zeros. If
     * {@code length > siz}, truncate the array.
     *
     * @param buf the array of bytes to be sent
     * @param off offset in the array to start sending from
     * @param siz the number of bytes to be sent
     * @throws IOException if an I/O error occurs
     */
    public void send(byte[] buf, int off, int siz) throws IOException {
        int bufamt = buf.length - off;
        this.stream.getWriteBuffer().appendBytes(buf, off, bufamt < siz ? bufamt : siz);
        for (int i = bufamt; i < siz; ++i) {
            this.stream.getWriteBuffer().appendByte((byte) 0);
        }
    }

    /**
     * Receives a single character from the backend, without advancing the current protocol stream
     * position.
     *
     * @return the character received
     * @throws IOException if an I/O Error occurs
     */
    public int peekChar() throws IOException {
        try {
            return this.stream.peek().get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }

    /**
     * Receives a single character from the backend
     *
     * @return the character received
     * @throws IOException if an I/O Error occurs
     */
    public int receiveChar() throws IOException {
        try {
            return this.stream.read().get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }

    /**
     * Receives a four byte integer from the backend
     *
     * @return the integer received from the backend
     * @throws IOException if an I/O error occurs
     */
    public int receiveInteger4() throws IOException {
        try {
            this.stream.read(_int4buf).get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }

        return (_int4buf[0] & 0xFF) << 24 | (_int4buf[1] & 0xFF) << 16 | (_int4buf[2] & 0xFF) << 8
                | _int4buf[3] & 0xFF;
    }

    /**
     * Receives a two byte integer from the backend
     *
     * @return the integer received from the backend
     * @throws IOException if an I/O error occurs
     */
    public int receiveInteger2() throws IOException {
        try {
            this.stream.read(_int2buf).get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }

        return (_int2buf[0] & 0xFF) << 8 | _int2buf[1] & 0xFF;
    }

    /**
     * Receives a fixed-size string from the backend.
     *
     * @param len the length of the string to receive, in bytes.
     * @return the decoded string
     * @throws IOException if something wrong happens
     */
    public String receiveString(int len) throws IOException {
        byte[] buffer = new byte[len];
        try {
            this.stream.read(buffer).get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }

        return encoding.decode(buffer, 0, len);
    }

    /**
     * Receives a fixed-size string from the backend, and tries to avoid "UTF-8 decode failed"
     * errors.
     *
     * @param len the length of the string to receive, in bytes.
     * @return the decoded string
     * @throws IOException if something wrong happens
     */
    public EncodingPredictor.DecodeResult receiveErrorString(int len) throws IOException {
        byte[] buffer = new byte[len];
        try {
            this.stream.read(buffer).get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }

        EncodingPredictor.DecodeResult res;
        try {
            String value = encoding.decode(buffer, 0, len);
            // no autodetect warning as the message was converted on its own
            return new EncodingPredictor.DecodeResult(value, null);
        } catch (IOException e) {
            res = EncodingPredictor.decode(buffer, 0, len);
            if (res == null) {
                Encoding enc = Encoding.defaultEncoding();
                String value = enc.decode(buffer, 0, len);
                return new EncodingPredictor.DecodeResult(value, enc.name());
            }

            return res;
        }
    }

    /**
     * Receives a null-terminated string from the backend. If we don't see a null, then we assume
     * something has gone wrong.
     *
     * @return string from back end
     * @throws IOException if an I/O error occurs, or end of file
     */
    public String receiveString() throws IOException {
        try {
            byte[] data = this.stream.readUntil((byte) '\0').get();
            return encoding.decode(data, 0, data.length - 1);
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }

    /**
     * Read a tuple from the back end. A tuple is a two dimensional array of bytes. This variant reads
     * the V3 protocol's tuple representation.
     *
     * @return tuple from the back end
     * @throws IOException if a data I/O error occurs
     */
    public byte[][] receiveTupleV3() throws IOException, OutOfMemoryError {
        // TODO: use l_msgSize
        int l_msgSize = receiveInteger4();
        int l_nf = receiveInteger2();
        byte[][] answer = new byte[l_nf][];

        OutOfMemoryError oom = null;
        for (int i = 0; i < l_nf; ++i) {
            int l_size = receiveInteger4();
            if (l_size != -1) {
                try {
                    answer[i] = new byte[l_size];
                    receive(answer[i], 0, l_size);
                } catch (OutOfMemoryError oome) {
                    oom = oome;
                    skip(l_size);
                }
            }
        }

        if (oom != null) {
            throw oom;
        }

        return answer;
    }

    /**
     * Reads in a given number of bytes from the backend
     *
     * @param siz number of bytes to read
     * @return array of bytes received
     * @throws IOException if a data I/O error occurs
     */
    public byte[] receive(int siz) throws IOException {
        byte[] answer = new byte[siz];
        receive(answer, 0, siz);
        return answer;
    }

    /**
     * Reads in a given number of bytes from the backend
     *
     * @param buf buffer to store result
     * @param off offset in buffer
     * @param siz number of bytes to read
     * @throws IOException if a data I/O error occurs
     */
    public void receive(byte[] buf, int off, int siz) throws IOException {
        try {
            this.stream.read(buf, off, siz).get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }

    public void skip(int size) throws IOException {
        try {
            this.stream.skip(size).wait();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }


    /**
     * Copy data from an input stream to the connection.
     *
     * @param inStream  the stream to read data from
     * @param remaining the number of bytes to copy
     * @throws IOException if a data I/O error occurs
     */
    public void sendStream(InputStream inStream, int remaining) throws IOException {
        int expectedLength = remaining;
        if (streamBuffer == null) {
            streamBuffer = new byte[8192];
        }

        while (remaining > 0) {
            int count = (remaining > streamBuffer.length ? streamBuffer.length : remaining);
            int readCount;

            try {
                readCount = inStream.read(streamBuffer, 0, count);
                if (readCount < 0) {
                    throw new EOFException(
                            GT.tr("Premature end of input stream, expected {0} bytes, but only read {1}.",
                                    expectedLength, expectedLength - remaining));
                }
            } catch (IOException ioe) {
                while (remaining > 0) {
                    send(streamBuffer, count);
                    remaining -= count;
                    count = (remaining > streamBuffer.length ? streamBuffer.length : remaining);
                }
                throw new PGBindException(ioe);
            }

            send(streamBuffer, readCount);
            remaining -= readCount;
        }
    }


    /**
     * Flush any pending output to the backend.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void flush() throws IOException {
        try {
            this.stream.flush();
        } catch (Throwable throwable) {
            throw new IOException(throwable.getMessage(), throwable);
        }
    }

    /**
     * Consume an expected EOF from the backend
     */
    public void receiveEOF() throws IOException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.stream.read()
                .whenComplete((value, error) -> {
                    if (error != null) {
                        result.completeExceptionally(error);
                        return;
                    }

                    if (value < 0) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(
                                new PSQLException(GT.tr("Expected an EOF from server, got: {0}", value),
                                        PSQLState.COMMUNICATION_ERROR));
                    }
                });
        try {
            result.get();
        } catch (Throwable err) {
            throw new IOException(err.getMessage(), err);
        }
    }

    /**
     * Closes the connection
     *
     * @throws IOException if an I/O Error occurs
     */
    @Override
    public void close() throws IOException {
        this.netSocket.close();
    }

    public void setNetworkTimeout(int milliseconds) throws IOException {
        //TODO need to set this
    }

    public int getNetworkTimeout() throws IOException {
        //TODO need to handle this
        return 100;
    }
}
