package org.postgresql.test.core;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.junit.Test;
import org.postgresql.core.NetSocketStream;
import org.postgresql.util.VertxHelper;

public class NetSocketStreamTest {

    @Test
    public void testReadWrite() throws Throwable {
        Vertx vertx = Vertx.vertx();
        NetClient netClient = vertx.createNetClient();
        NetSocket socket = VertxHelper
                .vertxTCompletableFuture((Handler<AsyncResult<NetSocket>> h) -> netClient.connect(80, "www.jdzmj.org", h))
                .get();
        NetSocketStream stream = new NetSocketStream(socket);
        stream.getWriteBuffer().appendString("GET")
                .appendString("\r\n\r\n");
        stream.flush();
        byte[] data = new byte[20];
        stream.read(data).get();
        System.out.println(new String(data));
        socket.close();
    }
}
