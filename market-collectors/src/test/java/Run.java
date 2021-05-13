import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;

import java.util.Date;

public class Run {
  public static void main(String[] args) {

    VertxOptions vertxOptions = new VertxOptions();
    Vertx vertx = Vertx.vertx(vertxOptions);
    vertx.createHttpClient(new HttpClientOptions().setDefaultHost("api.huobiasia.vip")
        .setProxyOptions(new ProxyOptions().setHost("localhost").setPort(1080).setType(ProxyType.HTTP)))

        .webSocket("/ws", ar -> {
          WebSocket websocket = ar.result();

          if (ar.failed()) {
            ar.cause().printStackTrace();
          } else {
            websocket.writeTextMessage("{\"id\":1620696851384,\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@kline_1m\",\"btcusdt@kline_5m\",\"btcusdt@kline_15m\",\"btcusdt@kline_30m\",\"btcusdt@kline_60m\",\"btcusdt@kline_4h\",\"btcusdt@kline_1d\",\"btcusdt@kline_1w\"]}");
            websocket.frameHandler(frame -> {
              if (frame.isText()) {
//                System.out.println(frame.textData());
//                websocket.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
              } else if (frame.isBinary()) {
                System.out.println(frame.binaryData().toJson().toString());
              } else if (frame.isClose()) {
                System.out.println("close");
              } else if (frame.isPing()) {
                System.out.println("ping: " + frame.textData());
                websocket.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
              } else if (frame.isContinuation()) {
                System.out.println("continuation");
              }
            });

            websocket.exceptionHandler(throwable -> {
              throwable.printStackTrace();
              System.out.println(new Date());
            });
            websocket.endHandler(endAr -> {
              System.out.println(websocket.textHandlerID() + " end handler");
            });

            websocket.closeHandler(closeAr -> {
              System.out.println(websocket.textHandlerID() + " close handler " + new Date());
            });
            System.out.println("success");
          }
        });
  }
}
