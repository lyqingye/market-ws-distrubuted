import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;

import java.util.Date;

public class Run {
  public static void main(String[] args) {
//    Vertx.vertx().createNetServer()
//        .connectHandler(ar -> {
//
//        })
//        .listen(15701,"0.0.0.0")
//        .onFailure(Throwable::printStackTrace);

//    Vertx.vertx().createNetClient()
//        .connect(15701,"192.168.248.128")
//        .onSuccess(ar -> {
//          System.out.println();
//        }).onFailure(Throwable::printStackTrace);

//    Vertx.vertx().createNetClient()
//        .connect(65497, "192.168.2.94")
//        .onSuccess(ar -> {
//          System.out.println();
//        }).onFailure(Throwable::printStackTrace);


    VertxOptions vertxOptions = new VertxOptions();
    Vertx vertx = Vertx.vertx(vertxOptions);
    vertx.createHttpClient(new HttpClientOptions().setDefaultHost("market-pre.sgpexchange.com"))

        .webSocket("/", ar -> {
          WebSocket websocket = ar.result();

          if (ar.failed()) {
            ar.cause().printStackTrace();
          } else {
            websocket.writeTextMessage("{ \"method\":\"kline.subscribe\", \"id\":1, \"params\":[ \"AITDUSDT\", 60 ] }");
            websocket.frameHandler(frame -> {
              if (frame.isText()) {
                System.out.println(frame.textData());
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
