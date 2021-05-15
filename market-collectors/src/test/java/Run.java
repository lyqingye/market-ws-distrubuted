import io.vertx.core.Vertx;

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

    Vertx.vertx().createNetClient()
        .connect(65497, "192.168.2.94")
        .onSuccess(ar -> {
          System.out.println();
        }).onFailure(Throwable::printStackTrace);
//    VertxOptions vertxOptions = new VertxOptions();
//    Vertx vertx = Vertx.vertx(vertxOptions);
//    vertx.createHttpClient(new HttpClientOptions().setDefaultHost("api.huobiasia.vip")
//        .setProxyOptions(new ProxyOptions().setHost("localhost").setPort(1080).setType(ProxyType.HTTP)))
//
//        .webSocket("/ws", ar -> {
//          WebSocket websocket = ar.result();
//
//          if (ar.failed()) {
//            ar.cause().printStackTrace();
//          } else {
//            websocket.writeTextMessage("{\"id\":1620696851384,\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@kline_1m\",\"btcusdt@kline_5m\",\"btcusdt@kline_15m\",\"btcusdt@kline_30m\",\"btcusdt@kline_60m\",\"btcusdt@kline_4h\",\"btcusdt@kline_1d\",\"btcusdt@kline_1w\"]}");
//            websocket.frameHandler(frame -> {
//              if (frame.isText()) {
////                System.out.println(frame.textData());
////                websocket.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
//              } else if (frame.isBinary()) {
//                System.out.println(frame.binaryData().toJson().toString());
//              } else if (frame.isClose()) {
//                System.out.println("close");
//              } else if (frame.isPing()) {
//                System.out.println("ping: " + frame.textData());
//                websocket.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
//              } else if (frame.isContinuation()) {
//                System.out.println("continuation");
//              }
//            });
//
//            websocket.exceptionHandler(throwable -> {
//              throwable.printStackTrace();
//              System.out.println(new Date());
//            });
//            websocket.endHandler(endAr -> {
//              System.out.println(websocket.textHandlerID() + " end handler");
//            });
//
//            websocket.closeHandler(closeAr -> {
//              System.out.println(websocket.textHandlerID() + " close handler " + new Date());
//            });
//            System.out.println("success");
//          }
//        });
  }
}
