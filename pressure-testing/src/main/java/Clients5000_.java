import io.vertx.core.Vertx;

public class Clients5000_ {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    for (int i = 0; i < 5000; i++) {
      vertx.createHttpClient().webSocket(7776, "localhost", "/")
          .onSuccess(socket -> {
            socket.writeTextMessage("{\n" +
                "\t\"id\": \"id1\",\n" +
                "\t\"sub\": \"market.btcusdt.kline.1min\"\n" +
                "}");
            socket.writeTextMessage("{\n" +
                "\t\"id\": \"id1\",\n" +
                "\t\"sub\": \"market.ETH-USDT.kline.1min\"\n" +
                "}");
            socket.writeTextMessage("{\n" +
                "\t\"id\": \"id1\",\n" +
                "\t\"sub\": \"market.DOG-USDT.kline.1min\"\n" +
                "}");
            socket.writeTextMessage("{\n" +
                "\t\"id\": \"id1\",\n" +
                "\t\"sub\": \"market.EOS-USDT.kline.1min\"\n" +
                "}");
            socket.frameHandler(frame -> {
              if (frame.isText()) {
                System.out.println(frame.textData());
              }
            });
          })
          .onFailure(Throwable::printStackTrace);
    }
  }
}
