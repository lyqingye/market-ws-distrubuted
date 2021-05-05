import io.vertx.core.Vertx;

public class Clients5000_ {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    for (int i = 0; i < 5000; i++) {
      vertx.createHttpClient().webSocket(7776, "localhost", "/")
        .onSuccess(socket -> {
          socket.writeTextMessage("{\n" +
            "\t\"id\": \"id1\",\n" +
            "\t\"sub\": \"market.BTC-USDT.kline.1min\"\n" +
            "}");
        });
    }
  }
}
