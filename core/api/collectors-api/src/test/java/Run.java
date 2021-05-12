import com.tqxd.jys.api.collectors.CollectorOpenApi;
import io.vertx.core.Vertx;

public class Run {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    CollectorOpenApi.createProxy(vertx);
  }
}
