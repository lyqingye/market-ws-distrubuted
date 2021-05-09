import com.tqxd.jys.core.spi.MessageBusFactory;
import io.vertx.core.Vertx;

public class Run {
  public static void main(String[] args) {
    MessageBusFactory.create(Vertx.vertx())
      .onFailure(Throwable::printStackTrace)
      .onSuccess(none -> {
        System.out.println();
      });
    System.out.println();
  }
}
