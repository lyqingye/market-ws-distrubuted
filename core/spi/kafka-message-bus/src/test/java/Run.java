import com.tqxd.jys.core.spi.MessageBus;

import java.util.ServiceLoader;

public class Run {
  public static void main(String[] args) {
    ServiceLoader<MessageBus> bootstraps = ServiceLoader.load(MessageBus.class);
    System.out.println();
  }
}
