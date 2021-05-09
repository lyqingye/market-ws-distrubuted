package com.tqxd.jys.core.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

public final class BootstrapFactory {
  private static volatile Bootstrap INSTANCE;

  public static Bootstrap create() {
    if (INSTANCE == null) {
      synchronized (BootstrapFactory.class) {
        ServiceLoader<Bootstrap> bootstraps = ServiceLoader.load(Bootstrap.class);
        Iterator<Bootstrap> it = bootstraps.iterator();
        if (!it.hasNext()) {
          throw new IllegalStateException("could not found bootstrap service! just using default bootstrap");
        } else {
          INSTANCE = it.next();
        }
      }
    }
    return INSTANCE;
  }
}
