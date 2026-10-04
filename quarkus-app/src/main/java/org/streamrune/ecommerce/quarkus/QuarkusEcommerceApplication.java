package org.streamrune.ecommerce.quarkus;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;

public class QuarkusEcommerceApplication implements QuarkusApplication {
  public static void main(String[] args) {
    Quarkus.run(QuarkusEcommerceApplication.class, args);
  }

  @Override
  public int run(String... args) {
    Quarkus.waitForExit();
    return 0;
  }
}
