package ch.qos.logback.core.rolling;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class Futures {
  public static <T> CompletableFuture<T> completable(Future<T> future) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return future.get();
          } catch (Throwable t) {
            return null;
          }
        });
  }
}
