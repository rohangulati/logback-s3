package ch.qos.logback.core.rolling;

import java.io.File;

public interface Storage {
  void start();

  void put(String key, String path, File file);

  void stop();
}
