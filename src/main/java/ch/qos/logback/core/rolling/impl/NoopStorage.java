package ch.qos.logback.core.rolling.impl;

import ch.qos.logback.core.rolling.Storage;

import java.io.File;

public class NoopStorage implements Storage {
  public static final NoopStorage INSTANCE = new NoopStorage();

  private NoopStorage() {
    // no instance creation
  }

  @Override
  public void start() {}

  @Override
  public void put(String key, String path, File file) {}

  @Override
  public void stop() {}
}
