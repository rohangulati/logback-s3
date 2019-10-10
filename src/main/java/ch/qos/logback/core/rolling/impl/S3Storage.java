package ch.qos.logback.core.rolling.impl;

import ch.qos.logback.core.rolling.Storage;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.File;

public class S3Storage implements Storage {

  private AmazonS3 s3;

  @Override
  public void start() {
    this.s3 =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new InstanceProfileCredentialsProvider(true))
            .build();
  }

  @Override
  public void put(String key, String path, File file) {
    s3.putObject(key, path, file);
  }

  @Override
  public void stop() {}
}
