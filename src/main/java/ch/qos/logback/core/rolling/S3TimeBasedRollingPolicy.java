package ch.qos.logback.core.rolling;

import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.Compressor;
import ch.qos.logback.core.rolling.helper.FileFilterUtil;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.rolling.impl.NoopStorage;
import ch.qos.logback.core.rolling.impl.S3Storage;

import java.io.File;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.*;

public class S3TimeBasedRollingPolicy<E> extends TimeBasedRollingPolicy<E> {

  private String bucket;
  private String folderPatternStr;
  private FileNamePattern folderPattern;
  private int concurrency = 4;
  private boolean enableLogUpload;
  private Storage storage;

  private CompletableFuture<?> uploadFuture;
  // for uploading
  private ExecutorService executor;
  private Compressor compressor;

  @Override
  public void start() {
    this.folderPattern = new FileNamePattern(folderPatternStr, this.context);
    super.start();
    this.compressor = new Compressor(compressionMode);
    this.compressor.setContext(this.context);
    this.executor = Executors.newFixedThreadPool(concurrency);
    this.storage = (enableLogUpload ? new S3Storage() : NoopStorage.INSTANCE);
    this.storage.start();
    // register shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHookRunnable()));
  }

  @Override
  public void rollover() throws RolloverFailure {
    super.rollover();
    String elapsedPeriodsFileName =
        timeBasedFileNamingAndTriggeringPolicy.getElapsedPeriodsFileName();
    triggerUpload(elapsedPeriodsFileName);
  }

  private void triggerUpload(String fileName) {
    if (compressionMode == CompressionMode.NONE) {
      if (getParentsRawFileProperty() != null) {
        uploadFuture = upload(fileName);
      }
    } else {
      // compress in normal rollover is done async, so using the compression future and doing upload
      // after the compression is complete
      uploadFuture =
          Futures.completable(compressionFuture)
              .thenCompose(
                  a -> {
                    // compression done one start uploading
                    String compressedName = fileName + "." + compressionMode.name().toLowerCase();
                    return upload(compressedName);
                  });
    }
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getFolderPattern() {
    return folderPatternStr;
  }

  public void setFolderPattern(String folderPattern) {
    this.folderPatternStr = folderPattern;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  public boolean isEnableLogUpload() {
    return enableLogUpload;
  }

  public void setEnableLogUpload(boolean enableLogUpload) {
    this.enableLogUpload = enableLogUpload;
  }

  @Override
  public String toString() {
    return S3TimeBasedRollingPolicy.class.getCanonicalName() + hashCode();
  }

  private String compressSync(String file) {
    if (compressionMode != CompressionMode.NONE) {
      String stem = FileFilterUtil.afterLastSlash(file);
      // compress sync
      compressor.compress(file, file, stem);
      return file + "." + compressionMode.name().toLowerCase();
    }
    return file;
  }

  private CompletableFuture<?> upload(String fileName) {
    if (fileName == null) {
      return CompletableFuture.completedFuture(null);
    }
    File file = new File(fileName);
    if (!file.exists()) {
      return CompletableFuture.completedFuture(null);
    }
    StringBuilder sb = new StringBuilder();
    if (folderPattern != null) {
      sb.append(folderPattern.convert(new Date())).append("/");
    }
    sb.append(file.getName());
    final Future<?> future = executor.submit(new S3Uploader(file, sb.toString()));
    return Futures.completable(future);
  }

  private void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
    // wait for the last rolled file to be compressed and uploaded
    if (uploadFuture != null) {
      uploadFuture.get(10, TimeUnit.MINUTES);
    }
    // make the compressor executor shutdown
    context.getScheduledExecutorService().shutdown();
    // make the uploader executor shutdown
    executor.shutdown();
    // wait for all tasks to complete
    context.getScheduledExecutorService().awaitTermination(10, TimeUnit.MINUTES);
    executor.awaitTermination(10, TimeUnit.MINUTES);
  }

  private class ShutdownHookRunnable implements Runnable {
    @Override
    public void run() {
      try {
        // Find the last file name which is not compressed
        String file = getActiveFileName();
        upload(compressSync(file));
        shutdown();
      } catch (Throwable t) {
        executor.shutdownNow();
      }
    }
  }

  private class S3Uploader implements Runnable {
    private final File file;
    private final String path;

    S3Uploader(File file, String path) {
      this.file = file;
      this.path = path;
    }

    @Override
    public void run() {
      try {
        addInfo(MessageFormat.format("Uploading file {0} to s3", file));
        S3TimeBasedRollingPolicy.this.storage.put(bucket, path, file);
        addInfo(MessageFormat.format("Successfully uploaded file {0} to s3", file));
      } catch (Throwable throwable) {
        addInfo(MessageFormat.format("Could not upload file {0} to s3", file), throwable);
      }
    }
  }
}
