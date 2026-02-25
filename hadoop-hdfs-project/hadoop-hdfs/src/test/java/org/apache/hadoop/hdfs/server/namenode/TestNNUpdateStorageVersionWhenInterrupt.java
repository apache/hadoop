package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNNUpdateStorageVersionWhenInterrupt {
  static String nnDir =
      GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator + "namenode";
  static NNStorage nnStorage;
  static GenericTestUtils.LogCapturer nnStorageLog;

  @BeforeAll
  public static void setUp() throws IOException, URISyntaxException {
    String scheme = "file:///";
    Collection<URI> dirs = new ArrayList<>();
    dirs.add(new URI(scheme + nnDir));
    nnStorage = new NNStorage(new Configuration(), dirs, dirs);

    StorageDirectory sd = new StorageDirectory(new File(nnDir));
    Path versionFile = sd.getVersionFile().toPath();
    Files.createDirectories(versionFile.getParent());
    if (!Files.exists(versionFile)) {
      Files.createFile(versionFile);
    }

    nnStorageLog = GenericTestUtils.LogCapturer.captureLogs(NNStorage.LOG);
  }

  @Test
  public void test()
      throws IOException, URISyntaxException, InterruptedException, TimeoutException {
    Thread thread = new updateVersionFileThread(nnStorage);
    assertEquals(1, nnStorage.getNumStorageDirs());

    thread.start();
    thread.interrupt();

    GenericTestUtils.waitFor(
        () -> nnStorageLog.getOutput().contains("java.nio.channels.ClosedByInterruptException"),
        200, 20000);
    assertEquals(1, nnStorage.getNumStorageDirs());
  }

  private static class updateVersionFileThread extends Thread {
    NNStorage nnStorage;

    public updateVersionFileThread(NNStorage nnStorage) {
      this.nnStorage = nnStorage;
    }

    @Override
    public void run() {
      try {
        nnStorage.writeAll();
      } catch (IOException ignored) {

      }
    }
  }

}
