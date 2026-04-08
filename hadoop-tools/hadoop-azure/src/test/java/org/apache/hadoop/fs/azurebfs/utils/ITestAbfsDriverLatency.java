package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

/**
 * Measures e2e read latency through the ABFS driver.
 *
 * Compare P50/P90/P99 from this test against AbfsCheckLatency
 * (plain HTTP app) to isolate driver overhead from network latency.
 *
 * Run:
 *   mvn test -Dtest=ITestAbfsDriverLatency
 *            -Dlatency.jobs=10
 *            -Dlatency.files=100
 *            -Dlatency.chunkKB=64
 *            -Dlatency.fileSizeMB=1024
 */
public class ITestAbfsDriverLatency extends AbstractAbfsIntegrationTest {

  // ===== CONFIG =====
  private int NUM_JOBS;
  private int FILES_PER_JOB;
  private int CHUNK_SIZE_KB;
  private int FILE_SIZE_MB;

  private int CHUNK_SIZE;
  private long FILE_SIZE;

  // one buffer per thread — no allocations per chunk
  private ThreadLocal<byte[]> threadBuffer;

  // all chunk latencies across all jobs and files
  private final List<Long> readLatencies =
      Collections.synchronizedList(new ArrayList<>());

  // test file paths created during setup
  private final List<Path> testFiles = new ArrayList<>();

  // ===== CONSTRUCTOR =====
  public ITestAbfsDriverLatency() throws Exception {
    super();
  }

  // ===== SETUP =====
  @BeforeEach
  public void setUp() throws Exception {
    NUM_JOBS      = Integer.parseInt(System.getProperty("latency.jobs",       "2"));
    FILES_PER_JOB = Integer.parseInt(System.getProperty("latency.files",      "5"));
    CHUNK_SIZE_KB = Integer.parseInt(System.getProperty("latency.chunkKB",    "64"));
    FILE_SIZE_MB  = Integer.parseInt(System.getProperty("latency.fileSizeMB", "10"));

    CHUNK_SIZE    = CHUNK_SIZE_KB * 1024;
    FILE_SIZE     = (long) FILE_SIZE_MB * 1024 * 1024;

    threadBuffer  = ThreadLocal.withInitial(() -> new byte[CHUNK_SIZE]);

    System.out.println("===== CONFIG =====");
    System.out.printf("Jobs:        %d%n",    NUM_JOBS);
    System.out.printf("Files/job:   %d%n",    FILES_PER_JOB);
    System.out.printf("Chunk size:  %d KB%n", CHUNK_SIZE_KB);
    System.out.printf("File size:   %d MB%n", FILE_SIZE_MB);
    System.out.println();

    createTestFiles();

    // warmup — not recorded
    System.out.println("Warmup...");
    readFile(getFileSystem(), testFiles.get(0));
    readLatencies.clear();
    Thread.sleep(2000);
  }

  // ===== TEARDOWN =====
  @AfterEach
  public void tearDown() throws Exception {
    FileSystem fs = getFileSystem();
    for (Path p : testFiles) {
      try { fs.delete(p, false); }
      catch (Exception ignored) {}
    }
    testFiles.clear();
    readLatencies.clear();
  }

  // ===== TEST =====
  @Test
  public void testE2EReadLatency() throws Exception {
    System.out.println("Starting benchmark...\n");
    long wallStart = System.currentTimeMillis();

    List<Thread> threads = new ArrayList<>();
    for (int j = 0; j < NUM_JOBS; j++) {
      final int jobId = j;
      Thread t = new Thread(() -> runJob(jobId));
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) t.join();
    long wallMs = System.currentTimeMillis() - wallStart;

    printResults(wallMs);
  }

  // ===== CREATE TEST FILES =====
  private void createTestFiles() throws Exception {
    FileSystem fs = getFileSystem();
    int total = NUM_JOBS * FILES_PER_JOB;

    System.out.println("Creating " + total + " test files of "
        + FILE_SIZE_MB + " MB each...");

    for (int i = 0; i < total; i++) {
      Path path = new Path(getTestPath1(), "latency_test_file_" + i);
      testFiles.add(path);

      try (FSDataOutputStream out = fs.create(path, true)) {
        byte[] buf = new byte[4 * 1024 * 1024];
        long written = 0;
        while (written < FILE_SIZE) {
          int toWrite = (int) Math.min(buf.length, FILE_SIZE - written);
          out.write(buf, 0, toWrite);
          written += toWrite;
        }
      }
    }

    System.out.println("Done.\n");
  }

  // ===== ONE JOB =====
  private void runJob(int jobId) {
    try {
      FileSystem fs = getFileSystem();

      int start = jobId * FILES_PER_JOB;
      int end   = start + FILES_PER_JOB;

      for (int i = start; i < end; i++) {
        try {
          readFile(fs, testFiles.get(i));
        } catch (Exception e) {
          System.err.println("Job " + jobId
              + " failed on file " + i + ": " + e.getMessage());
        }
      }

      System.out.println("Job " + jobId + " done");

    } catch (Exception e) {
      System.err.println("Job " + jobId + " failed: " + e.getMessage());
    }
  }

  // ===== READ ONE FILE via ABFS driver =====
  private void readFile(FileSystem fs, Path path) throws Exception {
    byte[] buf = threadBuffer.get();

    try (FSDataInputStream in = fs.open(path)) {
      int n;
      while (true) {
        long start = System.currentTimeMillis();
        n = in.read(buf, 0, CHUNK_SIZE);
        long latency = System.currentTimeMillis() - start;

        if (n == -1) break;

        readLatencies.add(latency);
        // discard — no processing
      }
    }
  }

  // ===== HELPER =====
  private Path getTestPath1() throws IOException {
    return new Path(getFileSystem().getUri().toString()
        + "/latency-benchmark");
  }

  // ===== PRINT RESULTS =====
  private void printResults(long wallMs) {
    int chunksPerFile = (int) Math.ceil((double) FILE_SIZE / CHUNK_SIZE);

    System.out.println("\n===== RESULTS =====");
    System.out.printf("Wall time:      %d ms%n", wallMs);
    System.out.printf("Total reads:    %d%n",     readLatencies.size());
    System.out.printf("Expected reads: %d%n",
        NUM_JOBS * FILES_PER_JOB * chunksPerFile);

    System.out.println("\n===== READ LATENCY via ABFS DRIVER =====");
    System.out.printf("Min:  %d ms%n",   readLatencies.stream().mapToLong(l->l).min().orElse(0));
    System.out.printf("Avg:  %.0f ms%n", readLatencies.stream().mapToLong(l->l).average().orElse(0));
    System.out.printf("P50:  %d ms%n",   percentile(50));
    System.out.printf("P90:  %d ms%n",   percentile(90));
    System.out.printf("P99:  %d ms%n",   percentile(99));
    System.out.printf("Max:  %d ms%n",   readLatencies.stream().mapToLong(l->l).max().orElse(0));
  }

  // ===== PERCENTILE =====
  private long percentile(double p) {
    List<Long> sorted = new ArrayList<>(readLatencies);
    Collections.sort(sorted);
    int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
    return sorted.get(Math.max(idx, 0));
  }
}