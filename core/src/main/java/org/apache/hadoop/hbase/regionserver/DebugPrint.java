package org.apache.hadoop.hbase.regionserver;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DebugPrint {

private static final AtomicBoolean enabled = new AtomicBoolean(false);
  private static final Object sync = new Object();
  public static StringBuilder out = new StringBuilder();

  static public void enable() {
    enabled.set(true);
  }
  static public void disable() {
    enabled.set(false);
  }

  static public void reset() {
    synchronized (sync) {
      enable(); // someone wants us enabled basically.

      out = new StringBuilder();
    }
  }
  static public void dumpToFile(String file) throws IOException {
    FileWriter f = new FileWriter(file);
    synchronized (sync) {
      f.write(out.toString());
    }
    f.close();
  }

  public static void println(String m) {
    if (!enabled.get()) {
      System.out.println(m);
      return;
    }

    synchronized (sync) {
      String threadName = Thread.currentThread().getName();
      out.append("<");
      out.append(threadName);
      out.append("> ");
      out.append(m);
      out.append("\n");
    }
  }
}