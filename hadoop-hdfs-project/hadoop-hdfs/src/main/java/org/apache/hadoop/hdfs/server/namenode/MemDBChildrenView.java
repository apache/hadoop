package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

class MemDBChildrenView extends DBChildrenView {
  private final NavigableMap<ByteBuffer, Long> childrenMap;

  MemDBChildrenView(NavigableMap<ByteBuffer, Long> childrenMap) {
    this.childrenMap = childrenMap;
  }

  private ByteBuffer start;

  @Override
  public int size() {
    return childrenMap.size();
  }

  @Override
  public void seekTo(ByteBuffer start) {
    this.start = start;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Iterator<Map.Entry<ByteBuffer, Long>> iterator() {
    return childrenMap.tailMap(start).entrySet().iterator();
  }
}
