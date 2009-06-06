package org.apache.hadoop.hbase.io.hfile;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


/**
 * Simple one RFile soft reference cache.
 */
public class SimpleBlockCache implements BlockCache {
  private static class Ref extends SoftReference<ByteBuffer> {
    public String blockId;
    public Ref(String blockId, ByteBuffer buf, ReferenceQueue q) {
      super(buf, q);
      this.blockId = blockId;
    }
  }
  private Map<String,Ref> cache = 
    new HashMap<String,Ref>();

  private ReferenceQueue q = new ReferenceQueue();
  public int dumps = 0;
  
  /**
   * Constructor
   */
  public SimpleBlockCache() {
    super();
  }
  
  void processQueue() {
    Ref r;
    while ( (r = (Ref)q.poll()) != null) {
      cache.remove(r.blockId);
      dumps++;
    }
  }

  /**
   * @return the size
   */
  public synchronized int size() {
    processQueue();
    return cache.size();
  }
  @Override
  public synchronized ByteBuffer getBlock(String blockName) {
    processQueue(); // clear out some crap.
    Ref ref = cache.get(blockName);
    if (ref == null)
      return null;
    return ref.get();
  }

  @Override
  public synchronized void cacheBlock(String blockName, ByteBuffer buf) {
    cache.put(blockName, new Ref(blockName, buf, q));
  }
}
