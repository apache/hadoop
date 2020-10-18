/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A global compressor/decompressor pool used to save and reuse 
 * (possibly native) compression/decompression codecs.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CodecPool {
  private static final Logger LOG = LoggerFactory.getLogger(CodecPool.class);
  
  /**
   * A global compressor pool used to save the expensive 
   * construction/destruction of (possibly native) decompression codecs.
   */
  private static final Map<Class<Compressor>, Set<Compressor>> compressorPool =
    new HashMap<Class<Compressor>, Set<Compressor>>();
  
  /**
   * A global decompressor pool used to save the expensive 
   * construction/destruction of (possibly native) decompression codecs.
   */
  private static final Map<Class<Decompressor>, Set<Decompressor>> decompressorPool =
    new HashMap<Class<Decompressor>, Set<Decompressor>>();

  private static <T> LoadingCache<Class<T>, AtomicInteger> createCache(
      Class<T> klass) {
    return CacheBuilder.newBuilder().build(
        new CacheLoader<Class<T>, AtomicInteger>() {
          @Override
          public AtomicInteger load(Class<T> key) throws Exception {
            return new AtomicInteger();
          }
        });
  }

  /**
   * Map to track the number of leased compressors
   */
  private static final LoadingCache<Class<Compressor>, AtomicInteger> compressorCounts =
      createCache(Compressor.class);

   /**
   * Map to tracks the number of leased decompressors
   */
  private static final LoadingCache<Class<Decompressor>, AtomicInteger> decompressorCounts =
      createCache(Decompressor.class);

  private static <T> T borrow(Map<Class<T>, Set<T>> pool,
                             Class<? extends T> codecClass) {
    T codec = null;
    
    // Check if an appropriate codec is available
    Set<T> codecSet;
    synchronized (pool) {
      codecSet = pool.get(codecClass);
    }

    if (codecSet != null) {
      synchronized (codecSet) {
        if (!codecSet.isEmpty()) {
          codec = codecSet.iterator().next();
          codecSet.remove(codec);
        }
      }
    }
    
    return codec;
  }

  private static <T> boolean payback(Map<Class<T>, Set<T>> pool, T codec) {
    if (codec != null) {
      Class<T> codecClass = ReflectionUtils.getClass(codec);
      Set<T> codecSet;
      synchronized (pool) {
        codecSet = pool.get(codecClass);
        if (codecSet == null) {
          codecSet = new HashSet<T>();
          pool.put(codecClass, codecSet);
        }
      }

      synchronized (codecSet) {
        return codecSet.add(codec);
      }
    }
    return false;
  }
  
  @SuppressWarnings("unchecked")
  private static <T> int getLeaseCount(
      LoadingCache<Class<T>, AtomicInteger> usageCounts,
      Class<? extends T> codecClass) {
    return usageCounts.getUnchecked((Class<T>) codecClass).get();
  }

  private static <T> void updateLeaseCount(
      LoadingCache<Class<T>, AtomicInteger> usageCounts, T codec, int delta) {
    if (codec != null) {
      Class<T> codecClass = ReflectionUtils.getClass(codec);
      usageCounts.getUnchecked(codecClass).addAndGet(delta);
    }
  }

  /**
   * Get a {@link Compressor} for the given {@link CompressionCodec} from the 
   * pool or a new one.
   *
   * @param codec the <code>CompressionCodec</code> for which to get the 
   *              <code>Compressor</code>
   * @param conf the <code>Configuration</code> object which contains confs for creating or reinit the compressor
   * @return <code>Compressor</code> for the given 
   *         <code>CompressionCodec</code> from the pool or a new one
   */
  public static Compressor getCompressor(CompressionCodec codec, Configuration conf) {
    Compressor compressor = borrow(compressorPool, codec.getCompressorType());
    if (compressor == null) {
      compressor = codec.createCompressor();
      LOG.info("Got brand-new compressor ["+codec.getDefaultExtension()+"]");
    } else {
      compressor.reinit(conf);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Got recycled compressor");
      }
    }
    if (compressor != null &&
        !compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      updateLeaseCount(compressorCounts, compressor, 1);
    }
    return compressor;
  }
  
  public static Compressor getCompressor(CompressionCodec codec) {
    return getCompressor(codec, null);
  }
  
  /**
   * Get a {@link Decompressor} for the given {@link CompressionCodec} from the
   * pool or a new one.
   *  
   * @param codec the <code>CompressionCodec</code> for which to get the 
   *              <code>Decompressor</code>
   * @return <code>Decompressor</code> for the given 
   *         <code>CompressionCodec</code> the pool or a new one
   */
  public static Decompressor getDecompressor(CompressionCodec codec) {
    Decompressor decompressor = borrow(decompressorPool, codec.getDecompressorType());
    if (decompressor == null) {
      decompressor = codec.createDecompressor();
      LOG.info("Got brand-new decompressor ["+codec.getDefaultExtension()+"]");
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Got recycled decompressor");
      }
    }
    if (decompressor != null &&
        !decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      updateLeaseCount(decompressorCounts, decompressor, 1);
    }
    return decompressor;
  }
  
  /**
   * Return the {@link Compressor} to the pool.
   * 
   * @param compressor the <code>Compressor</code> to be returned to the pool
   */
  public static void returnCompressor(Compressor compressor) {
    if (compressor == null) {
      return;
    }
    // if the compressor can't be reused, don't pool it.
    if (compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      return;
    }
    compressor.reset();
    if (payback(compressorPool, compressor)) {
      updateLeaseCount(compressorCounts, compressor, -1);
    }
  }
  
  /**
   * Return the {@link Decompressor} to the pool.
   * 
   * @param decompressor the <code>Decompressor</code> to be returned to the 
   *                     pool
   */
  public static void returnDecompressor(Decompressor decompressor) {
    if (decompressor == null) {
      return;
    }
    // if the decompressor can't be reused, don't pool it.
    if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      return;
    }
    decompressor.reset();
    if (payback(decompressorPool, decompressor)) {
      updateLeaseCount(decompressorCounts, decompressor, -1);
    }
  }

  /**
   * Return the number of leased {@link Compressor}s for this
   * {@link CompressionCodec}
   */
  public static int getLeasedCompressorsCount(CompressionCodec codec) {
    return (codec == null) ? 0 : getLeaseCount(compressorCounts,
        codec.getCompressorType());
  }

  /**
   * Return the number of leased {@link Decompressor}s for this
   * {@link CompressionCodec}
   */
  public static int getLeasedDecompressorsCount(CompressionCodec codec) {
    return (codec == null) ? 0 : getLeaseCount(decompressorCounts,
        codec.getDecompressorType());
  }
}
