/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * The <code>PoolMap</code> maps a key to a collection of values, the elements
 * of which are managed by a pool. In effect, that collection acts as a shared
 * pool of resources, access to which is closely controlled as per the semantics
 * of the pool.
 *
 * <p>
 * In case the size of the pool is set to a non-zero positive number, that is
 * used to cap the number of resources that a pool may contain for any given
 * key. A size of {@link Integer#MAX_VALUE} is interpreted as an unbounded pool.
 * </p>
 *
 * @param <K>
 *          the type of the key to the resource
 * @param <V>
 *          the type of the resource being pooled
 */
public class PoolMap<K, V> implements Map<K, V> {
  private PoolType poolType;

  private int poolMaxSize;

  private Map<K, Pool<V>> pools = new ConcurrentHashMap<K, Pool<V>>();

  public PoolMap(PoolType poolType, int poolMaxSize) {
    this.poolType = poolType;
    this.poolMaxSize = poolMaxSize;
  }

  @Override
  public V get(Object key) {
    Pool<V> pool = pools.get(key);
    return pool != null ? pool.get() : null;
  }

  @Override
  public V put(K key, V value) {
    Pool<V> pool = pools.get(key);
    if (pool == null) {
      pools.put(key, pool = createPool());
    }
    return pool != null ? pool.put(value) : null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    Pool<V> pool = pools.remove(key);
    if (pool != null) {
      remove((K) key, pool.get());
    }
    return null;
  }

  public boolean remove(K key, V value) {
    Pool<V> pool = pools.get(key);
    return pool != null ? pool.remove(value) : false;
  }

  @Override
  public Collection<V> values() {
    Collection<V> values = new ArrayList<V>();
    for (Pool<V> pool : pools.values()) {
      Collection<V> poolValues = pool.values();
      if (poolValues != null) {
        values.addAll(poolValues);
      }
    }
    return values;
  }

  public Collection<V> values(K key) {
    Collection<V> values = new ArrayList<V>();
    Pool<V> pool = pools.get(key);
    if (pool != null) {
      Collection<V> poolValues = pool.values();
      if (poolValues != null) {
        values.addAll(poolValues);
      }
    }
    return values;
  }


  @Override
  public boolean isEmpty() {
    return pools.isEmpty();
  }

  @Override
  public int size() {
    return pools.size();
  }

  public int size(K key) {
    Pool<V> pool = pools.get(key);
    return pool != null ? pool.size() : 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return pools.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    if (value == null) {
      return false;
    }
    for (Pool<V> pool : pools.values()) {
      if (value.equals(pool.get())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    for (Pool<V> pool : pools.values()) {
      pool.clear();
    }
    pools.clear();
  }

  @Override
  public Set<K> keySet() {
    return pools.keySet();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    Set<Map.Entry<K, V>> entries = new HashSet<Entry<K, V>>();
    for (Map.Entry<K, Pool<V>> poolEntry : pools.entrySet()) {
      final K poolKey = poolEntry.getKey();
      final Pool<V> pool = poolEntry.getValue();
      for (final V poolValue : pool.values()) {
        if (pool != null) {
          entries.add(new Map.Entry<K, V>() {
            @Override
            public K getKey() {
              return poolKey;
            }

            @Override
            public V getValue() {
              return poolValue;
            }

            @Override
            public V setValue(V value) {
              return pool.put(value);
            }
          });
        }
      }
    }
    return null;
  }

  protected interface Pool<R> {
    public R get();

    public R put(R resource);

    public boolean remove(R resource);

    public void clear();

    public Collection<R> values();

    public int size();
  }

  public enum PoolType {
    Reusable, ThreadLocal, RoundRobin;

    public static PoolType valueOf(String poolTypeName,
        PoolType defaultPoolType, PoolType... allowedPoolTypes) {
      PoolType poolType = PoolType.fuzzyMatch(poolTypeName);
      if (poolType != null) {
        boolean allowedType = false;
        if (poolType.equals(defaultPoolType)) {
          allowedType = true;
        } else {
          if (allowedPoolTypes != null) {
            for (PoolType allowedPoolType : allowedPoolTypes) {
              if (poolType.equals(allowedPoolType)) {
                allowedType = true;
                break;
              }
            }
          }
        }
        if (!allowedType) {
          poolType = null;
        }
      }
      return (poolType != null) ? poolType : defaultPoolType;
    }

    public static String fuzzyNormalize(String name) {
      return name != null ? name.replaceAll("-", "").trim().toLowerCase() : "";
    }

    public static PoolType fuzzyMatch(String name) {
      for (PoolType poolType : values()) {
        if (fuzzyNormalize(name).equals(fuzzyNormalize(poolType.name()))) {
          return poolType;
        }
      }
      return null;
    }
  }

  protected Pool<V> createPool() {
    switch (poolType) {
    case Reusable:
      return new ReusablePool<V>(poolMaxSize);
    case RoundRobin:
      return new RoundRobinPool<V>(poolMaxSize);
    case ThreadLocal:
      return new ThreadLocalPool<V>(poolMaxSize);
    }
    return null;
  }

  /**
   * The <code>ReusablePool</code> represents a {@link PoolMap.Pool} that builds
   * on the {@link LinkedList} class. It essentially allows resources to be
   * checked out, at which point it is removed from this pool. When the resource
   * is no longer required, it should be returned to the pool in order to be
   * reused.
   *
   * <p>
   * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of
   * the pool is unbounded. Otherwise, it caps the number of consumers that can
   * check out a resource from this pool to the (non-zero positive) value
   * specified in {@link #maxSize}.
   * </p>
   *
   * @param <R>
   *          the type of the resource
   */
  @SuppressWarnings("serial")
  public class ReusablePool<R> extends ConcurrentLinkedQueue<R> implements Pool<R> {
    private int maxSize;

    public ReusablePool(int maxSize) {
      this.maxSize = maxSize;

    }

    @Override
    public R get() {
      return poll();
    }

    @Override
    public R put(R resource) {
      if (size() < maxSize) {
        add(resource);
      }
      return null;
    }

    @Override
    public Collection<R> values() {
      return this;
    }
  }

  /**
   * The <code>RoundRobinPool</code> represents a {@link PoolMap.Pool}, which
   * stores its resources in an {@link ArrayList}. It load-balances access to
   * its resources by returning a different resource every time a given key is
   * looked up.
   *
   * <p>
   * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of
   * the pool is unbounded. Otherwise, it caps the number of resources in this
   * pool to the (non-zero positive) value specified in {@link #maxSize}.
   * </p>
   *
   * @param <R>
   *          the type of the resource
   *
   */
  @SuppressWarnings("serial")
  class RoundRobinPool<R> extends CopyOnWriteArrayList<R> implements Pool<R> {
    private int maxSize;
    private int nextResource = 0;

    public RoundRobinPool(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public R put(R resource) {
      if (size() < maxSize) {
        add(resource);
      }
      return null;
    }

    @Override
    public R get() {
      if (size() < maxSize) {
        return null;
      }
      nextResource %= size();
      R resource = get(nextResource++);
      return resource;
    }

    @Override
    public Collection<R> values() {
      return this;
    }

  }

  /**
   * The <code>ThreadLocalPool</code> represents a {@link PoolMap.Pool} that
   * builds on the {@link ThreadLocal} class. It essentially binds the resource
   * to the thread from which it is accessed.
   *
   * <p>
   * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of
   * the pool is bounded only by the number of threads that add resources to
   * this pool. Otherwise, it caps the number of threads that can set a value on
   * this {@link ThreadLocal} instance to the (non-zero positive) value
   * specified in {@link #maxSize}.
   * </p>
   *
   *
   * @param <R>
   *          the type of the resource
   */
  static class ThreadLocalPool<R> extends ThreadLocal<R> implements Pool<R> {
    private static final Map<ThreadLocalPool<?>, AtomicInteger> poolSizes = new HashMap<ThreadLocalPool<?>, AtomicInteger>();

    private int maxSize;

    public ThreadLocalPool(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public R put(R resource) {
      R previousResource = get();
      if (previousResource == null) {
        AtomicInteger poolSize = poolSizes.get(this);
        if (poolSize == null) {
          poolSizes.put(this, poolSize = new AtomicInteger(0));
        }
        poolSize.incrementAndGet();
      }
      this.set(resource);
      return previousResource;
    }

    @Override
    public void remove() {
      super.remove();
      AtomicInteger poolSize = poolSizes.get(this);
      if (poolSize != null) {
        poolSize.decrementAndGet();
      }
    }

    @Override
    public int size() {
      AtomicInteger poolSize = poolSizes.get(this);
      return poolSize != null ? poolSize.get() : 0;
    }

    @Override
    public boolean remove(R resource) {
      R previousResource = super.get();
      if (resource != null && resource.equals(previousResource)) {
        remove();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void clear() {
      super.remove();
    }

    @Override
    public Collection<R> values() {
      List<R> values = new ArrayList<R>();
      values.add(get());
      return values;
    }
  }
}
