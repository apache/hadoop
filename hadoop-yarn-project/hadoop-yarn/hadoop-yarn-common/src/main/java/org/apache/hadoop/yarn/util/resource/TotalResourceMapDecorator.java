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

package org.apache.hadoop.yarn.util.resource;

import org.apache.commons.collections4.map.AbstractMapDecorator;
import org.apache.commons.collections4.set.AbstractSetDecorator;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Class that decorates a Map with a {@link Resource} object providing support
 * to keep a sum of all resource values held.
 * @param <K> key
 * @param <V> value
 */
public class TotalResourceMapDecorator<K, V extends Resource>
    extends AbstractMapDecorator<K, V>
    implements Consumer<K> {
  private Resource total = Resource.newInstance(0, 0);
  public TotalResourceMapDecorator(Map<K, V> decorated) {
    super(decorated);
  }

  @Override
  public V put(K key, V value) {
    V previous = super.put(key, value);
    Resource previousResource = previous == null ? Resources.none() : previous;
    Resources.addTo(total, Resources.subtract(value, previousResource));
    return previous;
  }

  @Override
  public V remove(Object key) {
    V previous = super.remove(key);
    Resources.subtractFrom(total, previous);
    return previous;
  }

  @Override
  public Set<K> keySet() {
    return new RemovalObserverSetDecorator<K>(this, super.keySet());
  }

  /**
   * Sum of resource values in the decorated object.
   * @return Sum of resource values
   */
  public Resource getTotalResource() {
    return Resources.clone(total);
  }

  /**
   * Callback to keep track of a deleted object from an iterator.
   * @param key object removed from the decorated map
   */
  @Override
  public void accept(K key) {
    Resources.subtractFrom(total, super.get(key));
  }

  /**
   * Notify an observer about removal of an item.
   * @param <K> The iterator value type
   */
  class RemovalObserverSetDecorator<K> extends AbstractSetDecorator<K> {
    Consumer<K> aggregatedValue;

    RemovalObserverSetDecorator(
        Consumer<K> aggregatedValue,
        Set<K> decorated) {
      super(decorated);
      this.aggregatedValue = aggregatedValue;
    }

    @Override
    public Iterator<K> iterator() {
      return new RemovalObserverIteratorDecorator<K>(
          aggregatedValue,
          super.iterator());
    }

    /**
     * Notify an observer about removal of an item.
     * @param <T> The iterator value type
     */
    class RemovalObserverIteratorDecorator<T> implements Iterator<T> {
      Iterator<T> decorated;
      Consumer<T> removalObserver;
      T lastValue;

      RemovalObserverIteratorDecorator(
          Consumer<T> aggregatedValue,
          Iterator<T> decorated) {
        this.decorated = decorated;
        this.removalObserver = aggregatedValue;
      }

      @Override
      public boolean hasNext() {
        return decorated.hasNext();
      }

      @Override
      public T next() {
        lastValue = decorated.next();
        return lastValue;
      }

      @Override
      public void remove() {
        removalObserver.accept(lastValue);
        decorated.remove();
      }
    }
  }
}
