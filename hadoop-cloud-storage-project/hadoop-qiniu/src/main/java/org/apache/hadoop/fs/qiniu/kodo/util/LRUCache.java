package org.apache.hadoop.fs.qiniu.kodo.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;


public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private OnLRUCacheRemoveListener<K, V> onLRUCacheRemoveListener;
    private final int maxCapacity;

    public LRUCache(int maxCapacity) {
        super((int) Math.ceil(maxCapacity / 0.75f) + 1,
                0.75f,
                true
        );
        this.maxCapacity = maxCapacity;
    }

    public void setOnLRUCacheRemoveListener(OnLRUCacheRemoveListener<K, V> onLRUCacheRemoveListener) {
        this.onLRUCacheRemoveListener = onLRUCacheRemoveListener;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (size() > maxCapacity) {
            if (onLRUCacheRemoveListener != null) onLRUCacheRemoveListener.onRemove(eldest);
            return true;
        }
        return false;
    }

    @Override
    public synchronized V get(Object key) {
        return super.get(key);
    }

    @Override
    public synchronized V remove(Object key) {
        return super.remove(key);
    }

    @Override
    public synchronized V put(K key, V value) {
        return super.put(key, value);
    }

    public synchronized void removeIf(Predicate<Map.Entry<K, V>> filter) {
        this.entrySet().removeIf(filter);
    }
}
