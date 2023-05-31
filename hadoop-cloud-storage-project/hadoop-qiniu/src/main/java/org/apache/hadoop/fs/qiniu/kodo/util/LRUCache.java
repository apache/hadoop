package org.apache.hadoop.fs.qiniu.kodo.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;


public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private OnLRUCacheRemoveListener<K, V> onLRUCacheRemoveListener;
    private final int maxCapacity;

    public LRUCache(int maxCapacity) {
        super((int) Math.ceil(maxCapacity / 0.75f) + 1,
                0.75f,  // HashMap默认负载因子 0.75
                true    // 是否按访问顺序逆序排列, 访问后将排在最后
        );
        this.maxCapacity = maxCapacity;
    }

    public void setOnLRUCacheRemoveListener(OnLRUCacheRemoveListener<K, V> onLRUCacheRemoveListener) {
        this.onLRUCacheRemoveListener = onLRUCacheRemoveListener;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        // 插入新元素之后若大小超过了容量阈值，则自动删除表头元素
        // 由于开启了accessOrder = true，故最近访问的将排在表为，则表头为最长时间未使用的节点，需要被删除
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
