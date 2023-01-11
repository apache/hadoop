package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import java.util.Map;

public interface OnLRUCacheRemoveListener<K, V> {
    void onRemove(Map.Entry<K, V> entry);
}