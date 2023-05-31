package org.apache.hadoop.fs.qiniu.kodo.util;

import com.google.gson.Gson;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DiskCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.KeyBlockIdCacheKey;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class LRUCacheTest {
    @Test
    public void test() {
        LRUCache<KeyBlockIdCacheKey, Path> cache = new LRUCache<>(3);
        cache.put(KeyBlockIdCacheKey.get("key3", 1), Paths.get("p1"));
        cache.put(KeyBlockIdCacheKey.get("key2", 2), Paths.get("p2"));
        cache.put(KeyBlockIdCacheKey.get("key4", 3), Paths.get("p3"));
        cache.put(KeyBlockIdCacheKey.get("key3", 4), Paths.get("p4"));
        System.out.println(cache);
        for (Map.Entry<KeyBlockIdCacheKey, Path> e : cache.entrySet()) {
            System.out.println(e);
        }
        Gson gson = new Gson();
        String json = gson.toJson(DiskCacheBlockReader.PersistentData.fromMap(cache, 3600));
        System.out.println(json);

        DiskCacheBlockReader.PersistentData m1 = gson.fromJson(json, DiskCacheBlockReader.PersistentData.class);

        System.out.println(m1.toLRUCache(5));
    }
}
