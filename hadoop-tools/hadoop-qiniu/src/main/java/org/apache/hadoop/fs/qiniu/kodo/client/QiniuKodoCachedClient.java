package org.apache.hadoop.fs.qiniu.kodo.client;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.qiniu.kodo.util.LRUCache;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class QiniuKodoCachedClient implements IQiniuKodoClient {
    private final LRUCache<String, QiniuKodoFileInfo> cache;
    private final IQiniuKodoClient source;

    public QiniuKodoCachedClient(
            IQiniuKodoClient source,
            int maxCapacity
    ) {
        this.source = source;
        this.cache = new LRUCache<>(maxCapacity);
    }

    @Override
    public boolean exists(String key) throws IOException {
        if (cache.containsKey(key)) {
            return true;
        }
        return source.exists(key);
    }

    @Override
    public long getLength(String key) throws IOException {
        QiniuKodoFileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            return fileInfo.size;
        }
        return source.getLength(key);
    }

    @Override
    public boolean upload(InputStream stream, String key, boolean overwrite) throws IOException {
        boolean success = source.upload(stream, key, overwrite);
        if (success) {
            cache.remove(key);
        }
        return success;
    }

    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        return source.fetch(key, offset, size);
    }

    @Override
    public QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException {
        QiniuKodoFileInfo fileInfo = cache.get(keyPrefix);
        if (fileInfo != null) {
            return fileInfo;
        }

        fileInfo = cache.get(QiniuKodoUtils.keyToDirKey(keyPrefix));
        if (fileInfo != null) {
            return fileInfo;
        }

        fileInfo = source.listOneStatus(keyPrefix);
        if (fileInfo != null) {
            cache.put(fileInfo.key, fileInfo);
        }

        return fileInfo;
    }

    @Override
    public List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        return source.listNStatus(keyPrefix, n);
    }

    @Override
    public List<QiniuKodoFileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        return source.listStatus(key, useDirectory);
    }

    @Override
    public boolean copyKey(String oldKey, String newKey) throws IOException {
        return source.copyKey(oldKey, newKey);
    }

    @Override
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        return source.copyKeys(oldPrefix, newPrefix);
    }

    @Override
    public boolean renameKey(String oldKey, String newKey) throws IOException {
        boolean result = source.renameKey(oldKey, newKey);
        cache.remove(oldKey);
        return result;
    }

    @Override
    public boolean renameKeys(String oldPrefix, String newPrefix) throws IOException {
        boolean result = source.renameKeys(oldPrefix, newPrefix);
        cache.removeIf(e -> e.getKey().startsWith(oldPrefix));
        return result;
    }

    @Override
    public boolean deleteKey(String key) throws IOException {
        boolean result = source.deleteKey(key);
        cache.remove(key);
        return result;
    }

    @Override
    public boolean deleteKeys(String prefix) throws IOException {
        boolean result = source.deleteKeys(prefix);
        cache.removeIf(e -> e.getKey().startsWith(prefix));
        return result;
    }

    @Override
    public boolean makeEmptyObject(String key) throws IOException {
        QiniuKodoFileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            return false;
        }
        cache.remove(key);
        return source.makeEmptyObject(key);
    }

    @Override
    public QiniuKodoFileInfo getFileStatus(String key) throws IOException {
        QiniuKodoFileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            return fileInfo;
        }
        fileInfo = source.getFileStatus(key);
        if (fileInfo != null) {
            cache.put(key, fileInfo);
        }
        return fileInfo;
    }

    @Override
    public RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException {
        return source.listStatusIterator(prefixKey, useDirectory);
    }
}
