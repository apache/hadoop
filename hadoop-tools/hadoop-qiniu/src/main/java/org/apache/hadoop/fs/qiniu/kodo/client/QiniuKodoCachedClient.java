package org.apache.hadoop.fs.qiniu.kodo.client;

import org.apache.hadoop.fs.FileAlreadyExistsException;
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
    public void upload(InputStream stream, String key, boolean overwrite) throws IOException {
        source.upload(stream, key, overwrite);
        cache.remove(key);
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
    public void copyKey(String oldKey, String newKey) throws IOException {
        source.copyKey(oldKey, newKey);
    }

    @Override
    public void copyKeys(String oldPrefix, String newPrefix) throws IOException {
        source.copyKeys(oldPrefix, newPrefix);
    }

    @Override
    public void renameKey(String oldKey, String newKey) throws IOException {
        try {
            source.renameKey(oldKey, newKey);
        } finally {
            cache.remove(oldKey);
        }
    }

    @Override
    public void renameKeys(String oldPrefix, String newPrefix) throws IOException {
        try {
            source.renameKeys(oldPrefix, newPrefix);
        } finally {
            cache.removeIf(e -> e.getKey().startsWith(oldPrefix));
        }
    }

    @Override
    public void deleteKey(String key) throws IOException {
        try {
            source.deleteKey(key);
        } finally {
            cache.remove(key);
        }
    }

    @Override
    public void deleteKeys(String prefix) throws IOException {
        try {
            source.deleteKeys(prefix);
        } finally {
            cache.removeIf(e -> e.getKey().startsWith(prefix));
        }
    }

    @Override
    public void makeEmptyObject(String key) throws IOException {
        QiniuKodoFileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            throw new FileAlreadyExistsException(key);
        }
        cache.remove(key);
        source.makeEmptyObject(key);
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
