package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.http.Response;
import com.qiniu.storage.model.FileInfo;
import org.apache.hadoop.fs.qiniu.kodo.util.LRUCache;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public class QiniuKodoCachedClient implements IQiniuKodoClient {
    private final LRUCache<String, FileInfo> cache;
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
        FileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            return fileInfo.fsize;
        }
        return source.getLength(key);
    }

    @Override
    public Response upload(InputStream stream, String key, boolean overwrite) throws IOException {
        Response response = source.upload(stream, key, overwrite);
        if (response.isOK()) {
            cache.remove(key);
        }
        return response;
    }

    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        return source.fetch(key, offset, size);
    }

    @Override
    public FileInfo listOneStatus(String keyPrefix) throws IOException {
        FileInfo fileInfo = cache.get(keyPrefix);
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
    public List<FileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        return source.listNStatus(keyPrefix, n);
    }

    @Override
    public List<FileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        return source.listStatus(key, useDirectory);
    }

    @Override
    public boolean copyKey(String oldKey, String newKey) throws IOException {
        boolean result = source.copyKey(oldKey, newKey);
        cache.remove(oldKey);
        return result;
    }

    @Override
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        boolean result = source.copyKeys(oldPrefix, newPrefix);
        cache.removeIf(e -> e.getKey().startsWith(oldPrefix));
        return result;
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
        FileInfo fileInfo = cache.get(key);
        if (fileInfo != null) {
            return false;
        }
        cache.remove(key);
        return source.makeEmptyObject(key);
    }

    @Override
    public FileInfo getFileStatus(String key) throws IOException {
        FileInfo fileInfo = cache.get(key);
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
    public Iterator<FileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException {
        return source.listStatusIterator(prefixKey, useDirectory);
    }
}
