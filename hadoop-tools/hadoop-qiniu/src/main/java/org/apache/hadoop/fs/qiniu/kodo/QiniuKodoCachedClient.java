package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.model.FileInfo;
import org.apache.hadoop.fs.qiniu.kodo.util.LRUCache;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class QiniuKodoCachedClient implements IQiniuKodoClient {
    private final LRUCache<String, FileInfo> cache = new LRUCache<>(100);
    private final IQiniuKodoClient source;

    private void removeKeyPrefixInCache(String keyPrefix) {
        for (String key : cache.keySet()) {
            if (key.startsWith(keyPrefix)) {
                cache.remove(key);
            }
        }
    }

    public QiniuKodoCachedClient(IQiniuKodoClient source) {
        this.source = source;
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
    public Response upload(InputStream stream, String key, boolean overwrite) throws QiniuException {
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
        removeKeyPrefixInCache(oldPrefix);
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
        removeKeyPrefixInCache(oldPrefix);
        return result;
    }

    @Override
    public boolean deleteKey(String key) throws IOException {
        boolean result = source.deleteKey(key);
        cache.remove(key);
        return result;
    }

    @Override
    public boolean deleteKeys(String prefix, boolean recursive) throws IOException {
        boolean result = source.deleteKeys(prefix, recursive);
        removeKeyPrefixInCache(prefix);
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
        cache.put(key, fileInfo);
        return fileInfo;
    }
}
