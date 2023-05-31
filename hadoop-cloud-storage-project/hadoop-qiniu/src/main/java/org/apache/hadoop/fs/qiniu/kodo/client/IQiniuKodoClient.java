package org.apache.hadoop.fs.qiniu.kodo.client;


import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface IQiniuKodoClient {
    boolean exists(String key) throws IOException;

    long getLength(String key) throws IOException;

    void upload(InputStream stream, String key, boolean overwrite) throws IOException;

    InputStream fetch(String key, long offset, int size) throws IOException;

    QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException;

    List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException;

    List<QiniuKodoFileInfo> listStatus(String key, boolean useDirectory) throws IOException;

    RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException;

    void copyKey(String oldKey, String newKey) throws IOException;

    void copyKeys(String oldPrefix, String newPrefix) throws IOException;

    void renameKey(String oldKey, String newKey) throws IOException;

    void renameKeys(String oldPrefix, String newPrefix) throws IOException;

    void deleteKey(String key) throws IOException;

    void deleteKeys(String prefix) throws IOException;

    void makeEmptyObject(String key) throws IOException;

    QiniuKodoFileInfo getFileStatus(String key) throws IOException;

}
