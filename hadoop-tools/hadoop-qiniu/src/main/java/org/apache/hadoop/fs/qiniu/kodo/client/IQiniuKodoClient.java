package org.apache.hadoop.fs.qiniu.kodo.client;


import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface IQiniuKodoClient {
    boolean exists(String key) throws IOException;

    long getLength(String key) throws IOException;

    boolean upload(InputStream stream, String key, boolean overwrite) throws IOException;

    InputStream fetch(String key, long offset, int size) throws IOException;

    QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException;

    List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException;

    List<QiniuKodoFileInfo> listStatus(String key, boolean useDirectory) throws IOException;

    RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException;

    boolean copyKey(String oldKey, String newKey) throws IOException;

    boolean copyKeys(String oldPrefix, String newPrefix) throws IOException;

    boolean renameKey(String oldKey, String newKey) throws IOException;

    boolean renameKeys(String oldPrefix, String newPrefix) throws IOException;

    boolean deleteKey(String key) throws IOException;

    boolean deleteKeys(String prefix) throws IOException;

    boolean makeEmptyObject(String key) throws IOException;

    QiniuKodoFileInfo getFileStatus(String key) throws IOException;

}
