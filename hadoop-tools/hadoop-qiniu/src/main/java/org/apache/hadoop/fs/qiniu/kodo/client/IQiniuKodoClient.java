package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.http.Response;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public interface IQiniuKodoClient {
    boolean exists(String key) throws IOException;

    long getLength(String key) throws IOException;

    Response upload(InputStream stream, String key, boolean overwrite) throws IOException;

    InputStream fetch(String key, long offset, int size) throws IOException;

    FileInfo listOneStatus(String keyPrefix) throws IOException;

    List<FileInfo> listNStatus(String keyPrefix, int n) throws IOException;

    List<FileInfo> listStatus(String key, boolean useDirectory) throws IOException;

    Iterator<FileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException;

    boolean copyKey(String oldKey, String newKey) throws IOException;

    boolean copyKeys(String oldPrefix, String newPrefix) throws IOException;

    boolean renameKey(String oldKey, String newKey) throws IOException;

    boolean renameKeys(String oldPrefix, String newPrefix) throws IOException;

    boolean deleteKey(String key) throws IOException;

    boolean deleteKeys(String prefix) throws IOException;

    boolean makeEmptyObject(String key) throws IOException;

    FileInfo getFileStatus(String key) throws IOException;

}
