package org.apache.hadoop.fs.qiniu.kodo.client;


import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public interface IQiniuKodoClient {
    /**
     * Check if the key exists
     *
     * @param key key
     * @return true if exists
     * @throws IOException io exception
     */
    boolean exists(String key) throws IOException;

    /**
     * Get file length by key
     *
     * @param key object key
     */
    long getLength(String key) throws IOException;

    /**
     * Upload a file to qiniu kodo by input stream
     */
    void upload(InputStream stream, String key, boolean overwrite) throws IOException;

    /**
     * Get file InputStream by key, offset and length
     */
    InputStream fetch(String key, long offset, int size) throws IOException;

    /**
     * Get first match file status by key prefix
     */
    QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException;

    /**
     * Get file statuses by key prefix no more than n
     */
    List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException;

    /**
     * list file status by key prefix and useDirectory
     * If the parameter useDirectory is true, the method will list all files with directory structure
     * otherwise, the method will list all files with the same prefix
     */
    List<QiniuKodoFileInfo> listStatus(String key, boolean useDirectory) throws IOException;

    /**
     * list file status by key prefix and useDirectory
     * If the parameter useDirectory is true, the method will list all files with directory structure
     * otherwise, the method will list all files with the same prefix
     */
    RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException;

    /**
     * Copy a file to another file by key
     */
    void copyKey(String oldKey, String newKey) throws IOException;

    void copyKeys(String oldPrefix, String newPrefix) throws IOException;

    /**
     * Rename a file to another file by key
     */
    void renameKey(String oldKey, String newKey) throws IOException;

    /**
     * Rename files by key prefix to another new prefix
     */
    void renameKeys(String oldPrefix, String newPrefix) throws IOException;

    /**
     * delete a file by key
     */
    void deleteKey(String key) throws IOException;

    /**
     * delete files by key prefix
     *
     * @param prefix key prefix
     * @throws IOException exception
     */
    void deleteKeys(String prefix) throws IOException;

    /**
     * Make an empty object by key
     */
    void makeEmptyObject(String key) throws IOException;

    /**
     * Get file status by key
     * If the file does not exist, return null
     * other exceptions will be thrown
     */
    QiniuKodoFileInfo getFileStatus(String key) throws IOException;

}
