package org.apache.hadoop.fs.qiniu.kodo;


import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

class QiniuKodoClient {

    // 仅有一个 bucket
    private String bucket;

    private Auth auth;

    private Client client;

    private UploadManager uploadManager;
    private BucketManager bucketManager;

    QiniuKodoClient(Auth auth, Configuration configuration, String bucket) {
        this.client = new Client(configuration);
        this.auth = auth;
        this.uploadManager = new UploadManager(configuration);
        this.bucketManager = new BucketManager(auth, configuration, this.client);
        this.bucket = bucket;
    }

    QiniuKodoOutputStream create(String key, int bufferSize, boolean overwrite) throws IOException {
        StringMap policy = new StringMap();
        policy.put("insertOnly", overwrite ? "0" : "1");
        String token = auth.uploadToken(bucket, key, 7 * 24 * 3600, policy);
        return new QiniuKodoOutputStream(this, key, token, bufferSize);
    }

    Response upload(InputStream stream, String key, String token) throws IOException {
        try {
            return uploadManager.put(stream, key, token, null, null);
        } catch (QiniuException e) {
            if (e.response != null) {
                return e.response;
            } else {
                throw e;
            }
        }
    }

    QiniuKodoInputStream open(String key, int bufferSize) throws IOException {
        String[] domains = domains();
        if (domains == null || domains.length == 0) {
            throw new IOException("can't get bucket domain");
        }
        String publicUrl = domains[0] + "/" + key;
        String url = auth.privateDownloadUrl(publicUrl, 7 * 24 * 3600);
        return new QiniuKodoInputStream(this, url, bufferSize);
    }

    InputStream get(String url, long from, long to) throws IOException {
        try {
            StringMap header = new StringMap();
            Response response = this.client.get(url, header);
            return response.bodyStream();
        } catch (QiniuException e) {
            if (e.response != null) {
                throw new IOException(e.response + "");
            } else {
                throw e;
            }
        }
    }

    List<FileInfo> listStatus(String key, boolean withDelimiter) throws IOException {
        List<FileInfo> retFiles = new ArrayList<>();
        String marker = null;
        FileListing fileListing = null;
        while (true) {
            fileListing = bucketManager.listFilesV2(bucket, key, marker, 100, withDelimiter ? utils.PATH_SEPARATOR : "");

            if (fileListing.items != null) {
                for (FileInfo file : fileListing.items) {
                    if (file == null || key.equals(file.key)) {
                        continue;
                    }
                    retFiles.add(file);
                }
            }

            if (fileListing.commonPrefixes != null) {
                for (String dirPath : fileListing.commonPrefixes) {
                    FileInfo dir = new FileInfo();
                    dir.key = dirPath;
                    retFiles.add(dir);
                }
            }

            if (fileListing.isEOF()) {
                break;
            }

            marker = fileListing.marker;
        }

        return retFiles;
    }

    boolean renameKey(String oldKey, String newKey) throws IOException {
        Response response = bucketManager.rename(bucket, oldKey, newKey);
        return throwExceptionWhileResponseNotSuccess(response);
    }

    boolean renameKeys(String oldPrefix, String newKeyPrefix) throws IOException {
        boolean hasPrefixObject = false;
        String marker = null;
        FileListing fileListing = null;
        BucketManager.BatchOperations operations = null;
        while (true) {
            fileListing = bucketManager.listFilesV2(bucket, oldPrefix, marker, 100, "");
            if (fileListing.isEOF()) {
                break;
            }

            if (fileListing.items != null) {
                operations = new BucketManager.BatchOperations();
                for (FileInfo file : fileListing.items) {
                    // oldPrefix 最后处理
                    if (file == null || file.key.equals(oldPrefix)) {
                        hasPrefixObject = true;
                        continue;
                    }
                    String destKey = file.key.replaceFirst(oldPrefix, newKeyPrefix);
                    operations.addRenameOp(bucket, file.key, destKey);
                }

                Response response = bucketManager.batch(operations);
                if (!throwExceptionWhileResponseNotSuccess(response)) {
                    return false;
                }
            }

            marker = fileListing.marker;
        }

        if (!hasPrefixObject) {
            return true;
        }

        // 处理 oldPrefix
        Response response = bucketManager.rename(bucket, oldPrefix, newKeyPrefix);
        return throwExceptionWhileResponseNotSuccess(response);
    }

    boolean deleteKey(String key) throws IOException {
        Response response = bucketManager.delete(bucket, key);
        return throwExceptionWhileResponseNotSuccess(response);
    }

    boolean deleteKeys(String prefix) throws IOException {
        boolean hasPrefixObject = false;
        String marker = null;
        FileListing fileListing = null;
        BucketManager.BatchOperations operations = null;
        while (true) {
            fileListing = bucketManager.listFilesV2(bucket, prefix, marker, 100, "");
            if (fileListing.isEOF()) {
                break;
            }

            if (fileListing.items != null) {
                operations = new BucketManager.BatchOperations();
                for (FileInfo file : fileListing.items) {
                    // oldPrefix 最后处理
                    if (file == null || file.key.equals(prefix)) {
                        hasPrefixObject = true;
                        continue;
                    }
                    operations.addDeleteOp(bucket, file.key);
                }

                Response response = bucketManager.batch(operations);
                if (!throwExceptionWhileResponseNotSuccess(response)) {
                    return false;
                }
            }

            marker = fileListing.marker;
        }

        if (!hasPrefixObject) {
            return true;
        }

        // 处理 oldPrefix
        Response response = bucketManager.delete(bucket, prefix);
        return throwExceptionWhileResponseNotSuccess(response);
    }

    boolean mkdir(String key) throws IOException {
        byte[] content = new byte[]{};
        String token = auth.uploadToken(bucket, null, 3 * 3600L, null);
        Response response = uploadManager.put(content, key, token);
        return throwExceptionWhileResponseNotSuccess(response);
    }

    /**
     * 不存在不抛异常，返回为空，只有在其他错误时抛异常
     */
    FileInfo getFileStatus(String key) throws IOException {
        try {
            FileInfo fileInfo = bucketManager.stat(bucket, key);
            if (fileInfo != null) {
                fileInfo.key = key;
            }
            return fileInfo;
        } catch (QiniuException e) {
            if (e.response != null && e.response.statusCode == 612) {
                return null;
            }
            throw e;
        }
    }

    String[] domains() throws IOException {
        return bucketManager.domainList(bucket);
    }

    boolean throwExceptionWhileResponseNotSuccess(Response response) throws IOException {
        if (response == null) {
            return false;
        }

        if (response.isOK()) {
            return true;
        }

        throw new IOException(response + "");
    }

}
