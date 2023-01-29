package org.apache.hadoop.fs.qiniu.kodo;


import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.*;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import okhttp3.Request;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QiniuKodoClient {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoClient.class);

    // 仅有一个 bucket
    private final String bucket;

    private final Auth auth;

    private final Client client;

    private final UploadManager uploadManager;
    public final BucketManager bucketManager;

    private QiniuKodoRegionManager.QiniuKodoRegion region;

    private final boolean useHttps;

    private String downloadDomain;
    private final FileSystem.Statistics statistics;
    private final boolean useSign;
    private final int signExpires;

    public QiniuKodoClient(String bucket, QiniuKodoFsConfig fsConfig, FileSystem.Statistics statistics) throws QiniuException, AuthorizationException {
        this.bucket = bucket;
        this.statistics = statistics;

        this.auth = getAuth(fsConfig);

        Configuration configuration = new Configuration();

        // 如果找不到区域配置，那就auto
        String regionIdConfig = fsConfig.regionId;
        if (regionIdConfig != null) region = QiniuKodoRegionManager.getRegionById(regionIdConfig);
        configuration.region = region == null ? Region.autoRegion() : region.getRegion();

        this.useHttps = fsConfig.useHttps;
        configuration.useHttpsDomains = this.useHttps;

        this.client = new Client(configuration);
        this.uploadManager = new UploadManager(configuration);
        this.bucketManager = new BucketManager(auth, configuration, this.client);

        // 如果找不到区域配置，那就发起请求获取区域信息
        if (region == null)
            region = QiniuKodoRegionManager.getRegionById(bucketManager.getBucketInfo(bucket).getRegion());

        // 设置下载域名，若未配置，则走源站
        if ((downloadDomain = fsConfig.download.domain) == null)
            downloadDomain = bucket + "." + region.getRegionEndpoint();

        this.useSign = fsConfig.download.sign.enable;
        this.signExpires = fsConfig.download.sign.expires;
    }

    private static Auth getAuth(QiniuKodoFsConfig fsConfig) throws AuthorizationException {
        String ak = fsConfig.auth.accessKey;
        String sk = fsConfig.auth.secretKey;
        if (StringUtils.isNullOrEmpty(ak)) {
            throw new AuthorizationException(String.format(
                    "Qiniu access key can't empty, you should set it with %s in core-site.xml",
                    fsConfig.auth.ACCESS_KEY
            ));
        }
        if (StringUtils.isNullOrEmpty(sk)) {
            throw new AuthorizationException(String.format(
                    "Qiniu secret key can't empty, you should set it with %s in core-site.xml",
                    fsConfig.auth.SECRET_KEY
            ));
        }
        return Auth.create(ak, sk);
    }

    /**
     * 根据key和overwrite生成上传token
     */
    public String getUploadToken(String key, boolean overwrite) {
        StringMap policy = new StringMap();
        policy.put("insertOnly", overwrite ? 0 : 1);
        return auth.uploadToken(bucket, key, 7 * 24 * 3600, policy);
    }

    /**
     * 给定一个输入流将读取并上传对应文件
     */
    public Response upload(InputStream stream, String key, boolean overwrite) throws QiniuException {
        return uploadManager.put(stream, key, getUploadToken(key, overwrite), null, null);
    }


    /**
     * 通过HEAD来获取指定的key大小
     */
    public int getLength(String key) throws IOException {
        Request.Builder requestBuilder = new Request.Builder().url(getFileUrlByKey(key)).head();

        try {
            Response response = client.send(requestBuilder, null);
            String len = response.header("content-length", null);
            if (len == null) {
                throw new IOException(String.format("Cannot get object length by key: %s", key));
            }

            return Integer.parseInt(len);
        } catch (QiniuException e) {
            if (e.response != null && e.response.statusCode == 612) {
                throw new FileNotFoundException("key: " + key);
            }
            throw e;
        }
    }

    /**
     * 根据指定的key和文件大小获取一个输入流
     */
    public InputStream fetch(String key, long offset, int size) throws IOException {
        try {
            StringMap header = new StringMap();
            header.put("Range", String.format("bytes=%d-%d", offset, offset + size - 1));
            String url = getFileUrlByKey(key);
            LOG.debug("fetch content by url: {}", url);
            Response response = this.client.get(url, header);
            return response.bodyStream();
        } catch (QiniuException e) {
            if (e.response == null) {
                throw e;
            }
            throw new IOException(e.response.toString());
        }
    }

    /**
     * 若 withDelimiter 为 true, 则列举出分级的目录效果
     * 否则，将呈现出所有前缀为key的对象
     */
    public List<FileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        List<FileInfo> retFiles = new ArrayList<>();

        String marker = null;
        FileListing fileListing;
        do {
            fileListing = bucketManager.listFilesV2(bucket, key, marker, 100, useDirectory ? QiniuKodoUtils.PATH_SEPARATOR : "");
            if (statistics != null) statistics.incrementReadOps(1);

            // 列举出除自身外的所有对象
            if (fileListing.items != null) {
                for (FileInfo file : fileListing.items) {
                    if (key.equals(file.key)) continue;
                    retFiles.add(file);
                }
                fileListing = bucketManager.listFilesV2(bucket, key, marker, 100, useDirectory ? QiniuKodoUtils.PATH_SEPARATOR : "");
                if (statistics != null) statistics.incrementReadOps(1);
            }

            // 列举出目录
            if (fileListing.commonPrefixes != null) {
                for (String dirPath : fileListing.commonPrefixes) {
                    FileInfo dir = new FileInfo();
                    dir.key = dirPath;
                    retFiles.add(dir);
                }
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());

        return retFiles;
    }

    /**
     * 复制对象
     */
    public boolean copyKey(String oldKey, String newKey) throws IOException {
        Response response = bucketManager.copy(bucket, oldKey, bucket, newKey);
        return response.isOK();
    }

    /**
     * 批量复制对象
     */
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFilesV2(bucket, oldPrefix, marker, 100, "");
            if (fileListing.items != null) {
                BucketManager.BatchOperations operations = null;
                for (FileInfo file : fileListing.items) {
                    if (operations == null) operations = new BucketManager.BatchOperations();
                    String destKey = file.key.replaceFirst(oldPrefix, newPrefix);
                    LOG.debug(" == copy old: {} new: {}", file.key, destKey);
                    operations.addCopyOp(bucket, file.key, bucket, destKey);
                }
                if (operations == null) continue;
                Response response = bucketManager.batch(operations);
                if (!response.isOK()) return false;
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());
        return true;
    }

    /**
     * 重命名指定 key 的对象
     */
    public boolean renameKey(String oldKey, String newKey) throws IOException {
        if (Objects.equals(oldKey, newKey)) return true;
        Response response = bucketManager.rename(bucket, oldKey, newKey);
        if (statistics != null) statistics.incrementReadOps(1);
        return response.isOK();
    }

    /**
     * 批量重命名 key 为指定前缀的对象
     */
    public boolean renameKeys(String oldPrefix, String newPrefix) throws IOException {
        boolean hasPrefixObject = false;

        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFilesV2(bucket, oldPrefix, marker, 100, "");
            if (statistics != null) statistics.incrementReadOps(1);

            if (fileListing.items != null) {
                BucketManager.BatchOperations operations = null;
                for (FileInfo file : fileListing.items) {
                    if (file.key.equals(oldPrefix)) {
                        // 标记一下 prefix 本身留到最后再去修改
                        hasPrefixObject = true;
                        continue;
                    }
                    if (operations == null) operations = new BucketManager.BatchOperations();
                    String destKey = file.key.replaceFirst(oldPrefix, newPrefix);
                    LOG.debug(" == rename old: {} new: {}", file.key, destKey);
                    operations.addRenameOp(bucket, file.key, destKey);
                }
                if (operations == null) continue;
                Response response = bucketManager.batch(operations);
                if (statistics != null) statistics.incrementReadOps(1);

                if (!response.isOK()) return false;
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());

        if (hasPrefixObject) {
            Response response = bucketManager.rename(bucket, oldPrefix, newPrefix);
            return response.isOK();
        }
        return true;
    }

    /**
     * 仅删除一层 key
     */
    public boolean deleteKey(String key) throws IOException {
        Response response = bucketManager.delete(bucket, key);
        if (statistics != null) statistics.incrementReadOps(1);
        return response.isOK();
    }

    /**
     * 删除该前缀的所有文件夹
     */
    public boolean deleteKeys(String prefix, boolean recursive) throws IOException {
        boolean hasPrefixObject = false;

        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFilesV2(bucket, prefix, marker, 100, "");
            for (FileInfo file : fileListing.items) {
                // 略过自身
                if (file.key.equals(prefix)) continue;
                // 除去自身外还有子文件文件，但未 recursive 抛出异常
                if (!recursive) throw new IOException("file" + prefix + "is not empty");
            }

            if (statistics != null) statistics.incrementReadOps(1);

            if (fileListing.items != null) {
                BucketManager.BatchOperations operations = new BucketManager.BatchOperations();
                boolean shouldExecBatch = false;
                for (FileInfo file : fileListing.items) {
                    if (file.key.equals(prefix)) {
                        // 标记一下 prefix 本身留到最后再去删除
                        hasPrefixObject = true;
                        continue;
                    }
                    shouldExecBatch = true;
                    operations.addDeleteOp(bucket, file.key);
                }
                if (!shouldExecBatch) continue;
                Response response = bucketManager.batch(operations);
                if (statistics != null) statistics.incrementReadOps(1);

                if (!response.isOK()) return false;
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());

        if (hasPrefixObject) {
            Response response = bucketManager.delete(bucket, prefix);
            if (statistics != null) statistics.incrementReadOps(1);

            return response.isOK();
        }
        return true;
    }

    /**
     * 使用对象存储模拟文件系统，文件夹只是作为一个空白文件，仅用于表示文件夹的存在性与元数据的存储
     * 该 makeEmptyObject 仅创建一层空文件
     */
    public boolean makeEmptyObject(String key) throws IOException {
        byte[] content = new byte[]{};
        String token = auth.uploadToken(bucket, null, 3 * 3600L, null);
        Response response = uploadManager.put(content, key, token);
        return response.isOK();
    }

    /**
     * 不存在不抛异常，返回为空，只有在其他错误时抛异常
     */
    public FileInfo getFileStatus(String key) throws IOException {
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


    /**
     * 构造某个文件的下载url
     */
    private String getFileUrlByKey(String key) throws IOException {
        DownloadUrl downloadUrl = new DownloadUrl(downloadDomain, useHttps, key);
        String url = downloadUrl.buildURL();
        if (!useSign) {
            return url;
        }
        return auth.privateDownloadUrl(url, signExpires);
    }
}
