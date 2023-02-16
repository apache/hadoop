package org.apache.hadoop.fs.qiniu.kodo.client;


import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.ProxyConfiguration;
import com.qiniu.http.Response;
import com.qiniu.storage.*;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import okhttp3.Request;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoUtils;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.BatchOperationConsumer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.ListingProducer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.Product;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.BatchOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.CopyOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.DeleteOperator;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;
import org.apache.hadoop.fs.qiniu.kodo.config.ProxyConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.CopyConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.ListProducerConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.region.QiniuKodoPublicRegions;
import org.apache.hadoop.fs.qiniu.kodo.config.region.QiniuKodoRegion;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class QiniuKodoClient implements IQiniuKodoClient {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoClient.class);

    // 仅有一个 bucket
    private final String bucket;

    private final Auth auth;

    private final Client client;

    public final UploadManager uploadManager;
    public final BucketManager bucketManager;

    private final boolean useDownloadHttps;

    private String downloadDomain;
    private final FileSystem.Statistics statistics;
    private final boolean downloadUseSign;
    private final int downloadSignExpires;

    private final int uploadSignExpires;
    private final boolean useNoCacheHeader;
    private final QiniuKodoFsConfig fsConfig;

    public QiniuKodoClient(String bucket, QiniuKodoFsConfig fsConfig, FileSystem.Statistics statistics) throws QiniuException, AuthorizationException {
        this.bucket = bucket;
        this.statistics = statistics;
        this.fsConfig = fsConfig;
        this.auth = getAuth(fsConfig);

        Configuration configuration = new Configuration();

        if (fsConfig.upload.v2.enable) {
            configuration.resumableUploadAPIVersion = Configuration.ResumableUploadAPIVersion.V2;
            configuration.resumableUploadAPIV2BlockSize = fsConfig.upload.v2.blockSize();
        } else {
            configuration.resumableUploadAPIVersion = Configuration.ResumableUploadAPIVersion.V1;
        }
        configuration.resumableUploadMaxConcurrentTaskCount = fsConfig.upload.maxConcurrentTasks;
        configuration.useHttpsDomains = fsConfig.upload.useHttps;
        configuration.accUpHostFirst = fsConfig.upload.accUpHostFirst;
        configuration.useDefaultUpHostIfNone = fsConfig.upload.useDefaultUpHostIfNone;

        QiniuKodoRegion region = null;

        // 配置七牛配置对象的region
        if (fsConfig.region.id == null) {
            // 没配置regionId默认当公有云处理，走autoRegion
            configuration.region = Region.autoRegion();
        } else {
            // 先尝试公有云获取
            region = QiniuKodoPublicRegions.getRegionById(fsConfig.region.id);

            if (region == null) {
                // 公有云找不到相应id，寻找用户自定义配置的私有云
                try {
                    region = fsConfig.region.custom.getCustomRegion(fsConfig.region.id);
                } catch (MissingConfigFieldException e) {
                    throw new QiniuException(e);
                }
            }
        }


        this.useDownloadHttps = fsConfig.download.useHttps;
        configuration.useHttpsDomains = fsConfig.upload.useHttps;

        if (fsConfig.proxy.enable) {
            ProxyConfig proxyConfig = fsConfig.proxy;
            configuration.proxy = new ProxyConfiguration(
                    proxyConfig.hostname,
                    proxyConfig.port,
                    proxyConfig.username,
                    proxyConfig.password,
                    proxyConfig.type
            );
        }
        this.client = new Client(configuration);
        this.uploadManager = new UploadManager(configuration);
        this.bucketManager = new BucketManager(auth, configuration, this.client);

        // 设置下载域名，若未配置，则走源站
        downloadDomain = fsConfig.download.domain;
        if (downloadDomain == null) {
            // 尝试获取下载域名
            // 如果找不到区域配置，那就当作公有云发起请求获取区域信息, 这将用于下载域名的构造
            if (region == null) {
                String regionId = bucketManager.getBucketInfo(bucket).getRegion();
                region = QiniuKodoPublicRegions.getRegionById(regionId);
            }
            downloadDomain = bucket + "." + region.getRegionEndpoint();
        }

        this.downloadUseSign = fsConfig.download.sign.enable;
        this.downloadSignExpires = fsConfig.download.sign.expires;
        this.uploadSignExpires = fsConfig.upload.sign.expires;
        this.useNoCacheHeader = fsConfig.download.useNoCacheHeader;
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
        return auth.uploadToken(bucket, key, uploadSignExpires, policy);
    }

    /**
     * 给定一个输入流将读取并上传对应文件
     */
    @Override
    public Response upload(InputStream stream, String key, boolean overwrite) throws IOException {
        if (stream.available() > 0) {
            return uploadManager.put(stream, key, getUploadToken(key, overwrite), null, null);
        }
        int b = stream.read();
        if (b == -1) {
            // 空流
            return uploadManager.put(new byte[0], key, getUploadToken(key, overwrite));
        }
        // 有内容，还得拼回去
        SequenceInputStream sis = new SequenceInputStream(new ByteArrayInputStream(new byte[]{(byte) b}), stream);
        return uploadManager.put(sis, key, getUploadToken(key, overwrite), null, null);
    }


    /**
     * 通过HEAD来获取指定的key大小
     */
    @Override
    public long getLength(String key) throws IOException {
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


    @Override
    public boolean exists(String key) throws IOException {
        Request.Builder requestBuilder = new Request.Builder().url(getFileUrlByKey(key)).head();

        try {
            Response response = client.send(requestBuilder, null);
            // 找到了
            return response.isOK();
        } catch (QiniuException e) {
            if (e.response != null && e.response.statusCode == 404) {
                // 文件找不到
                return false;
            }
            throw e;
        }
    }

    /**
     * 根据指定的key和文件大小获取一个输入流
     */
    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        try {
            StringMap header = new StringMap();
            header.put("Range", String.format("bytes=%d-%d", offset, offset + size - 1));
            if (useNoCacheHeader) {
                header.put("X-QN-NOCACHE", 1);
            }
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
     * 获取一个指定前缀的对象
     */
    @Override
    public FileInfo listOneStatus(String keyPrefix) throws IOException {
        FileListing list = bucketManager.listFiles(bucket, keyPrefix, null, 1, "");
        if (list.items.length == 0) {
            return null;
        } else {
            return list.items[0];
        }
    }

    public List<FileInfo> listStatusOld(String key, boolean useDirectory) throws IOException {
        LOG.info("key: {}, useDirectory: {}", key, useDirectory);
        List<FileInfo> retFiles = new ArrayList<>();

        String marker = null;
        FileListing fileListing;
        do {
            fileListing = bucketManager.listFiles(bucket, key, marker, 1000, useDirectory ? QiniuKodoUtils.PATH_SEPARATOR : "");

            incrementOneReadOps();

            // 列举出除自身外的所有对象
            if (fileListing.items != null) {
                for (FileInfo file : fileListing.items) {
                    if (key.equals(file.key)) {
                        continue;
                    }
                    retFiles.add(file);
                }
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

    ExecutorService service = Executors.newFixedThreadPool(8);

    /**
     * 若 useDirectory 为 true, 则列举出分级的目录效果
     * 否则，将呈现出所有前缀为key的对象
     */
    @Override
    public List<FileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        LOG.info("key: {}, useDirectory: {}", key, useDirectory);

        ListProducerConfig listConfig = fsConfig.client.list;
        // 最终结果
        List<FileInfo> retFiles = new ArrayList<>();

        // 消息队列
        BlockingQueue<Product<FileInfo, QiniuException>> fileInfoQueue = new LinkedBlockingQueue<>(listConfig.bufferSize);

        // 生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, key,
                false, listConfig.singleRequestLimit,
                useDirectory, listConfig.useListV2,
                listConfig.offerTimeout
        );

        // 生产者线程
        service.submit(producer);

        for (; ; ) {
            Product<FileInfo, QiniuException> product = fileInfoQueue.poll();
            // 缓冲区队列为空
            if (product == null) {
                continue;
            }
            // EOF
            if (product.isEOF()) {
                break;
            }
            // Exception
            if (product.hasException()) {
                throw product.getException();
            }
            if (product.hasValue()) {
                // 既不为空也不为EOF
                retFiles.add(product.getValue());
            }
        }

        return retFiles;
    }

    /**
     * 复制对象
     */
    @Override
    public boolean copyKey(String oldKey, String newKey) throws IOException {
        Response response = bucketManager.copy(bucket, oldKey, bucket, newKey);
        return response.isOK();
    }

    /**
     * 批量复制对象
     */

    public boolean copyKeysOld(String oldPrefix, String newPrefix) throws IOException {
        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFiles(bucket, oldPrefix, marker, 100, "");
            if (fileListing.items != null) {
                BucketManager.BatchOperations operations = null;
                for (FileInfo file : fileListing.items) {
                    if (operations == null) {
                        operations = new BucketManager.BatchOperations();
                    }
                    String destKey = file.key.replaceFirst(oldPrefix, newPrefix);
                    LOG.debug(" == copy old: {} new: {}", file.key, destKey);
                    operations.addCopyOp(bucket, file.key, bucket, destKey);
                }
                if (operations == null) {
                    continue;
                }
                Response response = bucketManager.batch(operations);
                if (!response.isOK()) {
                    return false;
                }
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());
        return true;
    }

    @Override
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        CopyConfig copyConfig = fsConfig.client.copy;
        // 消息队列
        // 对象列举生产者队列
        BlockingQueue<Product<FileInfo, QiniuException>> fileInfoQueue = new LinkedBlockingQueue<>(copyConfig.listProducer.bufferSize);
        // 批量复制队列
        BlockingQueue<BatchOperator> operatorQueue = new LinkedBlockingQueue<>(copyConfig.batchConsumer.bufferSize);

        // 对象列举生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, oldPrefix,
                true, copyConfig.listProducer.singleRequestLimit,
                false, copyConfig.listProducer.useListV2,
                copyConfig.listProducer.offerTimeout
        );

        // 生产者线程
        service.submit(producer);

        // 消费者线程
        int consumerCount = copyConfig.batchConsumer.count;
        BatchOperationConsumer[] consumers = new BatchOperationConsumer[consumerCount];
        Future<?>[] futures = new Future[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new BatchOperationConsumer(
                    operatorQueue, bucketManager,
                    copyConfig.batchConsumer.singleBatchRequestLimit,
                    copyConfig.batchConsumer.pollTimeout
            );
            futures[i] = service.submit(consumers[i]);
        }

        // 从生产者队列取出产品并加工后放入消费者队列
        for (; ; ) {
            Product<FileInfo, QiniuException> product = fileInfoQueue.poll();
            // 缓冲区队列为空
            if (product == null) continue;
            // EOF
            if (product.isEOF()) break;
            // Exception
            if (product.hasException()) throw product.getException();
            // 无值
            if (!product.hasValue()) continue;

            String fromFileKey = product.getValue().key;
            String toFileKey = fromFileKey.replaceFirst(oldPrefix, newPrefix);
            BatchOperator operator = new CopyOperator(bucket, fromFileKey, bucket, toFileKey);

            boolean success;
            do {
                success = operatorQueue.offer(operator);
            } while (!success);
        }

        LOG.debug("生产者生产完毕");
        // 发送关闭信号
        for (int i = 0; i < consumerCount; i++) {
            consumers[i].stop();
        }
        // 等待所有的消费者消费完毕
        for (int i = 0; i < consumerCount; i++) {
            try {
                futures[i].get();
                LOG.debug("消费者{}号消费完毕", i);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    /**
     * 重命名指定 key 的对象
     */
    @Override
    public boolean renameKey(String oldKey, String newKey) throws IOException {
        if (Objects.equals(oldKey, newKey)) {
            return true;
        }
        Response response = bucketManager.rename(bucket, oldKey, newKey);
        incrementOneReadOps();

        return response.isOK();
    }

    /**
     * 批量重命名 key 为指定前缀的对象
     */
    @Override
    public boolean renameKeys(String oldPrefix, String newPrefix) throws IOException {
        boolean hasPrefixObject = false;

        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFiles(bucket, oldPrefix, marker, 100, "");
            incrementOneReadOps();

            if (fileListing.items != null) {
                BucketManager.BatchOperations operations = null;
                for (FileInfo file : fileListing.items) {
                    if (file.key.equals(oldPrefix)) {
                        // 标记一下 prefix 本身留到最后再去修改
                        hasPrefixObject = true;
                        continue;
                    }
                    if (operations == null) {
                        operations = new BucketManager.BatchOperations();
                    }
                    String destKey = file.key.replaceFirst(oldPrefix, newPrefix);
                    LOG.debug(" == rename old: {} new: {}", file.key, destKey);
                    operations.addRenameOp(bucket, file.key, destKey);
                }
                if (operations == null) {
                    continue;
                }
                Response response = bucketManager.batch(operations);
                incrementOneReadOps();

                if (!response.isOK()) {
                    return false;
                }
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
    @Override
    public boolean deleteKey(String key) throws IOException {
        Response response = bucketManager.delete(bucket, key);
        incrementOneReadOps();
        return response.isOK();
    }

    /**
     * 删除该前缀的所有文件夹
     */
    public boolean deleteKeys1(String prefix, boolean recursive) throws IOException {
        boolean hasPrefixObject = false;

        FileListing fileListing;

        // 为分页遍历提供下一次的遍历标志
        String marker = null;

        do {
            fileListing = bucketManager.listFiles(bucket, prefix, marker, 100, "");
            for (FileInfo file : fileListing.items) {
                // 略过自身
                if (file.key.equals(prefix)) {
                    continue;
                }
                // 除去自身外还有子文件文件，但未 recursive 抛出异常
                if (!recursive) {
                    throw new IOException("file" + prefix + "is not empty");
                }
            }
            incrementOneReadOps();

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
                if (!shouldExecBatch) {
                    continue;
                }
                Response response = bucketManager.batch(operations);
                if (statistics != null) {
                    statistics.incrementReadOps(1);
                }

                if (!response.isOK()) {
                    return false;
                }
            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());

        if (hasPrefixObject) {
            Response response = bucketManager.delete(bucket, prefix);
            incrementOneReadOps();
            return response.isOK();
        }
        return true;
    }

    @Override
    public boolean deleteKeys(String prefix, boolean recursive) throws IOException {
        // 消息队列
        BlockingQueue<Product<FileInfo, QiniuException>> fileInfoQueue = new LinkedBlockingQueue<>(500);
        BlockingQueue<BatchOperator> operatorQueue = new LinkedBlockingQueue<>(200);

        // 生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefix,
                false, 100,
                false, false,
                10
        );

        // 生产者线程
        service.submit(producer);

        // 消费者
        BatchOperationConsumer consumer = new BatchOperationConsumer(
                operatorQueue, bucketManager, 20, 10
        );
        service.submit(consumer);
        service.submit(consumer);


        for (; ; ) {
            Product<FileInfo, QiniuException> product = fileInfoQueue.poll();
            // 缓冲区队列为空
            if (product == null) {
                continue;
            }
            // EOF
            if (product.isEOF()) {
                break;
            }
            // Exception
            if (product.hasException()) {
                throw product.getException();
            }
            // 无值
            if (!product.hasValue()) {
                continue;
            }

            try {
                boolean success;
                do {
                    success = operatorQueue.offer(new DeleteOperator(bucket, product.getValue().key), 1, TimeUnit.SECONDS);
                } while (!success);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    private void incrementOneReadOps() {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }
    }

    /**
     * 使用对象存储模拟文件系统，文件夹只是作为一个空白文件，仅用于表示文件夹的存在性与元数据的存储
     * 该 makeEmptyObject 仅创建一层空文件
     */

    public boolean makeEmptyObject(String key, boolean overwrite) throws IOException {
        byte[] content = new byte[]{};
        StringMap policy = new StringMap();
        policy.put("insertOnly", overwrite ? 0 : 1);
        String token = auth.uploadToken(bucket, null, uploadSignExpires, policy);
        Response response = uploadManager.put(content, key, token);
        return response.isOK();
    }

    @Override
    public boolean makeEmptyObject(String key) throws IOException {
        return this.makeEmptyObject(key, false);
    }

    /**
     * 不存在不抛异常，返回为空，只有在其他错误时抛异常
     */
    @Override
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
        DownloadUrl downloadUrl = new DownloadUrl(downloadDomain, useDownloadHttps, key);
        String url = downloadUrl.buildURL();
        if (downloadUseSign) {
            return auth.privateDownloadUrl(url, downloadSignExpires);
        }
        return url;
    }
}
