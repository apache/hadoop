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
import org.apache.hadoop.fs.qiniu.kodo.client.batch.BatchOperationConsumer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.ListingProducer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.BatchOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.CopyOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.DeleteOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.RenameOperator;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;
import org.apache.hadoop.fs.qiniu.kodo.config.ProxyConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListAndBatchBaseConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListProducerConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

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
    private final QiniuKodoFsConfig fsConfig;
    private final ExecutorService service;
    private final DownloadHttpClient downloadHttpClient;

    public QiniuKodoClient(String bucket, QiniuKodoFsConfig fsConfig, FileSystem.Statistics statistics) throws QiniuException, AuthorizationException {
        this.bucket = bucket;
        this.statistics = statistics;
        this.fsConfig = fsConfig;
        this.auth = getAuth(fsConfig);
        this.service = Executors.newFixedThreadPool(fsConfig.client.nThread);

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


        // 配置七牛配置对象的 region
        if (fsConfig.customRegion.id == null) {
            // 没配置regionId默认当公有云处理
            // 公有云走autoRegion
            configuration.region = Region.autoRegion();
        } else {
            // 私有云走自定义Region配置
            try {
                configuration.region = fsConfig.customRegion.getCustomRegion();
            } catch (MissingConfigFieldException e) {
                throw new QiniuException(e);
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

        // 设置下载域名
        this.downloadDomain = fsConfig.download.domain;
        if (this.downloadDomain == null) {
            // 下载域名未配置时
            if (fsConfig.customRegion.id == null) {
                // 公有云通过uc query api获取源站域名构造下载链接
                this.downloadDomain = UCQuery.getDownloadDomainFromUC(client, fsConfig.auth.accessKey, bucket);
            } else {
                // 私有云未配置下载域名时抛出异常
                throw new QiniuException(new MissingConfigFieldException(String.format(
                        "download domain can't be empty, you should set it with %s in core-site.xml",
                        fsConfig.download.KEY_DOMAIN
                )));
            }
        }

        this.downloadUseSign = fsConfig.download.sign.enable;
        this.downloadSignExpires = fsConfig.download.sign.expires;
        this.uploadSignExpires = fsConfig.upload.sign.expires;
        this.downloadHttpClient = new DownloadHttpClient(configuration, fsConfig.download.useNoCacheHeader);
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
        Request.Builder requestBuilder = new Request.Builder()
                .url(getFileUrlByKey(key))
                .header("Accept-Encoding", "identity")    // Content-Length返回的是压缩后的大小
                .head();

        try {
            Response response = client.send(requestBuilder, null);
            String len = response.header("content-length", null);
            if (len == null) {
                throw new IOException(String.format("Cannot get object length by key: %s", key));
            }

            return Integer.parseInt(len);
        } catch (QiniuException e) {
            if (e.response == null) {
                throw e;
            }
            switch (e.response.statusCode) {
                case 612:
                case 404:
                    throw new FileNotFoundException("key: " + key);
                default:
                    throw e;
            }
        }
    }


    @Override
    public boolean exists(String key) throws IOException {
        return getFileStatus(key) != null;
    }

    /**
     * 根据指定的key和文件大小获取一个输入流
     */
    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        return downloadHttpClient.fetch(getFileUrlByKey(key), offset, size);
    }

    /**
     * 获取一个指定前缀的对象
     */
    @Override
    public FileInfo listOneStatus(String keyPrefix) throws IOException {
        List<FileInfo> ret = listNStatus(keyPrefix, 1);
        if (ret.isEmpty()) {
            return null;
        }
        return ret.get(0);
    }

    /**
     * 获取指定前缀的最多前n个对象
     */
    public List<FileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        FileListing listing = bucketManager.listFiles(bucket, keyPrefix, null, n, "");
        if (listing.items == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(listing.items);
    }

    public Iterator<FileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException {
        ListProducerConfig listConfig = fsConfig.client.list;
        // 消息队列
        BlockingQueue<FileInfo> fileInfoQueue = new LinkedBlockingQueue<>(listConfig.bufferSize);

        // 生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefixKey, false,
                listConfig.singleRequestLimit,
                useDirectory, listConfig.useListV2,
                listConfig.offerTimeout
        );

        // 生产者线程
        Future<Exception> future = service.submit(producer);

        return new Iterator<FileInfo>() {
            @Override
            public boolean hasNext() {
                if (!fileInfoQueue.isEmpty()) {
                    return true;
                }
                // 生产缓冲区队列为空，且生产任务完成，则没有下一个了，返回false
                return !future.isDone();
            }

            @Override
            public FileInfo next() {
                if (!hasNext()) {
                    return null;
                }

                try {
                    return fileInfoQueue.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    return null;
                }
            }
        };
    }

    /**
     * 若 useDirectory 为 true, 则列举出分级的目录效果
     * 否则，将呈现出所有前缀为key的对象
     */
    @Override
    public List<FileInfo> listStatus(String prefixKey, boolean useDirectory) throws IOException {
        LOG.info("key: {}, useDirectory: {}", prefixKey, useDirectory);

        ListProducerConfig listConfig = fsConfig.client.list;
        // 最终结果
        List<FileInfo> retFiles = new ArrayList<>();

        // 消息队列
        BlockingQueue<FileInfo> fileInfoQueue = new LinkedBlockingQueue<>(listConfig.bufferSize);

        // 生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefixKey, false,
                listConfig.singleRequestLimit,
                useDirectory, listConfig.useListV2,
                listConfig.offerTimeout
        );

        // 生产者线程
        Future<Exception> future = service.submit(producer);

        // 只要生产者未生产完毕或队列非空就不断地轮询队列
        while (!future.isDone() || !fileInfoQueue.isEmpty()) {
            FileInfo product = fileInfoQueue.poll();
            // 缓冲区队列为空，等待下一次轮询
            if (product == null) {
                continue;
            }
            // 跳过自身
            if (product.key.equals(prefixKey)) {
                continue;
            }
            retFiles.add(product);
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
     * 列举并批处理
     *
     * @param config    生产消费相关的配置
     * @param prefixKey 生产列举的key前缀
     * @param f         消费操作函数
     */
    private boolean listAndBatch(
            ListAndBatchBaseConfig config,
            String prefixKey,
            Function<FileInfo, BatchOperator> f
    ) throws IOException {
        // 消息队列
        // 对象列举生产者队列
        BlockingQueue<FileInfo> fileInfoQueue = new LinkedBlockingQueue<>(config.listProducer.bufferSize);
        // 批处理队列
        BlockingQueue<BatchOperator> operatorQueue = new LinkedBlockingQueue<>(config.batchConsumer.bufferSize);

        // 对象列举生产者
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefixKey, true,
                config.listProducer.singleRequestLimit, false,
                config.listProducer.useListV2,
                config.listProducer.offerTimeout
        );
        // 生产者线程
        Future<Exception> producerFuture = service.submit(producer);

        // 消费者线程
        int consumerCount = config.batchConsumer.count;
        BatchOperationConsumer[] consumers = new BatchOperationConsumer[consumerCount];
        Future<?>[] futures = new Future[consumerCount];

        // 多消费者共享一个队列
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new BatchOperationConsumer(
                    operatorQueue, bucketManager,
                    config.batchConsumer.singleBatchRequestLimit,
                    config.batchConsumer.pollTimeout
            );
            futures[i] = service.submit(consumers[i]);
        }

        // 从生产者队列取出产品并加工后放入消费者队列
        while (!producerFuture.isDone() || !fileInfoQueue.isEmpty()) {
            FileInfo product = fileInfoQueue.poll();
            // 缓冲区队列为空
            if (product == null) {
                continue;
            }

            boolean success;
            do {
                success = operatorQueue.offer(f.apply(product));
            } while (!success);
        }

        // 生产完毕
        try {
            if (producerFuture.get() != null) {
                throw producerFuture.get();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        LOG.debug("生产者生产完毕");

        // 等待消费队列为空
        while (true) {
            if (operatorQueue.isEmpty()) {
                break;
            }
        }

        // 向所有消费者发送关闭信号
        for (int i = 0; i < consumerCount; i++) {
            consumers[i].stop();
        }

        // 等待所有的消费者消费完毕
        for (int i = 0; i < consumerCount; i++) {
            try {
                if (futures[i].get() != null) {
                    throw (Exception) futures[i].get();
                }
                LOG.debug("消费者{}号消费完毕", i);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return true;
    }

    @Override
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        return listAndBatch(
                fsConfig.client.copy,
                oldPrefix,
                (FileInfo fileInfo) -> {
                    String fromFileKey = fileInfo.key;
                    String toFileKey = fromFileKey.replaceFirst(oldPrefix, newPrefix);
                    return new CopyOperator(bucket, fromFileKey, bucket, toFileKey);
                }
        );
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
        return listAndBatch(
                fsConfig.client.rename,
                oldPrefix,
                (FileInfo fileInfo) -> {
                    String fromFileKey = fileInfo.key;
                    String toFileKey = fromFileKey.replaceFirst(oldPrefix, newPrefix);
                    return new RenameOperator(bucket, fromFileKey, toFileKey);
                }
        );
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

    @Override
    public boolean deleteKeys(String prefix) throws IOException {
        return listAndBatch(
                fsConfig.client.delete,
                prefix,
                e -> new DeleteOperator(bucket, e.key)
        );
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
