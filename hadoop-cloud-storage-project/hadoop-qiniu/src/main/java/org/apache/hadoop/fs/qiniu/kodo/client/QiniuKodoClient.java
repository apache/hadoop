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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.BatchOperationConsumer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.ListingProducer;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.BatchOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.CopyOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.DeleteOperator;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.RenameOperator;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListAndBatchBaseConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListProducerConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QiniuKodoClient implements IQiniuKodoClient {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoClient.class);

    static {
        Client.setAppName("Hadoop " + VersionInfo.getVersion());
    }

    private final String bucket;

    private final Auth auth;

    private final Client client;

    public final UploadManager uploadManager;
    public final BucketManager bucketManager;

    private final boolean useDownloadHttps;

    private final String downloadDomain;
    private final FileSystem.Statistics statistics;
    private final boolean downloadUseSign;
    private final int downloadSignExpires;

    private final int uploadSignExpires;
    private final QiniuKodoFsConfig fsConfig;
    private final ExecutorService service;
    private final DownloadHttpClient downloadHttpClient;


    public QiniuKodoClient(
            String bucket,
            QiniuKodoFsConfig fsConfig,
            FileSystem.Statistics statistics
    ) throws QiniuException, AuthorizationException {
        this.bucket = bucket;
        this.statistics = statistics;

        this.fsConfig = fsConfig;
        this.auth = getAuth(fsConfig);
        this.service = Executors.newFixedThreadPool(fsConfig.client.nThread);

        Configuration configuration = buildQiniuConfiguration(fsConfig);
        this.useDownloadHttps = fsConfig.download.useHttps;
        this.client = new Client(configuration);
        this.uploadManager = new UploadManager(configuration);
        this.bucketManager = new BucketManager(auth, configuration, this.client);
        this.downloadDomain = buildDownloadHost(fsConfig, bucketManager, bucket);
        this.downloadUseSign = fsConfig.download.sign.enable;
        this.downloadSignExpires = fsConfig.download.sign.expires;
        this.uploadSignExpires = fsConfig.upload.sign.expires;
        this.downloadHttpClient = new DownloadHttpClient(configuration, fsConfig.download.useNoCacheHeader);
    }

    private static Configuration buildQiniuConfiguration(QiniuKodoFsConfig fsConfig) throws QiniuException {
        Configuration configuration = new Configuration();
        configuration.region = buildRegion(fsConfig);
        if (fsConfig.upload.v2.enable) {
            configuration.resumableUploadAPIVersion = Configuration.ResumableUploadAPIVersion.V2;
            configuration.resumableUploadAPIV2BlockSize = fsConfig.upload.v2.blockSize();
        } else {
            configuration.resumableUploadAPIVersion = Configuration.ResumableUploadAPIVersion.V1;
        }
        configuration.resumableUploadMaxConcurrentTaskCount = fsConfig.upload.maxConcurrentTasks;
        configuration.useHttpsDomains = fsConfig.useHttps;
        configuration.accUpHostFirst = fsConfig.upload.accUpHostFirst;
        configuration.useDefaultUpHostIfNone = fsConfig.upload.useDefaultUpHostIfNone;
        configuration.proxy = buildQiniuProxyConfiguration(fsConfig);
        return configuration;
    }

    private static ProxyConfiguration buildQiniuProxyConfiguration(QiniuKodoFsConfig fsConfig) {
        if (!fsConfig.proxy.enable) {
            return null;
        }
        return new ProxyConfiguration(
                fsConfig.proxy.hostname,
                fsConfig.proxy.port,
                fsConfig.proxy.username,
                fsConfig.proxy.password,
                fsConfig.proxy.type
        );
    }

    private static String buildDownloadHost(
            QiniuKodoFsConfig fsConfig,
            BucketManager bucketManager,
            String bucket
    ) throws QiniuException {
        // first use user defined download domain
        if (fsConfig.download.domain != null) {
            return fsConfig.download.domain;
        }
        // if not defined download domain, use bucket default origin domain
        return bucketManager.getDefaultIoSrcHost(bucket);
    }

    private static Region buildRegion(QiniuKodoFsConfig fsConfig) throws QiniuException {
        if (fsConfig.customRegion.id != null) {
            // This is private cloud region configuration
            try {
                return fsConfig.customRegion.getCustomRegion();
            } catch (MissingConfigFieldException e) {
                throw new QiniuException(e);
            }

        }
        // This is public cloud region configuration, auto detect region by uc server
        return Region.autoRegion();
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
     * Generate a upload token with key and overwrite flag
     */
    public String getUploadToken(String key, boolean overwrite) {
        StringMap policy = new StringMap();
        policy.put("insertOnly", overwrite ? 0 : 1);
        return auth.uploadToken(bucket, key, uploadSignExpires, policy);
    }

    private class QiniuUploader {
        private final boolean overwrite;

        private QiniuUploader(boolean overwrite) {
            this.overwrite = overwrite;
        }

        void upload(String key, InputStream stream) throws IOException {
            if (!uploadManager.put(stream, key, getUploadToken(key, overwrite),
                    null, null).isOK()) {
                throw new IOException("Upload failed"
                        + " bucket: " + bucket
                        + " key: " + key
                        + " overwrite: " + overwrite);
            }
        }

        void uploadArray(String key, byte[] data) throws IOException {
            if (!uploadManager.put(data, key, getUploadToken(key, overwrite)).isOK()) {
                throw new IOException("Upload failed"
                        + " bucket: " + bucket
                        + " key: " + key
                        + " overwrite: " + overwrite);
            }
        }

        void uploadEmpty(String key) throws IOException {
            uploadArray(key, new byte[0]);
        }
    }


    @Override
    public void upload(InputStream stream, String key, boolean overwrite) throws IOException {
        QiniuUploader uploader = new QiniuUploader(overwrite);
        if (stream.available() > 0) {
            uploader.upload(key, stream);
            return;
        }
        // If stream cannot read available bytes, we need to read first byte to check if stream is empty
        int b = stream.read();
        if (b == -1) {
            // If stream is empty, we need to upload a empty file
            uploader.uploadEmpty(key);
            return;
        }
        // If stream is not empty, we need to upload first byte and then upload the rest
        SequenceInputStream sis = new SequenceInputStream(new ByteArrayInputStream(new byte[]{(byte) b}), stream);
        uploader.upload(key, sis);
    }


    @Override
    public long getLength(String key) throws IOException {
        try {
            Response response = client.head(getFileUrlByKey(key),
                    new StringMap().put("Accept-Encoding", "identity"));
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
        // If use http head request to check file exists, maybe has more high performance
        // But if do this, will cause some cache problem, if file deleted but still return 200
        return getFileStatus(key) != null;
    }


    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        return downloadHttpClient.fetch(getFileUrlByKey(key), offset, size);
    }


    @Override
    public QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException {
        List<QiniuKodoFileInfo> ret = listNStatus(keyPrefix, 1);
        if (ret.isEmpty()) {
            return null;
        }
        return ret.get(0);
    }


    public List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        FileListing listing = bucketManager.listFiles(bucket, keyPrefix, null, n, "");
        if (listing.items == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(listing.items).map(QiniuKodoClient::qiniuFileInfoToMyFileInfo).collect(Collectors.toList());
    }


    public RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) {
        ListProducerConfig listConfig = fsConfig.client.list;
        BlockingQueue<FileInfo> fileInfoQueue = new LinkedBlockingQueue<>(listConfig.bufferSize);

        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefixKey, false,
                listConfig.singleRequestLimit,
                useDirectory, listConfig.useListV2,
                listConfig.offerTimeout
        );

        Future<Exception> future = service.submit(producer);
        return new RemoteIterator<QiniuKodoFileInfo>() {
            @Override
            public boolean hasNext() throws IOException {
                while (true) {
                    // If queue is not empty, return true means has next element
                    if (!fileInfoQueue.isEmpty()) {
                        return true;
                    }

                    // If producer thread is done and queue is empty, means no next element
                    if (future.isDone() && fileInfoQueue.isEmpty()) {
                        try {
                            Exception e = future.get();
                            // If producer thread throw exception, throw IOException here
                            if (e != null) {
                                throw new IOException(e);
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            throw new IOException(e);
                        }
                        return false;
                    }

                    // If producer thread is not done and queue is empty, wait a while
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
            }

            @Override
            public QiniuKodoFileInfo next() throws IOException {
                if (!hasNext()) {
                    return null;
                }

                try {
                    return qiniuFileInfoToMyFileInfo(fileInfoQueue.poll(Long.MAX_VALUE, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public List<QiniuKodoFileInfo> listStatus(String prefixKey, boolean useDirectory) throws IOException {
        return RemoteIterators.toList(listStatusIterator(prefixKey, useDirectory));
    }


    @Override
    public void copyKey(String oldKey, String newKey) throws IOException {
        bucketManager.copy(bucket, oldKey, bucket, newKey);
    }

    /**
     * list files by key prefix and do batch operation
     */
    private void listAndBatch(
            ListAndBatchBaseConfig config,
            String prefixKey,
            Function<FileInfo, BatchOperator> f
    ) throws IOException {
        BlockingQueue<FileInfo> fileInfoQueue = new LinkedBlockingQueue<>(config.listProducer.bufferSize);

        BlockingQueue<BatchOperator> operatorQueue = new LinkedBlockingQueue<>(config.batchConsumer.bufferSize);

        // Create producer
        ListingProducer producer = new ListingProducer(
                fileInfoQueue, bucketManager, bucket, prefixKey, true,
                config.listProducer.singleRequestLimit, false,
                config.listProducer.useListV2,
                config.listProducer.offerTimeout
        );
        // Producer thread
        Future<Exception> producerFuture = service.submit(producer);

        // Create consumers
        int consumerCount = config.batchConsumer.count;
        BatchOperationConsumer[] consumers = new BatchOperationConsumer[consumerCount];
        Future<?>[] futures = new Future[consumerCount];

        // multiple consumers share one queue
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new BatchOperationConsumer(
                    operatorQueue, bucketManager,
                    config.batchConsumer.singleBatchRequestLimit,
                    config.batchConsumer.pollTimeout
            );
            futures[i] = service.submit(consumers[i]);
        }

        // Take product from producer queue and put into consumer queue after processing
        while (!producerFuture.isDone() || !fileInfoQueue.isEmpty()) {
            FileInfo product = fileInfoQueue.poll();
            // Buffer queue is empty
            if (product == null) {
                continue;
            }

            boolean success;
            do {
                success = operatorQueue.offer(f.apply(product));
            } while (!success);
        }

        try {
            if (producerFuture.get() != null) {
                throw producerFuture.get();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        LOG.debug("Producer finished");

        // Wait for all operators to be consumed
        while (true) {
            if (operatorQueue.isEmpty()) {
                break;
            }
        }

        // Send stop signal to all consumers
        for (int i = 0; i < consumerCount; i++) {
            consumers[i].stop();
        }

        // Wait for all consumers to finish
        for (int i = 0; i < consumerCount; i++) {
            try {
                if (futures[i].get() != null) {
                    throw (Exception) futures[i].get();
                }
                LOG.debug("Consumer {} finished", i);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void copyKeys(String oldPrefix, String newPrefix) throws IOException {
        listAndBatch(
                fsConfig.client.copy,
                oldPrefix,
                (FileInfo fileInfo) -> {
                    String fromFileKey = fileInfo.key;
                    String toFileKey = fromFileKey.replaceFirst(oldPrefix, newPrefix);
                    return new CopyOperator(bucket, fromFileKey, bucket, toFileKey);
                }
        );
    }


    @Override
    public void renameKey(String oldKey, String newKey) throws IOException {
        if (Objects.equals(oldKey, newKey)) {
            return;
        }
        bucketManager.rename(bucket, oldKey, newKey);
        incrementOneReadOps();
    }


    @Override
    public void renameKeys(String oldPrefix, String newPrefix) throws IOException {
        listAndBatch(
                fsConfig.client.rename,
                oldPrefix,
                (FileInfo fileInfo) -> {
                    String fromFileKey = fileInfo.key;
                    String toFileKey = fromFileKey.replaceFirst(oldPrefix, newPrefix);
                    return new RenameOperator(bucket, fromFileKey, toFileKey);
                }
        );
    }


    @Override
    public void deleteKey(String key) throws IOException {
        bucketManager.delete(bucket, key);
        incrementOneReadOps();
    }


    @Override
    public void deleteKeys(String prefix) throws IOException {
        listAndBatch(
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


    @Override
    public void makeEmptyObject(String key) throws IOException {
        QiniuUploader uploader = new QiniuUploader(false);
        uploader.uploadEmpty(key);
    }


    @Override
    public QiniuKodoFileInfo getFileStatus(String key) throws IOException {
        try {
            FileInfo fileInfo = bucketManager.stat(bucket, key);
            if (fileInfo != null) {
                fileInfo.key = key;
            }
            return qiniuFileInfoToMyFileInfo(fileInfo);
        } catch (QiniuException e) {
            if (e.response != null && e.response.statusCode == 612) {
                return null;
            }
            throw e;
        }
    }


    /**
     * Build a file download url by key
     */
    private String getFileUrlByKey(String key) throws IOException {
        DownloadUrl downloadUrl = new DownloadUrl(downloadDomain, useDownloadHttps, key);
        String url = downloadUrl.buildURL();
        if (downloadUseSign) {
            return auth.privateDownloadUrl(url, downloadSignExpires);
        }
        return url;
    }

    private static QiniuKodoFileInfo qiniuFileInfoToMyFileInfo(FileInfo fileInfo) {
        if (fileInfo == null) return null;
        return new QiniuKodoFileInfo(
                fileInfo.key,
                fileInfo.fsize,
                fileInfo.putTime / 10000
        );
    }
}
