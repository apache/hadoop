package org.apache.hadoop.fs.qiniu.kodo.performance;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoPerformanceBaseTest.class);
    private static final String DEFAULT_PREPARE_FILE_DIR = "/testPrepare";
    private static final String DEFAULT_KODO_TEST_DIR = "/testKodo";
    private static final String DEFAULT_S3A_TEST_DIR = "/testS3A";
    private final QiniuKodoFileSystem kodoFs = new QiniuKodoFileSystem();
    private final S3AFileSystem s3aFs = new S3AFileSystem();

    private static final Map<String, Map<String, Object>> testResult = new HashMap<>();

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        if (enableKodoTest()) {
            kodoFs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        }

        if (enableS3ATest()) {
            s3aFs.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
        }

        if (!testResult.containsKey(getSceneString())) {
            testResult.put(getSceneString(), new HashMap<>());
        }

        testResult.get(getSceneString()).put("data", testInputData());

        if (getSceneDescription() != null) {
            testResult.get(getSceneString()).put("description", getSceneDescription());
        }
    }

    protected TestPrepareHelper getPrepareHelper() throws Exception {
        Field field = QiniuKodoFileSystem.class.getDeclaredField("kodoClient");
        field.setAccessible(true);
        IQiniuKodoClient client = (IQiniuKodoClient) field.get(kodoFs);
        return new TestPrepareHelper(kodoFs, client,
                new Path(DEFAULT_PREPARE_FILE_DIR),
                Executors.newFixedThreadPool(10));
    }

    /**
     * 构造测试任务的执行器
     */
    abstract protected ExecutorService buildExecutorService();

    // 需要用户实现该方法
    abstract protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception;

    protected Map<String, Object> testInputData() {
        return new HashMap<>();
    }

    protected long testS3AImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs, buildExecutorService());
    }

    protected long testKodoImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs, buildExecutorService());
    }

    protected long timeoutN() {
        return 10;
    }

    protected TimeUnit timeoutUnit() {
        return TimeUnit.MINUTES;
    }

    protected void awaitAllExecutors(ExecutorService service) throws InterruptedException, TimeoutException {
        long n = timeoutN();
        TimeUnit unit = timeoutUnit();

        service.shutdown();
        // 规定时间内没结束
        if (!service.awaitTermination(n, unit)) {
            throw new TimeoutException(String.format("timeout: %d, %s", n, unit));
        }
    }

    protected String getKodoTestDir() {
        return DEFAULT_KODO_TEST_DIR;
    }

    protected String getS3ATestDir() {
        return DEFAULT_S3A_TEST_DIR;
    }

    protected String getSceneString() {
        return this.getClass().getSimpleName();
    }

    // 构造该测试场景的文件夹名称
    protected String getSceneWorkDirName() {
        return getSceneString();
    }

    // 测试场景描述
    protected String getSceneDescription() {
        return null;
    }

    protected boolean enableS3ATest() {
        return true;
    }

    protected boolean enableKodoTest() {
        return true;
    }

    @Test
    public void testS3A() throws Exception {
        if (!enableS3ATest()) {
            return;
        }
        long time = testS3AImpl(String.format("%s/%s", getS3ATestDir(), getSceneWorkDirName()), s3aFs);
        LOG.info(getSceneString() + " (S3A) " + "cost time: " + time);
        testResult.get(getSceneString()).put("s3aTime", time);
    }


    @Test
    public void testKodo() throws Exception {
        if (!enableKodoTest()) {
            return;
        }
        long time = testKodoImpl(String.format("%s/%s", getKodoTestDir(), getSceneWorkDirName()), kodoFs);
        LOG.info(getSceneString() + " (Kodo) " + "cost time: " + time);

        testResult.get(getSceneString()).put("kodoTime", time);
    }

    @After
    public void exportTestData() {
        Gson gson = new Gson();
        String json = gson.toJson(testResult);
        LOG.info(json);
    }
}
