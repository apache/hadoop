package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.storage.Region;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class QiniuKodoFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystem.class);

    private URI uri;
    private String bucket;
    private String username;
    private Path workingDir;

    private int blockSize = Constants.QINIU_DEFAULT_VALUE_BLOCK_SIZE;

    private QiniuKodoClient kodoClient;

    private Configuration conf;
    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.conf = conf;
        bucket = name.getHost();
        LOG.info("== bucket:" + bucket);

        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        LOG.info("== uri:" + uri);

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        LOG.info("== username:" + username);

        workingDir = new Path("/user", username).makeQualified(uri, null);
        LOG.info("== workingDir:" + workingDir);

        blockSize = conf.getInt(Constants.QINIU_PARAMETER_BLOCK_SIZE, Constants.QINIU_DEFAULT_VALUE_BLOCK_SIZE);
        LOG.info("== blockSize:" + blockSize);

        com.qiniu.storage.Configuration qiniuConfig = new com.qiniu.storage.Configuration();
        qiniuConfig.region = Region.autoRegion();

        String accessKey = conf.get(Constants.QINIU_PARAMETER_ACCESS_KEY);
        if (accessKey == null || accessKey.length() == 0) {
            throw new IOException("Qiniu access key can't empty, you should set it with "
                    + Constants.QINIU_PARAMETER_ACCESS_KEY + " in core-site.xml");
        }

        String secretKey = conf.get(Constants.QINIU_PARAMETER_SECRET_KEY);
        if (secretKey == null || secretKey.length() == 0) {
            throw new IOException("Qiniu secret key can't empty, you should set it with "
                    + Constants.QINIU_PARAMETER_SECRET_KEY + " in core-site.xml");
        }

        Auth auth = Auth.create(accessKey, secretKey);
        kodoClient = new QiniuKodoClient(auth, qiniuConfig, bucket);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * 打开一个文件，返回一个可以被读取的输入流
     */
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.info("== open, path:" + path);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.info("== open, key:" + key);

        return new FSDataInputStream(kodoClient.open(key, bufferSize));
    }

    /**
     * 创建一个文件，返回一个可以被写入的输出流
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        LOG.info("== create, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.info("== create, key:" + key + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        return new FSDataOutputStream(kodoClient.create(key, bufferSize, overwrite), null);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Append is not supported.");
    }


    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.info("== rename, SrcPath:" + src + " dstPath:" + dst);

        if (src.isRoot()) {
            if (LOG.isDebugEnabled()) {
                LOG.info("== cannot rename the root of a filesystem");
            }
            return false;
        }


        // 判断是否是文件
        FileStatus file = getFileStatus(src);
        if (file == null) {
            throw new FileNotFoundException("can't find file:" + src);
        }

        String srcKey = QiniuKodoUtils.pathToKey(workingDir, src);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dst);

        if (file.isDirectory()) {
            LOG.info("== rename file, srcKey:" + srcKey + " dstKey:" + dstKey);
            return kodoClient.renameKeys(srcKey, dstKey);
        } else {
            LOG.info("== rename dir, srcKey:" + srcKey + " dstKey:" + dstKey);
            return kodoClient.renameKey(srcKey, dstKey);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.info("== delete, path:" + path + " recursive:" + recursive);

        // 判断是否是文件
        FileStatus file = getFileStatus(path);
        if (file == null) {
            throw new FileNotFoundException("can't find file:" + path);
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.info("== delete, key:" + key);

        if (file.isDirectory()) {
            return deleteDir(key, recursive);
        } else {
            return deleteFile(key);
        }
    }

    private boolean deleteFile(String fileKey) throws IOException {
        fileKey = QiniuKodoUtils.keyToFileKey(fileKey);
        LOG.info("== delete, fileKey:" + fileKey);

        return kodoClient.deleteKey(fileKey);
    }

    private boolean deleteDir(String dirKey, boolean recursive) throws IOException {
        dirKey = QiniuKodoUtils.keyToDirKey(dirKey);
        LOG.info("== deleteDir, dirKey:" + dirKey + " recursive:" + recursive);

        List<FileInfo> files = kodoClient.listStatus(dirKey, false);

        // 有子文件文件，但未 recursive 抛出异常
        if (files != null && files.size() > 0 && !recursive) {
            throw new IOException("file is not empty");
        }

        return kodoClient.deleteKeys(dirKey);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        LOG.info("== listStatus, path:" + path);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.info("== listStatus, key:" + key);

        List<FileInfo> files = kodoClient.listStatus(key, true);
        List<FileStatus> fileStatuses = new ArrayList<>();
        for (FileInfo file : files) {
            if (file != null) {
                fileStatuses.add(fileInfoToFileStatus(file));
            }
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(Path newPath) {
        LOG.info("== setWorkingDirectory, path:" + newPath);
        workingDir = newPath;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        while (path != null) {
            LOG.info("== mkdirs, path:" + path + " permission:" + permission);
            mkdir(path);
            path = path.getParent();
        }
        return true;
    }

    /**
     * 仅仅只创建当前路径文件夹
     */
    private boolean mkdir(Path path) throws IOException {
        if (path.isRoot()) return true;
        LOG.info("== mkdir, path:" + path);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.info("== mkdir 01, key:" + key);

        // 1. 检查是否存在同名文件
        key = QiniuKodoUtils.keyToFileKey(key);
        LOG.info("== mkdir file, key:" + key);

        FileInfo file = kodoClient.getFileStatus(key);
        if (file != null) {
            throw new IOException("file already exist:" + path);
        }

        // 2. 检查是否存在同名路径
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.info("== mkdir dir, key:" + key);

        file = kodoClient.getFileStatus(key);
        if (file != null) {
            return false;
        }

        // 3. 创建路径
        return kodoClient.makeEmptyObject(key);
    }

    /**
     * 获取一个路径的文件详情
     */
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.info("== getFileStatus, path:" + path);

        // 未处理文件总大小

        // 1. key 可能是实际文件或文件夹, 也可能是中间路径
        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.info("== getFileStatus 01, key:" + key);

        // 先尝试查找 key
        FileInfo file = kodoClient.getFileStatus(key);

        // 能查找到, 直接返回文件信息
        if (file != null) return fileInfoToFileStatus(file);

        // 2. 非路径 key，转路径
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.info("== getFileStatus 02, key:" + key);

        file = kodoClient.getFileStatus(key);
        if (file != null) return fileInfoToFileStatus(file);

        throw new FileNotFoundException("can't find file:" + path);
    }

    /**
     * 七牛SDK的文件信息转换为 hadoop fs 的文件信息
     */
    private FileStatus fileInfoToFileStatus(FileInfo file) {
        if (file == null) return null;

        LOG.info("== file conv, key:" + file.key);
        // 七牛的文件上传时间记录的单位为100ns
        // hadoop的文件上传时间记录的单位为1ms即1000ns
        long putTime = file.putTime / 10;

        return new FileStatus(
                file.fsize, // 文件大小
                QiniuKodoUtils.isKeyDir(file.key),
                blockSize,
                Constants.QINIU_DEFAULT_VALUE_BLOCK_SIZE,
                putTime, // modification time
                putTime, // access time
                null,   // permission
                username,   // owner
                username,   // group
                null,   // symlink
                QiniuKodoUtils.keyToPath(uri, workingDir, file.key) // 将 key 还原成 hadoop 绝对路径
        );
    }
}
