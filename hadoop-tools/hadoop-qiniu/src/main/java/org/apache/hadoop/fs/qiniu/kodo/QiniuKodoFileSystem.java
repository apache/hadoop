package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.storage.model.FileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoCachedClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.download.EmptyInputStream;
import org.apache.hadoop.fs.qiniu.kodo.download.QiniuKodoBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.download.QiniuKodoInputStream;
import org.apache.hadoop.fs.qiniu.kodo.upload.QiniuKodoOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Stack;

public class QiniuKodoFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystem.class);

    private URI uri;
    private String username;
    private Path workingDir;

    private IQiniuKodoClient kodoClient;

    private QiniuKodoFsConfig fsConfig;
    private QiniuKodoBlockReader blockReader;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        this.fsConfig = new QiniuKodoFsConfig(getConf());

        LOG.debug("QiniuKodoConfig: {}", fsConfig);

        String bucket = name.getHost();
        LOG.debug("== bucket:" + bucket);

        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        LOG.debug("== uri:" + uri);

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        LOG.debug("== username:" + username);

        workingDir = new Path("/user", username).makeQualified(uri, null);
        LOG.debug("== workingDir:" + workingDir);

        kodoClient = new QiniuKodoClient(bucket, fsConfig, statistics);
        if (fsConfig.client.cache.enable) {
            kodoClient = new QiniuKodoCachedClient(kodoClient);
        }

        blockReader = new QiniuKodoBlockReader(fsConfig, kodoClient);
    }

    private volatile boolean makeSureWorkdirCreatedFlag = false;

    /**
     * 工作目录为相对路径使用的目录，其必须得存在，故需要检查是否被创建
     * 需要在创建文件，创建文件夹前进行检查
     * 需要在工作目录被改变的时候重新检查
     */
    private synchronized void makeSureWorkdirCreated(Path path) throws IOException {
        if (makeSureWorkdirCreatedFlag) return;
        if (!path.makeQualified(uri, workingDir).toString().startsWith(
                workingDir.makeQualified(uri, workingDir).toString())) {
            return;
        }
        mkdirs(workingDir);
        makeSureWorkdirCreatedFlag = true;
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
        IOException fnfeDir = new FileNotFoundException("Can't open " + path +
                " because it is a directory");

        LOG.debug("open, path:" + path);

        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);

        // root
        if (key.length() == 0) throw fnfeDir;

        long len = kodoClient.getLength(key);
        // 空文件内容
        if (len == 0) {
            return new FSDataInputStream(new EmptyInputStream());
        }
        return new FSDataInputStream(
                new QiniuKodoInputStream(
                        key,
                        blockReader,
                        len, statistics
                )
        );
    }

    @Override
    public void close() throws IOException {
        super.close();
        blockReader.close();
    }

    /**
     * 创建一个文件，返回一个可以被写入的输出流
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        LOG.debug("== create, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        if (path.isRoot()) throw new IOException("Cannot create file named /");
        makeSureWorkdirCreated(path);

        mkdirs(path.getParent());
        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== create, key:" + key + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        return new FSDataOutputStream(new QiniuKodoOutputStream(kodoClient, key, overwrite, blockReader), statistics);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
                                                 EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
                                                 Progressable progress) throws IOException {
        boolean overwrite = flags.contains(CreateFlag.OVERWRITE);
        LOG.debug("== create, path:" + path + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        if (path.isRoot()) throw new IOException("Cannot create file named /");

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== create, key:" + key + " permission:" + permission + " overwrite:" + overwrite + " bufferSize:" + bufferSize + " replication:" + replication + " blockSize:" + blockSize);

        return new FSDataOutputStream(new QiniuKodoOutputStream(kodoClient, key, overwrite, blockReader), statistics);

    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Append is not supported.");
    }


    @Override
    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        // TODO: 需要考虑重命名本地缓存池中的缓存
        if (srcPath.isRoot()) {
            // Cannot rename root of file system
            LOG.debug("Cannot rename the root of a filesystem");
            return false;
        }
        makeSureWorkdirCreated(dstPath);

        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            return false;
        }
        FileStatus srcStatus;
        try {
            srcStatus = getFileStatus(srcPath);
        } catch (FileNotFoundException fnde) {
            srcStatus = null;
        }

        FileStatus dstStatus;
        try {
            dstStatus = getFileStatus(dstPath);
        } catch (FileNotFoundException fnde) {
            dstStatus = null;
        }
        if (dstStatus == null) {
            // If dst doesn't exist, check whether dst dir exists or not
            try {
                dstStatus = getFileStatus(dstPath.getParent());
            } catch (FileNotFoundException e) {
                return false;
            }
            if (!dstStatus.isDirectory()) {
                throw new FileAlreadyExistsException(String.format(
                        "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                        dstPath.getParent()));
            }
        } else {
            assert srcStatus != null;
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                return !srcStatus.isDirectory();
            } else if (dstStatus.isDirectory()) {
                // If dst is a directory
                dstPath = new Path(dstPath, srcPath.getName());
                FileStatus[] statuses;
                try {
                    statuses = listStatus(dstPath);
                } catch (FileNotFoundException fnde) {
                    statuses = null;
                }
                if (statuses != null && statuses.length > 0) {
                    // If dst exists and not a directory / not empty
                    throw new FileAlreadyExistsException(String.format(
                            "Failed to rename %s to %s, file already exists or not empty!",
                            srcPath, dstPath));
                }
            } else {
                // If dst is not a directory
//                if (srcStatus.isFile()) return false;
//                throw new FileAlreadyExistsException(String.format(
//                        "Failed to rename %s to %s, file already exists!",
//                        srcPath, dstPath));
                return false;
            }
        }

        boolean succeed;
        if (srcStatus.isDirectory()) {
            succeed = copyDirectory(srcPath, dstPath);
        } else {
            succeed = copyFile(srcPath, dstPath);
        }

        return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
    }

    private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        String srcKey = QiniuKodoUtils.pathToKey(workingDir, srcPath);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dstPath);
        return kodoClient.copyKey(srcKey, dstKey);
    }

    private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        String srcKey = QiniuKodoUtils.pathToKey(workingDir, srcPath);
        srcKey = QiniuKodoUtils.keyToDirKey(srcKey);
        String dstKey = QiniuKodoUtils.pathToKey(workingDir, dstPath);
        dstKey = QiniuKodoUtils.keyToDirKey(dstKey);

        if (dstKey.startsWith(srcKey)) {
            LOG.warn("Cannot rename a directory to a subdirectory of self");
            return false;
        }
        return kodoClient.copyKeys(srcKey, dstKey);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        // TODO 同时删除本地缓存
        LOG.debug("== delete, path:" + path + " recursive:" + recursive);

        // 判断是否是文件
        FileStatus file;
        try {
            file = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        LOG.debug("== delete, key:" + key);

        if (file.isDirectory()) {
            return deleteDir(key, recursive);
        } else {
            return deleteFile(key);
        }
    }

    private boolean deleteFile(String fileKey) throws IOException {
        fileKey = QiniuKodoUtils.keyToFileKey(fileKey);
        LOG.debug("== delete, fileKey:" + fileKey);

        return kodoClient.deleteKey(fileKey);
    }

    private boolean deleteDir(String dirKey, boolean recursive) throws IOException {
        dirKey = QiniuKodoUtils.keyToDirKey(dirKey);
        LOG.debug("== deleteDir, dirKey:" + dirKey + " recursive:" + recursive);
        return kodoClient.deleteKeys(dirKey, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        LOG.debug("== listStatus, path:" + path);

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        key = QiniuKodoUtils.keyToDirKey(key);
        LOG.debug("== listStatus, key:" + key);

        // 尝试列举
        List<FileInfo> files = kodoClient.listStatus(key, true);
        if (!files.isEmpty()) {
            // 列举成功
            return files.stream()
                    .filter(Objects::nonNull)
                    .map(this::fileInfoToFileStatus)
                    .toArray(FileStatus[]::new);
        }
        // 列举为空

        // 可能文件夹本身就不存在
        if (getFileStatus(path) == null) {
            throw new FileNotFoundException(path.toString());
        }

        // 文件夹存在，的确是空文件夹
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path newPath) {
        workingDir = newPath;
        makeSureWorkdirCreatedFlag = false;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    /**
     * 递归地创建文件夹
     */
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        // 如果该文件夹发现已存在，那么直接返回结果
        if (null != kodoClient.getFileStatus(
                QiniuKodoUtils.keyToDirKey(
                        QiniuKodoUtils.pathToKey(workingDir, path)
                )
        )) {
            return true;
        }
        Stack<Path> stack = new Stack<>();
        while (path != null) {
            LOG.debug("== mkdirs, path:" + path);
            stack.push(path);
            path = path.getParent();
        }
        // 从顶层文件夹开始循环创建
        while (!stack.isEmpty()) {
            mkdir(stack.pop());
        }
        return true;
    }

    /**
     * 仅仅只创建当前路径文件夹
     */
    private boolean mkdir(Path path) throws IOException {
        LOG.debug("== mkdir, path:" + path);

        // 根目录
        if (path.isRoot()) {
            return true;
        }

        String key = QiniuKodoUtils.pathToKey(workingDir, path);
        String dirKey = QiniuKodoUtils.keyToDirKey(key);
        String fileKey = QiniuKodoUtils.keyToFileKey(key);

        // 列举第一条
        FileInfo file = kodoClient.listOneStatus(key);

        // 找不到则创建文件夹
        if (file == null) {
            return kodoClient.makeEmptyObject(dirKey);
        }

        // 找到了，是已存在的文件夹
        if (dirKey.equals(file.key)) {
            return true;
        }

        // 找到了，但是是已存在的文件
        if (fileKey.equals(file.key)) {
            throw new FileAlreadyExistsException(path.toString());
        }

        // TODO 是属于一个前缀，属于文件系统不一致的情景，需要修复依次创建出其各级父目录
        throw new UnsupportedFileSystemException("unimplementation");
    }


    /**
     * 获取一个路径的文件详情
     */
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("== getFileStatus, path:" + path);

        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String key = QiniuKodoUtils.pathToKey(workingDir, qualifiedPath);


        // Root always exists
        if (key.length() == 0) {
            return new FileStatus(0, true, 1, 0, 0, 0, null, username, username, qualifiedPath);
        }

        // 1. key 可能是实际文件或文件夹, 也可能是中间路径

        // 先尝试查找 key
        FileInfo file = kodoClient.getFileStatus(key);

        // 能查找到, 直接返回文件信息
        if (file != null) {
            return fileInfoToFileStatus(file);
        }

        // 2. 有可能是文件夹路径但是不存在末尾/
        // 添加尾部/后再次获取
        String newKey = QiniuKodoUtils.keyToDirKey(key);

        // 找不到表示文件夹的空对象，故只能列举是否有该前缀的对象
        file = kodoClient.listOneStatus(newKey);
        if (file == null) {
            throw new FileNotFoundException("can't find file:" + path);
        }

        // 是文件夹本身
        if (file.key.equals(newKey)) {
            return fileInfoToFileStatus(file);
        }

        // 是文件夹前缀
        FileInfo newDir = new FileInfo();
        newDir.key = newKey;
        kodoClient.makeEmptyObject(newKey);
        return fileInfoToFileStatus(newDir);
    }


    /**
     * 七牛SDK的文件信息转换为 hadoop fs 的文件信息
     */
    private FileStatus fileInfoToFileStatus(FileInfo file) {
        if (file == null) return null;

        LOG.debug("== file conv, key:" + file.key);

        long putTime = file.putTime / 10000;
        boolean isDir = QiniuKodoUtils.isKeyDir(file.key);

        return new FileStatus(
                file.fsize, // 文件大小
                isDir,
                0,
                fsConfig.download.blockSize,
                putTime, // modification time
                putTime, // access time
                isDir ? new FsPermission(461) : null,   // permission
                username,   // owner
                username,   // group
                null,   // symlink
                QiniuKodoUtils.keyToPath(uri, workingDir, file.key) // 将 key 还原成 hadoop 绝对路径
        );
    }
}