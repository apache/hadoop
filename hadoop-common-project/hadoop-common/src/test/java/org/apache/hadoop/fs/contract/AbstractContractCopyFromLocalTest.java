package org.apache.hadoop.fs.contract;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public abstract class AbstractContractCopyFromLocalTest extends
        AbstractFSContractTestBase {

    private static final Charset ASCII = StandardCharsets.US_ASCII;
    private File file;

    @Override
    public void teardown() throws Exception {
        super.teardown();
        if (file != null) {
            file.delete();
        }
    }


    private Path mkTestDir(String prefix) throws IOException {
        FileSystem fs = getFileSystem();
        File file = File.createTempFile(prefix, ".dir");
        Path dest = path(file.getName());
        fs.delete(dest, false);
        mkdirs(dest);
        return dest;
    }

    // file - dir: should be mismatch error
    @Test
    public void testSourceIsFileAndDestinationIsDirectory() throws Throwable {
        describe("Source is a file and destination is a directory should fail");

        file = File.createTempFile("test", ".txt");
        Path source = new Path(file.toURI());
        Path destination = mkTestDir("test");

        intercept(PathExistsException.class,
                () -> getFileSystem().copyFromLocalFile(source, destination));
    }

    // file - dir: no overwrite of directory even when flag is set
    @Test
    public void testCopyFileNoOverwriteDirectory() throws Throwable {
        describe("Source is a file and destination is directory, overwrite" +
                " flag should not change behaviour");

        file = createTempFile("hello");
        Path dest = upload(file, true);
        FileSystem fs = getFileSystem();
        fs.delete(dest, false);
        fs.mkdirs(dest);

        intercept(PathExistsException.class,
                () -> upload(file, true));
    }

    private Path createTempDirectory(String prefix) throws IOException {
        return new Path(Files.createTempDirectory("srcDir").toUri());
    }

    // source is directory and dest is file failure
    // dir - file: should be mismatch error
    @Test
    public void testSourceIsDirectoryAndDestinationIsFile() throws Throwable {
        describe("Source is a directory and destination is a file should fail");

        file = File.createTempFile("local", "");
        Path source = createTempDirectory("srcDir");
        Path destination = upload(file, false);

        intercept(FileAlreadyExistsException.class,
                () -> getFileSystem().copyFromLocalFile(
                        false, true, source, destination));
    }


    // are empty directories copied?
    // source is directory and dest is directory too
    // is the source cleaned up properly after deletion
    // does the source stay the same when it should
    // directory: is the destination NOT overwritten if the flag isn't there

    // source is directory and dest has a parent of a file
    // dest is dir or file overwriting a file
    // source is empty dir and dest is empty dir
    // source is dir with empty dir underneath, dest is the same

    // source is file, destination is file
    @Test
    public void testCopyEmptyFile() throws Throwable {
        file = File.createTempFile("test", ".txt");
        Path dest = upload(file, true);
        assertPathExists("uploaded file", dest);
    }

    // source is file, destination is file, contents are checked
    @Test
    public void testCopyFile() throws Throwable {
        String message = "hello";
        file = createTempFile(message);
        Path dest = upload(file, true);
        assertPathExists("uploaded file not found", dest);
        FileSystem fs = getFileSystem();
        FileStatus status = fs.getFileStatus(dest);
        assertEquals("File length of " + status,
                message.getBytes(ASCII).length, status.getLen());
        assertFileTextEquals(dest, message);
    }

    // file: is the destination NOT overwritten if the flag isn't there
    @Test
    public void testCopyFileNoOverwrite() throws Throwable {
        file = createTempFile("hello");
        Path dest = upload(file, true);
        // HADOOP-15932: the exception type changes here
        intercept(PathExistsException.class,
                () -> upload(file, false));
    }

    // file - file: is the destination overwritten when flag
    @Test
    public void testCopyFileOverwrite() throws Throwable {
        file = createTempFile("hello");
        Path dest = upload(file, true);
        String updated = "updated";
        FileUtils.write(file, updated, ASCII);
        upload(file, true);
        assertFileTextEquals(dest, updated);
    }

    @Test
    public void testCopyMissingFile() throws Throwable {
        file = File.createTempFile("test", ".txt");
        file.delete();
        // first upload to create
        intercept(FileNotFoundException.class, "",
                () -> upload(file, true));
    }

    /*
     * The following path is being created on disk and copied over
     * /parent/ (trailing slash to make it clear it's  a directory
     * /parent/test1.txt
     * /parent/child/test.txt
     */
    @Test
    public void testCopyTreeDirectoryWithoutDelete() throws Throwable {
        java.nio.file.Path srcDir = Files.createTempDirectory("parent");
        java.nio.file.Path childDir = Files.createTempDirectory(srcDir, "child");
        java.nio.file.Path secondChild = Files.createTempDirectory(srcDir, "secondChild");
        java.nio.file.Path parentFile = Files.createTempFile(srcDir, "test1", ".txt");
        java.nio.file.Path childFile = Files.createTempFile(childDir, "test2", ".txt");

        Path src = new Path(srcDir.toUri());
        Path dst = path(srcDir.getFileName().toString());
        getFileSystem().copyFromLocalFile(false, true, src, dst);

        java.nio.file.Path parent = srcDir.getParent();

        assertNioPathExists("Parent directory", srcDir, parent);
        assertNioPathExists("Child directory", childDir, parent);
        assertNioPathExists("Second Child directory", secondChild, parent);
        assertNioPathExists("Parent file", parentFile, parent);
        assertNioPathExists("Child file", childFile, parent);

        assertTrue("Folder was deleted when it shouldn't have!",
                Files.exists(srcDir));
    }

    @Test
    public void testCopyDirectoryWithDelete() throws Throwable {
        java.nio.file.Path srcDir = Files.createTempDirectory("parent");
        Files.createTempFile(srcDir, "test1", ".txt");

        Path src = new Path(srcDir.toUri());
        Path dst = path(srcDir.getFileName().toString());
        getFileSystem().copyFromLocalFile(true, true, src, dst);

        assertFalse("Source directory should've been deleted",
                Files.exists(srcDir));
    }

    @Test
    public void testLocalFilesOnly() throws Throwable {
        Path dst = path("testLocalFilesOnly");
        intercept(IllegalArgumentException.class,
                () -> {
                    getFileSystem().copyFromLocalFile(false, true, dst, dst);
                    return "copy successful";
                });
    }

    public Path upload(File srcFile, boolean overwrite) throws IOException {
        Path src = new Path(srcFile.toURI());
        Path dst = path(srcFile.getName());
        getFileSystem().copyFromLocalFile(false, overwrite, src, dst);
        return dst;
    }

    public void assertFileTextEquals(Path path, String expected)
            throws IOException {
        assertEquals("Wrong data in " + path,
                expected, IOUtils.toString(getFileSystem().open(path), ASCII));
    }

    /**
     * Create a temp file with some text.
     * @param text text for the file
     * @return the file
     * @throws IOException on a failure
     */
    public File createTempFile(String text) throws IOException {
        File f = File.createTempFile("test", ".txt");
        FileUtils.write(f, text, ASCII);
        return f;
    }

    private void assertNioPathExists(String message,
                                     java.nio.file.Path toVerify,
                                     java.nio.file.Path parent) throws IOException {
        Path qualifiedPath = path(parent.relativize(toVerify).toString());
        assertPathExists(message, qualifiedPath);
    }
}
