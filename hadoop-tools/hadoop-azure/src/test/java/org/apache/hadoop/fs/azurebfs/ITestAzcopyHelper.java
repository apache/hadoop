package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class ITestAzcopyHelper extends ITestAzureBlobFileSystemAuthorization  {

    public ITestAzcopyHelper() throws Exception {
        super.setup();
    }

    File hadoopAzureDir;
    String azcopyDirPath;

    public void downloadAzcopyExecutableIfNotPresent() throws IOException, InterruptedException {
        // Find the hadoop-azure directory from the current working directory
        File currentDir = new File(System.getProperty("user.dir"));
        if (!currentDir.isDirectory() && !currentDir.getName().equals("hadoop-azure")) {
            hadoopAzureDir = findHadoopAzureDir(currentDir);
            if (hadoopAzureDir == null) {
                throw new FileNotFoundException("hadoop-azure directory not found");
            }
        } else {
            hadoopAzureDir = currentDir;
        }

        // Check if azcopy directory is present in the hadoop-azure directory, create it if it doesn't exist
        azcopyDirPath = hadoopAzureDir.getAbsolutePath() + "/azcopy";
        File azcopyDir = new File(azcopyDirPath);
        if (!azcopyDir.exists()) {
            boolean created = azcopyDir.mkdir();
        }

        // Check if azcopy is present in the azcopy directory
        String azcopyPath = azcopyDirPath + "/azcopy";
        File azcopyFile = new File(azcopyPath);
        if (!azcopyFile.exists()) {
            // If azcopy is not present, download and extract it
            String downloadUrl = "https://aka.ms/downloadazcopy-v10-linux";
            String downloadCmd = "wget " + downloadUrl + " -O azcopy.tar.gz" + " --no-check-certificate";
            String[] downloadCmdArr = {"bash", "-c", downloadCmd};
            Process downloadProcess = Runtime.getRuntime().exec(downloadCmdArr);
            downloadProcess.waitFor();

            // Extract the azcopy executable from the tarball
            String extractCmd = "tar -xf azcopy.tar.gz -C " + hadoopAzureDir.getAbsolutePath();
            String[] extractCmdArr = {"bash", "-c", extractCmd};
            Process extractProcess = Runtime.getRuntime().exec(extractCmdArr);
            extractProcess.waitFor();

            // Rename the azcopy_linux_amd64_* directory to 'azcopy' and move it to the hadoop-azure directory
            String renameCmd = "mv " + hadoopAzureDir.getAbsolutePath() + "/azcopy_linux_amd64_*/* " + azcopyDirPath;
            String[] renameCmdArr = {"bash", "-c", renameCmd};
            Process renameProcess = Runtime.getRuntime().exec(renameCmdArr);
            renameProcess.waitFor();

            // Remove the downloaded tarball and azcopy folder
            String cleanupCmd = "rm -rf " + hadoopAzureDir.getAbsolutePath() + "/azcopy_linux_amd64_* azcopy.tar.gz";
            String[] cleanupCmdArr = {"bash", "-c", cleanupCmd};
            Process cleanupProcess = Runtime.getRuntime().exec(cleanupCmdArr);
            cleanupProcess.waitFor();

            // Set the execute permission on the azcopy executable
            String chmodCmd = "chmod +x " + azcopyDirPath;
            String[] chmodCmdArr = {"bash", "-c", chmodCmd};
            Process chmodProcess = Runtime.getRuntime().exec(chmodCmdArr);
            chmodProcess.waitFor();
        }
        // Change working directory to the hadoop-azure directory
        System.setProperty("user.dir", hadoopAzureDir.getAbsolutePath());
    }

    private File findHadoopAzureDir(File dir) {
        if (dir == null) {
            return null;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            return null;
        }
        for (File file : files) {
            if (file.isDirectory() && file.getName().equals("hadoop-azure")) {
                return file;
            } else {
                File hadoopAzureDir = findHadoopAzureDir(file);
                if (hadoopAzureDir != null) {
                    return hadoopAzureDir;
                }
            }
        }
        return null;
    }

    private String getSAS(String accountName, String containerName, String path, String operation) throws IOException {
        MockSASTokenProvider mockSASTokenProvider = (MockSASTokenProvider) getFileSystem().getAbfsStore().getClient().getSasTokenProvider();
        return mockSASTokenProvider.getSASToken(accountName, containerName, path, operation);
    }

    @Test
    public void testAzcopyDownload() throws IOException, InterruptedException {
        downloadAzcopyExecutableIfNotPresent();
        String SAS = getSAS(this.getAccountName(), this.getFileSystemName(), this.getTestUrl(), SASTokenProvider.WRITE_OPERATION);
        String url = makeQualified(new Path(this.getFileSystemName())).toUri().toString();
        createScript(azcopyDirPath, "createFile.sh", azcopyDirPath, SAS, url);
    }

    public static void createScript(String folderPath, String scriptName, String azcopyPath, String sasToken, String containerName) {
        String blobPath = containerName + "?" + sasToken; // construct the blob path
        String scriptContent = "blobPath=\"" + blobPath + "\"\n"
                + "echo $blobPath\n"
                + azcopyPath + "/azcopy copy \"" + azcopyPath + "/NOTICE.txt\" $blobPath\n"; // construct the script content
        File scriptFile = new File(folderPath, scriptName);
        try {
            FileWriter writer = new FileWriter(scriptFile);
            writer.write(scriptContent);
            writer.close();
            boolean written = scriptFile.setExecutable(true); // make the script executable
            System.out.println("Script created at " + scriptFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}