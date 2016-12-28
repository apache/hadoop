package org.apache.hadoop.yarn.applications.tensorflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by muzhongz on 16-12-20.
 */
public class LaunchContainerThread implements Runnable {

    private static final Log LOG = LogFactory.getLog(LaunchContainerThread.class);

    // Allocated container
    private Container container;
    private String tfServerJar;
    // Memory to request for the container on which the shell command will run
    private long containerMemory = 10;

    // Container retry options
    private ContainerRetryPolicy containerRetryPolicy =
            ContainerRetryPolicy.NEVER_RETRY;
    private Set<Integer> containerRetryErrorCodes = null;
    private int containerMaxRetries = 0;
    private int containrRetryInterval = 0;

    private ApplicationMaster appMaster;

    private ApplicationMaster.NMCallbackHandler containerListener;
    private TFServerAddress serverAddress = null;

    private LaunchContainerThread(Container container, String tfServerJar, long containerMemory,
                                 ContainerRetryPolicy containerRetryPolicy, Set<Integer> containerRetryErrorCodes,
                                 int containerMaxRetries, int containrRetryInterval, ApplicationMaster appMaster,
                                 ApplicationMaster.NMCallbackHandler containerListener) {
        this.container = container;
        this.tfServerJar = tfServerJar;
        this.containerMemory = containerMemory;
        this.containerRetryPolicy = containerRetryPolicy;
        this.containerRetryErrorCodes = containerRetryErrorCodes;
        this.containerMaxRetries = containerMaxRetries;
        this.containrRetryInterval = containrRetryInterval;
        this.appMaster = appMaster;
        this.containerListener = containerListener;
    }

    public LaunchContainerThread(Container container, String tfServerJar, long containerMemory,
                                 ContainerRetryPolicy containerRetryPolicy, Set<Integer> containerRetryErrorCodes,
                                 int containerMaxRetries, int containrRetryInterval, ApplicationMaster appMaster,
                                 ApplicationMaster.NMCallbackHandler containerListener, TFServerAddress serverAddress) {
        this(container,
                tfServerJar,
                containerMemory,
                containerRetryPolicy,
                containerRetryErrorCodes,
                containerMaxRetries,
                containrRetryInterval,
                appMaster,
                containerListener);
        this.serverAddress = serverAddress;
        if (this.serverAddress == null) {
            LOG.info("server address is null");
        }
    }

    @Override
    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
        LOG.info("Setting up container launch container for containerid="
                + container.getId());

        FileSystem fs = null;
        try {
            fs = FileSystem.get(appMaster.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        TFContainer tfContainer = new TFContainer(appMaster);

        // Set the java environment
        Map<String, String> env = tfContainer.setJavaEnv(appMaster.getConfiguration(), null);

        // Set the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        ApplicationId appId = appMaster.getAppAttempId().getApplicationId();

        try {
            tfContainer.addToLocalResources(fs, tfServerJar, TFContainer.SERVER_JAR_PATH, appId.toString(),
                    localResources, null);
        } catch (IOException e) {
            e.printStackTrace();
        }


        LOG.info("clusterspec: " + this.serverAddress.getClusterSpec().toString());
        this.serverAddress.getClusterSpec().testClusterString();
        ClusterSpec cs = this.serverAddress.getClusterSpec();

        StringBuilder command = null;
        try {
            command = tfContainer.makeCommands(containerMemory,
                    cs.getBase64EncodedJsonString(),
                    this.serverAddress.getJobName(),
                    this.serverAddress.getTaskIndex());
        } catch (JsonProcessingException e) {
            LOG.info("cluster spec cannot convert into base64 json string!");
            e.printStackTrace();
        } catch (ClusterSpecException e) {
            e.printStackTrace();
        }

        List<String> commands = new ArrayList<String>();
         commands.add(command.toString());
        //commands.add("/home/muzhongz/tfonyarn/tf_server.sh");
        if (serverAddress != null) {
            LOG.info(serverAddress.getJobName() + " : " + serverAddress.getAddress() + ":" + serverAddress.getPort());
        }

        ContainerRetryContext containerRetryContext =
                ContainerRetryContext.newInstance(
                        containerRetryPolicy, containerRetryErrorCodes,
                        containerMaxRetries, containrRetryInterval);
        for (String cmd : commands) {
            LOG.info("command: " + cmd.toString());
        }
        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, appMaster.getAllTokens().duplicate(),
                null, containerRetryContext);
        containerListener.addContainer(container.getId(), container);
        appMaster.getNMClientAsync().startContainerAsync(container, ctx);
    }

}