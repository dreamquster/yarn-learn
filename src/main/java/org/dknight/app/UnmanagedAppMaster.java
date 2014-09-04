package org.dknight.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Created by User: fanming.chen at Date: 14-9-4 Time: 上午11:10
 */
public class UnmanagedAppMaster {

    private static final Log LOG = LogFactory.getLog(UnmanagedAppMaster.class);

    private YarnClient rmClient;

    private ApplicationAttemptId attemptId;

    private boolean amCompleted;

    private String classpath;

    private String amCmd;

    //AppMaster->client to communicate with RM
    private AMRMClientAsync amrmClient;

    private AMRMCallBackHanlder amrmCallBackHanlder;

    private NMClientAsync nmClientAsync;

    private NMCallBackHandler containerListener;

    private List<Thread> launchThreads;


    private boolean containCompleted = false;

    public UnmanagedAppMaster(ApplicationAttemptId attemptId, Configuration yarnConf, String classpath, String amCmd) throws IOException, YarnException {
        rmClient = YarnClient.createYarnClient();
        rmClient.init(yarnConf);

        amrmCallBackHanlder = new AMRMCallBackHanlder();
        this.amrmClient = AMRMClientAsync.createAMRMClientAsync(300, amrmCallBackHanlder);
        this.amrmClient.init(yarnConf);

        containerListener = new NMCallBackHandler();
        this.nmClientAsync = new NMClientAsyncImpl(containerListener);
        this.nmClientAsync.init(yarnConf);

        this.attemptId = attemptId;
        this.classpath = classpath;
        this.amCmd = amCmd;
        this.amCompleted = false;
    }

    public void start() throws YarnException, IOException {
        rmClient.start();
        amrmClient.start();
        nmClientAsync.start();


        ApplicationReport report =
                rmClient.getApplicationReport(attemptId.getApplicationId());
        if (report.getYarnApplicationState() != YarnApplicationState.ACCEPTED) {
            throw new YarnException(
                    "Umanaged AM must be in ACCEPTED state before launching");
        }
        Credentials credentials = new Credentials();
        Token<AMRMTokenIdentifier> token =
                rmClient.getAMRMToken(attemptId.getApplicationId());

        UserGroupInformation.getCurrentUser().addToken(token.getService(), token);
        UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
        UserGroupInformation.AuthenticationMethod authMethod = loginUser.getAuthenticationMethod();

        // Service will be empty but that's okay, we are just passing down only
        // AMRMToken down to the real AM which eventually sets the correct
        // service-address.
        credentials.addToken(token.getService(), token);
        File tokenFile = File.createTempFile("unmanagedAMRMToken","",
                new File(System.getProperty("user.dir")));
        try {
            FileUtil.chmod(tokenFile.getAbsolutePath(), "600");
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        tokenFile.deleteOnExit();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(tokenFile,
                true));
        credentials.writeTokenStorageToStream(os);
        os.close();

        Map<String, String> env = System.getenv();
        ArrayList<String> envAMList = new ArrayList<String>();
        boolean setClasspath = false;
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.equals("CLASSPATH")) {
                setClasspath = true;
                if(classpath != null) {
                    value = value + File.pathSeparator + classpath;
                }
            }
            envAMList.add(key + "=" + value);
        }

        if(!setClasspath && classpath!=null) {
            envAMList.add("CLASSPATH="+classpath);
        }

        Resource containResource = Records.newRecord(Resource.class);
        containResource.setMemory(256);
        containResource.setVirtualCores(2);
        String racks[] = new String[]{"default-rack"};
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(containResource, null,racks , Priority.newInstance(10), true);
        amrmClient.registerApplicationMaster("localHost", -1, null);
        amrmClient.addContainerRequest(request);


        while (!amCompleted) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {}
        }

        amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "fanming.chen", null);
//        ContainerId containerId = ContainerId.newInstance(attemptId, 0);
//
//        String hostname = InetAddress.getLocalHost().getHostName();
//        envAMList.add(ApplicationConstants.Environment.CONTAINER_ID.name() + "=" + containerId);
//        envAMList.add(ApplicationConstants.Environment.NM_HOST.name() + "=" + hostname);
//        envAMList.add(ApplicationConstants.Environment.NM_HTTP_PORT.name() + "=0");
//        envAMList.add(ApplicationConstants.Environment.NM_PORT.name() + "=0");
//        envAMList.add(ApplicationConstants.Environment.LOCAL_DIRS.name() + "= /tmp");
//        envAMList.add(ApplicationConstants.APP_SUBMIT_TIME_ENV + "="
//                + System.currentTimeMillis());
//
//        envAMList.add(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME + "=" +
//                tokenFile.getAbsolutePath());
//
//        String[] envAM = new String[envAMList.size()];
//        Process amProc = Runtime.getRuntime().exec(amCmd, envAMList.toArray(envAM));
//
//        final BufferedReader errReader =
//                new BufferedReader(new InputStreamReader(amProc
//                        .getErrorStream()));
//        final BufferedReader inReader =
//                new BufferedReader(new InputStreamReader(amProc
//                        .getInputStream()));
//
//        // read error and input streams as this would free up the buffers
//        // free the error stream buffer
//        Thread errThread = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    String line = errReader.readLine();
//                    while((line != null) && !isInterrupted()) {
//                        System.err.println(line);
//                        line = errReader.readLine();
//                    }
//                } catch(IOException ioe) {
//                    LOG.warn("Error reading the error stream", ioe);
//                }
//            }
//        };
//        Thread outThread = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    String line = inReader.readLine();
//                    while((line != null) && !isInterrupted()) {
//                        System.out.println(line);
//                        line = inReader.readLine();
//                    }
//                } catch(IOException ioe) {
//                    LOG.warn("Error reading the out stream", ioe);
//                }
//            }
//        };
//        try {
//            errThread.start();
//            outThread.start();
//        } catch (IllegalStateException ise) { }
//
//        // wait for the process to finish and check the exit code
//        try {
//            int exitCode = amProc.waitFor();
//            LOG.info("AM process exited with value: " + exitCode);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } finally {
//            amCompleted = true;
//        }
//
//        try {
//            // make sure that the error thread exits
//            // on Windows these threads sometimes get stuck and hang the execution
//            // timeout and join later after destroying the process.
//            errThread.join();
//            outThread.join();
//            errReader.close();
//            inReader.close();
//        } catch (InterruptedException ie) {
//            LOG.info("ShellExecutor: Interrupted while reading the error/out stream",
//                    ie);
//        } catch (IOException ioe) {
//            LOG.warn("Error while closing the error/out stream", ioe);
//        }
//        amProc.destroy();
    }

    public void stop() {
        for (Thread stopThread : launchThreads) {
            try {
                stopThread.join(3000);
            } catch (InterruptedException e) {
                LOG.error("Thread " + stopThread.getId() + " can not stoped ");
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        rmClient.stop();

        amrmClient.stop();
        nmClientAsync.stop();
    }


    private class AMRMCallBackHanlder implements AMRMClientAsync.CallbackHandler {
        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus containerStatus : statuses) {
                if (containerStatus.getState() == ContainerState.COMPLETE) {
                    containCompleted = true;
                }
            }

        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());
                // + ", containerToken"
                // +allocatedContainer.getContainerToken().getIdentifier().toString());

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public float getProgress() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onError(Throwable e) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    private class NMCallBackHandler implements NMClientAsync.CallbackHandler {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    private class LaunchContainerRunnable implements Runnable {

        private  Container container;
        private  NMCallBackHandler containListener;

        public LaunchContainerRunnable(Container container, NMCallBackHandler containListener) {
            this.container = container;
            this.containListener = containListener;
        }



        @Override
        public void run() {
            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);

            List<String> commands = new LinkedList<String>();
            commands.add(amCmd);
            ctx.setCommands(commands);

            nmClientAsync.startContainerAsync(container, ctx);
        }
    }
}
