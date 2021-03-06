/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dknight.app;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

/**
 * The UnmanagedLauncher is a simple client that launches and unmanaged AM. An
 * unmanagedAM is an AM that is not launched and managed by the RM. The client
 * creates a new application on the RM and negotiates a new attempt id. Then it
 * waits for the RM app state to reach be YarnApplicationState.ACCEPTED after
 * which it spawns the AM in another process and passes it the container id via
 * env variable Environment.CONTAINER_ID. The AM can be in any
 * language. The AM can register with the RM using the attempt id obtained
 * from the container id and proceed as normal.
 * The client redirects app stdout and stderr to its own stdout and
 * stderr and waits for the AM process to exit. Then it waits for the RM to
 * report app completion.
 */
public class UnmanagedAMLauncher {
    private static final Log LOG = LogFactory.getLog(UnmanagedAMLauncher.class);

    private Configuration conf;

    // Handle to talk to the Resource Manager/Applications Manager
    private YarnClient rmClient;

    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // cmd to start AM
    private String amCmd = null;
    // set the classpath explicitly
    private String classpath = null;

    private volatile boolean amCompleted = false;

    private UnmanagedAppMaster unmanagedAppMaster;


    /**
     * @param args
     *          Command line arguments
     */
    public static void main(String[] args) {
        try {
            UnmanagedAMLauncher client = new UnmanagedAMLauncher();
            LOG.info("Initializing Client");
            boolean doRun = client.init(args);
            if (!doRun) {
                System.exit(0);
            }
            client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running Client", t);
            System.exit(1);
        }
    }

    /**
     */
    public UnmanagedAMLauncher(Configuration conf) throws Exception {
        // Set up RPC
        this.conf = conf;
        String classUrl = this.getClass().getResource("/").getPath();
        StringBuffer confStrBuilder = new StringBuffer(classUrl);
        int targetnIdx = confStrBuilder.indexOf("classes");
        confStrBuilder = confStrBuilder.replace(targetnIdx, confStrBuilder.length(), "test-classes/yarn-conf.xml");

        this.conf.addResource(new Path(confStrBuilder.toString()));
    }

    public UnmanagedAMLauncher() throws Exception {
        this(new Configuration());
    }

    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("Client", opts);
    }

    public boolean init(String[] args) throws ParseException {

        Options opts = new Options();
        opts.addOption("appname", true,
                "Application Name. Default value - UnmanagedAM");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true,
                "RM Queue in which this application is to be submitted");
        opts.addOption("master_memory", true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption("cmd", true, "command to start unmanaged AM (required)");
        opts.addOption("classpath", true, "additional classpath");
        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for client to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        appName = cliParser.getOptionValue("appname", "UnmanagedAM");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        classpath = cliParser.getOptionValue("classpath", null);

        amCmd = cliParser.getOptionValue("cmd");
        if (amCmd == null) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No cmd specified for application master");
        }

        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        rmClient = YarnClient.createYarnClient();
        rmClient.init(yarnConf);

        return true;
    }

    public void launchAM(ApplicationAttemptId attemptId)
            throws IOException, YarnException {
        unmanagedAppMaster = new UnmanagedAppMaster(attemptId, conf, classpath, amCmd);
        unmanagedAppMaster.start();
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("Starting Client");

        // Connect to ResourceManager
        rmClient.start();
        try {
            // Create launch context for app master
            LOG.info("Setting up application submission context for ASM");
            ApplicationSubmissionContext appContext = rmClient.createApplication()
                    .getApplicationSubmissionContext();
            ApplicationId appId = appContext.getApplicationId();

            // set the application name
            appContext.setApplicationName(appName);

            // Set the priority for the application master
            Priority pri = Records.newRecord(Priority.class);
            pri.setPriority(amPriority);
            appContext.setPriority(pri);

            // Set the queue to which this application is to be submitted in the RM
            appContext.setQueue(amQueue);
            // Set up the container launch context for the application master
            ContainerLaunchContext amContainer = Records
                    .newRecord(ContainerLaunchContext.class);
            appContext.setAMContainerSpec(amContainer);

            // unmanaged AM
            appContext.setUnmanagedAM(true);
            LOG.info("Setting unmanaged AM");

            // Submit the application to the applications manager
            LOG.info("Submitting application to ASM");
            rmClient.submitApplication(appContext);

            // Monitor the application to wait for launch state
            ApplicationReport appReport = monitorApplication(appId,
                    EnumSet.of(YarnApplicationState.ACCEPTED));
            ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();
            LOG.info("Launching application with id: " + attemptId);

            // launch AM
            launchAM(attemptId);

            // Monitor the application for end state
            appReport = monitorApplication(appId, EnumSet.of(
                    YarnApplicationState.KILLED, YarnApplicationState.FAILED,
                    YarnApplicationState.FINISHED));

            YarnApplicationState appState = appReport.getYarnApplicationState();
            FinalApplicationStatus appStatus = appReport.getFinalApplicationStatus();

            LOG.info("App ended with state: " + appReport.getYarnApplicationState()
                    + " and status: " + appStatus);

            boolean success;
            if (YarnApplicationState.FINISHED == appState
                    && FinalApplicationStatus.SUCCEEDED == appStatus) {
                LOG.info("Application has completed successfully.");
                success = true;
            } else {
                LOG.info("Application did finished unsuccessfully." + " YarnState="
                        + appState.toString() + ", FinalStatus=" + appStatus.toString());
                success = false;
            }

            return success;
        } finally {
            rmClient.stop();
        }
    }

    /**
     * Monitor the submitted application for completion. Kill application if time
     * expires.
     *
     * @param appId
     *          Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private ApplicationReport monitorApplication(ApplicationId appId,
            Set<YarnApplicationState> finalState) throws YarnException,
            IOException {

        long foundAMCompletedTime = 0;
        final int timeToWaitMS = 10000;
        StringBuilder expectedFinalState = new StringBuilder();
        boolean first = true;
        for (YarnApplicationState state : finalState) {
            if (first) {
                first = false;
                expectedFinalState.append(state.name());
            } else {
                expectedFinalState.append("," + state.name());
            }
        }

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = rmClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for" + ", appId="
                    + appId.getId() + ", appAttemptId="
                    + report.getCurrentApplicationAttemptId() + ", clientToAMToken="
                    + report.getClientToAMToken() + ", appDiagnostics="
                    + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
                    + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
                    + report.getTrackingUrl() + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            if (finalState.contains(state)) {
                return report;
            }

            // wait for 10 seconds after process has completed for app report to
            // come back
            if (amCompleted) {
                if (foundAMCompletedTime == 0) {
                    foundAMCompletedTime = System.currentTimeMillis();
                } else if ((System.currentTimeMillis() - foundAMCompletedTime)
                        > timeToWaitMS) {
                    LOG.warn("Waited " + timeToWaitMS/1000
                            + " seconds after process completed for AppReport"
                            + " to reach desired final state. Not waiting anymore."
                            + "CurrentState = " + state
                            + ", ExpectedStates = " + expectedFinalState.toString());
                    throw new RuntimeException("Failed to receive final expected state"
                            + " in ApplicationReport"
                            + ", CurrentState=" + state
                            + ", ExpectedStates=" + expectedFinalState.toString());
                }
            }
        }
    }
}
