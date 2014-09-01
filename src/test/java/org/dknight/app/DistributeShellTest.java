package org.dknight.app;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.yarn.applications.distributedshell.*;

/**
 * Created by DKNIGHT on 2014/8/31.
 */
public class DistributeShellTest  {

    private static final Log LOG = LogFactory.getLog(DistributeShellTest.class);

    private MiniDFSCluster miniDFSCluster;

    private MiniYARNCluster miniYARNCluster;

    private String classpathDir = "";
    @Before
    public void setUp() throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\Programs\\hadoop-2.2.0");
        classpathDir = this.getClass().getResource("/").getPath();
//        Configuration configuration = new Configuration();
//        Path tempDir = Files.createTempDirectory("yarn-learn");
//        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toString());
//        miniDFSCluster = new MiniDFSCluster.Builder(configuration).numDataNodes(1).build();


        Configuration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        conf.set("yarn.log.dir", classpathDir);
        conf.set("hadoop.tmp.dir", classpathDir);
        miniYARNCluster = new MiniYARNCluster("yarn-learn", 1, 1, 1);
        miniYARNCluster.init(conf);
        miniYARNCluster.start();
        miniYARNCluster.getConfig().writeXml(
                new FileOutputStream(new File(classpathDir + "yarn-conf.xml"))
        );
    }

    @Test
    public void distributeShellNullTest() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new org.apache.hadoop.fs.Path(
                new File(classpathDir+"yarn-conf.xml").getAbsolutePath()));
        boolean result = false;
        String[] args = {"-jar",classpathDir + "yarn-learn-1.0-SNAPSHOT.jar","-shell_command", "dir",
                "-num_containers", "1",
                "-priority", "10",
				"-debug"};
        try {
            Client client = new Client(conf);
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }
}
