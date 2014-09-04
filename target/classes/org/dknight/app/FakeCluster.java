package org.dknight.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by User: fanming.chen at Date: 14-9-3 Time: 下午2:19
 */
public class FakeCluster {

    public static void main(String args[]) throws IOException {
        String classpathDir = FakeCluster.class.getResource("/").getPath();
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
        MiniYARNCluster miniYARNCluster = new MiniYARNCluster("yarn-learn", 2, 1, 1);
        miniYARNCluster.init(conf);
        miniYARNCluster.start();
        miniYARNCluster.getConfig().writeXml(
                new FileOutputStream(new File(classpathDir + "yarn-conf.xml"))
        );

        int exitCode = System.in.read();
        System.exit(exitCode);
    }
}
