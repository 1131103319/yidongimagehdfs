package org.apache.yidong.yidongimagehdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;

public class HdfsClient {
    public static Configuration conf;
    public static FileSystem fs;

    public static void init() throws Exception {
        conf = new HdfsConfiguration();
        conf.addResource(new Path("/etc/hdfs1/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hdfs1/conf/hdfs-site.xml"));
        System.setProperty("java.security.krb5.conf", "/etc/hdfs1/conf/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        String hostname = "";
        InetAddress ip = InetAddress.getLocalHost();
        hostname = ip.getHostName();
        // 通过hostname拼接krb5User
        String krb5User = "hdfs/" + hostname.trim() + "@TDH";
        //       krb5相关认证操作

        UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(krb5User,"/etc/hdfs1/conf/hdfs.keytab");
    }
    public static FileSystem getFileSystem() throws Exception {
        if(fs == null) {
            synchronized (HdfsClient.class) {
                if (fs == null) {
                    init();
                    fs = FileSystem.get(conf);
                }
            }
        }
        return fs;
    }
}
