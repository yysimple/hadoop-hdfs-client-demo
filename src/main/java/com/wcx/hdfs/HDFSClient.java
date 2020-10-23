package com.wcx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: hdfs客户端类
 *
 * @author: WuChengXing
 * @create: 2020-10-22 14:34
 **/
public class HDFSClient {

    /**
     * hadoop 连接地址
     */
    private final String HADOOP_URI = "hdfs://hadoop102:9000";

    /**
     * hdfs上的文件目录
     */
    private final String HADOOP_DIR = "/java/hadoop/test";

    /**
     * hadoop的用户
     */
    private final String HADOOP_USER = "root";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        //  hdfs的配置
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // 获取hdfs的客户端
        // FileSystem fileSystem = FileSystem.get(conf);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        // 创建一个文件
        fileSystem.mkdirs(new Path("/java/hadoop/test"));
        // 关闭资源
        fileSystem.close();

        System.out.println("over......");
    }

    /**
     * 测试本地文件上传到 hdfs
     *
     * @throws Exception
     */
    @Test
    public void testCopyFromLocalFile() throws Exception {
        FileSystem fs = getFS();
        fs.copyFromLocalFile(new Path("F:\\personal-project\\hadoop\\atguigu-bigdata\\hadoop-hdfs-client-demo\\src\\main\\java\\com\\wcx\\hdfs\\HDFSClient.java"),
                new Path(HADOOP_DIR + "/HDFSClient.java"));
        fs.close();
    }

    /**
     * 从 hdfs 下载到本地
     *
     * @throws Exception
     */
    @Test
    public void testCopyToLocalFile() throws Exception {
        FileSystem fs = getFS();
        fs.copyToLocalFile(false, new Path(HADOOP_DIR + "/HDFSClient.java"),
                new Path("F:\\HDFSClient.java"), true);
        fs.close();
    }

    /**
     * 从 hdfs 上删除文件
     *
     * @throws Exception
     */
    @Test
    public void testRemoveFile() throws Exception {
        FileSystem fs = getFS();
        fs.delete(new Path(HADOOP_DIR + "/HDFSClient.java"), true);
        fs.close();
    }

    /**
     * 修改文件名称
     *
     * @throws Exception
     */
    @Test
    public void testUpdateFileName() throws Exception {
        FileSystem fs = getFS();
        fs.rename(new Path(HADOOP_DIR + "/HDFSClient.java"), new Path(HADOOP_DIR + "/aaa.java"));
        fs.close();
    }

    /**
     * 查看文件详情
     *
     * @throws Exception
     */
    @Test
    public void testGetFileDetail() throws Exception {
        FileSystem fs = getFS();
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus locatedFileStatus = listFiles.next();
            // 文件名称
            System.out.println(locatedFileStatus.getPath().getName());
            // 长度
            System.out.println(locatedFileStatus.getLen());
            // 获取权限
            System.out.println(locatedFileStatus.getPermission());
            // 分组
            System.out.println(locatedFileStatus.getGroup());
            System.out.println(locatedFileStatus.getAccessTime());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        fs.close();
    }

    /**
     * 获取hadoop的连接  root用户
     *
     * @return
     */
    public FileSystem getFS() {
        //  hdfs的配置
        Configuration conf = new Configuration();
        // 获取hdfs的客户端
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(HADOOP_URI), conf, HADOOP_USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }
}
