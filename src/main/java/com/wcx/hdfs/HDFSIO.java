package com.wcx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: hdfs的io流
 *
 * @author: WuChengXing
 * @create: 2020-10-23 22:13
 **/
public class HDFSIO {

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

    /**
     * 进行io流上传到hdfs上面
     */
    @Test
    public void putFileToHDFS() throws IOException {
        FileSystem fs = getFS();
        // 获取文件输入流
        FileInputStream fileInputStream = new FileInputStream(new File("E:\\test\\a.txt"));

        // 获取输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/a.txt"));

        // 流的拷贝
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, fs.getConf());

        // 关闭资源
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fs.close();
    }

    /**
     * 从hdfs上面下载文件
     */
    @Test
    public void getFileToHDFS() throws IOException {
        FileSystem fs = getFS();
        // 获取文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("E:\\test\\a.txt"));

        // 获取输入流
        FSDataInputStream open = fs.open(new Path("/a.txt"));

        // 流的拷贝
        IOUtils.copyBytes(open, fileOutputStream, fs.getConf());

        // 关闭资源
        IOUtils.closeStream(open);
        IOUtils.closeStream(fileOutputStream);
        fs.close();
    }

    /**
     * 读取第一块
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        FileSystem fs = getFS();

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/test/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 读取第二块
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        FileSystem fs = getFS();

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, fs.getConf());

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
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
