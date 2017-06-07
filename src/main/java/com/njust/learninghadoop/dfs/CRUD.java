package com.njust.learninghadoop.dfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2017/5/31.
 */
public class CRUD {

    FileSystem fs = null;
    @Before
    public void init() throws Exception {
        fs = FileSystem.get(new URI("hdfs://192.168.175.102:9000"), new Configuration(), "hadoop");
    }

    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("H:\\jdk-7u60-windows-x64.exe"),new Path("/jdk.exe"));
        fs.close();
    }

    @Test
    public void query() throws IOException {
        fs.copyToLocalFile(new Path("/1.txt"),new Path("F:\\2.txt"));
        fs.close();
    }

    @Test
    public void stream() throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/1.txt"));
        //IOUtils.copy(inputStream,System.out);
        byte[] buffer = new byte[1];
        IOUtils.copyLarge(inputStream, System.out, 2, 30,buffer);
        fs.close();
    }

    @Test
    public void fileStatus() throws Exception{
        //列出根目录下的所有的文件，包括目录
        //如果是文件的话，那么就是文件本身
        FileStatus[] fileStatuses = fs.listStatus(new Path("/jdk.exe"));
        for(FileStatus statuses:fileStatuses){
            BlockLocation[] blockLocations = fs.getFileBlockLocations(statuses, 0L, statuses.getLen());

            for (BlockLocation bl:blockLocations){
                /*因为每一个bl有多个副本，所以返回的是一个数组类型的数据*/
                System.out.println(bl.getHosts());
                for (String name : bl.getNames()){
                    System.out.println(name);
                }

                System.out.println(bl.getTopologyPaths()[0]);

                //获取的是这个bl的长度和在文件中的偏移量

                System.out.println("bl.getLength"+bl.getLength());
                System.out.println("bl.getOffset"+bl.getOffset());
            }
        }
        fs.close();
    }

}
