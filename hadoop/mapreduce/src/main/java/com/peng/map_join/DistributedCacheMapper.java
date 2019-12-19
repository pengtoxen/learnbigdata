package com.peng.map_join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();
    HashMap<String, String> map = new HashMap<>();
    /**
     * 执行一次,将pd表缓存起来
     * 先缓存pd表
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        //获取输入字节流
        FileInputStream fis = new FileInputStream(cacheFiles[0].getPath());
        //获取转换流
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        //获取缓存流
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        //开始一行一行读数据
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            //将数据放入map中
            map.put(fields[0],fields[1]);
        }
        fis.close();
        isr.close();
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //通过pid获取pname
        String[] fields = line.split("\t");
        //拿到一行的pid,去缓存中查找,有的话取出值
        //没有不做操作
        //相当于在mapper端实现了join的操作
        String pid = fields[1];
        String pname = map.get(pid);
        String str = line + "\t" + pname;
        k.set(str);
        context.write(k, NullWritable.get());
    }
}
