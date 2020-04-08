package com.atguigu.mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString();
        String[] split = text.split(":");
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            v.set(split[0]);
            context.write(k,v);
        }
    }
}
