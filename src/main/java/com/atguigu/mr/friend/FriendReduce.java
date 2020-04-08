package com.atguigu.mr.friend;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FriendReduce extends Reducer<Text,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> strings = new ArrayList<>();
        for (Text value : values) {
            String s = value.toString();
            strings.add(s);
        }
        String join = StringUtils.join(strings, ",");
        k.set(key);
        v.set(join);
        context.write(k,v);
    }
}
