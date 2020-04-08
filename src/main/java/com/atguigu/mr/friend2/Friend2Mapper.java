package com.atguigu.mr.friend2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Friend2Mapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();
        String[] split = s.split("\t");
        String[] split1 = split[1].split(",");
        v.set(split[0]);
        for (String s1 : split1) {
            for (String s2 : split1) {
                if(s1.equals(s2)){
                    continue;
                }
                if(s1.compareTo(s2)<0){
                    k.set(s1+"-"+s2);
                }else{
                    k.set(s2+"-"+s1);
                }
                context.write(k,v);

            }
        }
    }
}
