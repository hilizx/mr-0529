package com.atguigu.mr.order;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		System.out.println("----");
		System.out.println("----");
		System.out.println("----");

		for (NullWritable nullWritable : values) {
			context.write(key, NullWritable.get());
		}
	}

}