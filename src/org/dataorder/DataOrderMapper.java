package org.dataorder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataOrderMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		DataOrderParser parser = new DataOrderParser(value);
		
		outputKey.set(parser.getProduct());
		outputValue.set(parser.getName());
		
		// 제품을 outputKey로 사람을 outputValue로 하여 출력
		context.write(outputKey, outputValue);
	}
}
