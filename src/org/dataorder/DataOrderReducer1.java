package org.dataorder;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataOrderReducer1 extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 같은 제품을 산 사람들을 ','로 연결하여 result에 넣기
		HashMap<String,String> map = new HashMap<String,String>();
		String txt = "";
		
		for(Text value : values) {
			if(!map.containsKey(value)) {
				map.put(value.toString(), key.toString());
				txt = txt + value + ",";
			}
		}
		txt = txt.substring(0, txt.length()-1);
		
		result.set(txt);
		
		context.write(key, result);
	}
}
