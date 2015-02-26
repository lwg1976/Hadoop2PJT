package org.dataorder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataOrderReducer2 extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 같은 제품을 산 사람들을 ','로 연결하여 result에 넣기
		ArrayList<String> list = new ArrayList<String>();
		String txt = "";
		
		for(Text value : values) {
			list.add(value.toString());			
		}
		
		Collections.sort(list);
		
		for(int i=0; i<list.size(); i++) {
			txt = txt + list.get(i);
			if(i != list.size()-1) {
				txt = txt + ",";
			}
		}
		
		result.set(txt);
		
		context.write(key, result);
	}
}
