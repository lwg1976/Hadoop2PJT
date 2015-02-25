package org.dataorder;

import org.apache.hadoop.io.Text;

public class DataOrderParser {
	private String name;
	private String product;
	
	public DataOrderParser(Text text) {
		try {
			String[] colums = text.toString().split(",");
			this.name = colums[0];
			this.product = colums[1];
		} catch (Exception e) {
			System.out.println("Error parsing a record : " + e.getMessage());
		}
	}
	
	public String getName() { return name; }
	public void setName(String name) { this.name = name; }
	public String getProduct() { return product; }
	public void setProduct(String product) { this.product = product; }
}