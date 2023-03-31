package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class ProductPair implements Serializable{
	private static final long serialVersionUID = 1L;
	private String p1;
	private String p2;
	
	public ProductPair(String p1, String p2) {
		this.p1 = new String(p1);
		this.p2 = new String(p2);
	}
	
	public void setP1(String p1) {
		this.p1 = p1;
	}
	
	public String getP1() {
		return p1;
	}
	
	public void setP2(String p2) {
		this.p2 = p2;
	}
	
	public String getP2() {
		return p2;
	}
	
	public String toString() {
		return p1+","+p2;
	}
}
