package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class Station implements Serializable{
	private String id;
	private String longitude, latitude, name;
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getLongitude() {
		return longitude;
	}
	
	public void setLongitude(String longitude) {
		this.longitude = new String(longitude);
	}
	
	public String getLatitude() {
		return longitude;
	}
	
	public void setLatitude(String latitude) {
		this.latitude = new String(latitude);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = new String(name);
	}
}
