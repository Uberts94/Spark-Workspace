package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StationTimestamp implements Serializable{
	private int station, used_slots, free_slots, hour;
	private String timestamp;
	
	public int getStation() {
		return station;
	}
	
	public void setStation(int station) {
		this.station = station;
	}
	
	public String getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		hour = Integer.parseInt(timestamp.split(" ")[0].split(":")[0]);
		this.timestamp = new String(DateTool.DayOfTheWeek(timestamp));
	}
	
	public int getUsed_slots() {
		return used_slots;
	}
	
	public void setUsed_slots(int used_slots) {
		this.used_slots = used_slots;
	}
	
	public int getFree_slots() {
		return free_slots;
	}
	
	public void setFree_slots(int free_slots) {
		this.free_slots = free_slots;
	}
	
	public int getHour() {
		return hour;
	}
}
