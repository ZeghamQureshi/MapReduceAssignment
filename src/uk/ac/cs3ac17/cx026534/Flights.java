package uk.ac.cs3ac17.cx026534;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class Flights 
{
	private String flightID;
	private String deptFAA, destFAA;
	private String dept_time_gmt, flight_time;
	private List<String> passengers;

	/*
	 * constructor to create FLight
	 */
	Flights(String id, String deptFAA, String destFAA, String timeGMT, String flightTime)
	{
		this.flightID = id;
		this.deptFAA = deptFAA;
		this.destFAA = destFAA;
		this.dept_time_gmt = timeGMT;
		this.flight_time = flightTime;
		
		passengers = new ArrayList<String>();
	}
	
	/*
	 * Add string pass_id into list of passengers
	 */
	public void fillPassengers(String pass_id)
	{
		passengers.add(pass_id);
	}
	
	/*
	 * return a list of passengers
	 */
	public List<String> getPassengers()
	{
		return passengers;
	}
	
	public int returnNumPass()
	{
		int num = 0;
		
		for(int i = 0; i < passengers.size(); i++)
		{
			num++;
		}
		
		return num;
	}
	
	/*
	 * print passenger
	 */
	public void printPassengers()
	{
		for(int i = 0; i < passengers.size(); i++)
		{
			System.out.println("\t\t" + passengers.get(i));
		}
	}
	
	/*
	 * Print the passengers to PrintStream p1 buffer.
	 */
	public void printStreamOutput(PrintStream p1)
	{
		String output = "";
		for(int i = 0; i < passengers.size(); i++)
		{
			p1.println("\t\t" + passengers.get(i));
		}
		
	}
	
	/*
	 * add a list of strings (s1) to passengers list
	 */
	public void combinePassengers(List<String> s1)
	{
		passengers.addAll(s1);
	}
	
	/*
	 * return FlightID
	 */
	public String getFlightID()
	{
		return flightID;
	}
	
	/*
	 * return departure airport faa code
	 */
	public String getDeptFAA()
	{
		return deptFAA;
	}
	
	/*
	 * return destination airport code
	 */
	public String getDest()
	{
		return destFAA;
	}
	
	/*
	 * return departure time
	 */
	public String getDeptTime()
	{
		return dept_time_gmt;
	}
	
	/*
	 * return flight time
	 */
	public String getFlightTime()
	{
		return flight_time;
	}
	
	
}
