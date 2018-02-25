package uk.ac.cs3ac17.cx026534;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class Reduce extends Thread
{

	private ArrayList<Pair<String, Flights>> reduceInput;
	private String flightID;
	private Flights flight;
	
	/*
	 * Create reduce thread with input as ArrayList<Pair<String, Flights>>
	 */
	Reduce(ArrayList<Pair<String, Flights>> input)
	{
		reduceInput = new ArrayList<Pair<String, Flights>>();
		reduceInput = input;
		
	}
	
	public void run()
	{
		flightID = reduceInput.get(0).getFirst();
		flight = reduceInput.get(0).getSecond();
		for(int i = 1; i < reduceInput.size(); i++)
		{
			flight.combinePassengers(reduceInput.get(i).getSecond().getPassengers());
		}
	}
	
	/*
	 * return data as a LinekdHashMap
	 */
	public LinkedHashMap<String, Flights> returnReduce()
	{
		LinkedHashMap<String, Flights> temprtn = new LinkedHashMap<String, Flights>();
		temprtn.put(flightID, flight);
		return temprtn;
	}
	
}
