package uk.ac.cs3ac17.cx026534;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Mapper extends Thread
{
	private List<String[]> passenger_data;
	private Map<String, Flights> flightsData;
	
	/*
	 * Mapper and Combiner
	 */
	Mapper()
	{
		passenger_data = new ArrayList<String[]>();
		flightsData = new LinkedHashMap<String, Flights>();
	}
	

	
	public void run()
	{
		
		//Implement processing
		int size = passenger_data.size();
		for(int i = 0; i < size; i++)
		{
			String id, flightId, from, dest, dept_time, flight_time;
			id = passenger_data.get(i)[0];
			flightId = passenger_data.get(i)[1];
			from = passenger_data.get(i)[2];
			dest = passenger_data.get(i)[3];
			dept_time = passenger_data.get(i)[4];
			flight_time = passenger_data.get(i)[5];
			
			if(flightsData.size() == 0)
			{
				Flights flight = new Flights(flightId, from, dest, dept_time, flight_time);
				flight.fillPassengers(id); //add this passenger
				flightsData.put(flightId, flight);
				continue;
			}
		
			//No need for loop -> good thing about hashmaps
			if(!flightsData.containsKey(flightId)) //Key is flightID, value is flight object -> search for key that matches flightID
			{
				//if null, means flight does not exist in this map -> create new flight object
				Flights flight = new Flights(flightId, from, dest, dept_time, flight_time);
				flight.fillPassengers(id);
				flightsData.put(flightId, flight);
			}
			else //if something is returned, which means this this flight exists as an object already -> need to find it and add passenger
			{
				flightsData.get(flightId).fillPassengers(id);
			}
				
		}
		
		
		
		//Mapping and combining have been complete -> shuffle sort time from different maps
		System.out.println("");

	}
	
	/*
	 * add passenger data in the format of String[] 'line' to passenger data
	 */
	public void addPassengerData(String[] line)
	{
		passenger_data.add(line);
	}
	
	/*
	 * return Map<String, Flights> to caller
	 */
	public Map<String, Flights> returnMap()
	{
		return flightsData;
	}
	
	public void printData()
	{
		for (Map.Entry<String, Flights> entry : flightsData.entrySet()) {
		    String key = entry.getKey();
		    Flights value = entry.getValue();		    
		    System.out.println("key: " + key + "\t Flights " + value.getFlightID());
		}
	}

}
