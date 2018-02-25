package uk.ac.cs3ac17.cx026534;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class ShuffleSort extends Thread
{
	
	private ArrayList<Pair<String, Flights>> data;
	private LinkedHashMap<String, Flights> reduceOutput;
	private String threadname;
	private Reduce reduceThread;
	
	ShuffleSort(String name)
	{
		threadname = name;
		data = new ArrayList<Pair<String, Flights>>();
	}
	
	public void run()
	{
		reduceThread = new Reduce(data);
		reduceThread.start();
		try {
			reduceThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void sortData(String s1, Flights f1)
	{
		Pair<String, Flights> tempPair = new Pair<String, Flights>();
		tempPair.setFirst(s1);
		tempPair.setSecond(f1);
		data.add(tempPair);
		
	}
	
	public LinkedHashMap<String, Flights> returnReduce()
	{
		reduceOutput = new LinkedHashMap<String, Flights>();
		reduceOutput = reduceThread.returnReduce();
		
		return reduceOutput;
	}
	
}
