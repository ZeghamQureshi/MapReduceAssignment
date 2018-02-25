package uk.ac.cs3ac17.cx026534;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobController extends Thread
{
	PrintStream printError;
	PrintStream printErrorPassengerData;
	
	private String passenger_csv;
	private String airport_30_csv;
	private List<String[]> passenger_data;
	private List<String[]> airport_30_data;
	private int DataSize, chunks;
	
	private Mapper mapper1;
	private Mapper mapper2;
	private Mapper mapper3;
	private Mapper mapper4;
	private Mapper mapper5;
	
	private Map<String, Flights> intOutput1;
	private Map<String, Flights> intOutput2;
	private Map<String, Flights> intOutput3;
	private Map<String, Flights> intOutput4;
	private Map<String, Flights> intOutput5;
	
	private ArrayList<Map<String, Flights>> ssInput; //shufflesort
	private ArrayList<ShuffleSort> ssThreads; //shufflesort
	private List<String> uniqueData;
	private Map<String, Flights> output;
	
	private static final long ONE_MINUTE_IN_MILLIS=60000;//millisecs

	/*
	 * Constructor of the JobController for the MapReduce software prototype. THis will set up necessary paramerters and 
	 * data strcutures needed fopr the entire execution of the MapReduce system. Handles method calls for data input as well as data 
	 * verification/handling/correction. 
	 * 
	 * The JobController will also control all mapper and reducer threads. THis acts as the primary node in which
	 * all other nodes interact with. Relevant intermediate output buffers and final output buffers are also created.
	 * 
	 * No paramters needed. 
	 */
	JobController() throws Exception
	{
		//PrintStreams
		printError = new PrintStream("FAA Code Error Log.txt");
		printErrorPassengerData = new PrintStream("Passenger Error log.txt");
		
		
		passenger_csv = "resources/AComp_Passenger_data.csv";
		airport_30_csv = "resources/Top30_airports_LatLong.csv";
		passenger_data = new ArrayList<String[]>();
		airport_30_data = new ArrayList<String[]>();
		
		mapper1 = new Mapper();
		mapper2 = new Mapper();
		mapper3 = new Mapper();
		mapper4 = new Mapper();
		mapper5 = new Mapper();
		
		intOutput1 = new LinkedHashMap<String, Flights>();
		intOutput2 = new LinkedHashMap<String, Flights>();
		intOutput3 = new LinkedHashMap<String, Flights>();
		intOutput4 = new LinkedHashMap<String, Flights>();
		intOutput5 = new LinkedHashMap<String, Flights>();
		ssInput = new ArrayList<Map<String, Flights>>();
		output = new LinkedHashMap<String, Flights>();
		uniqueData = new ArrayList<String>();
		ssThreads = new ArrayList<ShuffleSort>();
		
		openCSV1(); //Open Airpot data -> needs to be input for relevant look up
		openCSV2(); //Open and check passenger data
		checkCSV2Syntax();
		dataVerif();
		//printData();
		//Delete uplicate data
		deleteDuplicate();
		
		DataSize = passenger_data.size();
		chunks = DataSize / 5;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Thread#run()
	 * 
	 * Run exectuion thread
	 */
	public void run()
	{
		for(int i = 0; i < chunks; i++)
		{
			mapper1.addPassengerData(passenger_data.get(i));
		}
		
		for(int i = chunks; i < chunks*2; i++)
		{
			mapper2.addPassengerData(passenger_data.get(i));
		}
		
		for(int i = chunks * 2; i < chunks*3; i++)
		{
			mapper3.addPassengerData(passenger_data.get(i));
		}
		
		for(int i = chunks * 3; i < chunks*4; i++)
		{
			mapper4.addPassengerData(passenger_data.get(i));
		}
		
		for(int i = chunks * 4; i < chunks*5; i++)
		{
			mapper5.addPassengerData(passenger_data.get(i));
		}
		
		if(chunks*5 < passenger_data.size())
		{
			
			for(int i = chunks*5; i < passenger_data.size(); i++)
			{
				mapper5.addPassengerData(passenger_data.get(i));
			}
		}
		
		
		mapper1.start();
		mapper2.start();
		mapper3.start();
		mapper4.start();
		mapper5.start();
		
		try {
			mapper1.join();
			mapper2.join();
			mapper3.join();
			mapper4.join();
			mapper5.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		intOutput1 = mapper1.returnMap();
		intOutput2 = mapper2.returnMap();
		intOutput3 = mapper3.returnMap();
		intOutput4 = mapper4.returnMap();
		intOutput5 = mapper5.returnMap();
		
		shuffleSortInput(); //shuffleSort input
		/*
		 * We need as many shufflesort threads as there are unique flights
		 */
		setUnique();
		reduce();
		
		for (Map.Entry<String, Flights> entry : output.entrySet())
		{
			System.out.println(entry.getKey());
		}
		
		
		printData();
		
		
		
	}
	
	/*
	 * Private method that opens first CSV file
	 */
	private void openCSV1()
	{
		String line = ""; //String storing the line data
		String tempLine = "";
        String cvsSplitBy = ","; //What splits the data
        String airport, airport_FAA, lat, longt;
        int count = 1;
	

        try (BufferedReader br = new BufferedReader(new FileReader(airport_30_csv))) 
        {        	
            while ((line = br.readLine()) != null)  //Read line, while line is not null
            {
                // use comma as separator
            	String[] lineData = new String[4];
            	lineData = line.split(cvsSplitBy); //Extract line of data 
            	
            	if(lineData.length == 4)
            	{
                	airport = lineData[0]; //PASENGER ID and ID checks            	
                	airport_FAA = lineData[1];
                	lat = lineData[2];
                	longt = lineData[3];
                	
                	if(checkCSV1Syntax(airport, airport_FAA, lat, longt))
                	{
                		airport_30_data.add(lineData);
                	}
                	else
                	{
                		System.out.println("REMOVED CORRUPTED AIRPORT NO." + count);
                		count++;
                		continue;
                	}
                	
            	}
            }
            	//add lineData array into Arraylist containing all data: All rows
            	//if strings match syntax

        } catch (IOException  e) {
        }

        
	}
		
	/*
	 * private method that opens second CSV file
	 */
	private void openCSV2()
	{
        String line = ""; //String storing the line data
        String cvsSplitBy = ","; //What splits the data
        String passenger_id, flight_id, frm_airport_FAA, dest_airport_FAA, dept_time_gmt, flight_time_mins;
        int count = 1;
        boolean firstError = false;
	

        try (BufferedReader br = new BufferedReader(new FileReader(passenger_csv))) 
        {

            while ((line = br.readLine()) != null)  //Read line, while line is not null
            {
                // use comma as separator
            	String[] lineData = line.split(cvsSplitBy); //Extract line of data 
 
            	if(firstError == false)
            	{
            		passenger_id = lineData[0].substring(3);
            		lineData[0] = passenger_id;
            		firstError = true;
            	}
            	
            	else
            	{
            		passenger_id = lineData[0];
            	}
            	 //PASENGER ID and ID checks            	
            	flight_id = lineData[1];
            	frm_airport_FAA = lineData[2];
            	dest_airport_FAA = lineData[3];
            	dept_time_gmt = lineData[4];
            	flight_time_mins = lineData[5];
            	
            	//add lineData array into Arraylist containing all data: All rows
            	//if strings match syntax
            	
            	
            	
            	if(!dept_time_gmt.equals("0"))
            	{
                    passenger_data.add(lineData);
                    continue;

            	}
            
            
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        

	}
	
	/*
	 * Check input data for errors. If errors exist, will not add data into input data structure
	 */	
	private void checkCSV2Syntax()
	{
		/*
	
		 */
		
		for(int i = 0; i < passenger_data.size(); i++)
		{
		
			String s1 = passenger_data.get(i)[0];
			String s2 = passenger_data.get(i)[1];
			String s3 = passenger_data.get(i)[2];
			String s4 = passenger_data.get(i)[3];
			String s5 = passenger_data.get(i)[4];
			String s6 = passenger_data.get(i)[5];
			
			Pattern p1 = Pattern.compile("[A-Z]{3}\\d{4}[A-Z]{2}\\d"); //XXXnnnnXXn - Passenger ID
			Pattern p2 = Pattern.compile("[A-Z]{3}\\d{4}[A-Z]"); //XXXnnnnX - Flight ID
			Pattern p3 = Pattern.compile("[A-Z]{3}"); //xxx - FAA code
			Pattern p4 = Pattern.compile("\\d{10}"); //n[10] - dept time (gmt) in EPOCH
			Pattern p5 = Pattern.compile("\\d{1,4}"); //n[1-4] - flight time (mins)
			
		   	Matcher m1 = p1.matcher(s1); //passenger id
		   	Matcher m2 = p2.matcher(s2); //flight id
		   	Matcher m3 = p3.matcher(s3); //to FAA
		   	Matcher m4 = p3.matcher(s4); //from FAA
		   	Matcher m5 = p4.matcher(s5); //dept time
		   	Matcher m6 = p5.matcher(s6); //flight time
		   	
		   	if(s1.equals(null))
		   	{
		   		passenger_data.remove(i);
		   		
		   	}
		   	
		   	
		   	if(!m1.matches())
		   	{
		   		passenger_data.remove(i);
		   		
		   	}
		   	
		   	else if(!m2.matches())
		   	{
		   		if(FlightIDCorrection(i))
		   		{
		   			
		   		}
		   		
		   		else
		   		{
		   			passenger_data.remove(i);
		   			
		   		}
		   		
		   	}
	   	
		}
  
	}
	
	/*
	 * Check file one for errors (syntax) and apply error handling/cprrection methods
	 */
	private boolean checkCSV1Syntax(String s1, String s2, String s3, String s4)
	{
		
		Pattern p1 = Pattern.compile("[A-Z\\s/]{3,20}"); //Airport Name
		Pattern p2 = Pattern.compile("[A-Z]{3}"); //XXX
		Pattern p3 = Pattern.compile("\\p{Punct}?\\d{1,2}\\p{Punct}\\d{6}"); //xxx - FAA code
		Pattern p4 = Pattern.compile("\\p{Punct}?\\d{1,3}\\p{Punct}\\d{6}"); //xxx - FAA code


	   	Matcher m1 = p1.matcher(s1); //passenger id
	   	Matcher m2 = p2.matcher(s2); //flight id
	   	Matcher m3 = p3.matcher(s3); //to FAA
		Matcher m4 = p4.matcher(s4); //to FAA
	   	
	   	if(!m1.matches())
	   	{
	   		return false;
	   	}
	   	
	   	else if(!m2.matches())
	   	{
	   		return false;
	   	}
	   	else if(!m3.matches())
	   	{
	   		return false;
	   	}
	   	else if(!m4.matches())
	   	{
	   		return false;
	   	}
	   	
	   	else
	   	{
	   		return true;
	   	}

  
	}
		
	/*
	 * Check if data within the different input files match, and match check for any discrepencies. 
	 */
	private void dataVerif()
	{
		int count = 0;
		for(int i = 0; i < passenger_data.size(); i++)
		{
			/*
			 * if either of these return false, that means in the passenger data, one of the FAA codes is incorrect,
			 * we need to find the correct value. For this, all we need to do is match flight id's
			 */
			if(passengerDataVerification(passenger_data.get(i)[2]) == false)
			{
				//data fix
				matchFAA(i, 2, passenger_data.get(i)[1]);
			}
			else if(passengerDataVerification(passenger_data.get(i)[3]) == false)
			{
				//data fix - check this passengers flight ID and match the aiport FAA code
				matchFAA(i, 3, passenger_data.get(i)[1]);
			}
		}
	}
	
	/*
	 * Match passenger to FAA code
	 */
	private boolean passengerDataVerification(String faa)
	{		
	
		for(int i = 0; i < 30; i++)
		{
			String tempFAA = airport_30_data.get(i)[1];
			if(faa.equals(tempFAA))
			{
				return true;
			}
		}


		return false;
	}

	/*
	 * Match FAA code for data correction
	 */
	private void matchFAA(int index, int array, String flightID)
	{
		

			for(int i = 0; i < passenger_data.size(); i++)
			{
				if(i == index)
				{
					continue;
				}
				
				//If flightID matches
				if(passenger_data.get(i)[1].equals(flightID))
				{
					printError.println("Error Code: " + passenger_data.get(index)[array] + " - Correct Code: " + passenger_data.get(i)[array]);
					System.out.println("Error Code: " + passenger_data.get(index)[array] + " - Correct Code: " + passenger_data.get(i)[array]);					
					passenger_data.get(index)[array] = passenger_data.get(i)[array];
					break;
				}
			}
		
		} 
		
	
	/*
	 * Correct the FAA code errors within input data
	 */
	private boolean FlightIDCorrection(int index)
	{
		

		String tempFromFAA = passenger_data.get(index)[3];
		String tempDestFAA = passenger_data.get(index)[3];
		String tempDeptTime = passenger_data.get(index)[4];
		
		for(int i = 0; i < passenger_data.size(); i++)
		{
			if(tempDeptTime.equals(passenger_data.get(i)[4]))
			{
				if(tempFromFAA.equals(passenger_data.get(i)[2]))
				{
					if(tempDestFAA.equals(passenger_data.get(i)[3]))
					{
						System.out.println("Error ID: " + passenger_data.get(index)[1] + " - Fixed ID: " + 
						passenger_data.get(i)[1]);
						passenger_data.get(index)[1] = passenger_data.get(i)[1];
						return true;
					}
				}
			}
		}
	
		
		return false;
	}
	
	/*
	 * Delete duplicate data
	 */
	private void deleteDuplicate()
	{
		for(int i = 0; i < passenger_data.size(); i++)
		{
			String passiD = passenger_data.get(i)[0];
			
			for(int j = 0; j < passenger_data.size(); j++)
			{
				if(i != j)
				{
					if(passiD.equals(passenger_data.get(j)[0]))
					{
						printErrorPassengerData.println("Removed Data: " + passenger_data.get(j)[0]);
						System.out.println("Removed Data: " + passenger_data.get(j)[0]);
						passenger_data.remove(j);
						j--;
						
					}
				}
			}
		}
	}
	
	/*
	 * Prepare intermediate outputs for shufflesort input
	 */
	private void shuffleSortInput()
	{
		
		for(Map.Entry<String, Flights> entry: intOutput1.entrySet())
		{
			Map<String, Flights> tempMap = new LinkedHashMap<String, Flights>();
			tempMap.put(entry.getKey(), entry.getValue());
			ssInput.add(tempMap);		
		}
		
		for(Map.Entry<String, Flights> entry: intOutput2.entrySet())
		{
			Map<String, Flights> tempMap = new LinkedHashMap<String, Flights>();
			tempMap.put(entry.getKey(), entry.getValue());
			ssInput.add(tempMap);		
		}
		
		for(Map.Entry<String, Flights> entry: intOutput3.entrySet())
		{
			Map<String, Flights> tempMap = new LinkedHashMap<String, Flights>();
			tempMap.put(entry.getKey(), entry.getValue());
			ssInput.add(tempMap);		
		}
		
		for(Map.Entry<String, Flights> entry: intOutput4.entrySet())
		{
			Map<String, Flights> tempMap = new LinkedHashMap<String, Flights>();
			tempMap.put(entry.getKey(), entry.getValue());
			ssInput.add(tempMap);		
		}
		
		for(Map.Entry<String, Flights> entry: intOutput5.entrySet())
		{
			Map<String, Flights> tempMap = new LinkedHashMap<String, Flights>();
			tempMap.put(entry.getKey(), entry.getValue());
			ssInput.add(tempMap);		
		}
	}
	
	/*
	 * Find unioque data sets for intermediate input (reducer)
	 */
	private void setUnique()
	{
		uniqueData.add(ssInput.get(0).keySet().iterator().next().toString());
		
		for(int i = 1; i < ssInput.size(); i++)
		{
			for(Map.Entry<String, Flights> entry: ssInput.get(i).entrySet())
			{
				if(!NotUnique(entry.getKey())) //if returned false, data is not in uniqueData set
				{
					uniqueData.add(entry.getKey());
				}
			}
		}	
	}
	
	/*
	 * check if input is unique
	 */
	private boolean NotUnique(String s1)
	{
		for(int i = 0; i < uniqueData.size(); i++)
		{
			if(s1.equals(uniqueData.get(i)))
			{
				return true;
			}
		}
		
		return false;
	}
	
	/*
	 * Create and execute reducer thread
	 */
	private void reduce()
	{
		for(int i = 0; i < uniqueData.size(); i++)
		{
			ssThreads.add(new ShuffleSort(uniqueData.get(i))); //create shufflesort thread for this flight
			
			for(int j = 0; j < ssInput.size(); j++)
			{
				for (Map.Entry<String, Flights> entry : ssInput.get(j).entrySet())
				{
					if(uniqueData.get(i).equals(entry.getKey())) //condition to check which data gets sent to which SS thread
					{
						//if true, send to thread created in i
						ssThreads.get(i).sortData(entry.getKey(), entry.getValue());
					}
					
					else
					{
						continue;
					}
				}
			}
			
			ssThreads.get(i).start();
		}
		
		for(int i = 0; i < ssThreads.size(); i++)
		{
			try {
				ssThreads.get(i).join();
				output.putAll(ssThreads.get(i).returnReduce());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * create output files
	 */
	private void printData()
	{
		try {
		//Task 1
			PrintStream out1 = new PrintStream("Airports.txt"); //Print Stream
			String output1 = "";
			System.out.println("\n\tOBJECTIVE 1 \n");
			for(int i = 0; i < airport_30_data.size() ; i++)
	        {
				int num = 0;
				String airportName = airport_30_data.get(i)[0];
				String airportFAA = airport_30_data.get(i)[1];	
				
				for (Map.Entry<String, Flights> entry : output.entrySet())
	        	{
					if(entry.getValue().getDeptFAA().equals(airportFAA))
					{
						num++;
					}
	        	}
				out1.println("Airpots: " + airportName + " (" + airportFAA + ")");
				out1.println("Number of flights: " + num);	
				out1.println("");	
	        }
        
		
		//Task 2/3 - Determine no of flights from each airport
		String output2 = "";
		System.out.println("\tOBJECTIVE 2 & 3 \n");
        

		System.out.println(output1);
        System.out.println(output2);
        
        
			PrintStream out2 = new PrintStream("Flights and Passengers.txt"); //Print Stream
			
			for(int i = 0; i < airport_30_data.size() ; i++)
	        {
	        	String airportName = airport_30_data.get(i)[0];
	        	String airportFAA = airport_30_data.get(i)[1];
	        	out2.println("========================================================================"); //Line between airports
	        	out2.println("Airport: " + airportName + " (" + airportFAA + ")");
	        	
	        	for (Map.Entry<String, Flights> entry : output.entrySet())
	        	{
	        		if(entry.getValue().getDeptFAA().equals(airportFAA))
	        		{
	        			//Calculate dept time/date
	        			String date = entry.getValue().getDeptTime();
	        			long epoch = Long.parseLong(date);
	        			epoch = epoch * 1000;
	        			Date d = new Date(epoch);
	        			
	        			//Calculate arrival time
	        			String length = entry.getValue().getFlightTime();
	        			long arrivalMin = Long.parseLong(length);
	        			epoch = epoch + (arrivalMin * 60000);
	        			Date arrival = new Date(epoch);

	        			out2.println("\tFlight ID: " + entry.getValue().getFlightID());
	        			out2.println("\tDept Time: " + d.toString());
	        			out2.println("\tArrival Time: " + arrival.toString());
	        			out2.println("\tFlight Length (mins)" + length);
	        			out2.println("\tNo. of passengers: " + entry.getValue().returnNumPass());
	        			out2.println("\tPassengers:");
	        			entry.getValue().printStreamOutput(out2);
	        			
	        			
	        			;	
	        		}
	        	}
	        	
	        }
		} 
        catch (FileNotFoundException e) 
        {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        
        
	}
}
	