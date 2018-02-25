package uk.ac.cs3ac17.cx026534;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapReduceEngine 
{
	
	public static void main(String[] args)
	{	
		JobController job1;
		try {
			job1 = new JobController();
			job1.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
