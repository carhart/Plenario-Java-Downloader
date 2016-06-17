package rawdata;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;



public class ClientPlenarioRawData {

	private static final int OFFSET_STEP = 1000; //this is equal to the RESPONSE_LIMIT fixed in Plenario (= 1000 currently)

	public static void main(String[] args) throws IOException {

		String query = "";

		//compose the query...
		
		//-- header...
		query += "http://plenar.io/v1/api/detail/"; 
		
		//-- start date and end date
		query += "?obs_date__ge=" + "2001%2F01%2F01"	 
	    	  + "&obs_date__le=" + "2014%2F12%2F31";	
		

		//-- geometry location
		query += /*"";*/ "&location_geom__within=" 
		  + "%7B%22type%22%3A%22Feature%22%2C%22properties%22%3A%7B%7D%2C%22geometry%22%3A%7B%22type%22%3A%22Polygon%22%2C%22" 
		  + "coordinates%22%3A%5B%5B%5B"
		  + "-87.715187" + "%2C" + "41.937530"  //FIRST VERTEX
		  + "%5D%2C%5B"
		  + "-87.670212" + "%2C" + "41.911092"   //SECOND VERTEX
		  + "%5D%2C%5B"
		  + "-87.649269" + "%2C" + "41.879020"   //THIRD VERTEX
		  + "%5D%2C%5B"
		  + "-87.669010" + "%2C" + "41.878764"   //FOURTH VERTEX
		  + "%5D%2C%5B"
		  + "-87.717762" + "%2C" + "41.934338"   //FIFTH VERTEX
		  + "%5D%2C%5B"
		  + "-87.715187" + "%2C" + "41.937530"   //LAST VERTEX, EQUAL TO THE FIRST ONE
		  + "%5D%5D%5D%7D%7D";

		
		
		//-- community area (range)
		//query += "&community_area=42"; //"&community_area__ge=37&community_area__le=42";
		
		//-- dataset name
		query += "&dataset_name=" + "crimes_2001_to_present";
		//"crimes_2001_to_present", "building_permits", "business_licenses" 
		//"311_service_requests_abandoned_vehicles",
		//"311_service_requests_vacant_and_abandoned_building"
		
		//-- data type
		query += "&data_type=" + "csv";
		
		String outputFileName = "Crimes_2001-2014_Area2";//;datasetName; //"customizedDatasetName"
		//"Crimes_2010-2014_Area2","BuildingPermits_2010-2014_Area2","BusinessLicenses_2010-2014_Area2"
		//"311AbandVehicles_2010-2014_Area2",
		//"311VacAbandBuilding_2010-2014_Area2"
		
		
		executeQuery(query, outputFileName); 

		//System.out.println(query);
		
	}//main


	/**
	 * Metodo per eseguire una semplice query su Plenar.io
	 * @throws IOException 
	 */
	private static void executeQuery(String str_base_query, String outputFileName) throws IOException{		
		
		//create the output file
		PrintWriter outputFile = new PrintWriter(outputFileName + ".csv");
		
		boolean completed = false; //the query is completed when all the pages are processed 
		int numPages = 0; //number of pages results are split
		int offSet = 0;   //offset for the query, due to the limitation of Plenario (RESPONSE_LIMIT = 1000)
		int totalNumOfResults = 0; //total number of results, summed up on all the returned pages
		
		System.out.println("#Page:\t" + "#Instances");
		
		while (!completed) {

			//complete the query, by appending the right offset value
			String offset = "offset=" + offSet;
			String str_query = str_base_query + "&" + offset; 
	
			//Submit the query to Plenario
			try{
				//create the http_query and submit it to Plenario
				HttpClient client = new DefaultHttpClient();
				HttpGet http_query = new HttpGet(str_query);
				HttpResponse response = client.execute(http_query);
				
				//has the query been correctly executed ?
				if(response.getStatusLine().getReasonPhrase().contains("Error")){ //Error
					System.out.println("Query Error!!!");
				}//if
				
				else {	//OK, correctly executed
					//get the results in an array structure and update some variables
					String results = EntityUtils.toString(response.getEntity());
					String resultsArray [] = results.split("\n");
					int results_size = resultsArray.length; 
					int numOfInstances = results_size - 1; //the first row is the attribute list 
					numPages++; //a new page of the query response
					totalNumOfResults += numOfInstances; //update the total number

					//the first row is the attribute list, so it must be printed 
					//only on the first page
					int firstRowToPrint = 0;
					if (numPages == 1)
						firstRowToPrint = 0;
					else
						firstRowToPrint = 1;
					
					//print the results page on the file and then flush
					for (int i = firstRowToPrint; i < resultsArray.length; i++ )
							outputFile.print(resultsArray[i]);

					outputFile.flush();

					System.out.println("Page " + numPages + ":\t" + numOfInstances + "(" + offset + ")");
					
					//if there are no other pages, stop; otherwise, 
					//update offset and re-submit the query
					if (resultsArray.length < OFFSET_STEP)
						completed = true;
					else
						offSet = offSet + OFFSET_STEP;
					
				}//else

			}catch(Exception e){
				e.printStackTrace();
			}
			
		}//while
		
		System.out.println("TOTAL:\t" + totalNumOfResults);
		
	} //executeQuery


}
