package salesForce.salesFore_Service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
//import org.json.JSONArray;
import org.json.JSONException;
//import org.json.JSONException;
//import org.json.JSONObject;
//import org.json.JSONTokener;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Hello world!
 *
 */
public class App 
{
    	static final String USERNAME = "xxxx.malla@te.dev";
    	static final String PASSWORD = "tedev@xxxxxxxxxxxx";
    	static final String LOGINURL = "https://login.salesforce.com";
    	static final String GRANTSERVICE = "/services/oauth2/token?grant_type=password";
    	static final String CLIENTID = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    	static final String CLIENTSECRET = "xxxxxxxxxxxxxxx";
    	private static String REST_ENDPOINT = "/services/data";
    	private static String API_VERSION = "/v32.0";
    	private static String baseUri;
    	private static Header oauthHeader;
    	private static Header prettyPrintHeader = new BasicHeader("X-PrettyPrint", "1");
    	private static String email;
    	private static String name;
    	private static String mobilePhone;
    	private static String leadCompany;
    	private static String leadId;
    	private static String LastModifiedDate;

    	public static void main(String[] args) {
    		
    	 

    		HttpClient httpclient = HttpClientBuilder.create().build();

    		// Assemble the login request URL
    		String loginURL = LOGINURL + GRANTSERVICE + "&&&client_id=" + CLIENTID + "&&&client_secret=" + CLIENTSECRET
    				+ "&&&username=" + USERNAME + "&&&password=" + PASSWORD;

    		// Login requests must be POSTs
    		HttpPost httpPost = new HttpPost(loginURL);
    		HttpResponse response = null;

    		try {
    			// Execute the login POST request
    			response = httpclient.execute(httpPost);
    		} catch (ClientProtocolException cpException) {
    			cpException.printStackTrace();
    		} catch (IOException ioException) {
    			ioException.printStackTrace();
    		}

    		// verify response is HTTP OK
    		final int statusCode = response.getStatusLine().getStatusCode();
    		if (statusCode != HttpStatus.SC_OK) {
    			System.out.println("Error authenticating to Force.com: " + statusCode);
    			// Error is in EntityUtils.toString(response.getEntity())
    			return;
    		}

    		String getResult = null;
    		try {
    			getResult = EntityUtils.toString(response.getEntity());
    		} catch (IOException ioException) {
    			ioException.printStackTrace();
    		}
    		JSONObject jsonObject = null;
    		String loginAccessToken = null;
    		String loginInstanceUrl = null;
    		try {
    			jsonObject = (JSONObject) new JSONTokener(getResult).nextValue();
    			loginAccessToken = jsonObject.getString("access_token");
    			loginInstanceUrl = jsonObject.getString("instance_url");
    		} catch (JSONException jsonException) {
    			//jsonException.printStackTrace();
    			System.out.println("Exception in getting access token");
    		}
    		System.out.println(response.getStatusLine());
    		System.out.println("Successful login");
    		System.out.println("  instance URL: " + loginInstanceUrl);
    		System.out.println("  access token/session ID: " + loginAccessToken);

    		baseUri = loginInstanceUrl + REST_ENDPOINT + API_VERSION;
    		oauthHeader = new BasicHeader("Authorization", "OAuth " + loginAccessToken);
    		System.out.println("oauthHeader1: " + oauthHeader);
    		System.out.println("\n" + response.getStatusLine());
    		System.out.println("Successful login");
    		System.out.println("instance URL: " + loginInstanceUrl);
    		System.out.println("access token/session ID: " + loginAccessToken);
    		System.out.println("baseUri: " + baseUri);
    		
    		//queryAccounts();
    		 //createBillSummary();
    		 createMeterReadings();
    		 //release connection
    		 //httpPost.releaseConnection();
    	}

    	// DB connection
    	public static Connection getDBConnection() {
    		try {
    			Class.forName("oracle.jdbc.driver.OracleDriver");
    		} catch (ClassNotFoundException e) {
    			System.out.println("Where is your Oracle JDBC Driver?");
    			e.printStackTrace();
    			return null;
    		}
    		System.out.println("Oracle JDBC Driver Registered!");
    		Connection connection = null;
    		try {
    			connection = DriverManager.getConnection("jdbc:oracle:thin:@xx.xxx.50.xxx:xxxx:dproixxx", "dprxxxx",
    					"dprzzzz");
    		} catch (SQLException e) {
    			System.out.println("Connection Failed! Check output console");
    			e.printStackTrace();
    			return null;
    		}
    		if (connection != null) {
    			System.out.println("You made it, take control your database now!");
    			return connection;
    		} else {
    			System.out.println("Failed to make connection!");
    			return null;
    		}

    	}

    	// query Contacts using REST http get
    	public static void queryAccounts() {
    		System.out.println("\n_______________ Accounts QUERY _______________");
    		try {

    			// Set up the HTTP objects needed to make the request.
    			HttpClient httpClient = HttpClientBuilder.create().build();
    			//String uri = baseUri + "/query?q=Select+FirstName+,+LastName+,+LocationCity__c+,+LocationCountry__c+,+LocationPostalCode__c+,+LocationState__c+,+LocationStreet__c+,+Notification_Email__c+,+PersonMobilePhone+,+LastModifiedDate+From+Account+Where+LastModifiedDate+=+2018-05-16T10:25:39Z";
    			String query = "Select FirstName,LastName,LocationCity__c,LocationCountry__c,LocationPostalCode__c,LocationState__c,LocationStreet__c,Notification_Email__c,PersonMobilePhone,LastModifiedDate From Account Where LastModifiedDate > LAST_MONTH";
    			
    			String toolingQuery = ""; 
    			 try { 
    				 toolingQuery = java.net.URLEncoder.encode(query, "UTF-8"); 
    				 } 
    			 catch (UnsupportedEncodingException e) { 
    				 // TODO Auto-generated catch block 
    				 e.printStackTrace(); 
    				 }
    				 
    			//String uri = baseUri + "/query?q=Select+FirstName+,+LastName+,+LocationCity__c+,+LocationCountry__c+,+LocationPostalCode__c+,+LocationState__c+,+LocationStreet__c+,+Notification_Email__c+,+PersonMobilePhone+,+LastModifiedDate+From+Account+Where+LastModifiedDate+>=+TODAY";
    			 String uri = baseUri + "/query?q="+toolingQuery;
    			System.out.println("Query URL: " + uri);
    			HttpGet httpGet = new HttpGet(uri);
    			System.out.println("oauthHeader2: " + oauthHeader);
    			httpGet.addHeader(oauthHeader);
    			httpGet.addHeader(prettyPrintHeader);
    			Connection conn=null;
    			Statement stmt = null; 
    			
    			// Make the request.
    			HttpResponse response = httpClient.execute(httpGet);

    			// Process the result
    			int statusCode = response.getStatusLine().getStatusCode();
    			if (statusCode == 200) {
    				String response_string = EntityUtils.toString(response.getEntity());
    				try {
    					JSONObject json = new JSONObject(response_string);
    					System.out.println("JSON result of Query:\n" + json.toString(1));
    					JSONArray j = json.getJSONArray("records");
    				    conn = getDBConnection();
    				    stmt = conn.createStatement();
    				    
    					for (int i = 0; i < j.length(); i++) {
    						if (json.getJSONArray("records").getJSONObject(i).isNull("Notification_Email__c")){
    							email = null;
    						}
    						else
    						{
    							email = json.getJSONArray("records").getJSONObject(i).getString("Notification_Email__c");
    						}
    						if (json.getJSONArray("records").getJSONObject(i).isNull("PersonMobilePhone")){
    							mobilePhone = null;
    						}
    						else
    						{
    							mobilePhone = json.getJSONArray("records").getJSONObject(i).getString("PersonMobilePhone");
    						}
    						name = json.getJSONArray("records").getJSONObject(i).getString("FirstName");
    						LastModifiedDate = json.getJSONArray("records").getJSONObject(i).getString("LastModifiedDate");
    						System.out.println("contact record record has: " + i + ". " + email + " " + mobilePhone + " "
    								+ name +" "+LastModifiedDate);
    						String legacy_query = "INSERT INTO CONTACTS (name, email,phone_number,last_modified_date) VALUES ('"+name+"', '"+email+"', '"+mobilePhone+"', '"+LastModifiedDate+"')";
    						System.out.println(legacy_query);
    						stmt.executeUpdate(legacy_query);
    					}
    					//stmt.close();
    					conn.close();
    				} catch (JSONException je) {
    					je.printStackTrace();
    				}catch (SQLException e) {
    					System.out.println(e.getMessage());
    				}
    			} else {
    				System.out.println("Query was unsuccessful. Status code returned is " + statusCode);
    				System.out.println("An error has occured. Http status: " + response.getStatusLine().getStatusCode());
    				// System.out.println(getBody(response.getEntity().getContent()));
    				System.exit(-1);
    			}
    		} catch (IOException ioe) {
    			ioe.printStackTrace();
    		} catch (NullPointerException npe) {
    			npe.printStackTrace();
    		} 
    	}

    	// Create BillSummary using REST HttpPost
    	public static void createBillSummary() {
    		System.out.println("\n_______________ Bill Sumary INSERT _______________");

    		String uri = baseUri + "/sobjects/Bill_Summary_Detail__c/";
    		try {
    			// get DB Connection
    			Connection conn = getDBConnection();
    			Statement stmt = conn.createStatement();
    			
    			String sql = "SELECT Consumer_ID,to_char(Statement_Date_and_Time,'YYYY-MM-DD') Statement_Date_and_Time, Statement_Balance FROM BILL_SUMMARY";
    			ResultSet rs = stmt.executeQuery(sql);
    			while (rs.next()) {
    				// create the JSON object containing the new lead details.
    				JSONObject lead = new JSONObject();
    				lead.put("Statement_Date_and_Time__c", rs.getString("Statement_Date_and_Time"));
    				lead.put("Statement_Balance__c", rs.getString("Statement_Balance"));
    				lead.put("Consumer_ID__c", rs.getString("Consumer_ID"));

    				System.out.println("JSON for lead record to be inserted:\n" + lead.toString(1));

    				// Construct the objects needed for the request
    				HttpClient httpClient = HttpClientBuilder.create().build();

    				HttpPost httpPost = new HttpPost(uri);
    				httpPost.addHeader(oauthHeader);
    				httpPost.addHeader(prettyPrintHeader);
    				// The message we are going to post
    				StringEntity body = new StringEntity(lead.toString(1));
    				body.setContentType("application/json");
    				httpPost.setEntity(body);

    				// Make the request
    				HttpResponse response = httpClient.execute(httpPost);

    				// Process the results
    				int statusCode = response.getStatusLine().getStatusCode();
    				String statusMessage = response.getStatusLine().getReasonPhrase();
    				if (statusCode == 201) {
    					String response_string = EntityUtils.toString(response.getEntity());
    					JSONObject json = new JSONObject(response_string);
    					// Store the retrieved lead id to use when we update the lead.
    					leadId = json.getString("id");
    					System.out.println("New Lead id from response: " + leadId);
    				} else {
    					System.out.println(
    							"Insertion unsuccessful. Status code returned is " + statusCode + " " + statusMessage);
    				}
    			}
    			rs.close();
    			conn.close();
    		} catch (JSONException e) {
    			System.out.println("Issue creating JSON or processing results");
    			e.printStackTrace();
    		} catch (IOException ioe) {
    			ioe.printStackTrace();
    		} catch (NullPointerException npe) {
    			npe.printStackTrace();
    		} catch (SQLException se) {
    			// Handle errors for JDBC
    			se.printStackTrace();
    		} catch (Exception e) {
    			// Handle errors for Class.forName
    			e.printStackTrace();
    		}
    	}

    	// Create BillSummary using REST HttpPost
    	public static void createMeterReadings() {
    		System.out.println("\n_______________ Meater Reading INSERT _______________");

    		String uri = baseUri + "/sobjects/Meter_Readings__c/";
    		try {
    			// get DB Connection
    			Connection conn = getDBConnection();
    			Statement stmt = conn.createStatement();

    			String sql = "SELECT Consumer_ID,Energy_Type,Meter_Serial_Number,Energy_Type_Descriptor,to_char(Reading_Date_Time,'YYYY-MM-DD') Reading_Date_Time,Reading_Value,Reading_Source_Type FROM Meter_Readings";
    			ResultSet rs = stmt.executeQuery(sql);

    			// create the JSON object containing the new lead details.
    			while (rs.next()) {
    				JSONObject meterReading = new JSONObject();
    				meterReading.put("Consumer_ID__c", rs.getString("Consumer_ID"));
    				meterReading.put("Energy_Type__c", rs.getString("Energy_Type"));
    				meterReading.put("Meter_Serial_Number__c", rs.getString("Meter_Serial_Number"));
    				meterReading.put("Energy_Type_Descriptor__c", rs.getString("Energy_Type_Descriptor"));
    				meterReading.put("Reading_Date_Time__c", rs.getString("Reading_Date_Time"));
    				meterReading.put("Reading_Value__c", rs.getString("Reading_Value"));
    				meterReading.put("Reading_Source_Type__c", rs.getString("Reading_Source_Type"));

    				System.out.println("JSON for lead record to be inserted:\n" + meterReading.toString(1));

    				// Construct the objects needed for the request
    				HttpClient httpClient = HttpClientBuilder.create().build();

    				HttpPost httpPost = new HttpPost(uri);
    				httpPost.addHeader(oauthHeader);
    				httpPost.addHeader(prettyPrintHeader);
    				// The message we are going to post
    				StringEntity body = new StringEntity(meterReading.toString(1));
    				body.setContentType("application/json");
    				httpPost.setEntity(body);

    				// Make the request
    				HttpResponse response = httpClient.execute(httpPost);

    				// Process the results
    				int statusCode = response.getStatusLine().getStatusCode();
    				String statusMessage = response.getStatusLine().getReasonPhrase();
    				if (statusCode == 201) {
    					String response_string = EntityUtils.toString(response.getEntity());
    					JSONObject json = new JSONObject(response_string);
    					// Store the retrieved lead id to use when we update the lead.
    					leadId = json.getString("id");
    					System.out.println("New Meeter reading from response: " + leadId);
    				} else {
    					System.out.println(
    							"Insertion unsuccessful. Status code returned is " + statusCode + " " + statusMessage);
    				}
    			}
    			rs.close();
    			conn.close();
    		} catch (JSONException e) {
    			System.out.println("Issue creating JSON or processing results");
    			e.printStackTrace();
    		} catch (IOException ioe) {
    			ioe.printStackTrace();
    		} catch (NullPointerException npe) {
    			npe.printStackTrace();
    		} catch (SQLException se) {
    			// Handle errors for JDBC
    			se.printStackTrace();
    		} catch (Exception e) {
    			// Handle errors for Class.forName
    			e.printStackTrace();
    		}

    	}

    	
    	public static Date StringToDate(String s) {

    		Date result = null;
    		try {
    			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    			result = dateFormat.parse(s);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}

    		return result;
    	}

    }
