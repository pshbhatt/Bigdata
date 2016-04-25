package edu.okstate.cs.EHL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import net.sf.javaml.clustering.mcl.MarkovClustering;
import net.sf.javaml.clustering.mcl.SparseMatrix;

/**
 * @author Piyush, Prateek, Kiran and Prasanna
 * Date :03/21/2016
 * DataSimilarityAnalyser
 *
 */
public class DataSimilarityAnalyser {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	static ArrayList<String> DS1 = new ArrayList<String>();
	static ArrayList<String> DS2 = new ArrayList<String>();
	static ArrayList<String> DS1_type = new ArrayList<String>();
	static ArrayList<String> DS2_type = new ArrayList<String>();
	static ArrayList<String> DS1_unique = new ArrayList<String>();
	static ArrayList<String> DS2_unique = new ArrayList<String>();
	static ArrayList<Float> pairWiseDistance = new ArrayList<Float>();
	static String metaFile1;
	static String metaFile2;
	static BufferedReader br;
	static String line;
	static String delimiter = ",";
	static String HIVESITEPATH = "$HIVE_HOME/conf/hive-site.xml";
	static String HDFSSITEPATH = "$HADOOP_HOME/etc/hadoop/hdfs-site.xml";
	
public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		String option = args[0];
		String metadata1 = args[1];
		String metadata2 = args[2];
		if(option.equals("-retrieve")){
			retrieveSimilarity(metadata1, metadata2);
		} else if(option.equals("-compute")){
		computeSimilarity(metadata1, metadata2);	
		} else {
			System.out.println("Please provide a correct option: Either 'compute' or 'retrieve'");
			System.exit(0);
		}
	
				
	}
	/**
	 * Checks whether a log location has been set in hdfs-site.xml and 
	 * accordingly provides the result. 
	 * @param type
	 * @return
	 */
	public static boolean isLogLocationSet(int type){
		Configuration conf = new Configuration();
		conf.addResource(new Path(HDFSSITEPATH));
		if(type==0){
			String dataContent = conf.get("data.content.similarity.log.location");
			if(dataContent == null){
				return false;
			} else {
				return true;
			}
		} else {
			String dataUsage = conf.get("data.usage.similarity.log.location");
			if(dataUsage == null){
				return false;
			} else {
				return true;
			}
		}
	}
	
	/**
	 * Provides the log location from hdfs-site.xml based on the type.
	 * @param type
	 * @return
	 */
	public static String getLogLocation(int type){
		Configuration conf = new Configuration();
		conf.addResource(new Path(HDFSSITEPATH));
		if(type==0){
			String dataContent = conf.get("data.content.similarity.log.location");
			return dataContent;
		} else {
			String dataUsage = conf.get("data.usage.similarity.log.location");
			return dataUsage;
		}
	}
	
	/**
	 * Checks whether log location has been set and creates a table accordingly.
	 * @param tableName
	 * @param schemaName
	 * @param type
	 * @throws SQLException
	 * @throws IOException
	 */
	
	public static boolean schemaExists(String schemaName) throws SQLException, ClassNotFoundException{
		int i=1;
		
		Class.forName(driverName);
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		 Statement stmt = con.createStatement();
		 ResultSet res = stmt.executeQuery("SHOW SCHEMAS like" + "'" + schemaName + "'");
		    while(res.next()){
		    	
		    	System.out.println(res.getString(i));
		    	i++;
		    }
		    con.close();
		    if(i >= 1){
		    	return true;
		    } else {
		    	return false;
		    }
	}
	
	/**
	 * Creates the schema based on the schema name passed as an argument. 
	 * @param schemaName
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	public static void createSchema(String schemaName) throws SQLException, ClassNotFoundException{
		int i=1;

		 // Register driver and create driver instance
	      Class.forName(driverName);
		
		HiveConf cons = new HiveConf();
		cons.addResource(new Path(HIVESITEPATH));
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		    Statement stmt = con.createStatement();
		    stmt.executeQuery("CREATE SCHEMA " +schemaName);
		    con.close();
	}
	
	public static void createTable(String tableName,String schemaName,int type) throws SQLException, IOException, ClassNotFoundException{
		String location;
		String schema;
		String DCSA_schema = "DATASET1 STRING,DATASET2 STRING,SCORE DOUBLE";
		String DUPA_schema = "DATASET STRING,CLUSTER INTEGER";
		if(isLogLocationSet(type)==true){
			location = getLogLocation(type);
		} else {
			location = "/DSA";
		}
		int i=1;
	
		 // Register driver and create driver instance
	      Class.forName(driverName);
		
		HiveConf cons = new HiveConf();
		cons.addResource(new Path(HIVESITEPATH));
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		    Statement stmt = con.createStatement();
		    stmt.executeQuery("Use" + schemaName);
		    if(type == 0)
		    {
		    	schema = DCSA_schema;
		    }
		    else
		    {
		    	schema = DUPA_schema;
		    }
		    stmt.execute("CREATE TABLE " +tableName + "(" + schema + ") LOCATION "+location);
		    con.close();
		
	}
	
	/**
	 * Checks whether the table exists based on the table name passed as an argument.
	 * @param tableName
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	public static boolean tableExists(String tableName, String schemaName) throws SQLException, ClassNotFoundException{
		int i=1;
		schemaName = schemaName.substring(0, schemaName.indexOf("."));
		
		 // Register driver and create driver instance
	      Class.forName(driverName);
		HiveConf cons = new HiveConf();
		cons.addResource(new Path(HIVESITEPATH));
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		    Statement stmt = con.createStatement();
		    stmt.executeQuery("Use " + schemaName);
		    ResultSet res = stmt.executeQuery("SHOW TABLES like" + "'" + tableName + "'");
		    while(res.next()){
		    	
		    	System.out.println(res.getString(i));
		    	i++;
		    }
		    con.close();
		    if(i >= 1){
		    	return true;
		    } else {
		    	return false;
		    }
		
	}
	
	
	public static void computeContentSimilarity(String Dataset_1, String Dataset_2) throws SQLException,IOException, ClassNotFoundException
	{
		double linguisticScore = 0.0;
		double structureMatchingScore = 0.0;
		double constraintMatchingScore = 0.0;
		double alpha = 0.316;
		double beta = 0.450;
		double gamma =0.234;
		double contentSimilarityScore = 0.0;
		
		//check if schema exists and create one if false is returned
		if(!schemaExists("EHL_DSA"))
		{
			createSchema("EHL_DSA");
		}
		
		//check if table exists and create one if false is returned
		if(!tableExists("DCSA","EHL_DSA"))
		{
			createTable("DCSA","EHL_DSA",0);
		}
		
		//Check whether the entry for ContentSimilarity exists in DCSA table
		if(entryExistsInDCSA("DCSA","EHL_DSA",Dataset_1,Dataset_2))
		{
			return;
		}
		else  //compute ContentSimilarity if there is no entry for these datasets
		{
			linguisticScore = computeLinguisticMatchingScore(Dataset_1,Dataset_2);
			structureMatchingScore = computeStructuralMatchingScore(Dataset_1,Dataset_2);
			constraintMatchingScore = computeConstraintMatchingScore(Dataset_1,Dataset_2);
			
			contentSimilarityScore = (alpha * linguisticScore)+(beta * structureMatchingScore)+
					(gamma * constraintMatchingScore);
			
			//add an entry to DCSA after calculating the content similarity score
			addEntryInDCSA("DCSA","EHL_DSA",Dataset_1,Dataset_2,contentSimilarityScore);
		}
	}
	
	//Checks whether the computed similarity score exists for the passed Datasets in the DCSA table
	//returns true if entry exists
	//returns false if an entry doesn't exist
	public static boolean entryExistsInDCSA(String tableName,String schemaName,String Dataset_1,String Dataset_2) throws ClassNotFoundException, SQLException
	{
		int i = 1;

		 // Register driver and create driver instance
	      Class.forName(driverName);
		
		HiveConf cons = new HiveConf();
		cons.addResource(new Path(HIVESITEPATH));
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		    Statement stmt = con.createStatement();
		    stmt.executeQuery("Use " + schemaName);
		    ResultSet res = stmt.executeQuery("SELECT * FROM "+tableName+" WHERE DATASET1 LIKE '"+Dataset_1+"' AND "
		    + "DATASET2 LIKE '"+Dataset_2+"'");
		    while(res.next()){
		    	
		    	System.out.println(res.getString(1));
		    	i++;
		    }
		    con.close();
		    if(i > 1){
		    	return true;
		    } else {
		    	return false;
		    }
	}
	
	//Inserts a row in to the DCSA Table for similarity score for the passed data sets
	public static void addEntryInDCSA(String tableName,String schemaName,String Dataset_1,String Dataset_2,
			double contentSimilarityScore) throws ClassNotFoundException, SQLException
	{
		int result = -1;

		 // Register driver and create driver instance
	      Class.forName(driverName);
		
		HiveConf cons = new HiveConf();
		cons.addResource(new Path(HIVESITEPATH));
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		    Statement stmt = con.createStatement();
		    stmt.executeQuery("Use " + schemaName);
		    stmt.executeQuery("INSERT INTO "+tableName+" VALUES('"+Dataset_1+"',"+"'"+Dataset_2+"',"+"'"+contentSimilarityScore+"')");
		    
		 
		    if(result != -1)
		    {
		    	System.out.println("Similarity score inserted");
		    }
		    else
		    {
		    	System.out.println("Failed to add Similarity score");
		    }
		    con.close();
	}
	
	//Computes Structral matching score for the datasets passed
	 public static double computeStructuralMatchingScore(String Dataset_1, String Dataset_2) {
			String DS1;
			String DS2;
			  BufferedReader br = null;
			Configuration conf1=new Configuration();
	        try {
				conf1.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
			} catch (MalformedURLException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
	        conf1.reloadConfiguration();
	        DS1=conf1.get("data.similarity.analyzer.metadata."+Dataset_1);
		
		
	        Configuration conf2=new Configuration();
	        try {
				conf2.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        conf2.reloadConfiguration();
	        DS2=conf2.get("data.similarity.analyzer.metadata."+Dataset_2);
			// As we are not sure about the size of metadata, therefore, we are creating Arraylists
			ArrayList<String> DS1_data= new ArrayList<String>();
			ArrayList<String> DS2_data= new ArrayList<String>();
			String line = "";
			
			// We need a character to separate the words 
			String splitBy = " ";
			float dataUnion , datasetIntersection=0;
			double structuralMatchingScore;

		// Try-catch block for storing metadata1's data in ArrayList
			try {
				br = new BufferedReader(new FileReader(DS1));
				while ((line = br.readLine()) != null) {
				        // use space as separator
					String[] dataItem = line.split(splitBy);
					DS1_data.add(dataItem[0]);
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();        // Closing the File
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

		//Try-catch block for storing metadata2's data in ArrayList
				try {
					br = new BufferedReader(new FileReader(DS2));
					while ((line = br.readLine()) != null) {
					        // use space as separator
						String[] dataItem = line.split(splitBy);
						DS2_data.add(dataItem[0]);
					}

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (br != null) {
						try {
							br.close();  // Closing the File
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
			}
		  }
		  //Counting the Intersection (D1^D2)
			for(String temp : DS1_data)
			{
				if (DS2_data.contains(temp))  // Condition to check whether the element exists as common
				{
					datasetIntersection++;    // To increment Intersection count 
				}
			}

			// Calculating Union ( | D1 V D2 | ) : 
			// I have used the real concept of Union (Eliminating Common Elements from the result (Size of DS1 & size of DS2)
			
			dataUnion = DS1_data.size() + DS2_data.size() - datasetIntersection;
			
			// Calculating Structural Matching Score
			if(DS1_data.size() == 1 & DS2_data.size()==1)
		           /* As per specification, 
			        * If both the input datasets have only one data item 
			        * then this function will return 1 */
				structuralMatchingScore = 1;
				else
			    structuralMatchingScore = datasetIntersection/dataUnion;
			return structuralMatchingScore;
		  }
		
	 //Reads the file contents stored at Path : filePath
	 //Into a ArrayList : dataset
	 //Tokenizes the words based on passed delimiter
		  public static void readMetaData(String filePath, ArrayList dataset)
			{
				try {
					br = new BufferedReader(new FileReader(filePath));
					while ((line = br.readLine()) != null) {
					       // use delimiter as separator
						 StringTokenizer st = new StringTokenizer(line,delimiter,false);
					     while (st.hasMoreTokens()) {
					    	 //store token into datatype
					    	 dataset.add(st.nextToken());
					    	 break;
					     } //inner while
					         
					     } //outer while
					} //try
					catch (Exception e) {
					System.out.println(e);
					} 
			}
			
			//LinguistingMatching score
			//Calculates the Levenshtein distance between any two strings passed
			static public int levenshtein_Distance(String a, String b) {
		        a = a.toLowerCase();
		        b = b.toLowerCase();
		        // i == 0
		        int [] costs = new int [b.length() + 1];
		        for (int j = 0; j < costs.length; j++)
		            costs[j] = j;
		        for (int i = 1; i <= a.length(); i++) {
		            costs[0] = i;
		            int nw = i - 1;
		            for (int j = 1; j <= b.length(); j++) {
		                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
		                nw = costs[j];
		                costs[j] = cj;
		            }
		        }
		        return costs[b.length()];
		    }
			
			//LinguistingMatching score
			//Returns the number of words in a string
			static public int getWordCountInString(String s)
			{
				String delim = " \t";
				int words = 0;
				StringTokenizer st = new StringTokenizer(s,delim,false);
			     while (st.hasMoreTokens()) {
			    	 st.nextToken();
			    	words++;
			     } //inner while
			     
			     return words;
			}
			
			
			//LinguistingMatching score
			//Calculates the LMS(D1[DI1],DIx) between the datasets passed. Similarity score between
			//a data item in Dataset 1 and all data items in Dataset 2 is computed
			public static void calculate_pairWise_distance(ArrayList DS1, ArrayList DS2)
			{
				float totalDistance = 0;
				int numberItemsInDataset2 = DS2.size();
				for(int i =0; i<DS1.size(); i++)
				{
					//Take each data item from first dataset
					String s1 = (String)DS1.get(i);
					totalDistance = 0;
					for(int j=0;j<DS2.size();j++)
					{
						//Take data item from Dataset 2
						String s2 = (String)DS2.get(j); 
						totalDistance = totalDistance + (1-(levenshtein_Distance(s1,s2)/getWordCountInString(s1)));
					}
					
					//similarity score is the average of Levenshtein distances calculated between the
					//a dataitem in Dataset1 and all dataset items in second dataset
					totalDistance = totalDistance / numberItemsInDataset2;
					pairWiseDistance.add(totalDistance);
				}	
			}
			
			//LinguistingMatching score
			//Calculates the overall similarity score between two datasets passed.
			public static float calculateTotalLMSDistance(ArrayList pairWiseDistanceDataSet)
			{
				int numberItemsInDataSet1 = pairWiseDistanceDataSet.size();
				float totalDistance = 0;
				for(int i=0;i<pairWiseDistanceDataSet.size();i++)
				{
					//Get the similarity score calculated for each data item in first dataste against
					//all data items in dataset 2
					totalDistance = totalDistance + (float)pairWiseDistanceDataSet.get(i);
				}
				totalDistance = totalDistance / numberItemsInDataSet1;
				return totalDistance;
			}
			
			
			//LinguistingMatching score
			//Calls the functions that read the meta data files and
			//functions that calculate the LMS distance between two datasets passed
			public static double computeLinguisticMatchingScore(String Dataset_1, String Dataset_2)
			{
				DS1.removeAll(DS1);
				DS2.removeAll(DS2);
				//Get paths of the metadata files where they are stored
				  Configuration conf1=new Configuration();
			        try {
						conf1.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
					} catch (MalformedURLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
			        conf1.reloadConfiguration();
			        metaFile1=conf1.get("data.similarity.analyzer.metadata."+Dataset_1);
			        if(metaFile1 == null)
			        {
			        	System.out.println("Cannot access the metadata file");
			        	return -1;
			        }
				
			        Configuration conf2=new Configuration();
			        try {
						conf2.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			        conf2.reloadConfiguration();
			        metaFile2=conf2.get("data.similarity.analyzer.metadata."+Dataset_2);
			        if(metaFile2 == null)
			        {
			        	System.out.println("Cannot access the metadata file");
			        	return -1;
			        }
				
				
				//Read meta data file from given paths and store the 
			    //Data Item names in Datasets to Arraylists
				readMetaData(metaFile1,DS1);
				readMetaData(metaFile2, DS2);
				calculate_pairWise_distance(DS1,DS2);
				float lms = calculateTotalLMSDistance(pairWiseDistance);
				
				//return lms distance if the similarity score is >= 0.8
				if(lms >= 0.8)
				return lms;
				
				//return zero if the lms distance is < 0.8
				else
					return 0;			
			}
			
			 //ConstraintMatching Score
			//Read the passed metadata files and store into ArrayList variables
			//The dataname is stored into one ArrayList
			//the data type and uniqueness of data is stored into other ArrayLists
			  public static void readConstraintMetaData(String filePath, ArrayList dataset, ArrayList DS_type, ArrayList DS_unique)
				{
					try {
						br = new BufferedReader(new FileReader(filePath));
						while ((line = br.readLine()) != null) {
							int k = 0;
						       // use space as separator
							 StringTokenizer st = new StringTokenizer(line,delimiter,false);
						     while (st.hasMoreTokens()) {
						    	 String temp = st.nextToken();
						    	 
						    	 if(k == 0)
						    	 dataset.add(temp);
						    	 else if(k == 1 )
						    	 DS_type.add(temp);
						    	 else
						    	DS_unique.add(temp);
						    	
						    	 k++;
						     } //inner while
						         
						     } //outer while
						} //try
						catch (Exception e) {
						System.out.println(e);
						} 
				}
			 
			 //ConstraintMatching Score
			//Return the computed value of constrainMatchingScore based on metadata of datasets passed
			  public static double computeConstraintMatchingScore(String Dataset_1,String Dataset_2)
				{
				  int similarItemCount = 0;
				  double similarDatatypeCount = 0;
				  double sameUniquenessCount = 0;
				  double X = 0;
				  double Y = 0;
				  
				  DS1.removeAll(DS1);
				  DS2.removeAll(DS2);
				  DS1_type.removeAll(DS1_type);
				  DS2_type.removeAll(DS2_type);
				  DS1_unique.removeAll(DS1_unique);
				  DS2_unique.removeAll(DS2_unique);
				  
				  
				//Get paths of the metadata files where they are stored
				  Configuration conf1=new Configuration();
			        try {
						conf1.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
					} catch (MalformedURLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
			        conf1.reloadConfiguration();
			        metaFile1=conf1.get("data.similarity.analyzer.metadata."+Dataset_1);
			        if(metaFile1 == null)
			        {
			        	System.out.println("Cannot access the metadata file");
			        	return -1;
			        }
				
			        Configuration conf2=new Configuration();
			        try {
						conf2.addResource(new File(HDFSSITEPATH).getAbsoluteFile().toURI().toURL());
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			        conf2.reloadConfiguration();
			        metaFile2=conf2.get("data.similarity.analyzer.metadata."+Dataset_2);
			        if(metaFile2 == null)
			        {
			        	System.out.println("Cannot access the metadata file");
			        	return -1;
			        }
				
					//Read meta data file from given paths and store the 
				    //Data Item names in Datasets to Arraylists
				     readConstraintMetaData(metaFile1,DS1,DS1_type,DS1_unique);  
					readConstraintMetaData(metaFile2,DS2,DS2_type,DS2_unique);
					
					similarItemCount = calculateSimilarDataItemsCount(DS1,DS2);
					similarDatatypeCount = calculatesameDataTypeOrUniqueCount(DS1,DS2,0);
					sameUniquenessCount = calculatesameDataTypeOrUniqueCount(DS1,DS2,1);;
					
					X = similarDatatypeCount/similarItemCount;
					Y = sameUniquenessCount/similarItemCount;
					
					double res = (X+Y)/2;
					System.out.println("Res : "+res);
					
					if(res >= 0.65)
						return res;
					
					else			
					return 0;
		}
			  
			  //ConstraintMatching Score
			  //Return the count of similar data items with same type if flag = 0
			  //Return the count of similar data items with same uniqueness if flag = 1
			  public static double calculatesameDataTypeOrUniqueCount(ArrayList<String> DS1_data,ArrayList<String> DS2_data, int flag)
			  {
				  int count = 0;
				  ArrayList<String> List_DS1;
				  ArrayList<String> List_DS2;
				  if(flag == 0)
				  {
				  List_DS1 = new ArrayList<String>(DS1_type);
				  List_DS2 = new ArrayList<String>(DS2_type);
				  }
				  else
				  {
				  List_DS1 = new ArrayList<String>(DS1_unique);
				  List_DS2 = new ArrayList<String>(DS2_unique);
				  }
				  
				  for(int i=0;i<DS1_data.size();i++)
				  {
					  int index1 = i;
					  String element = DS1_data.get(i);
					  if(DS2_data.contains(element))
					  {
						  int index2 = DS2_data.indexOf(element);
						  
						  //check if the type/uniqueness if similar items match
						  if(List_DS1.get(index1).equalsIgnoreCase(List_DS2.get(index2)))
							  count++;
					  }
				  }			  
				  return count;
			  }

			  //ConstraintMatching Score
			  //Returns the count of similar data items in the passed datasets
			  public static int calculateSimilarDataItemsCount(ArrayList<String> DS1_data,ArrayList<String> DS2_data)
			  {
				  int count = 0;
				  for(int i=0;i<DS1_data.size();i++)
				  {
					  if(DS2_data.contains(DS1_data.get(i)))
					  {
						  count++;
					  }
				  }
				  return count;
			  }
			  public static void computeUsageSimilarity(String DS1,String DS2) throws IOException, ClassNotFoundException, SQLException 
				{
				  // It has been mentioned that the function is not returning any value	
					if(schemaExists("EHL_DSA"))
					{
						createSchema("EHL_DSA");                                                      
						// It will create schema if not present
					}
					
					if(tableExists("DUPA","EHL_DSA") != true)                                         
						// If the table doesn't exists within the schema,then, create a table name DUPA
					{
						createTable("DUPA","EHL_DSA",1);                                              
						// It will create table if not present
					}
					
					// The name of the file to open
					HiveConf cons = new HiveConf();

					cons.addResource(new Path(HIVESITEPATH));
							//Using the values from the UsageInfo.log. These values are constant.
			                SparseMatrix sparseMatrix = null;
			                SparseMatrix sparseMatrix1 = null;
			                sparseMatrix.add(0,0,1);
			                sparseMatrix.add(0,1,1);
			                sparseMatrix.add(0,2,0);
			                sparseMatrix.add(1,0,0);
			                sparseMatrix.add(1,1,1);
			                sparseMatrix.add(1,2,1);
			                sparseMatrix.add(2,0,1);
			                sparseMatrix.add(2,1,1);
			                sparseMatrix.add(2,2,1);
			                String splitBy = ",",clusterName = null;
			                int clusterID = 0;
			                String firstDataSetCluster=null ,secondDataSetCluster=null;
							BufferedReader br = null,br2=null;
			         // This flag is used as a signal to indicate whether the dataset is present or not
							int presentFlag =0;
							try {

								String sCurrentLine;
			         // Reading the DUPA.log file
								br = new BufferedReader(new FileReader("DUPA.log"));

								while ((sCurrentLine = br.readLine()) != null) {
									String [] dataItem = sCurrentLine.split(splitBy);
					// To check whether the cluster is present or not
									if(dataItem[1]==DS1)                   
									{
										firstDataSetCluster = dataItem[0]; // This will store the cluster number of DS1
									    presentFlag++;
									}
									if(dataItem[1]== DS2)
									{
										secondDataSetCluster = dataItem[0]; 
										presentFlag++;
									}
								}

							} catch (IOException e) {
								e.printStackTrace();                                 // TO handle various exception
							} finally {
								try {
									if (br != null)br.close();
								} catch (IOException ex) {
									ex.printStackTrace();
								}
							}
			              if ((firstDataSetCluster == secondDataSetCluster)&& (firstDataSetCluster != null))
			              {
			            	  System.out.println("Exist in the Same Cluster");             // It will print this message, if these two arguments have the same clusters
			              }
			              else if((firstDataSetCluster != null)&& (secondDataSetCluster != null)) 
			              {
			            	  System.out.println("In different Clusters");                 // It will print this message, if these two datasets are present in different clusters
			              }
				
			        // This statement will execute if both the datasets are not present in DUPA.log
			           if(presentFlag !=2)
			           {
			              br2 = new BufferedReader(new FileReader("$HADOOP_HOME/etc/UsageInfo.log"));

							String sCurrentLine;
							while ((sCurrentLine = br2.readLine()) != null) {
								String [] dataItem = sCurrentLine.split(splitBy);
								if(dataItem[1]==DS1)                   // To check whether the cluster is present or not
								{
									firstDataSetCluster = dataItem[0]; // This will store the cluster number of DS1
								    presentFlag++;
								}
								if(dataItem[1]== DS2)
								{
									secondDataSetCluster = dataItem[0]; 
									presentFlag++;
								}
							}
							
			                // Calling Markov's Clustering Algorithm
			                MarkovClustering mcl = new MarkovClustering();
			                
			                sparseMatrix =  mcl.expand(sparseMatrix);
			                double usageInfo = mcl.inflate(sparseMatrix1, 1.0, 1.0);
			                System.out.println(usageInfo);
			                Class.forName(driverName);
			        		
			        		HiveConf consf = new HiveConf();
			        // Establishing the connection
			        		consf.addResource(new Path(HIVESITEPATH));
			        		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
			        		    Statement stmt = con.createStatement();
			        // Updating the DUPA table with new values
			        		    stmt.executeQuery("Use EHL_DSA");
			        		    ResultSet res = stmt.executeQuery("INSERT INTO DUPA values ("+ clusterID+",'"+clusterName+"')");
			        		 
			        		    con.close();

						} 
				}

				
				
				/* Function Description: 12. computeSimilarity(DS1,DS2) 
				 *                       This function should call "computeContentSimilarity(DS1,DS2)" and "computeUsageSimilarity(DS1,DS2)"
				 *                       It will compute the similarity between the datasets
				 */
				
				public static void computeSimilarity(String DS1, String DS2) throws ClassNotFoundException, SQLException, IOException
				{
					// It will call the function specified in 7th Specification
					computeContentSimilarity(DS1,DS2);                                   
					// It will call the function specified in 11th Specification
					try {
						computeUsageSimilarity(DS1,DS2);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}                                
				}
				
				
/**
 * @param DS1
 * @param DS2
 * @throws ClassNotFoundException
 * @throws SQLException
 * @throws IOException
 * 
 */
public static void retrieveSimilarity(String DS1, String DS2) throws ClassNotFoundException, SQLException, IOException{
	if(!schemaExists("EHL_DSA")){
		createSchema("EHL_DSA");
	} 
	if(!tableExists("DUPA","EHL_DSA")||!tableExists("DCSA", "EHL_DSA")){
		System.out.println("Error: Table does not exist");
		System.exit(0);
	}
	if(tableExists("DUPA","EHL_DSA")&&tableExists("DCSA", "EHL_DSA")){
		computeContentSimilarity(DS1, DS2);	
		computeUsageSimilarity(DS1, DS2);
	} else {
		System.out.println("Warning: The argument does not exist in either of the tables");
	}
	
}
				
}




