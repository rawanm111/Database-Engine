package main.java;

import java.text.DateFormat;
import java.text.SimpleDateFormat; 
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.io.*;
import java.util.*;
import exceptions.DBAppException;
import main.java.Octree.OctreeElement;
import main.java.Octree.OctreeNode;

public class DBApp {
   
	public void init( ) throws FileNotFoundException{

        try {
                File metaDatafile = new File("src/main/resources/metaData.csv");
                PrintWriter metaDataFile = new PrintWriter(metaDatafile);
                String tableMetaData = "Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType, min, max";
                metaDataFile.write(tableMetaData);
                metaDataFile.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax )
			throws DBAppException ,IOException
			{
		      //should we be validating meta input?
		       
		       //getting table columns' data
	    boolean tableExist= false;
		BufferedReader br1= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
		String colRead;
		ArrayList<String[]> colSave = new ArrayList <>() ;
		//Looping on file to check if table exists
		if(!((colRead= (br1.readLine() )).split(",")[0].equals("Table Name")))
			{
	//System.out.println("hhh");
			init();
		}
		while((colRead= br1.readLine() )!=null) {
			String [] colCurrent= colRead.split(",");
			if(colCurrent[0].equals(strTableName)) {
				tableExist= true;
				colSave.add(colCurrent);
			}
		}
		br1.close();
		if(!tableExist) {	
			   Enumeration<String> colName= htblColNameType.keys();
			   Enumeration<String> colNameMin= htblColNameMin.keys();
			   Enumeration<String> colNameMax= htblColNameMax.keys();
			   //reading metaData file before the update
			   BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
			   String line="";
			   StringBuilder fillMeta= new StringBuilder(); 
			   while((line= br.readLine())!=null) {
	            	  
	            	  fillMeta.append(line).append('\n');
	               }
			   br.close();
			   //The updated meta data file 
               FileWriter metaDataFile = new FileWriter("src/main/resources/metaData.csv");
               metaDataFile.write(fillMeta.toString());
               String row = null;
		       while (colName.hasMoreElements()) {
		        	//Here we fill in MetaDataFile with table's columns
		        	 String name = colName.nextElement();
		        	 String min = colNameMin.nextElement();
		        	 String max = colNameMax.nextElement();
		        	 row = strTableName +","+name+","+htblColNameType.get(name)+","+isPrimaryKey(name,strClusteringKeyColumn)+","+"null"+",null,"+htblColNameMin.get(min)+","+htblColNameMax.get(max)+"\n";
					 metaDataFile.write(row);
		        }
				
		        metaDataFile.close();
		        try {
		        	//here we serialize created table
		        	table t = new table(strTableName,strClusteringKeyColumn); 
		            FileOutputStream fileOut = new FileOutputStream(strTableName+".ser");
		            ObjectOutputStream out = new ObjectOutputStream(fileOut);
		            out.writeObject(t);
		            out.close();
		            fileOut.close();
		         } catch (IOException i) {
		            i.printStackTrace();
		         }
		       
		}
		else {
			
			throw new DBAppException ("This table already exists");	
		}
			}
	
	public void insertIntoTable(String strTableName,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException, IOException, ParseException{
		
		if(recValidate(strTableName,htblColNameValue)) {
			  Vector<Object> data =new Vector<Object>();
			  row tuple = new row(htblColNameValue,strTableName);
			  tuple.setPrimaryKey(getPrimaryKey(strTableName,htblColNameValue));
			  int maxCountInPage = readConfig()[0];
	          table table = getTable(strTableName); //deserializing table to retrieve from memory
	          
	         //First Tuple Ever
	          if(table.getPageCounter()==0) {
	        	 // System.out.print("");
	        	  page p =new page();
	          	  Vector<row> rows = p.getRows();
	          	  rows.add(tuple);
	          	  p.setRows(rows);
	          	  //table.setpagecounter(table.getPageCounter()+1);
	          	  p.setPath("src/main/resources/"+strTableName+"page"+table.getPageCounter()+".ser"); //mi us 1
	          	  table.setpagecounter(table.getPageCounter()+1); //chng
	              //System.out.println(table.getPageCounter());
	          	  p.setRowsNumber(1);
	          	  table.getPages().add(p.getPath());
                  writePage(p);	  
                  writeTable(table);

	          }
	          //Not First Tuple Ever:
	          else {
	        	  page p = binaryPage(getPrimaryKey(strTableName,htblColNameValue), table.getPages());
	        	  row r = binaryTuple(getPrimaryKey(strTableName,htblColNameValue), p.getRows());
	        	  int rowindex = rowIndex(p.getRows(), r);
	        	  if(!(compareObj(r.getPrimaryKey(),tuple.getPrimaryKey())==0)) {
	        	  int pageindex = pageIndex(table.getPages(), p);
	        	  //If Target Page Has Space:
	        	  if(p.getRowsNumber()<maxCountInPage) {
	        		   // System.out.print("2");
	                    Vector<row> newPageTuples= new Vector<row>();
	           		    //A-it is the Smallest tuple in target page
	           		    if(compareObj((tuple.getPrimaryKey()),(p.getRows().get(0).getPrimaryKey()))==-1) {
	           			 newPageTuples.add(tuple); //adding new tuple
	               		 for(int j=0; j<p.getRowsNumber(); j++) { //shifting the rest of the tuples
	               			 newPageTuples.add(p.getRows().get(j));}
	           		    }	
	           		    //B-It is not the smallest tuple in target page
	               	     else { 
	                        for(int i=0; i<=rowindex; i++) { //adding unchanged page tuples first
	           			   newPageTuples.add(p.getRows().get(i));}
	           		     newPageTuples.add(tuple); //adding new tuple
	           		     for(int j=rowindex+1; j<p.getRows().size(); j++) { //shifting the rest of the tuples
	           			 newPageTuples.add(p.getRows().get(j));}
	           		     }
	           		     p.setRows(newPageTuples); //updating page with change
	           		     int incrRowsNumber = p.getRowsNumber()+1;
	         
	           		     p.setRowsNumber(incrRowsNumber); //incrementing 
	           		     //serializing p
	           		     writePage(p);
	           		     writeTable(table);
	        	  }
	        	  //Target Page Does Not Have Space
	        	  else {
	        		  page lastPage= getPageFromPath("src/main/resources/"+strTableName+"page"+(table.getPageCounter()-1)+".ser");
	        		  // System.out.print(table.getPageCounter());
	        		 
	        		  //if last Page not full, shift all
	        		  if(lastPage.getRowsNumber()<maxCountInPage) {
	        			     //lastpage
	                         
			 	          	 Vector<row> NLProws = new Vector<row>();
			 	          	 page beflastPage= getPageFromPath("src/main/resources/"+strTableName+"page"+(table.getPageCounter()-2)+".ser");
			 	          	 int  max=beflastPage.getRowsNumber()-1;
			 	          	 row beflastrow =  beflastPage.getRows().get(max);
			 	          	 NLProws.add(beflastrow);
			 	          	 for(int i=0;i<lastPage.getRowsNumber();i++) {
			 	          		NLProws.add(lastPage.getRows().get(i));
			 	          	 }
			 	          	 lastPage.setRows(NLProws);
			 	          	 int incr=lastPage.getRowsNumber()+1;
			 	          	 lastPage.setRowsNumber(incr);
			 	          	 writePage(lastPage);
			 	          	 
			 	          	 //shift everyth one step down
			 	          	 for(int i =table.getPageCounter()-2;i>pageindex;i--) {
			                	 //System.out.print("hey");
			                	 page currPage = getPageFromPath("src/main/resources/"+strTableName+"page"+(i)+".ser"); //minus 1
			 	 	          	  Vector<row> rowsCurr = new Vector<row>();
			 	 	          	  row last1 =getPageFromPath("src/main/resources/"+strTableName+"page"+(i-1)+".ser").getRows().get(maxCountInPage-1);
			                      rowsCurr.add(last1);
			 	 	              for(int j =0;j<currPage.getRowsNumber()-1;j++) {
			 	 	          		rowsCurr.add(currPage.getRows().get(j));}
			 	 	              currPage.setRows(rowsCurr);
			                    //  if(lastPage==currPage) {
			                    //	  currPage.setRowsNumber(currPage.getRowsNumber()+1);
			                    //  }
			                      currPage.setRows(rowsCurr);
			                      writePage(currPage);
			                  }
			 	             //updating target page 
					         
			        		 Vector<row> newPageTuples= new Vector<row>();
			        		 //A-it is the Smallest tuple in target page
			        		 if(compareObj((tuple.getPrimaryKey()),(p.getRows().get(0).getPrimaryKey()))==-1) { 
			        			 newPageTuples.add(tuple); //adding new tuple
			            		 for(int j=0; j<maxCountInPage-1; j++) { //shifting the rest of the tuples
			            			 newPageTuples.add(p.getRows().get(j));}
			        		 }	
			        		 //B-It is not the smallest tuple in target page
			            	 else { 
			                     for(int i=0; i<=rowindex; i++) { //adding unchanged page tuples first
			        			   newPageTuples.add(p.getRows().get(i));}
			        		     newPageTuples.add(tuple); //adding new tuple
			        		     for(int j=rowindex+1; j<p.getRows().size()-1; j++) { //shifting the rest of the tuples
			        			 newPageTuples.add(p.getRows().get(j));}
			        		 }
			        		  p.setRows(newPageTuples);
				              writePage(p);
				              writeTable(table);
		                    	
		                    	
		                    	
		                    
		        	  }
	        		  //if last page full, create new last page and shift all
		        	  else {
		        		 //System.out.print("4");
		        		 //creating new last page w adding to it akher haga f akher page
		        		 //law ana fel nos el target page
		        		 page newpage =new page();
		 	          	 Vector<row> rows = newpage.getRows();
		 	          	 row lastrow =  lastPage.getRows().get(maxCountInPage-1);
		 	          	 if(compareObj(lastrow.getPrimaryKey(),tuple.getPrimaryKey())==1){
		 	          	 rows.add(lastrow);
		 	          	 newpage.setRows(rows);
		 	          	 newpage.setPath("src/main/resources/"+strTableName+"page"+(table.getPageCounter())+".ser");
		 	          	 newpage.setRowsNumber(1);
		 	          	 table.getPages().add(newpage.getPath());
		 	          	 table.setpagecounter(table.getPageCounter()+1);
		 	          	 writePage(newpage);
		 	          	 //shifting ba2eet el hagat khatwa l taht
		 	          	// System.out.print(table.getPageCounter());
		 	          	 for(int i =table.getPageCounter()-2;i>pageindex;i--) {
		                	 //System.out.print("hey");
		                	 page currPage = getPageFromPath("src/main/resources/"+strTableName+"page"+(i)+".ser"); //minus 1
		 	 	          	  Vector<row> rowsCurr = new Vector<row>();
		 	 	          	  row last1 =getPageFromPath("src/main/resources/"+strTableName+"page"+(i-1)+".ser").getRows().get(maxCountInPage-1);
		                      rowsCurr.add(last1);
		 	 	              for(int j =0;j<currPage.getRowsNumber()-1;j++) {
		 	 	          		rowsCurr.add(currPage.getRows().get(j));}
		 	 	              currPage.setRows(rowsCurr);
		                    //  if(lastPage==currPage) {
		                    //	  currPage.setRowsNumber(currPage.getRowsNumber()+1);
		                    //  }
		                      currPage.setRows(rowsCurr);
		                      writePage(currPage);
		                  }
		                  //updating target page 
		         
		        		 Vector<row> newPageTuples= new Vector<row>();
		        		 //A-it is the Smallest tuple in target page
		        		 if(compareObj((tuple.getPrimaryKey()),(p.getRows().get(0).getPrimaryKey()))==-1) { 
		        			 newPageTuples.add(tuple); //adding new tuple
		            		 for(int j=0; j<maxCountInPage-1; j++) { //shifting the rest of the tuples
		            			 newPageTuples.add(p.getRows().get(j));}
		        		 }	
		        		 //B-It is not the smallest tuple in target page
		            	 else { 
		                     for(int i=0; i<=rowindex; i++) { //adding unchanged page tuples first
		        			   newPageTuples.add(p.getRows().get(i));}
		        		     newPageTuples.add(tuple); //adding new tuple
		        		     for(int j=rowindex+1; j<p.getRows().size()-1; j++) { //shifting the rest of the tuples
		        			 newPageTuples.add(p.getRows().get(j));}
		        		 }
		        		  p.setRows(newPageTuples);
			              writePage(p);
			              writeTable(table);}
			              //law ana akher haga ana shakhseyan
		 	          	 else {
		 	          		 rows.add(tuple);
		 	          		// System.out.print(tuple.getPrimaryKey());
			 	          	 newpage.setRows(rows);
			 	          	 newpage.setPath("src/main/resources/"+strTableName+"page"+(table.getPageCounter())+".ser");
			 	          	 newpage.setRowsNumber(1);
			 	          	 table.getPages().add(newpage.getPath());
			 	          	 table.setpagecounter(table.getPageCounter()+1);
			 	          	 writePage(newpage);
			 	          	 //shifting ba2eet el hagat khatwa l taht
			                 writeTable(table);
		 	          	 }
		        	  }
	        	  }
	        	 String col1,col2,col3,ref;
	        	 Object col1O,col2O,col3O;
	        	// ArrayList<Octree> newTrees=new ArrayList<Octree>();
                for(int j=0; j<table.getOctrees().size();j++) {
                	col1=table.getOctrees().get(j).col1;
                	col2=table.getOctrees().get(j).col2;
                	col3=table.getOctrees().get(j).col3;
                	col1O=tuple.getData().get(col1);
                	col2O=tuple.getData().get(col2);
                	col3O=tuple.getData().get(col3);
                	page page= binaryPage(getPrimaryKey(strTableName,tuple.getData()), table.getPages());//write after
                	ref=page.getPath();
                	table.getOctrees().get(j).insert(col1O, col2O, col3O, ref);
                	writePage(page);
	        	  }
	        	  writeTable(table);
	        	  
	          }
	        	  else{
	        		  throw new DBAppException ("You cannot insert duplicate keys");
	        	  }}
	       
	  	}
	  		
	  		else {
	  			throw new DBAppException ("This record is not valid to insert");
	  		}
		
	}

	public int pageIndex(Vector<String> pages, page targetPage) {
		for(int i=0; i<pages.size();i++) {
			if (pages.get(i).equals(targetPage.getPath())) {
				return i;
			}
		}
		return 0;
	}
	
	public int rowIndex(Vector<row> rows, row row) {
		for(int i=0; i<rows.size();i++) {
			if (rows.get(i)==row) {
				return i;
			}
		}
		return 0;
	}
	
    public void writePage(page p) throws IOException {
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(p.getPath());
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(p);
	        out.close();
	        fileOut.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeTable(table t) throws IOException {
		    String strTableName=t.getTableName();
		    FileOutputStream fileOut = new FileOutputStream(strTableName+".ser");
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(t);
	        out.close();
	        fileOut.close();
	}
	
	public boolean recValidate(String tableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ParseException {
	    boolean tableExist= false;
		BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
		String colRead;
		ArrayList<String[]> colSave = new ArrayList <>() ;
		//Looping on file to check if table exists
		
		while((colRead= br.readLine() )!=null) {
			String [] colCurrent= colRead.split(",");
			if(colCurrent[0].equals(tableName)) {
				tableExist= true;
				colSave.add(colCurrent);
			}
		}
		br.close();
		if(!tableExist) {
			
			return false;
	     } else {
		boolean hasPrimaryKey=false;
		String [] colCurrent;
		String primeCol = null;
		//Loop on metadata columns, get which column is primary
		for (int counter = 0; counter < colSave.size(); counter++) { 		    
			  colCurrent = colSave.get(counter);

			  if (colCurrent[3].equals("true")) {
				  primeCol= colCurrent[1];
			  }  
			  
	      } 
		//Loop on HashTable, check if primary column has a value
		Enumeration<String> e = htblColNameValue.keys();

        while (e.hasMoreElements()) {
            String key = e.nextElement();
        
            if(key.equals(primeCol)) {
            	
            	if(htblColNameValue.get(key)!=null) {
            		hasPrimaryKey=true;
            	}
            }
        }
        if (hasPrimaryKey==false) {
        	return false;
        }
        else {
            // Checking range (Min and Max) and Data Types
        	boolean dataTypes=true;
        	boolean corrRange=true;
        	String currDataType=null;
        	String currMin=null;
        	String currMax=null;
        	for (int i = 0; i < colSave.size(); i++) { 		    
				 currDataType= (colSave.get(i))[2]; //retrieve i'th column data type
				 currMin= (colSave.get(i))[6]; //retrieve i'th column data type
				 currMax= (colSave.get(i))[7]; //retrieve i'th column data type
				 Set<String> setOfKeys = htblColNameValue.keySet();
            	

                 for (String key : setOfKeys) {
                	

	                 if(key.equals((colSave.get(i))[1])) {
	                	

	                	 switch (currDataType) { 
	                	 case "java.lang.Integer":{
	                		 if(!(htblColNameValue.get(key).getClass().getName().equals("java.lang.Integer"))) {
	                        	 

	                			 dataTypes=false;
	    	                	 break;

	                		 }

	                		if((Integer)htblColNameValue.get(key)>Integer.parseInt(currMax)||(Integer)htblColNameValue.get(key)<Integer.parseInt(currMin) ) {
	                        	

	                			 corrRange=false;
	                		 }
	                	 }
	                	 break;
	                	 case "java.lang.String":{
	                		 if(!(htblColNameValue.get(key).getClass().getName().equals("java.lang.String"))) {
	                        	

	                			 dataTypes=false;
	    	                	 break;

	                		 }
	                		 if(((( (String) htblColNameValue.get(key)).compareTo(currMin)==1)&&(((String)htblColNameValue.get(key)).compareTo(currMax)==-1)))
	                	     {
	                        	

	                			 corrRange=false;
	                		 }
	                	 }
	                	 break;
                    	 case "java.lang.Double":{

	                		 if(!(htblColNameValue.get(key).getClass().getName().equals("java.lang.Double"))) {
	                        	

	                			 dataTypes=false;
	    	                	 break;

	                		 }
                        	

	                		if((Double)htblColNameValue.get(key)>Double.parseDouble(currMax)||(Double)htblColNameValue.get(key)<Double.parseDouble(currMin) ) {
	                        	 System.out.println((Double)htblColNameValue.get(key)>Double.parseDouble(currMax));

	                			 corrRange=false;
	                		 }
	                	 }
	                	 break;
	                	 case "java.util.Date":{
	                		
	                		 if(!(htblColNameValue.get(key).getClass().getName().equals("java.util.Date"))) {

	                			 dataTypes=false;
	    	                	 break;

	                		 }
	                		 DateFormat format= new SimpleDateFormat("yyy-MM-dd");
	                		 if(!((((Date) htblColNameValue.get(key)).compareTo(format.parse(currMin))>=0)&&(((Date)htblColNameValue.get(key)).compareTo(format.parse(currMax))<=0))) {
	                			 corrRange=false;
	                		 }
	                	 }

	                	 }
	                 }
			        }
				 
		      if(dataTypes==false) {
				return false; }
		      if (corrRange==false) {
		        return false;
		     }
        	}
        }
		
	    }
 return true;
	
}
	
	public boolean isPrimaryKey(String colname, String primaryk ) {
		return colname.equals(primaryk);
	}
	
	public void createPage(String strTableName) { 

        
		  try {
			    FileInputStream fileintable = new FileInputStream(strTableName+".ser");
	    	    ObjectInputStream intable = new ObjectInputStream(fileintable);
	            table t = (table)intable.readObject();
	            intable.close();
	            fileintable.close();
			    page p =new page();
	            FileOutputStream fileOut = new FileOutputStream("page"+t.getPageCounter()+".ser");
	            ObjectOutputStream out = new ObjectOutputStream(fileOut);
	            out.writeObject(p);
	            out.close();
	            fileOut.close();
	            t.setpagecounter(t.getPageCounter()+1);;
	         } catch (IOException i) {
	            i.printStackTrace();
	         }
		  catch (ClassNotFoundException c) {
		         System.out.println("table class not found");
		         c.printStackTrace();
		      }
//		  catch (IOException i) {
//	            i.printStackTrace();
//	         }
	}
	
	public Object getPrimaryKey(String strTableName,Hashtable<String,Object> htblColNameValue) {
		table ourTable =getTable(strTableName);
		Object pKey=null;
		for (String i : htblColNameValue.keySet()) {
		     if(ourTable.getPrimarykey().equals(i)) {
		    	 pKey=htblColNameValue.get(i);
		     }
		}
		return pKey;
	}

	public static table getTable(String strTableName) {

		table table =null;
		//We deserialize table to retrieve from memory
		try { FileInputStream fileInTable = new FileInputStream(strTableName+".ser");
   	         ObjectInputStream inTable = new ObjectInputStream(fileInTable);
             table = (table)inTable.readObject();
             inTable.close();
             fileInTable.close();
       	
		}
		 catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("table class not found");
	         c.printStackTrace();
	      }
		return table ;
	}
	
	public static  page getPageFromPath(String path) {
		page p=null;
	      try {
	    	  
	         FileInputStream fileIn = new FileInputStream(path); //deserialize page
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         p = (page) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("table class not found");
	         c.printStackTrace();
	      }
	      
	   
		return p;
	}

	public  page binaryPage( Object primaryKey, Vector <String> V ) {

        int low = 0;
        int high = V.size() - 1;

        while (low <= high) {
            int mid = ((low + high) / 2);
            page midPage= getPageFromPath(V.get(mid));   
           // System.out.print(((midPage.getRows()).get(0)).getPrimaryKey()+"\n");
            int incr=midPage.getRowsNumber()-1;
            if (((compareObj(((midPage.getRows()).get(0)).getPrimaryKey(),primaryKey)==-1)&&(compareObj(((midPage.getRows()).get(incr)).getPrimaryKey(),primaryKey)==1))||(midPage.getRowsNumber()==1)||(compareObj(((midPage.getRows()).get(0)).getPrimaryKey(),primaryKey)==0)||(compareObj(((midPage.getRows()).get(incr)).getPrimaryKey(),primaryKey)==0)) { 
            	//System.out.print("edkhol hena");
            	return getPageFromPath(V.get(mid));}
           else {
        	   page lastPage=getPageFromPath(V.get(V.size()-1));
        	   if(midPage.getPath().equals(lastPage.getPath())) {
        		   return midPage;
        	  
        	  }
        	   else{
        		   int nextPageIdx=mid+1;
                   page nxtPage= getPageFromPath(V.get(nextPageIdx));  
            	   if((compareObj(((nxtPage.getRows()).get(0)).getPrimaryKey(),primaryKey)==1)&&(compareObj(((midPage.getRows()).get(incr)).getPrimaryKey(),primaryKey)==-1) ){
            	     if(midPage.getRowsNumber()==readConfig()[0]) {
            		   return nxtPage;
            	     }
            	     else {
            	    	 return midPage;
            	     }
                    }
        	   }
        	  
	            	  if((compareObj(((midPage.getRows()).get(0)).getPrimaryKey(),primaryKey)==-1)) {
	            		low= mid+1;
	            	  }
	            	  else { 
	            		high = mid - 1;}
	            
	            }
        }

        return getPageFromPath(V.get(0));
    }
   
	public  row binaryTuple(  Object primaryKey,Vector <row> pageRows) { //should return the row to insert after
        int low = 0;
        int high = pageRows.size() -1;
        row last=pageRows.get(pageRows.size()-1);
        while (low <= high) {
            int mid = (low + high) / 2;
            row midRow= pageRows.get(mid);   
            if(last==midRow) {
            	return pageRows.get(mid);
            }else {
            if (((compareObj(midRow.getPrimaryKey(),primaryKey)==-1)&&(compareObj( pageRows.get(mid+1).getPrimaryKey(),primaryKey)==1))||(compareObj(midRow.getPrimaryKey(),primaryKey)==0)) { 
            	return pageRows.get(mid);}
           else { 
            	if((compareObj(midRow.getPrimaryKey(),primaryKey)==-1)) {
            		low= mid+1;
            	}else { 
            		high = mid - 1;}
            
            }}
        }


        return pageRows.get(0);
    }
	
	public static List<String> getCommonStrings(ArrayList<List<String>> lists) {
        if (lists == null || lists.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> commonStrings = new ArrayList<>(lists.get(0));

        for (int i = 1; i < lists.size(); i++) {
            commonStrings.retainAll(lists.get(i));
        }

        return commonStrings;
    }
   
	public static int compareObj(Object o1, Object o2) {
		 String currDataType= o1.getClass().getName();
		 int result ;
		 switch (currDataType) { 
  	 case "java.lang.Integer":{
  		 if((int)o1==(int)o2) {
  			result = 0;
  		 }
  		 else if ((int)o1<(int)o2) {
  				 result= -1;
  		 }
  		 else {
  			 result =  1; }
  		 break;
  	 }
  	 case "java.lang.String":{
  			 result = ((String) o1).compareTo((String)o2);    		
  		 break;
  	 }
  	 case "java.lang.Double":{
  		 if(Double.compare((Double)o1, (Double)o2) == 0) {
   			result = 0;
   		 }
   		 else if ((Double)o1<(Double)o2) {
   				 result= -1;
   		 }
   		 else {
   			 result =  1; }
   		 break;
  	 }
  	 case "java.util.Date":{
             result =((Date) o1).compareTo((Date)o2);
  		break;
  	 }
  	 default : result =0;
  	 break;
  	 }
		return result;
	}
    
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue )
			throws DBAppException, IOException, ArrayIndexOutOfBoundsException, ParseException
			{
//we first get table to search for page in it
table table= getTable(strTableName);
//String PKCol=table.getPrimarykey();
if(table.getPageCounter()==0) {return;}
String type= getPageFromPath(table.getPages().get(0)).getRows().get(0).getPrimaryKey().getClass().getName();
Object Key= typeCast(strClusteringKeyValue,type);
//search for page (binary) and get it
boolean tableExist= false;
BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
String colRead;
ArrayList<String[]> colSave = new ArrayList <>() ;
//row rowOld=null;
//Looping on file to check if table exists

while((colRead= br.readLine() )!=null) {
	String [] colCurrent= colRead.split(",");
	if(colCurrent[0].equals(strTableName)) {
		tableExist= true;
		colSave.add(colCurrent);
	}
}
br.close();
if(!tableExist) {
	
	throw new DBAppException("There is no such table");
 }
page page=null;
Hashtable<Octree, ArrayList<String>> isIndexed=isIndexed(table,htblColNameValue);
row row=null;
page= binaryPage(Key,table.getPages())	;

//search for key (binary search) within page
row = binaryTuple(Key, page.getRows() );
Hashtable<String, Object> rowOld=row.getData();
if(compareObj(row.getPrimaryKey(),Key)==0) {
//update row with new value:
htblColNameValue.put(table.getPrimarykey(),  Key );
Vector<Object> data =new Vector<Object>();//updated row data
//data.add(strClusteringKeyValue); //adding primary key

Hashtable<String,Object> oldRowData=row.getData();
Vector<row> rows= page.getRows();
int rowIndex= rowIndex(rows,row);
row.setData(htblColNameValue);

//update page with updated row
rows.set(rowIndex, row);
page.setRows(rows);

//serialize page 
FileOutputStream fileOut = new FileOutputStream(page.getPath());
ObjectOutputStream out = new ObjectOutputStream(fileOut);
out.writeObject(page);
out.close();
fileOut.close();




//To Update Octree(s):
//1- get xyz of old row
Vector<Octree> newtrees=new Vector<Octree>();
Enumeration<Octree> cols2= isIndexed.keys();
while (cols2.hasMoreElements()) {
	Octree current=cols2.nextElement();
	Object col1Val, col2Val, col3Val,col1Val2, col2Val2, col3Val2;
	col1Val=rowOld.get(current.col1);
	col2Val=rowOld.get(current.col2);
	col3Val=rowOld.get(current.col3);
	//2- remove old row from octree
	current.remove(col1Val, col2Val, col3Val);
	//3- add new row to octree
	//String type1=GetType(current.col1,table),type2=GetType(current.col2,table),type3=GetType(current.col3,table);
	//Object v1=row.getData().get(current.col1),v2=row.getData().get(current.col2),v3=row.getData().get(current.col3);
	col1Val2=row.getData().get(current.col1);
	col2Val2=row.getData().get(current.col2);
	col3Val2=row.getData().get(current.col3);
	
	current.insert(col1Val2, col2Val2, col3Val2, page.getPath());
	newtrees.add(current);
}
table.setOctrees(newtrees);
//serialize table
FileOutputStream fileOut2 = new FileOutputStream(strTableName+".ser");
ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
out2.writeObject(table);
out2.close();
fileOut2.close();
}
else {
	throw new DBAppException ("This tuple does not exist");
}}

    private String GetType(String col, table table) throws IOException {
    	BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
		String colRead;
		//ArrayList<String[]> colSave = new ArrayList <>() ;
		//Looping on file to check if table exists
		
		while((colRead= br.readLine() )!=null) {
			String [] colCurrent= colRead.split(",");
			if(colCurrent[0].equals(table.getTableName())) {
				if(colCurrent[1].equals(col)) {
					return colCurrent[2];
				}
					
			}
		}
		br.close();
		return colRead;
		
		
	}

	private List<String> OctSearch(Octree nextElement, Hashtable<String, Object> htblColNameValue,table table,String opx, String opy,String opz) {
		List<String> PagePaths= new ArrayList<String>();
		List<page> Pages= new ArrayList<page>();
		String co1=nextElement.col1;
		String co2=nextElement.col2;
		String co3=nextElement.col3;
		if(htblColNameValue.containsKey(co1)&&htblColNameValue.containsKey(co2)&&htblColNameValue.containsKey(co3)) {
			Object call1=htblColNameValue.get(co1),call2=htblColNameValue.get(co2),call3=htblColNameValue.get(co3);
            PagePaths= nextElement.searchElements(nextElement.getRoot(), call1, call2, call3,opx,opy,opz);
		}else {
			PagePaths=table.getPages();
		}
		 HashSet<String> set = new HashSet<>(PagePaths);
	     Vector<String> pages = new Vector<>(set);
		return  pages;
	}

	private Hashtable<Octree,ArrayList<String>> isIndexed(table table, Hashtable<String, Object> htblColNameValue) { //returns hashtable of all indexed columns in our input and their corresponsing trees
    	 Enumeration<String> colName= htblColNameValue.keys();
    	 ArrayList<String> colList = new ArrayList<>();
         while (colName.hasMoreElements()) {
             colList.add(colName.nextElement());
         }
    	 Hashtable<Octree,ArrayList<String>> colIndexed= new Hashtable<Octree,ArrayList<String>>();
    	 Vector<Octree> tableTrees= table.getOctrees();
    	 for(int i=0; i<tableTrees.size();i++){
    	    Octree use=tableTrees.get(i);
    	    colIndexed.put(use,new ArrayList<String>());
    		for(int j=0; j<colList.size();j++) {
            String col= colList.get(j);
    	    		if((use.col1).equals(col) || (use.col2).equals(col) || (use.col3).equals(col)) {
    	    			ArrayList<String> temp=colIndexed.get(use);
    	    			temp.add(col);
    	    			colIndexed.put(use,temp);
    	    }
	
    	 }}
		return colIndexed;
	}

	public 	Object typeCast(String key, String type) throws ParseException {
	
	 Object result ;
	 switch (type) { 
	 case "java.lang.Integer":{
		 result= Integer.parseInt(key); 
		 break;
	 }
	 case "java.lang.String":{
		result=	   key;  		
		 break;
	 }
	 case "java.lang.Double":{
		 result= Double.parseDouble(key);
		 break;
	 }
	 case "java.util.Date":{
       result= new SimpleDateFormat("dd/MM/yyyy").parse(key);
		break;
	 }
	 default : result =key;
	 break;
	 }
	return (Object)result;
}
	
    public void shifting(page p , table t) throws DBAppException, IOException {
		int pageindex =t.getPages().indexOf(p.getPath());
		String path ;
		page newpage;
		row firstrow ;
		for(int i =pageindex ;i<t.getPages().size() ;i++) {
			path=t.getPages().get(i);
			newpage = getPageFromPath(path);
			firstrow = newpage.getRows().get(0);
			p.getRows().add(firstrow);
			newpage.getRows().remove(0);
			if(newpage.getRows().isEmpty()) {
				t.getPages().remove(newpage);
			}
			//desrializepage(p);
			p=newpage;
			
		}
	}
  
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
    	table table = getTable(strTableName);
		Collection<Object> values = htblColNameValue.values();

		Set<String> keys = htblColNameValue.keySet();
		int numberOfPage = table.getPageCounter();
		
		Vector<Octree> octree = table.getOctrees();
		ArrayList<ArrayList<String>> indeces = new ArrayList<ArrayList<String>>();
		for(int i=0;i<octree.size();i++) {
			ArrayList<String> indeces2 = new ArrayList<String>();
			indeces2.add(octree.get(i).col1);
			indeces2.add(octree.get(i).col2);
			indeces2.add(octree.get(i).col3);
			indeces.add(indeces2);
		}
	
		ArrayList<Integer> indexForOctree = new ArrayList<Integer>();
		if(octree.size()!=0&&htblColNameValue.size()>=3) {
			for(int i=0;i<indeces.size();i++) {
				boolean thisOctree = false;

		        String col1 = octree.get(i).col1;
		        String col2 = octree.get(i).col2;
		        String col3 = octree.get(i).col3;

		        if(htblColNameValue.containsKey(col1)&&htblColNameValue.containsKey(col2)&&htblColNameValue.containsKey(col3)) {
		        	thisOctree =true;
		        }
		        	if(thisOctree){
		        
		        indexForOctree.add(i);
		        }
			}
			ArrayList<List<String>> finalPages = new ArrayList<List<String>>();
			for(int i=0;i<indexForOctree.size();i++) {
				finalPages.add(OctSearch(octree.get(indexForOctree.get(i)), htblColNameValue,table,"=" ,"=","="));
			    String col1 = octree.get(indexForOctree.get(i)).col1;
			    String col2 = octree.get(indexForOctree.get(i)).col2;
			    String col3 = octree.get(indexForOctree.get(i)).col3;
				octree.get(indexForOctree.get(i)).remove(htblColNameValue.get(col1),htblColNameValue.get(col2),htblColNameValue.get(col3));
			}
			 List<String> commonStrings = new ArrayList<>();
			 commonStrings=getCommonStrings(finalPages);
			
				for (int i = commonStrings.size()-1; i >= 0; i--) {
					page page = getPageFromPath(commonStrings.get(i));
					for (int j = page.getRowsNumber() - 1; j >= 0; j--) {
						boolean equal = true;
						Hashtable<String,Object> d =page.getRows().get(j).getData();
						
						for (String key : d.keySet()) {
				            if (htblColNameValue.containsKey(key)) {
				                Object value1 = htblColNameValue.get(key);
				                Object value2 = d.get(key);
				                if (!value1.equals(value2)) {
				                	equal = false;
				                }
				            } 
				        }
						
						if (equal) {
							
					        page.getRows().remove(j);
					        page.setRowsNumber(page.getRowsNumber() - 1);
					    }
						if (page.getRowsNumber() == 0) {
							table.getPages().remove(page.getPath());
							table.setpagecounter(table.getPageCounter() - 1);
						}	
					}
					
					writePage(page);
				}	
				writeTable(table);
			
			
		}
		else {
			
		
		
		for (int i = numberOfPage-1; i >= 0; i--) {
			page page = getPageFromPath(table.getPages().get(i));
			for (int j = getPageFromPath(table.getPages().get(i)).getRowsNumber() - 1; j >= 0; j--) {
				boolean equal = true;
				Hashtable<String,Object> d =page.getRows().get(j).getData();
				
				for (String key : d.keySet()) {
		            if (htblColNameValue.containsKey(key)) {
		                Object value1 = htblColNameValue.get(key);
		                Object value2 = d.get(key);
		                if (!value1.equals(value2)) {
		                	equal = false;
		                }
		            } 
		        }
				
				if (equal) {
				
					  for(int z =0 ;z<table.getOctrees().size();z++) {
						    String col1 = octree.get(z).col1;
						    String col2 = octree.get(z).col2;
						    String col3 = octree.get(z).col3;
						    Object o1=page.getRows().get(j).getData().get(col1);
						    Object o2=page.getRows().get(j).getData().get(col2);
						    Object o3=page.getRows().get(j).getData().get(col3);
						    octree.get(z).remove(o1,o2,o3);
				        }
			        page.getRows().remove(j);
			        page.setRowsNumber(page.getRowsNumber() - 1);
			      
			    }
				if (page.getRowsNumber() == 0) {
					table.getPages().remove(page.getPath());
					table.setpagecounter(table.getPageCounter() - 1);
				}	
			}
			
			writePage(page);
		}	
		writeTable(table);
		}
    }
    
	private int[] readConfig() {
	        Properties prop = new Properties();
	        String filePath = "src/main/resources/DBApp.config";
	        
	        InputStream is = null;
	        try {
	            is = new FileInputStream(filePath);
	        } catch (FileNotFoundException ex) {
	            ex.printStackTrace();
	        }
	        try {
	            prop.load(is);
	        } catch (IOException ex) {
	            ex.printStackTrace();
	        }
	        int[] arr = new int[2];
	        arr[0] = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
	        arr[1] = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
	        return arr;
	    }	
		
    public static void printTableContents(String tableName) {
    	 table table= getTable(tableName); //deserializes table
    	 for(int i=0; i<table.getPageCounter(); i++) {
    		 page p= getPageFromPath(table.getPages().get(i));
    		 System.out.printf("\n \nPage "+i+ " contents are: \n");
    		 for(int j=0; j<p.getRows().size();j++) {
    			 row r=p.getRows().get(j);
    			 System.out.print("\n");
    			 Enumeration<String> colNames= r.getData().keys();
    			 while (colNames.hasMoreElements()) {
 		        	//Here we fill in MetaDataFile with table's columns
 		        	 String name = colNames.nextElement();
 		        	 System.out.print(name+": "+r.getData().get(name)+" ");
 		        }
    		 }
    	 }
    	  
    	}
    
    public void createIndex(String strTableName,
    String[] strarrColName) throws DBAppException, IOException, ParseException{
    	boolean tableExist= false;
    	BufferedReader br2= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
    	String colRead2;
    	ArrayList<String[]> colSave = new ArrayList <>() ;
    	//Looping on file to check if table exists

    	while((colRead2= br2.readLine() )!=null) {
    		String [] colCurrent= colRead2.split(",");
    		if(colCurrent[0].equals(strTableName)) {
    			tableExist= true;
    			colSave.add(colCurrent);
    		}
    	}
    	br2.close();
    if(!tableExist) {
    		throw new DBAppException("There is no such table");
    	 }else {
	 if(strarrColName.length==3) { //no octree done on common columns
		   table table=getTable(strTableName); 
		    boolean alreadyIndexed=false;
		    for(int i=0; i<table.getOctrees().size();i++) {
		    	if(((table.getOctrees().get(i).col1).equals(strarrColName[0]))||
		    			((table.getOctrees().get(i).col2).equals(strarrColName[0])) ||
		    			((table.getOctrees().get(i).col3).equals(strarrColName[0])) ||
		    			((table.getOctrees().get(i).col1).equals(strarrColName[1])) ||
		    			((table.getOctrees().get(i).col2).equals(strarrColName[1])) ||
		    			((table.getOctrees().get(i).col3).equals(strarrColName[1])) ||
		    			((table.getOctrees().get(i).col1).equals(strarrColName[2])) ||
		    			((table.getOctrees().get(i).col2).equals(strarrColName[2])) ||
		    			((table.getOctrees().get(i).col3).equals(strarrColName[2]))
		    			) {
		    		alreadyIndexed=true;
		    	}
		    }
		    if(alreadyIndexed) {
		    	throw new DBAppException("At least one of these columns is already Indexed");
		    }
			BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
			String col1Max,col1Min,col2Max,col2Min,col3Max,col3Min,currDataType;
			Object col1MaxO = null,col1MinO = null,col2MaxO = null,col2MinO = null,col3MaxO = null,col3MinO = null;
			//Looping to get column max and min for each col
			String colRead;
			while((colRead= br.readLine() )!=null) {
				
				String [] colCurrent= colRead.split(",");
				currDataType=colCurrent[2];
				if(colCurrent[0].equals(strTableName)) {
			      if(colCurrent[1].equals(strarrColName[0])) {
			    	  col1Max=colCurrent[7];
			    	  col1Min=colCurrent[6];
			    	  col1MinO=typeCast(col1Min,currDataType);
			    	  col1MaxO=typeCast(col1Max,currDataType);
			      }
			      if(colCurrent[1].equals(strarrColName[1])) {
			    	  col2Max=colCurrent[7];
			    	  col2Min=colCurrent[6];
			    	  col2MinO=typeCast(col2Min,currDataType);
			    	  col2MaxO=typeCast(col2Max,currDataType);
			      }
			      if(colCurrent[1].equals(strarrColName[2])) {
			    	  col3Max=colCurrent[7];
			    	  col3Min=colCurrent[6];
			    	  col3MinO=typeCast(col3Min,currDataType);
			    	  col3MaxO=typeCast(col3Max,currDataType);
			      }
				}
			}
			br.close();
		    Octree octree= new Octree(col1MinO,col1MaxO,col2MinO,col2MaxO,col3MinO,col3MaxO);
		    octree.col1= strarrColName[0];
		    octree.col2= strarrColName[1];
		    octree.col3= strarrColName[2];
		    Object XValue=null;
		    Object YValue=null;
		    Object ZValue=null;
		    String ref="";
		    for(int i=0;i<getTable(strTableName).getPages().size();i++) {
		    	page page=getPageFromPath(getTable(strTableName).getPages().get(i)); 
		    	for(int j=0;j<page.getRowsNumber();j++) {
		    		XValue=page.getRows().get(j).getData().get(strarrColName[0]);
		    		YValue=page.getRows().get(j).getData().get(strarrColName[1]);
		    		ZValue=page.getRows().get(j).getData().get(strarrColName[2]);
		    		ref= page.getPath();
		    		octree.insert(XValue, YValue, ZValue, ref);
		    }
		    	writePage(page);
		    }
		      
		    Vector<Octree> newOct= table.getOctrees();
		    newOct.add(octree);
		    table.setOctrees(newOct);
		    String IndexName= "OctreeIndex/"+OctreeIndex(newOct,octree);
		    updateMetaData(strTableName,strarrColName[0],IndexName);
		    updateMetaData(strTableName,strarrColName[1],IndexName);
		    updateMetaData(strTableName,strarrColName[2],IndexName);
		    writeTable(table);
	  }else {
		 throw new DBAppException("We need 3 columns to create an octree index");
	 }}
 }
 
    public void updateMetaData(String strTableName, String strarrColName, String indexName) throws IOException {
	 
	 
     ArrayList<String> newMetaLines= new  ArrayList<String>();
	 BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
	   String line=""; 
	   String newLine="";
	   while((line= br.readLine())!=null) {
		  if((line.split(",")[0].equals(strTableName))&&(line.split(",")[1].equals(strarrColName))) {
			  newLine=strTableName+","+strarrColName+","+line.split(",")[2]+","+line.split(",")[3]+","+indexName+",Octree,"+line.split(",")[6]+","+line.split(",")[7];
			  newMetaLines.add(newLine);
	  }else {
		  newMetaLines.add(line);
	  }
      	  
         }
	   br.close();
	   //The updated meta data file 
	   File newMetaDatafile = new File("src/main/resources/metaData.csv");
	      PrintWriter newMetaDataFile = new PrintWriter(newMetaDatafile);
	      String write;
	      for(int j=0; j<newMetaLines.size();j++) {
	    	  write=newMetaLines.get(j)+"\n";
	    	  newMetaDataFile.write(write);
	      }
	      
	      newMetaDataFile.close();

}

    public int OctreeIndex(Vector<Octree> v, Octree o) {
	 for(int i=0;i<v.size();i++) {
		 if(v.get(i)==o) {
			 return i;
		 }
	 }
	 return 0;
 }
 
    public boolean isValidOperator(String operator) {
     return operator.equals(">") ||
            operator.equals(">=") ||
            operator.equals("<") ||
            operator.equals("<=") ||
            operator.equals("!=") ||
            operator.equals("=");
 }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
      		 throws DBAppException, IOException{
      			//Loop on operators 
          	    ArrayList<Integer> wntdIdx= new ArrayList<Integer> ();
          	    ArrayList<Integer> wntdIdxSave= new ArrayList<Integer> ();
          	    ArrayList<Integer> ltrIdx= new ArrayList<Integer> ();
          	    int hashIdx=0;
          	    Hashtable<Integer,ArrayList<Integer>> andHandle= new  Hashtable<Integer,ArrayList<Integer>>();
          	    ArrayList<SQLTerm> allTerms=new  ArrayList<SQLTerm>();
          	    for(int i=0; i<arrSQLTerms.length;i++) {
          	    	allTerms.add(arrSQLTerms[i]);
          	    }
          	    //int counter=0;
          	    for(int i=0;i<strarrOperators.length;i++) {
          	    	if(strarrOperators[i].equals("AND")) {
          	    		wntdIdx.add(i);
          	    		wntdIdxSave.add(i);
          	    	}else {
          	    		if(wntdIdx.size()>=2) {
          	    			andHandle.put(hashIdx,wntdIdx);
          	    			hashIdx++;
          	    			ltrIdx.add(i);
          	    			wntdIdx= new ArrayList<Integer> ();
          	    		}else {
          	    			if(!wntdIdx.isEmpty()) {
          	    				ltrIdx.add(wntdIdx.get(0));}
          	    			ltrIdx.add(i);
          	    			if(i!=0&&wntdIdxSave.size()!=0) {
          	    			wntdIdxSave.remove(wntdIdxSave.size()-1);}
          	    			//counter=0;
          	    			wntdIdx= new ArrayList<Integer> ();
          	    		}
          	    	}}
          	    	if(wntdIdx.size()==1) {
          	    		ltrIdx.add(wntdIdx.get(0));
          	    		wntdIdx= new ArrayList<Integer> ();}
          	    	if(wntdIdx.size()>=2) {
          	    		andHandle.put(hashIdx,wntdIdx);
      	    			hashIdx++;
      	    			wntdIdx= new ArrayList<Integer> ();
          	    	}
          	    		
          	    	if(wntdIdxSave.size()==1)
          	    		wntdIdxSave= new ArrayList<Integer> ();
          	    
          	    //Prepare later list operators for later
          	    ArrayList<String> laterOps=new ArrayList<String>();
          	    for(int i=0;i<ltrIdx.size();i++) {
          	    	laterOps.add(strarrOperators[ltrIdx.get(i)]);
          	    }
          	    Hashtable<Integer,ArrayList<SQLTerm>> andTerms= new  Hashtable<Integer,ArrayList<SQLTerm>>();
          	    ArrayList<SQLTerm> terms= new ArrayList<SQLTerm>();
          	    ArrayList<SQLTerm> saveterms= new ArrayList<SQLTerm>();
          	    Enumeration<Integer> handles= andHandle.keys();
          	    while(handles.hasMoreElements()) {
          	    	int currHan= handles.nextElement();
          	    	terms= new ArrayList<SQLTerm>();
          	    	for(int j=0;j<andHandle.get(currHan).size();j++) {
          	    	
          	    		int idx=andHandle.get(currHan).get(j);
          	    		terms.add(arrSQLTerms[idx]);
          	    		allTerms.remove(arrSQLTerms[idx]);
          	    		andTerms.put(currHan,terms);}
          	
       			int idx=andHandle.get(currHan).get(andHandle.get(currHan).size()-1)+1;
       			terms.add(arrSQLTerms[idx]);
       			allTerms.remove(arrSQLTerms[idx]);
       			andTerms.put(currHan,terms);
       		}
          	    Enumeration<Integer> handlesTerms= andTerms.keys();
          	    Hashtable<Integer, ArrayList<ArrayList<row>>> handleAllResults= new  Hashtable<Integer, ArrayList<ArrayList<row>>>();
          	    //ArrayList<row> handleResult= new  ArrayList<row>();
          	    int i=0;
          	    while(handlesTerms.hasMoreElements()) {
          	    	int currHan= handlesTerms.nextElement();
          	    	handleAllResults= handleAnd(andTerms.get(currHan),handleAllResults,i);
          	    	i++;
          	    }
          	    ArrayList<ArrayList<row>> resNotAnded= new ArrayList<ArrayList<row>>();
          	    for(int i2=0; i2<allTerms.size();i2++) 
          	    {    ///indexes vs indexes to know order
          	    	SQLTerm currTerm= allTerms.get(i2);
          	    	if(currTerm!=null) {
          	    	if(getTable(currTerm.getStrTableName())==null) {
          	    		throw new DBAppException("there is no such table");
          	    	}
          	    	Vector<String> pagesnow= getTable(currTerm.getStrTableName()).getPages();
          	    	resNotAnded.add(performSelect(pagesnow,currTerm));
          	    	
          	    }}
          	    ArrayList<ArrayList<row>> allTermsinOrder= new ArrayList<ArrayList<row>>();
          	    int countLtr=0;
          	    int countAnd=0;
          	    int countLtrT=0;
          	    int countAndH=0;
          	    Boolean taken=false;
          	    for(int j2=0; j2<=strarrOperators.length;j2++) {
          	    	if(j2==strarrOperators.length) {
          	    		if(resNotAnded.size()!=0 && resNotAnded.size()>countLtrT)
          	    		allTermsinOrder.add(resNotAnded.get(countLtrT));
          	    		if(handleAllResults.get(countAndH)!=null && !(handleAllResults.get(countAndH).isEmpty()))
          	    		allTermsinOrder.add(handleAllResults.get(countAndH).get(0));
          	    	}else if(wntdIdxSave.size()!=countAnd) { 
          	    		if(wntdIdxSave.size()>countAnd && wntdIdxSave.size()!=0 ) {
          	    		if(j2==wntdIdxSave.get(countAnd)){
          	    		if(!taken) {
          	    			if(handleAllResults.size()>countAndH&&handleAllResults.size()!=0) {
          	    			if(handleAllResults.get(countAndH).size()>0)
          	    			allTermsinOrder.add(handleAllResults.get(countAndH).get(0));
          	    			countAndH++;
          	    			countAnd++;
          	    			wntdIdxSave.remove(countAnd);
          	    			taken=true;
          	    			}}else {
          	    			countAnd++;
          	    		}
          	    	}}}else if(ltrIdx.size()!=0) {
          	    		if(j2==ltrIdx.get(countLtr)) {
          	    		allTermsinOrder.add(resNotAnded.get(countLtrT));
          	    		countLtr++;
          	    		countLtrT++;
          	    		taken=false;
          	    	}}
          	    }
          	    
          	    ArrayList<row> resultSet=new  ArrayList<row>();
          	    for(int i3=0;i3<laterOps.size();i3++) {
      	        	if(!(opValid(laterOps.get(i3)))) {
      	        		throw new DBAppException("This operator is not Valid");
      	        	}
      	        }
      	        for(int i3=0;i3<allTermsinOrder.size();i3++) {
      	        	ArrayList<row> thisTermRes=allTermsinOrder.get(i3);
      	        	if(i3==0) {
      	        		resultSet=thisTermRes;
      	        	}else {
      	        		if(i3!=0)
      	        		resultSet=performOperator(resultSet,thisTermRes,laterOps.get(i3-1));
      	        	}
      	        }
      	        Iterator<row> iterator = resultSet.iterator();
      	        return iterator;
      }
    
    private Hashtable<Integer, ArrayList<ArrayList<row>>> handleAnd(ArrayList<SQLTerm> wntdTerms, Hashtable<Integer, ArrayList<ArrayList<row>>> handleAllResults, int i) throws IOException, DBAppException {
		//check if all terms same table
    	boolean sameTable=true;
    	String tableName=wntdTerms.get(0).getStrTableName();
    	ArrayList<ArrayList<row>> result= new ArrayList<ArrayList<row>>();
    	for(int j=1;j<wntdTerms.size();j++) {
    		if(!(wntdTerms.get(j).getStrTableName().equals(tableName))) {
    			sameTable=false;
    		}
    	}
    	if(!sameTable) {
    		handleAllResults.put(i,result);
    		i++;
    	}else {
    		table table=  getTable(tableName);
    		Vector<Octree> tableTrees= getTable(tableName).getOctrees();
    		ArrayList<SQLTerm> laterTerms= new ArrayList<SQLTerm>();
    		Hashtable<Octree, ArrayList<Hashtable<String,SQLTerm>>> treez= new Hashtable<Octree, ArrayList<Hashtable<String,SQLTerm>>>();
    		for(int j=0; j<tableTrees.size(); j++) {
    			Octree currentTree= tableTrees.get(j);
    			String col1= currentTree.col1;
    			String col2= currentTree.col2;
    			String col3= currentTree.col3;
    			//ArrayList<SQLTerm> laterTerms= new ArrayList<SQLTerm>();
    			ArrayList<SQLTerm> x= new ArrayList<SQLTerm>();
    			ArrayList<SQLTerm> y= new ArrayList<SQLTerm>();
    			ArrayList<SQLTerm> z= new ArrayList<SQLTerm>();
    		    for(int p=0;p<wntdTerms.size();p++) {
    		    	SQLTerm currT= wntdTerms.get(p);
    		    	if(currT.getStrColumnName().equals(col1)) {
    		    		x.add(currT);
    		    	}else if(currT.getStrColumnName().equals(col2)) {
    		    		y.add(currT);
    		    	} else if(currT.getStrColumnName().equals(col3)) {
    		    		z.add(currT);
    		    	}else {
    		    		laterTerms.add(currT);
    		    	}
    		    }
    			ArrayList<Hashtable<String,SQLTerm>> octSearchHashes= new ArrayList<Hashtable<String,SQLTerm>>();
    			Hashtable<String,SQLTerm> Idxcolumn= new Hashtable<String,SQLTerm>();
    			int loop= Math.min(x.size(), Math.min(y.size(), z.size()));
    			for(int w=0; w<loop;w++ ) {
    				Idxcolumn.put("X", x.get(w));
    				Idxcolumn.put("Y", y.get(w));
    				Idxcolumn.put("Z", z.get(w));
    				x.remove(w);
    				y.remove(w);
    				z.remove(w);
    				octSearchHashes.add(Idxcolumn);
    				Idxcolumn= new Hashtable<String,SQLTerm>();
    			}
    			laterTerms.addAll(x);
    			laterTerms.addAll(y);
    			laterTerms.addAll(z);
    			treez.put(currentTree, octSearchHashes);
    			wntdTerms= laterTerms;
    		}
    		//solve later list to make list of results to e anded later
    		ArrayList<row> laterTermRes= new ArrayList<row>();
    		for(int l=0;l<laterTerms.size();l++) {
    			SQLTerm curr= laterTerms.get(l);
    			String currTableN= curr.getStrTableName();
    			table currTable= getTable(curr.getStrTableName());
	        	String col= curr.getStrColumnName();
	        	String operator=curr.getStrOperator();
	        	Object value=curr.getObjValue();
	        	if(!(tableValid(currTableN)&&colValid(currTable.getTableName(),col)&&isValidOperator(operator))) {
	        		throw new DBAppException("This SQLTerm is not Valid");
	        	}
	        	ArrayList<row> thisTermRes= performSelect(currTable.getPages(), curr);
	        	if(l==0)
	        	   laterTermRes.addAll(thisTermRes);
	        	else
	        	   laterTermRes=retainAllRows(laterTermRes,thisTermRes);
    		}
    		for(int r=0;r<treez.size();r++) {
    			 Enumeration<Octree> octsName= treez.keys();
    			 while(octsName.hasMoreElements()) {
    			 Octree curr=octsName.nextElement();
    			 ArrayList<Hashtable<String,SQLTerm>> treezHash= treez.get(curr);
    			 for(int t=0;t<treezHash.size();t++) {
    				 Hashtable<String,SQLTerm> currHash=treezHash.get(t);
    				 SQLTerm x=currHash.get("X");
    				 String opx=x.getStrOperator();
    				 Object obx=x.getObjValue();
    				 SQLTerm y=currHash.get("Y");
    				 String opy=y.getStrOperator();
    				 Object oby=y.getObjValue();
    				 SQLTerm z=currHash.get("Z");
    				 String opz=z.getStrOperator();
    				 Object obz=z.getObjValue();
    				 List<String> pagesPaths=curr.searchElements(curr.getRoot(), obx, oby, obz,opx,opy,opz);
    				 Vector<String> pagess= new Vector<String> ();
    				 for(int l=0; l<pagesPaths.size();l++) {
    					 pagess.add(pagesPaths.get(l));
    				 }
    				 HashSet<String> set = new HashSet<>(pagess);
    			     Vector<String> pages = new Vector<>(set);
    				 for(int l=0;l<3;l++) {
    					 SQLTerm currXYZ;
    					    if(l==0)
    		    			    currXYZ= currHash.get("X");
    					    else if (l==1)
    					    	 currXYZ= currHash.get("Y");
    					    else
    					    	 currXYZ= currHash.get("Z");
    					    String currTableN= currXYZ.getStrTableName();
    					    table currTable= getTable(currXYZ.getStrTableName());
    			        	String col= currXYZ.getStrColumnName();
    			        	String operator=currXYZ.getStrOperator();
    			        	Object value=currXYZ.getObjValue();
    			        	if(!(tableValid(currTableN)&&colValid(currTable.getTableName(),col)&&isValidOperator(operator))) {
    			        		throw new DBAppException("This SQLTerm is not Valid");
    			        	}
    			        	ArrayList<row> thisTermRes= performSelect(pages, currXYZ);
    			        	if(l==0)
    				        	   laterTermRes.addAll(thisTermRes);
    				        	else
    				        	   laterTermRes=retainAllRows(laterTermRes,thisTermRes);
    		    		}
    				 
    			 }
    			 }
    			 
    		}
    		ArrayList<ArrayList<row>> laterTermResA= new ArrayList<ArrayList<row>>();
    		laterTermResA.add(laterTermRes);
    		handleAllResults.put(i,laterTermResA);
    		
    	}
    	
    	return handleAllResults;
	}

    private ArrayList<row> performSelect(Vector<String> pages, SQLTerm term) throws IOException{
    	String opr= term.getStrOperator();
    	Object val= term.getObjValue();
    	String col= term.getStrColumnName();
    	page currP;
    	row currR;
    	Object currV = null;
    	ArrayList<row> result= new ArrayList<row>();

    	for(int i=0; i<pages.size();i++) {
    		currP= getPageFromPath(pages.get(i));
    		for(int j=0; j<currP.getRowsNumber(); j++) {
    			currR=currP.getRows().get(j);
    			currV= currR.getData().get(col);
    			if(performMath(currV,val,opr)) {
    				result.add(currR);
    			}
    				
    		}
    		writePage(currP);
    	}
    	return result;
    }

    private boolean performMath (Object one , Object two , String operator) {
    	String currDataType= one.getClass().getName();
		 int result ;
		 switch (currDataType) { 
 	 case "java.lang.Integer":{
 		 switch(operator){
 		 case "=":
 			 return (int)one==(int)two;
 		 case ">":
 			return (int)one>(int)two;
 		 case "<":
 			return (int)one<(int)two;
 		 case ">=":
 			return (int)one>=(int)two;
 		 case "<=":
 			return (int)one<=(int)two;
 		 case "!=":
 			return (int)one!=(int)two; 

 	 }}
 	 case "java.lang.String":{
 	  switch(operator){
 	    case "=":
		  return ((String)one).equals((String)two);
	    case ">":
		  return ((String) one).compareTo((String)two)>0;
	    case "<":
		  return ((String) one).compareTo((String)two)<0;
	    case ">=":
		  return (((String) one).compareTo((String)two)>0 ) || (((String)one).equals((String)two));
	    case "<=":
		  return (((String) one).compareTo((String)two)<0 ) || (((String)one).equals((String)two));
	    case "!=":
	    	return !(((String)one).equals((String)two));	
        
 	 }}
 	 case "java.lang.Double":{
 		switch(operator){
		 case "=":
			 return (double)one==(double)two;
		 case ">":
			return (double)one>(double)two;
		 case "<":
			return (double)one<(double)two;
		 case ">=":
			return (double)one>=(double)two;
		 case "<=":
			return (double)one<=(double)two;
		 case "!=":
			return (double)one!=(double)two; 

	 }
 	 }
 	 case "java.util.Date":{
 		switch(operator){
 	    case "=":
		  return ((Date)one).equals((Date)two);
	    case ">":
		  return ((Date) one).compareTo((Date)two)>0;
	    case "<":
		  return ((Date) one).compareTo((Date)two)<0;
	    case ">=":
		  return (((Date) one).compareTo((Date)two)>0 ) || (((Date)one).equals((Date)two));
	    case "<=":
		  return (((Date) one).compareTo((Date)two)<0 ) || (((Date)one).equals((Date)two));
	    case "!=":
	    	return !(((Date)one).equals((Date)two));	
        
 	 }
 	 }

 	
 	 }
		return false;
		
	
	
}

    private ArrayList<row> performOperator(ArrayList<row> one, ArrayList<row> two,String op ) {
    	switch(op){
    	case "AND":
    		
    		one=retainAllRows(one,two); break;
    		
    		
    		
    	case "OR":
    		
    		
    		one=removeAllRows(one,two);
    		one=addAllRows(one,two); break;
    		
    		
    		
    	case "XOR":
    		ArrayList<row> result = new ArrayList<>();
    		ArrayList<row> copyOne = new ArrayList<>(one);
            ArrayList<row> copyTwo = new ArrayList<>(two);

            copyOne=removeAllRows(copyOne,two);
            copyTwo=removeAllRows(copyTwo,one);
             
            result=addAllRows(result,copyOne); 
            result=addAllRows(result,copyTwo); 
            one=result;
            break;
    		
    		 }
		return one;
		
    }
 
    private ArrayList<row> addAllRows(ArrayList<row> one, ArrayList<row> two) {
    	ArrayList<row> common= new ArrayList<row>();
     	   
     	   for(int j=0; j<two.size();j++) {
         	   Object twoVal= two.get(j).getPrimaryKey();
         		   one.add(two.get(j));
         	
     	   }

        return one;
     	
	}

	private ArrayList<row> removeAllRows(ArrayList<row> one, ArrayList<row> two) {
    	  //ArrayList<row> common= new ArrayList<row>();
		 ArrayList<row> result= new  ArrayList<row>();
		 boolean common=false;
          for(int i=0; i<one.size();i++) {
       	   Object oneVal= one.get(i).getPrimaryKey(); ///make sure i set PK
       	   for(int j=0; j<two.size();j++) {
           	   Object twoVal= two.get(j).getPrimaryKey();
           if(	compareObj(oneVal,twoVal)==0) {
           	   common=true;
       	   }}
       	   if(!common) {
       		   result.add(one.get(i));
       	   }
          }
          return result;
       	
       	
	}

	private boolean opValid(String operator) {
	return operator.equals("AND") ||
            operator.equals("OR") ||
            operator.equals("XOR");
           
 
}
   
    private boolean colValid(String currTable, String col) throws IOException {
	boolean colExist= false;
	BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
	String colRead;
	
	//Looping on file to check if table exists
	
	while((colRead= br.readLine() )!=null) {
		String [] colCurrent= colRead.split(",");
		if(colCurrent[0].equals(currTable)) {
			if(colCurrent[1].equals(col)) {
				colExist=true;
			}
			
		}
	}
	br.close();
	return colExist;
}

    private  ArrayList<row> retainAllRows(ArrayList<row> one, ArrayList<row> two) {

       ArrayList<row> common= new ArrayList<row>();

       for(int i=0; i<one.size();i++) {
    	   Object oneVal= one.get(i).getPrimaryKey(); ///make sure i set PK
    	   for(int j=0; j<two.size();j++) {
        	   Object twoVal= two.get(j).getPrimaryKey();
        	   if(oneVal.getClass()==twoVal.getClass()) {
        	   if(compareObj(oneVal,twoVal)==0) {
        		   common.add(one.get(i));
        	   }
    	   }}
       }
       return common;
    	
    	
    	
    }

    private boolean tableValid(String currTable) throws IOException {
	boolean tableExist= false;
	BufferedReader br= new BufferedReader(new FileReader("src/main/resources/metaData.csv"));
	String colRead;
	
	//Looping on file to check if table exists
	
	while((colRead= br.readLine() )!=null) {
		String [] colCurrent= colRead.split(",");
		if(colCurrent[0].equals(currTable)) {
			tableExist= true;
			
		}
	}
	br.close();
	return tableExist;
}
    
    public static void main ( String [] args) throws DBAppException, IOException, ParseException {
    //creating and inserting
    String strTableName = "table"; 
    DBApp dbApp = new DBApp( );
//   dbApp.init();
//    Hashtable<String, String> htblColNameType = new Hashtable<String, String>( );
//    
//    //Creating Table
//    htblColNameType.put("id", "java.lang.Integer");
//    htblColNameType.put("name", "java.lang.String");
//    htblColNameType.put("gpa", "java.lang.Double");
//    Hashtable<String, String> htblColNameMin = new Hashtable<String, String>( );
//    Hashtable<String, String> htblColNameMax = new Hashtable<String, String>( );
//    htblColNameMin.put("id", "0000000");
//    htblColNameMin.put("name", "aaaaaaaa");
//    htblColNameMin.put("gpa", "0");
//    htblColNameMax.put("id", "9999999");
//    htblColNameMax.put("name", "zzzzzzzz");
//    htblColNameMax.put("gpa", "4.00");
//    //dbApp.createTable( strTableName, "id", htblColNameType ,htblColNameMin, htblColNameMax );
//    
//    
//    
//    //Inserts
//    Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( ); 
////    htblColNameValue.put("id",  5); 
////    htblColNameValue.put("name","Ahmed Noor"  );
////    htblColNameValue.put("gpa",  0.95  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
//// 
////    htblColNameValue.clear( );
////    htblColNameValue.put("id",  451 );
////    htblColNameValue.put("name", "new page" );
////    htblColNameValue.put("gpa",  1.25  );         
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////   
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  253 );
////    htblColNameValue.put("name", "Jhey2" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////  
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  57 );
////    htblColNameValue.put("name", "Jessss r" ); 
////    htblColNameValue.put("gpa",  1.6  ); 
////    dbApp.recValidate( strTableName , htblColNameValue );
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////   
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  529 ); //460
////    htblColNameValue.put("name", "kally" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue ); 
////   
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  701 ); //90
////    htblColNameValue.put("name", "Jmoshkela" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue ); 
////
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  231 );
////    htblColNameValue.put("name", "John Noor" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////   
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  89 );
////    htblColNameValue.put("name", "zoona" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////  
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  219 );
////    htblColNameValue.put("name", "ahmed2" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
////  
////    htblColNameValue.clear( ); 
////    htblColNameValue.put("id",  6 );
////    htblColNameValue.put("name", "zoona2" ); 
////    htblColNameValue.put("gpa",  1.5  ); 
////    dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
////    //Update
//   htblColNameValue.clear( ); 
//   htblColNameValue.put("name", "zoona2" );  
//   htblColNameValue.put("gpa",  2.0  );
//   //dbApp.updateTable(strTableName, "57", htblColNameValue);
//
////   
////   //Delete
//   htblColNameValue.clear( ); 
//   htblColNameValue.put("id",  5);
//   htblColNameValue.put("name", "Ahmed Noor" ); 
//   htblColNameValue.put("gpa",  0.95  ); 
//  // dbApp.deleteFromTable(strTableName, htblColNameValue);
//   
//
//   
//   //Create Index
//   String [] strarrColName= {"id","name","gpa"};
//   // dbApp.createIndex(strTableName, strarrColName);
//   SQLTerm[] arrSQLTerms;
//   arrSQLTerms = new SQLTerm[2];
//   arrSQLTerms[0] = new SQLTerm();
//   arrSQLTerms[0]._strTableName = "table";
//   arrSQLTerms[0]._strColumnName= "gpa";
//   arrSQLTerms[0]._strOperator = "=";
//   arrSQLTerms[0]._objValue = 1.5;
//   arrSQLTerms[1] = new SQLTerm();
////   arrSQLTerms[1]._strTableName = "table";
////   arrSQLTerms[1]._strColumnName= "name"; 
////   arrSQLTerms[1]._strOperator = "=";
////   arrSQLTerms[1]._objValue = "zoona2";
////   arrSQLTerms[2] = new SQLTerm();
////   arrSQLTerms[2]._strTableName = "table";
////   arrSQLTerms[2]._strColumnName= "name"; 
////   arrSQLTerms[2]._strOperator = "=";
////   arrSQLTerms[2]._objValue = "kally";
////   arrSQLTerms[3] = new SQLTerm();
////   arrSQLTerms[3]._strTableName = "table";
////   arrSQLTerms[3]._strColumnName= "name"; 
////   arrSQLTerms[3]._strOperator = "=";
////   arrSQLTerms[3]._objValue = "kally";
////   arrSQLTerms[4] = new SQLTerm();
//   arrSQLTerms[1]._strTableName = "table";
//   arrSQLTerms[1]._strColumnName= "id"; 
//   arrSQLTerms[1]._strOperator = "<";
//   arrSQLTerms[1]._objValue = 20;
//  // arrSQLTerms[5] = new SQLTerm();
////   arrSQLTerms[5]._strTableName = "table";
////   arrSQLTerms[5]._strColumnName= "id"; 
////   arrSQLTerms[5]._strOperator = "=";
////   arrSQLTerms[5]._objValue = 529;
//  
//   String[]strarrOperators = new String[1];
//   strarrOperators[0] = "XOR";
//
//
//
//   Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//   while (resultSet.hasNext()) {
//       row element = (row)resultSet.next();
//       Enumeration<Object> values = element.getData().elements();
//       while (values.hasMoreElements()) {
//    	   
//           System.out.println(values.nextElement());
//       }
//       System.out.println("\n");
//   }
//   
//   //Print 
//   table t=getTable(strTableName);
//   //t.getOctrees().get(0).printAllNodes();
//   printTableContents(strTableName);
//   //printTableContents("table");
   
	    
	}
	
	
}
