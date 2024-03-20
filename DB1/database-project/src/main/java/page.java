package main.java;
import java.io.Serializable;
import java.util.Vector;

public class page implements Serializable{
 //    private int maximumNumber;
     private String path;
     private int rowsNumber;
     private Vector<row> rows = new Vector<row>();;
     
     
     public page() {
 //   	 this.path=path;
//    	 this.maximumNumber=maximumNumber;
    	 rowsNumber=0;
  //  	 Vector<row> rows = new Vector<row>();
     }


//	public String getPath() {
//		return path;
//	}
//
//
//	public void setPath(String path) {
//		this.path = path;
//	}


	public Vector<row> getRows() {
		return rows;
	}


	public void setRows(Vector<row> rows) {
		this.rows = rows;
	}
	
	public int getRowsNumber() {
		return rowsNumber;
	}


	public void setRowsNumber(int rowsNumber) {
		this.rowsNumber = rowsNumber;
	}


	public String getPath() {
		return path;
	}


	public void setPath(String path) {
		this.path = path;
	}
     
}
