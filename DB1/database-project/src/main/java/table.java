package main.java;

import java.io.*;
import java.util.Vector;
public class table implements Serializable{
    private String tableName;
    private Vector<String> pages;
    private int pagecounter =0;
    private String primarykey ;
    private Vector<Octree> octrees;
    
    
    public table(String tableName ,String primarykey) {
        this.tableName = tableName;
        this.pages = new Vector<>();
        this.primarykey=primarykey;
        octrees= new Vector<Octree> ();
    }


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getPrimarykey() {
		return primarykey;
	}


	public void setPrimarykey(String primarykey) {
		this.primarykey = primarykey;
	}


	public Vector<String> getPages() {
		return pages;
	}


	public void setPages(Vector<String> pages) {
		this.pages = pages;
	}


	public int getPageCounter() {
		return pagecounter;
	}


	public void setpagecounter(int pagecounter) {
		this.pagecounter = pagecounter;
	}


	public Vector<Octree> getOctrees() {
		return octrees;
	}


	public void setOctrees(Vector<Octree> octrees) {
		this.octrees = octrees;
	}
}
