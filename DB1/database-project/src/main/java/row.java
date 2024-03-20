package main.java;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class row implements Serializable{
        private int id;
        private Hashtable<String,Object> data;
        private  String tableName;
        private Object primaryKey;
        
        public row(Hashtable<String,Object> data,String tableName) {
        //	this.id=id;
        	this.data=data;
       // 	this.primaryKey=primaryKey;
        	this.tableName=tableName;
        }

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public Hashtable<String,Object> getData() {
			return data;
		}

		public void setData(Hashtable<String,Object> data) {
			this.data = data;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public Object getPrimaryKey() {
			return primaryKey;
		}

		public void setPrimaryKey(Object primaryKey) {
			this.primaryKey = primaryKey;
		}

		
        
}
