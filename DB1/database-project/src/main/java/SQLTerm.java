package main.java;

public class SQLTerm {
     public String  _strTableName;
     public String _strColumnName;
     public String _strOperator;
     public Object _objValue;
    
    public SQLTerm(){
    	
    }

	public String getStrTableName() {
		return _strTableName;
	}

	public void setStrTableName(String strTableName) {
		this._strTableName = strTableName;
	}

	public String getStrColumnName() {
		return _strColumnName;
	}

	public void setStrColumnName(String strColumnName) {
		this._strColumnName = strColumnName;
	}

	public String getStrOperator() {
		return _strOperator;
	}

	public void setStrOperator(String strOperator) {
		this._strOperator = strOperator;
	}

	public Object getObjValue() {
		return _objValue;
	}

	public void setObjValue(Object objValue) {
		this._objValue = objValue;
	}
}
