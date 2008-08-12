package org.apache.hadoop.chukwa.extraction.engine;

public class DatabaseRecord 
{
	public static final String intType = "i";
	public static final String longType = "l";
	public static final String floatType = "f";
	public static final String doubleType = "d";
	
	public static final String tableField = "tbl";
	public static final String columnsNameField = "cols";
	public static final String columnsTypesField = "types";
	
	public static final String insertSQLCmde = "insert";
	public static final String updateSQLCmde = "update";
	public static final String sqlCmdeField = "cmde";
	
	ChukwaRecord record  = new ChukwaRecord();
	
	public DatabaseRecord()
	{
		this.record.add(Record.destinationField, "database");
	}

	public DatabaseRecord(ChukwaRecord record)
	{
		this.record = record;
	}

	
	public ChukwaRecord getRecord() {
		return this.record;
	}

	public void setRecord(ChukwaRecord record) {
		this.record = record;
	}

	public String getTable()
	{
		return this.record.getValue(tableField);
	}
	public void setTable(String tableName)
	{
		this.record.add(tableField, tableName);
	}
	
	public void setSqlCommand(String cmde)
	{
		this.record.add(sqlCmdeField, cmde);
	}
	
	public String getSqlCommand()
	{
		return this.record.getValue(sqlCmdeField);
	}
	
	public String[] getColumns()
	{
		return this.record.getValue(columnsNameField).split(",");
	}
	public void setColumns(String[] columns)
	{
		StringBuilder sb = new StringBuilder();
		for(String name: columns)
		{
			sb.append(name).append(",");
		}
		this.record.add(columnsNameField, sb.substring(0, sb.length()));
	}
	
	public String[] getColumnTypes()
	{
		return this.record.getValue(columnsTypesField).split(",");
	}
	
	public void setColumnTypes(String[] columnsTypes)
	{
		StringBuilder sb = new StringBuilder();
		for(String types: columnsTypes)
		{
			sb.append(types).append(",");
		}
		this.record.add(columnsTypesField, sb.substring(0, sb.length()));
	}

	public void setTime(long time) {this.record.setTime(time);
		
	}

	public void add(String key, String value) {
		this.record.add(key, value);
	}

	public long getTime() {
		return this.record.getTime();
	}

	public String getValue(String field) {
		return this.record.getValue(field);
	}
}
