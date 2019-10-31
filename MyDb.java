package jutil;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import java.sql.* ;

public class MyDb {
	public String name;
	public String host;
	public String dbName;

	private Connection conn;
	private Statement stmt;
	public String user;

	private String dbms;
	private String pw;

	public boolean debugMode = false;

	public String getDbms(){
		return this.dbms;
	}

	public MyDb(Map<String, String> creds){
		this.name = null;
		this.user = creds.get("user");
		this.pw = creds.get("pw");
		this.host = creds.get("host");
		this.dbName = creds.get("db_name");
		this.dbms = creds.get("dbms");
		this.setupDbConn();
	}

	public MyDb(String name, Map<String, String> creds){
		this.name = name;
		this.user = creds.get("user");
		this.pw = creds.get("pw");
		this.host = creds.get("host");
		this.dbName = creds.get("db_name");
		this.dbms = creds.get("dbms");
		this.setupDbConn();
	}

    private void setupDbConn(){
    	try {
    		switch(this.dbms){
    		case "sqlserver":
    		case "sqlServer":
    			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    			// note: hard coded to port 1433 for now
    			this.conn = DriverManager.getConnection("jdbc:sqlserver://"+this.host+":1433;databaseName="+this.dbName, this.user, this.pw);
    			this.stmt = this.conn.createStatement();
    			break;
    		case "oracle":
    			Class.forName("oracle.jdbc.driver.OracleDriver");
    			// note: hard coded to port 1521 for now
    			this.conn = DriverManager.getConnection("jdbc:oracle:thin:@"+this.host+":1521:"+this.dbName, this.user , this.pw);
    			this.stmt = this.conn.createStatement();
    			break;
    		default:
    			Debug.die("unknown DBMS: "+this.dbms);
    			break;
    		}
    	}
    	catch(Exception e){
    		Debug.pr(e.getMessage());
    		e.printStackTrace();
    	}
    }


    public List<Map<String, String>> selectRows(String sql){
    	if(this.debugMode){
    		Debug.pr("MyDb.selectRows()");
    		Debug.pr(this.name);
    		Debug.pr(sql);
    	}
    	List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
    	Map<String,String> row;

    	ResultSet rs;

    	try{
			rs = this.stmt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			Integer colCount = rsmd.getColumnCount();
			Integer rowIdx = 0;
			String colVal;

			Map<Integer, String> colTypes = new HashMap<Integer, String>();
			Map<Integer, String> colNames = new HashMap<Integer, String>();

			while(rs.next()){
				row = new HashMap<String, String>();
				for (int colIdx = 1; colIdx <= colCount; colIdx++) { // note colIdx starts at 1 not 0
					if(rowIdx == 0){
						// get column metadata
						colTypes.put(colIdx, rsmd.getColumnTypeName(colIdx));
						colNames.put(colIdx, rsmd.getColumnName(colIdx));
					}

					switch(colTypes.get(colIdx)){
					case "varchar":
					case "char":
						colVal = rs.getString(colIdx);
						break;
					case "int":
						colVal = Integer.toString(rs.getInt(colIdx));
						if(rs.wasNull())
							colVal = null;
						break;
					default:
						colVal = rs.getString(colIdx);
						break;
					}

					if(colVal != null)
						colVal = colVal.trim(); // assuming we want all vals trimmed

					row.put(colNames.get(colIdx), colVal);
				}

				rows.add(rowIdx, row);
				rowIdx++;
			}
    	}
        catch (SQLException e) {
        	Debug.pr("SQLException");
        	Debug.pr(e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
        	Debug.pr("NumberFormatException");
        	Debug.pr(e.getMessage());
			e.printStackTrace();
    	}  catch (Exception e) {
        	Debug.pr("Generic Exception");
        	Debug.pr(e.getMessage());
			e.printStackTrace();
    	}
    	return rows;
    }

    public Map<String, String> selectRow(String sql){
    	try{
    		return this.selectRows(sql).get(0);
    	}catch(Exception e){
    		return new HashMap<String, String>();
    	}
    }

    // return a list of strings instead of a map
    // only returns first column in select statement
    public List<String> selectList(String sql){
    	List<String> ret = new ArrayList<String>();
    	List<Map<String, String>> rows = this.selectRows(sql);

    	for(Map<String, String> row : rows)
    		ret.add(row.entrySet().iterator().next().getValue());

    	return ret;
    }

    // returns the value of the first column of the first row in a select statement
    // useful for grabbing single pieces of data from a table
    public String selectItem(String sql){
    	Map<String, String> row = this.selectRow(sql);
    	try{
    		return row.entrySet().iterator().next().getValue();
    	}catch(Exception e){
    		return null;
    	}
    }

    public void execute(String sql){
    	if(this.debugMode){
    		Debug.pr("MyDb.execute()");
    		Debug.pr(sql);
    	}
    	try{
    		this.stmt.executeUpdate(sql);
    	}
        catch (SQLException e) {
        	Debug.pr("SQLException");
            e.printStackTrace();
            Debug.die();
        }
    }

    public List<String> getTableColumns(String table){
    	String sql =
		"SELECT \n"+
		"	name \n"+
		"FROM \n"+
		"	sys.columns \n"+
		"WHERE \n"+
		"	object_id = OBJECT_ID("+Db.escStr(table)+") \n"+
		"ORDER BY \n"+
		"	column_id";

    	return this.selectList(sql);
    }

    public Map<String, String> getColumnStructure(String table, String column){
    	String sql = Db.getTableStructureSql(table);
    	List<Map<String, String>> rows = this.selectRows(sql);
    	return Db.filterForRow("column_name", column, rows);
    }

    public String getColumnDataType(String table, String column){
    	Map<String, String> structure = this.getColumnStructure(table, column);
    	return structure.get("data_type");
    }

	public List<String> getPrimaryKeys(String table){
		String primaryKey = null;
		String sql =
			"SELECT \n" +
			"	LOWER(columns.name) column_name \n" +
			"FROM  \n" +
			"	sys.tables \n" +
			"JOIN \n" +
			"	sys.columns ON ( \n" +
			"		columns.object_id = tables.object_id) \n" +
			"JOIN \n" +
			"	sys.index_columns index_columns ON ( \n" +
			"		index_columns.object_id = columns.object_id AND \n" +
			"		index_columns.column_id = columns.column_id) \n" +
			"JOIN \n" +
			"	sys.indexes ON ( \n" +
			"		indexes.object_id = index_columns.object_id AND \n" +
			"		indexes.index_id = index_columns.index_id AND \n" +
			"		indexes.is_primary_key = 1) \n" +
			"WHERE \n" +
			"	tables.name = "+Db.escStr(table)+" \n" +
			"ORDER BY \n" +
			"   columns.column_id";

		return this.selectList(sql);
	}
}
