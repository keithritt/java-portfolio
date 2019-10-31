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
import java.util.Set;

import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

abstract public class Db {
	private static boolean isInitialized = false;
	public static MyDb staticDb;
	public static String dbms = "sqlserver"; // default to sqlserver

	public static String getDbms(){
		if(Db.staticDb != null)
			return Db.staticDb.getDbms().toLowerCase();
		else
			return Db.dbms.toLowerCase();
	}

	public static Integer escInt(Integer var){
		return var;
	}

	public static Integer escInt(Object var){
		switch(var.getClass().getName()){
		default:
			return Integer.parseInt(var.toString());
		}
	}

	public static String escStr(String text){
		if(text.toLowerCase() == "null")
			Debug.die("need to handle 'null' being passed in");

		text=text.replaceAll("'","''");

		return "'"+text+"'";
	}

	public static String escNumber(Integer myNum){
		return myNum.toString();
	}

	public static String escNumber(Double myNum){
		return myNum.toString();
	}

	public static String escNumber(String myNum){
		if(Util.isNumeric(myNum))
			return myNum;
		else{
			Debug.die("invalid number passed to Db.escNumber(): "+myNum);
			return null;
		}
	}

	public static String escDynamic(String text, String dataType){
		String ret = null;
		switch(dataType){
		case "char":
		case "nvarchar":
		case "varchar":
			ret = Db.escStr(text);
			break;
		case "float":
		case "int":
		case "numeric":
		case "smallint":
			ret = Db.escNumber(text);
			break;
		case "datetime":
			ret = Db.escDateTime(text);
			Debug.expose(ret);
			Debug.die();
			break;
		case "image":
		case "varbinary":
			Debug.die("not coded yet - dataType: "+dataType);
			break;
		}

		return ret;
	}

	public static String escColumn(String text, String table, String column){
		String dataType = Db.getColumnDataType(table, column);
		return Db.escDynamic(text, dataType);
	}

	public static String escStrJoin(Set<String> texts){
		return Db.escStrJoin(new ArrayList<String>(texts));
	}

	// format a list of string for a IN () statement
	public static String escStrJoin(List<String> texts){
		List<String> tmp = new ArrayList<String>();
		for(String text: texts)
			tmp.add(Db.escStr(text));

		return "("+String.join(", ", tmp)+")";
	}

	public static String getColumnDataType(String table, String column){
		String sql =
		"SELECT \n" +
		"	types.Name data_type, \n" +
		"FROM \n" +
		"	sys.tables \n" +
		"JOIN \n" +
		"	sys.columns ON ( \n" +
		"		columns.object_id = tables.object_id) \n" +
		"JOIN \n" +
		"	sys.types ON ( \n" +
		"		types.user_type_id = columns.user_type_id) \n" +
		"WHERE \n" +
		"	tables.name = "+Db.escStr(table)+ "AND \n "+
		"   columns.name ="+Db.escStr(column);

		Map<String, String> row = Db.staticDb.selectRow(sql);

		return row.get("data_type");
	}

	public static void init(Map<String, String> creds){
		Db.init("", creds);
	}

	public static void init(String name, Map<String, String> creds){
		if(!Db.isInitialized){
			Db.staticDb = new MyDb(name, creds);
			Db.isInitialized = true;
		}
	}



    public static Map<String, String> getDbCredsFromXmlfile(String repoDir){
    	Map<String,String> creds = new HashMap<String, String>();

    	try{
    		 File afmProfjectFile = Db.getAfmProjectFile(repoDir);
             DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

             DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
             Document doc = dBuilder.parse(afmProfjectFile);
             doc.getDocumentElement().normalize();

             NodeList projectList = doc.getElementsByTagName("settings");

             for (int i = 0; i < projectList.getLength(); i++) {
               Element project  = (Element) projectList.item(i);
               Element preferences = (Element) project.getElementsByTagName("preferences").item(0);

               if(preferences.getAttribute("active").equals("true")){
            	   Element databases = (Element) project.getElementsByTagName("databases").item(0);
            	   NodeList databaseList = databases.getElementsByTagName("database");
            	   for (int j = 0; j < databaseList.getLength(); j++) {
            		   Element database = (Element) databaseList.item(j);
            		   if(database.getAttribute("role").equals("data")){
            			   Element login = (Element) database.getElementsByTagName("login").item(0);
            			   creds.put("user", login.getAttribute("account"));
            			   creds.put("pw", login.getAttribute("password"));

            			   Element engine = (Element) database.getElementsByTagName("engine").item(0);
            			   creds.put("dbms", engine.getAttribute("type"));
            			   Element jdbc = (Element) engine.getElementsByTagName("jdbc").item(0);
            			   creds.put("url", jdbc.getAttribute("url"));
            			   return creds;
            		   }
            	   }
               }
            }
    	}
    	catch(Exception e){
    		Debug.pr(e.getMessage());
    		e.printStackTrace();
    	}

    	return creds;
    }

    public static List<Map<String, String>> selectRows(String sql){
    	return Db.staticDb.selectRows(sql);
    }

    public static Map<String, String> selectRow(String sql){
    	return Db.staticDb.selectRow(sql);
    }

    public static List<String> selectList(String sql){
    	return Db.staticDb.selectList(sql);
    }

    public static void execute(String sql){
    	Db.staticDb.execute(sql);
    }

	public static String getTableStructureSql(String table){
		String sql =
			"SELECT \n" +
			"	LOWER(tables.name) table_name, \n" +
			"	LOWER(columns.name) column_name, \n" +
			"	types.Name data_type, \n" +
			"	columns.max_length, \n" +
			"	columns.precision, \n" +
			"	columns.scale, \n" +
			"	columns.is_nullable, \n" +
			"	columns.is_identity, \n" +
			"	ISNULL(indexes.is_primary_key, 0) is_primary_key, \n" +
			"   default_constraints.definition default_val \n"+
			"FROM \n" +
			"	sys.tables \n" +
			"JOIN \n" +
			"	sys.columns ON ( \n" +
			"		columns.object_id = tables.object_id) \n" +
			"JOIN \n" +
			"	sys.types ON ( \n" +
			"		types.user_type_id = columns.user_type_id) \n" +
			"LEFT  JOIN \n" +
			"	sys.index_columns index_columns ON ( \n" +
			"		index_columns.object_id = columns.object_id AND \n" +
			"		index_columns.column_id = columns.column_id) \n" +
			"LEFT  JOIN \n" +
			"	sys.indexes ON ( \n" +
			"		indexes.object_id = index_columns.object_id AND \n" +
			"		indexes.index_id = index_columns.index_id) \n" +
			"LEFT  JOIN \n" +
			"	sys.default_constraints ON ( \n" +
			"		default_constraints.object_id = index_columns.object_id) \n" +
			"WHERE \n" +
			"	tables.name = "+Db.escStr(table)+" \n" +
			"ORDER BY \n" +
			"   columns.name;";
		return sql;
	}

    // search a list of rows for a specific row value - returns first match
	public static Map<String, String> filterForRow(String column, String val, List<Map<String, String>> rows){
		Map<String, String> ret = null;
		for(Map<String, String> row : rows){
			if(row.get(column).equals(val)){
				return row;
			}
		}

		return ret;
	}

    // search a list of rows for a specific row value - returns first match
	public static Map<String, String> filterForRow(Map<String, String> filterSearches, List<Map<String, String>> rows){
		rowLoop:
		for(Map<String, String> row : rows){
			boolean match = true;
			for (Map.Entry<String,String> entry : filterSearches.entrySet()){
	            String searchKey = entry.getKey();
	            String searchVal = entry.getValue();

				String rowVal = row.get(searchKey);


        if(searchVal == null && rowVal == null){
        	// both are null - consider this a match
        }else if (searchVal == null){
        	Debug.pr("not a match - breaking - move on to next row in rows");
        	match = false;
        	continue rowLoop;
        }else if (rowVal == null){
        	Debug.pr("not a match - breaking - move on to next row in rows");
        	match = false;
        	continue rowLoop;
        }else if(!rowVal.equals(searchVal)){
        	Debug.pr("not a match - breaking - move on to next row in rows");
		    match = false;
		    continue rowLoop;
				}

			}
			if(match)
				return row;
	    }

		return null;
	}

	public static String[] getDisableForeignKeySql(MyDb myDb, String table, String column){
		String sql = Db.getForeignKeySql(table, column);

		List<Map<String, String>> rows = myDb.selectRows(sql);

		String ret[] = new String[rows.size()];

		int idx = 0;

		for(Map<String, String> row : rows){
			ret[idx++] = "ALTER TABLE afm."+row.get("table_name")+" DROP CONSTRAINT "+row.get("fk_name")+";";
		}

		return ret;
	}

	public static String[] getEnableForeignKeySql(MyDb myDb, String table, String column){
		String sql = Db.getForeignKeySql(table, column);

		List<Map<String, String>> rows = myDb.selectRows(sql);


		String ret[] = new String[rows.size()];

		int idx = 0;

		for(Map<String, String> row : rows){
			ret[idx++] = "ALTER TABLE afm."+row.get("table_name")+" ADD CONSTRAINT "+row.get("fk_name")+" FOREIGN KEY ("+row.get("col_name")+") REFERENCES "+table+"("+column+");";
		}

		return ret;
	}

	public static String getUpdateValueSql(String table, String pkCol, String pkVal, String col, String newVal){
		return "UPDATE "+table+" SET "+col+" = "+newVal+" WHERE "+pkCol+" = "+pkVal+";";
	}

	// return a list of primary keys for a table for a particular db
	public static String getPrimaryKeys(MyDb myDb, String table){
		return myDb.selectItem(Db.getPrimaryKeysSql(table));
	}

	// return the sql that can generate a comma separated list of all the columns that make up a tables primary key
	public static String getPrimaryKeysSql(String table){
		return
				"WITH base_data AS ( \n" +
				"	SELECT \n" +
				"		indexes.name index_name, \n" +
				"		LOWER(tables.name) table_name, \n" +
				"		LOWER(columns.name) column_name \n" +
				"	FROM  \n" +
				"		sys.tables \n" +
				"	JOIN \n" +
				"		sys.schemas ON ( \n" +
				"			schemas.schema_id = tables.schema_id AND \n" +
				"       	schemas.name = 'afm') \n" +
				"	JOIN \n" +
				"		sys.columns ON ( \n" +
				"			columns.object_id = tables.object_id) \n" +
				"	JOIN \n" +
				"		sys.index_columns index_columns ON ( \n" +
				"			index_columns.object_id = columns.object_id AND \n" +
				"			index_columns.column_id = columns.column_id) \n" +
				"	JOIN \n" +
				"		sys.indexes ON ( \n" +
				"			indexes.object_id = index_columns.object_id AND \n" +
				"			indexes.index_id = index_columns.index_id AND \n" +
				"			indexes.is_primary_key = 1) \n" +
				"	WHERE \n" +
				"		tables.name = "+Db.escStr(table)+" \n" +
				") \n" +
				"SELECT \n" +
				"	STUFF(( \n" +
				"		SELECT \n" +
				"			',' + inner_table.column_name \n" +
				"	FROM \n" +
				"		base_data inner_table \n" +
				"	WHERE \n" +
				"		inner_table.index_name = outer_table.index_name AND \n" +
				"		inner_table.table_name = inner_table.table_name \n" +
				"	ORDER BY \n" +
				"		inner_table.column_name \n" +
				"	FOR XML PATH('')), 1, LEN(','), '') AS column_list \n" +
				"FROM \n" +
				"	base_data outer_table \n" +
				"GROUP BY \n" +
				"	outer_table.index_name, \n" +
				"	outer_table.table_name \n" +
				"ORDER BY \n" +
				"	outer_table.table_name";
	}

	public static String getForeignKeySql(String table, String column){
		String sql =
		"SELECT \n" +
		"	foreign_keys.name fk_name, \n" +
		"	parent_tables.name table_name, \n" +
		"	parent_cols.name col_name \n" +
		"FROM \n" +
		"	sys.foreign_keys \n" +
		"JOIN \n" +
		"	sys.schemas ON ( \n" +
		"		schemas.schema_id = foreign_keys.schema_id AND \n" +
		"		schemas.name = 'afm') \n" +
		"JOIN \n" +
		"	sys.foreign_key_columns ON ( \n" +
		"		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n" +
		"JOIN \n" +
		"	sys.tables parent_tables ON ( \n" +
		"		parent_tables.object_id = foreign_keys.parent_object_id) \n" +
		"JOIN \n" +
		"	sys.all_columns parent_cols ON ( \n" +
		"		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n" +
		"		parent_cols.column_id = foreign_key_columns.parent_column_id) \n" +
		"JOIN \n" +
		"	sys.tables ref_tables ON ( \n" +
		"		ref_tables.object_id = foreign_keys.referenced_object_id  AND \n" +
		"		ref_tables.name = "+Db.escStr(table)+") \n" +
		"JOIN \n" +
		"	sys.all_columns ref_cols ON ( \n" +
		"		ref_cols.object_id =  foreign_key_columns.referenced_object_id AND \n" +
		"		ref_cols.column_id = foreign_key_columns.referenced_column_id AND \n" +
		"		ref_cols.name = "+Db.escStr(column)+")";

		return sql;
	}

	public static String getForeignKeySql(){
		String sql =
				"SELECT \n "+
				"	foreign_keys.name fk_name, \n "+
				"	parent_tables.name parent_table, \n "+
				"	parent_cols.name parent_column, \n "+
				"	ref_tables.name ref_table, \n "+
				"	ref_cols.name ref_column \n "+
				"FROM \n "+
				"	sys.foreign_keys \n "+
				"JOIN \n "+
				"	sys.schemas ON ( \n "+
				"		schemas.schema_id = foreign_keys.schema_id AND \n "+
				"		schemas.name = 'afm') \n "+
				"JOIN \n "+
				"	sys.foreign_key_columns ON ( \n "+
				"		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n "+
				"JOIN \n "+
				"	sys.tables parent_tables ON ( \n "+
				"		parent_tables.object_id = foreign_keys.parent_object_id) \n "+
				"JOIN \n "+
				"	sys.all_columns parent_cols ON ( \n "+
				"		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n "+
				"		parent_cols.column_id = foreign_key_columns.parent_column_id) \n "+
				"JOIN \n "+
				"	sys.tables ref_tables ON ( \n "+
				"		ref_tables.object_id = foreign_keys.referenced_object_id) \n "+
				"JOIN \n "+
				"	sys.all_columns ref_cols ON ( \n "+
				"		ref_cols.object_id =  foreign_key_columns.referenced_object_id AND \n "+
				"		ref_cols.column_id = foreign_key_columns.referenced_column_id) \n" +
				"ORDER BY \n" +
				"   foreign_keys.name, \n" +
				"   parent_tables.name, \n" +
				"   parent_cols.name";

		return sql;
	}
}
