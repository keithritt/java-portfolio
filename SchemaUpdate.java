package jutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.output.ThresholdingOutputStream;
import org.apache.commons.lang3.StringUtils;

public class Upgrade {
	public Map<String, MyDb> dbMap = new HashMap<String, MyDb>();

	private CustomSettings customSettings;

	private Printer ddlPrinter;
	private Printer dmlPrinter;
	private Printer ddlPrinterInverse;
	private Printer dmlPrinterInverse;

	List<String> allTableNames = new ArrayList<String>();
	List<String> allColumnNames;
	List<String> columnsToBeAltered = new ArrayList<String>();
	List<String> columnsToBeAdded = new ArrayList<String>();
	List<String> identityTables = new ArrayList<String>();

	//stats
	Set<String> uniqUpdateFields = new HashSet<String>();
	Map<String, Map<String, String>> tableData = new LinkedHashMap<String, Map<String, String>>();
	Map<String, Map<String, Map<String, String>>> columnData = new LinkedHashMap<String, Map<String, Map<String, String>>>();
	Map<String, Map<String, String>> constraintData = new LinkedHashMap<String, Map<String, String>>();
	Map<String, Map<String, String>> indexData = new LinkedHashMap<String, Map<String, String>>();

	private Map<String, List<Map<String, String>>> latestResults = new LinkedHashMap<String, List<Map<String, String>>>();

	private Gitty myGitty;

	public static void main(String[] args) {
		Debug.pr("Upgrade.main()");
		Upgrade myself = new Upgrade();
		myself.runWizard();
		Util.printMemoryUsage();
		Debug.pr("script end.");
	}

	public void runWizard(){
		String clientName = UserInterface.askWithOptions("What client would you like to upgrade?", repoList);


		this.customSettings = new CustomSettings(clientName);
		this.initDatabases();

		String fileSuffix = "";

		this.ddlPrinter = new Printer("data/upgrade-ddl"+fileSuffix+".sql");
		this.dmlPrinter = new Printer("data/upgrade-dml"+fileSuffix+".sql");
		this.ddlPrinterInverse = new Printer("data/upgrade-ddl-inverse"+fileSuffix+".sql");
		this.dmlPrinterInverse = new Printer("data/upgrade-dml-inverse"+fileSuffix+".sql");
		this.dmlPrinter.writeLine("BEGIN TRANSACTION;");
		this.dmlPrinterInverse.writeLine("BEGIN TRANSACTION;");
		this.setStructureData();
		this.compareColumns();
		this.dropConstraints();
		this.dropIndexes();
		this.applyColumnChanges();
		this.addConstraints(); // PKs must be added back before adding new tables
		this.addTables();
		this.rebuildIndexes();
		this.deleteDataFromNewTables();
		this.copyDataIntoNewTables();
		this.copyDataIntoExistingTables();
		this.compareTables(true); // auto fix
		this.closeFiles();
		this.printStats();
	}

	public void verifyConfigSettings(){
		UserInterface.print("Verifying upgrade settings");
		UserInterface.print("Client Name: " +this.customSettings.clientName);
		UserInterface.printClear("Past:");
		HashMap<String, String> pastCreds = (HashMap<String, String>)this.customSettings.dbCreds.get("past");
		UserInterface.print("DB Name: "+pastCreds.get("db_name"));
		UserInterface.printClear("Present:");
		HashMap<String, String> presentCreds = (HashMap<String, String>)this.customSettings.dbCreds.get("present");
		UserInterface.print("DB Name: "+presentCreds.get("db_name"));
		UserInterface.printClear("Future:");
		HashMap<String, String> futureCreds = (HashMap<String, String>)this.customSettings.dbCreds.get("future");
		UserInterface.print("DB Name: "+futureCreds.get("db_name"));

		if(!UserInterface.askBoolean("Are these settings correct?")){
			UserInterface.print("Please update Custom Settings to reflect current upgrade parameters.");
			Upgrade myself = new Upgrade();
			myself.runWizard();
		}
	}

	private void disableAllIdentityInsert(){
		for(String table: this.identityTables){
			this.disableIdentityInsert(table);
		}
	}

	private void enableIdentityInsert(String table){
		String sql = "SET IDENTITY_INSERT "+table+" ON;";
		this.saveDml(sql);
		this.testSql(sql);
	}

	private void disableIdentityInsert(String table){
		String sql = "SET IDENTITY_INSERT "+table+" OFF;";
		this.saveDml(sql);
		this.testSql(sql);
	}

	private void setIdentityTables(){
		String sql =
		"SELECT DISTINCT \n" +
		"	tables.name table_name \n" +
		"FROM \n" +
		"	sys.columns \n" +
		"JOIN \n" +
		"	sys.tables on ( \n" +
		"		tables.object_id = columns.object_id AND  \n" +
		"       tables.name IN "+Db.escStrJoin(this.allTableNames)+") \n " +
		"JOIN \n" +
		"	sys.schemas on ( \n" +
		"		schemas.schema_id = tables.schema_id) \n" +
		"WHERE \n" +
		"	columns.is_identity = 1 \n" +
		"ORDER BY \n" +
		"	tables.name";

		this.identityTables = this.dbMap.get("present").selectList(sql);
	}

	private void compareColumns(){
	  Iterator it = this.columnData.entrySet().iterator();
	  while (it.hasNext()) {
      // for now assume we want to trim both values
      Map.Entry colEntry = (Map.Entry)it.next();
      String fullColumnName = (String)colEntry.getKey();
      Debug.pr(fullColumnName);
      String[] parts = fullColumnName.split("\\.");
      String table = parts[0];
      String column = parts[1];

      Map colData = (Map)colEntry.getValue();
      Map<String, String> presentData = (Map<String, String>) colData.get("present");
      Map<String, String> futureData = (Map<String, String>) colData.get("future");
      if(futureData == null) // ignore columns not in future database
      	continue;
      Map<String, String> overview = (Map<String, String>) colData.get("overview");
      boolean match = this.compareColumnStructure(table, column, false);
      if(!match){
      	this.compareColumnStructure(table, column, true);
      	this.die();
      }
	   }
	}

	private void closeFiles(){
		this.ddlPrinter.close();
		this.dmlPrinter.close();
		this.ddlPrinterInverse.close();
		this.dmlPrinterInverse.close();
	}

	private void setStructureData(){
		UI.print("Upgrade.setStructureData()");
		this.setTableData();
		this.setColumnData();
		this.setIdentityTables();
		this.setColumnsActionLists(); // need to get a list of columns that will be altered or added in order to determine actions for constraints
		this.setConstraintData();
		this.setIndexData();
	}

	private void setColumnsActionLists(){
		List<String> ret = new ArrayList<String>();
	    Iterator it = this.columnData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry colEntry = (Map.Entry)it.next();
	    	String fullColumnName = colEntry.getKey().toString();
	    	Map colData = (Map)colEntry.getValue();
	    	Map overview = (Map)colData.get("overview");

	    	if(overview.get("action").equals("alter-column"))
	    		this.columnsToBeAltered.add(fullColumnName);
	    	if(overview.get("action").equals("add-column"))
	    		this.columnsToBeAdded.add(fullColumnName);
	    }
	}

	private void setColumnData(){
		String sql =
		"SELECT \n" +
		"	LOWER(tables.name) table_name, \n" +
		"	LOWER(columns.name) column_name, \n" +
		"	columns.column_id, \n" +
		"	types.name data_type, \n" +
		"	columns.max_length, \n" +
		"	columns.precision, \n" +
		"	columns.scale, \n" +
		"	columns.is_nullable, \n" +
		"	columns.is_identity, \n" +
		"	ISNULL(indexes.is_primary_key, 0) is_primary_key, \n" +
		"   default_constraints.definition default_val \n" +
		"FROM \n" +
		"	sys.tables \n" +
		"JOIN \n" +
		"	sys.schemas ON ( \n" +
		"		schemas.schema_id = tables.schema_id) \n" +
		"JOIN \n" +
		"	sys.columns ON ( \n" +
		"		columns.object_id = tables.object_id) \n" +
		"JOIN \n" +
		"	sys.types ON ( \n" +
		"		types.user_type_id = columns.user_type_id) \n" +
		"LEFT JOIN \n" +
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
		"	tables.name IN "+Db.escStrJoin(this.allTableNames)+" \n" +
		"ORDER BY \n" +
		"	tables.name, \n" +
		"   columns.column_id";

		this.runSelectRowsAllDbs(sql);

		List<Map<String, String>> pastColumnList = this.latestResults.get("past");
		List<Map<String, String>> presentColumnList = this.latestResults.get("present");
		List<Map<String, String>> futureColumnList = this.latestResults.get("future");

		Map<String, Map<String, String>> pastColumnData = new LinkedHashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> presentColumnData = new LinkedHashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> futureColumnData = new LinkedHashMap<String, Map<String, String>>();

		// get an alphabetized list of all columns names from all dbs
		Set columnNameSet = new HashSet<String>();
		for(Map<String, String> pastColumn : pastColumnList){
			String fullColumnName = pastColumn.get("table_name")+"."+pastColumn.get("column_name");
			columnNameSet.add(fullColumnName);
			pastColumnData.put(fullColumnName, pastColumn);
		}

		for(Map<String, String> presentColumn : presentColumnList){
			String fullColumnName = presentColumn.get("table_name")+"."+presentColumn.get("column_name");
			columnNameSet.add(fullColumnName);
			presentColumnData.put(fullColumnName, presentColumn);
		}

		for(Map<String, String> futureColumn : futureColumnList){
			String fullColumnName = futureColumn.get("table_name")+"."+futureColumn.get("column_name");
			columnNameSet.add(fullColumnName);
			futureColumnData.put(fullColumnName, futureColumn);
		}

		allColumnNames = new ArrayList<>(columnNameSet);
		Collections.sort(this.allColumnNames);

		for(String fullColumnName : this.allColumnNames){
			String[] parts = fullColumnName.split("\\.");
			String table = parts[0];
			String column = parts[1];

			Map<String, Map<String, String>> tmpColumnData = new LinkedHashMap<String, Map<String, String>>();
			Map<String, String> tmpOverview = new LinkedHashMap<String, String>();

			Map<String, String> tmpPastColumnData = pastColumnData.get(fullColumnName);
			Map<String, String> tmpPresentColumnData = presentColumnData.get(fullColumnName);
			Map<String, String> tmpFutureColumnData = futureColumnData.get(fullColumnName);

			tmpColumnData.put("past", tmpPastColumnData);
			tmpColumnData.put("present", presentColumnData.get(fullColumnName));
			tmpColumnData.put("future", futureColumnData.get(fullColumnName));

			// this has to be saved before we can add the overview data in order to allow this.compareColumnStructure() to work
			this.columnData.put(fullColumnName, tmpColumnData);

			boolean isTableInPast = this.tableData.get(table).get("past").equals("1");
			boolean isTableInPresent = this.tableData.get(table).get("present").equals("1");
			boolean isTableInFuture = this.tableData.get(table).get("future").equals("1");

			boolean isColInPast = tmpPastColumnData != null;
			boolean isColInPresent = tmpPresentColumnData != null;
			boolean isColInFuture = tmpFutureColumnData != null;

			String status = null;
			String statusCode = null;
			String action = null;

			if(isColInPast && !isColInPresent && !isColInFuture){
				status = "Column is only in past database.";
				statusCode = "past-only";
			}
			else if(!isColInPast && isColInPresent && !isColInFuture){
				status = "Column is only in present database.";
				statusCode = "present-only";
			}
			else if(!isColInPast && !isColInPresent && isColInFuture){
				status = "Column is only in future database.";
				statusCode = "future-only";
			}
			else if(isColInPast && isColInPresent && !isColInFuture){
				status = "Column is in past and present databases.";
				statusCode = "past-and-present";
			}
			else if(isColInPast && !isColInPresent && isColInFuture){
				status = "Column is in past and future databases.";
				statusCode = "past-and-future";
			}
			else if(!isColInPast && isColInPresent && isColInFuture){
				status = "Column is in present and future databases.";
				statusCode = "present-and-future";
			}
			else if(isColInPast && isColInPresent && isColInFuture){
				status = "Column is in all three databases.";
				statusCode = "all-three";
			}
			else
				Debug.die("Not coded yet.");

			// override status for new tables
			if(isTableInFuture && !isTableInPresent){
				status = "Column will be created when new table is created.";
				statusCode = "new-table";
			}

			tmpOverview.put("status", status);
			tmpOverview.put("status-code", statusCode);

			if(isTableInFuture && !isTableInPresent)
				action = "none";
			else if(isColInFuture && !isColInPresent)
				action = "add-column";
			else if(isColInFuture && isColInPresent){
				action = this.compareColumnStructure(table, column, false) ? "none" : "alter-column";
			}
			else if(isColInPresent && !isColInFuture)
				action = "none";
			else{
				Debug.expose(tmpOverview);
				Debug.die("Not coded yet.");
			}
			tmpOverview.put("action", action);
			tmpColumnData.put("overview", tmpOverview);
		}
		Util.printMemoryUsage();
	}

	private void setTableData(){
		UI.print("Upgrade.setTableData()");
		List<String> pastTables = getTables(this.dbMap.get("past"));
		List<String> presentTables = getTables(this.dbMap.get("present"));
		List<String> futureTables = getTables(this.dbMap.get("future"));

		Set<String> allTablesSet = new HashSet<String> (pastTables);
		allTablesSet.addAll(presentTables);
		allTablesSet.addAll(futureTables);

		List<String> allTables = new ArrayList<>(allTablesSet);
		Collections.sort(allTables);

		this.allTableNames = allTables;

		for(String table : allTables){
			Map<String, String> tmpMap = new LinkedHashMap<String, String>();
			boolean isInPast = pastTables.contains(table);
			boolean isInPresent = presentTables.contains(table);
			boolean isInFuture = futureTables.contains(table);

			tmpMap.put("past", isInPast ? "1" : "0" );
			tmpMap.put("present", isInPresent ? "1" : "0" );
			tmpMap.put("future", isInFuture ? "1" : "0" );

			String status = null;
			String statusCode = null;

			if(isInPast && !isInPresent && !isInFuture){
				status = "Table is only in past database.";
				statusCode = "past-only";
			}
			else if(!isInPast && isInPresent && !isInFuture){
				status = "Table is only in present database.";
				statusCode = "present-only";
			}
			else if(!isInPast && !isInPresent && isInFuture){
				status = "Table is only in future database.";
				statusCode = "future-only";
			}
			else if(isInPast && isInPresent && !isInFuture){
				status = "Table is in past and present databases.";
				statusCode = "past-and-present";
			}
			else if(isInPast && !isInPresent && isInFuture){
				status = "Table is in past and future databases.";
				statusCode = "past-and-future";
			}
			else if(!isInPast && isInPresent && isInFuture){
				status = "Table is in present and future databases.";
				statusCode = "present-and-future";
			}
			else if(isInPast && isInPresent && isInFuture){
				status = "Table is in all three databases.";
				statusCode = "all-three";
			}
			else
				Debug.die("Not coded yet.");

			tmpMap.put("status", status);
			tmpMap.put("status-code", statusCode);

			this.tableData.put(table, tmpMap);
		}
	}

	private void setConstraintData(){
		this.setPrimaryKeyData();
		this.setForeignKeyData();
		this.setDefaultConstraintData();
		this.setDefaultConstraintStatus();
	}

	private void setForeignKeyData(){
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
			"		schemas.schema_id = foreign_keys.schema_id) \n "+
			"JOIN \n "+
			"	sys.foreign_key_columns ON ( \n "+
			"		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n "+
			"JOIN \n "+
			"	sys.tables parent_tables ON ( \n "+
			"		parent_tables.object_id = foreign_keys.parent_object_id AND \n "+
			"		parent_tables.name IN "+Db.escStrJoin(this.allTableNames)+") \n "+
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

		this.runSelectRowsAllDbs(sql);

		List<Map<String, String>> tmpPastFkList = this.latestResults.get("past");
		List<Map<String, String>> tmpPresentFkList = this.latestResults.get("present");
		List<Map<String, String>> tmpFutureFkList = this.latestResults.get("future");

		List<Map<String, String>> pastFkList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> presentFkList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> futureFkList = new ArrayList<Map<String, String>>();

		// run through each tmpFkList to create list of cols
		// past
		String lastFkName = null;
		Map newFkMap = new HashMap<String, String>();

		for(Map tmpFkMap : tmpPastFkList){
			String tmpFkName = tmpFkMap.get("fk_name").toString();

			if(lastFkName == null){ // first iteration
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}else if(lastFkName.equals(tmpFkName)){ // FK with multi cols
				newFkMap.put("parent-column-list", newFkMap.get("parent-column-list")+","+tmpFkMap.get("parent_column"));
				newFkMap.put("ref-column-list", newFkMap.get("ref-column-list")+","+tmpFkMap.get("ref_column"));
			}
			else{ // different FK from previous iteration
				pastFkList.add(newFkMap);
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}

			lastFkName = tmpFkName;
		}

		if(tmpPastFkList.size() > 0)
			pastFkList.add(newFkMap);

		// present
		lastFkName = null;
		newFkMap = new HashMap<String, String>();
		for(Map tmpFkMap : tmpPresentFkList){
			String tmpFkName = tmpFkMap.get("fk_name").toString();

			if(lastFkName == null){ // first iteration
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}else if(lastFkName.equals(tmpFkName)){ // FK with multi cols
				newFkMap.put("parent-column-list", newFkMap.get("parent-column-list")+","+tmpFkMap.get("parent_column"));
				newFkMap.put("ref-column-list", newFkMap.get("ref-column-list")+","+tmpFkMap.get("ref_column"));
			}
			else{ // different FK from previous iteration
				presentFkList.add(newFkMap);
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}

			lastFkName = tmpFkName;

		}
		if(tmpPresentFkList.size() > 0)
			presentFkList.add(newFkMap);

		// future
		lastFkName = null;
		newFkMap = new HashMap<String, String>();
		for(Map tmpFkMap : tmpFutureFkList){
			String tmpFkName = tmpFkMap.get("fk_name").toString();

			if(lastFkName == null){ // first iteration
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}else if(lastFkName.equals(tmpFkName)){ // FK with multi cols
				newFkMap.put("parent-column-list", newFkMap.get("parent-column-list")+","+tmpFkMap.get("parent_column"));
				newFkMap.put("ref-column-list", newFkMap.get("ref-column-list")+","+tmpFkMap.get("ref_column"));
			}
			else{ // different FK from previous iteration
				futureFkList.add(newFkMap);
				newFkMap = new HashMap<String, String>();
				newFkMap.put("fk-name", tmpFkMap.get("fk_name"));
				newFkMap.put("parent-table", tmpFkMap.get("parent_table"));
				newFkMap.put("parent-column-list", tmpFkMap.get("parent_column"));
				newFkMap.put("ref-table", tmpFkMap.get("ref_table"));
				newFkMap.put("ref-column-list", tmpFkMap.get("ref_column"));
			}

			lastFkName = tmpFkName;
		}
		if(tmpFutureFkList.size() > 0)
			futureFkList.add(newFkMap);

		// get an alphabetized list of all fk identifiers

		HashSet<String> foreignKeyIdSet = new HashSet<String>();

		for(Map<String, String> tmpForeignKey : pastFkList){
			String foreignKeyId = "fk|"+tmpForeignKey.get("parent-table")+"|"+tmpForeignKey.get("parent-column-list")+"|"+tmpForeignKey.get("ref-table")+"|"+tmpForeignKey.get("ref-column-list");
			foreignKeyIdSet.add(foreignKeyId);
			tmpForeignKey.put("id", foreignKeyId);
		}

		for(Map<String, String> tmpForeignKey : presentFkList){
			String foreignKeyId = "fk|"+tmpForeignKey.get("parent-table")+"|"+tmpForeignKey.get("parent-column-list")+"|"+tmpForeignKey.get("ref-table")+"|"+tmpForeignKey.get("ref-column-list");
			foreignKeyIdSet.add(foreignKeyId);
			tmpForeignKey.put("id", foreignKeyId);
		}

		for(Map<String, String> tmpForeignKey : futureFkList){
			String foreignKeyId = "fk|"+tmpForeignKey.get("parent-table")+"|"+tmpForeignKey.get("parent-column-list")+"|"+tmpForeignKey.get("ref-table")+"|"+tmpForeignKey.get("ref-column-list");
			foreignKeyIdSet.add(foreignKeyId);
			tmpForeignKey.put("id", foreignKeyId);
		}

		List<String> allForeignKeyIds = new ArrayList<>(foreignKeyIdSet);
		Collections.sort(allForeignKeyIds);

		List<String> tmpCustomDroppedFks = new ArrayList<String>();
		List<String> tmpMissingFks = new ArrayList<String>();
		List<String> tmpPastOnlyFks = new ArrayList<String>();


		for(String fkIdentifier : allForeignKeyIds){
			// get tables and cols from id
			Map<String, String> tmpMap = new LinkedHashMap<String, String>();

			String[] tmpParts = fkIdentifier.split("\\|");
			String parentTable = tmpParts[1];
			String parentColumnList = tmpParts[2];
			String refTable = tmpParts[3];
			String refColumnList = tmpParts[4];

			tmpMap.put("type", "FK");
			tmpMap.put("parent-table", parentTable);
			tmpMap.put("parent-column-list", parentColumnList);
			tmpMap.put("ref-table", refTable);
			tmpMap.put("ref-column-list", refColumnList);

			// get fk names from each env
			Map<String, String> tmpPastFk = Db.filterForRow("id", fkIdentifier, pastFkList);
			Map<String, String> tmpPresentFk = Db.filterForRow("id", fkIdentifier, presentFkList);
			Map<String, String> tmpFutureFk = Db.filterForRow("id", fkIdentifier, futureFkList);

			String pastFkName = null;
			String presentFkName = null;
			String futureFkName = null;

			if(tmpPastFk != null)
				pastFkName = tmpPastFk.get("fk-name");

			if(tmpPresentFk != null)
				presentFkName = tmpPresentFk.get("fk-name");

			if(tmpFutureFk != null)
				futureFkName = tmpFutureFk.get("fk-name");

			tmpMap.put("past-name", pastFkName);
			tmpMap.put("present-name", presentFkName);
			tmpMap.put("future-name", futureFkName);

			// saving here so that it can be used by this.isForeignKeyLinkedToAlteredColumn()
			this.constraintData.put(fkIdentifier, tmpMap);

			String status = null;
			String statusCode = null;
			String action = null;

			if(pastFkName != null && presentFkName != null && futureFkName != null){
				status = "FK exists in all envs";
				statusCode = "all-three";
				// need to determine if this FK is linked to any columns that will be altered
				action = this.isForeignKeyLinkedToAlteredColumn(fkIdentifier) ? "drop" : "none";
			}
			else if(pastFkName != null && presentFkName == null && futureFkName != null){

				tmpCustomDroppedFks.add(fkIdentifier);

				if(this.customSettings.customDroppedForeignKeys.contains(fkIdentifier)){
					status = "FK exists in both default versions but not in present. So it was custom dropped. It will not be added back.";
					statusCode = "custom-fk-drop";
					action = "none";
				}
				else{
					Debug.pr("Custom Dropped FK needs to be added to the custom settings.");
					Debug.pr(fkIdentifier);
					Debug.expose(tmpMap);
					Debug.die();
				}
			}
			else if(pastFkName == null && presentFkName == null && futureFkName != null){
				status = "New FK added in new version.";
				statusCode = "new-fk";
				action = "none";
			}
			else if(pastFkName == null && presentFkName != null && futureFkName == null){

				if(this.customSettings.presentOnlyForeignKeys.contains(fkIdentifier)){
					status = "Custom FK. Will be dropped and readded.";
					statusCode = "custom-fk";
					action = this.isForeignKeyLinkedToAlteredColumn(fkIdentifier) ? "drop" : "none";
				}
				else{
					Debug.pr("Custom FK needs to be added to the custom settings.");
					Debug.pr(fkIdentifier);
					Debug.expose(tmpMap);
				}
			}
			else if(pastFkName != null && presentFkName != null && futureFkName == null){
				status = "FK is dropped in future version.";
				statusCode = "future-drop";
				action = "drop";
			}
			else if(pastFkName != null && presentFkName == null && futureFkName == null){
				status = "FK only exists in past DB.";
				statusCode = "past-only";
				action = "none";
			}
			else{
				Debug.expose(fkIdentifier);
				Debug.expose(tmpMap);
			}

			tmpMap.put("status", status);
			tmpMap.put("status-code", statusCode);
			tmpMap.put("action", action);

			this.constraintData.put(fkIdentifier, tmpMap);
		}
	}

	private void setPrimaryKeyData(){
		// use STUFF(..) FOR XML_PATH() to get a csv of primary key columns
		String sql =
			"WITH base_data AS ( \n" +
			"	SELECT \n" +
			"		indexes.name index_name, \n" +
			"		LOWER(tables.name) table_name, \n" +
			"		LOWER(columns.name) column_name \n" +
			"	FROM  \n" +
			"		sys.tables \n" +
			"	JOIN \n" +
			"		sys.schemas ON ( \n" +
			"			schemas.schema_id = tables.schema_id) \n" +
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
			"		tables.name IN "+Db.escStrJoin(this.allTableNames)+" \n" +
			") \n" +
			"SELECT \n" +
			"	outer_table.index_name, \n" +
			"	outer_table.table_name, \n" +
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

		this.runSelectRowsAllDbs(sql);

		List<Map<String, String>> pastPkList = this.latestResults.get("past");
		List<Map<String, String>> presentPkList = this.latestResults.get("present");
		List<Map<String, String>> futurePkList = this.latestResults.get("future");

		for(String table : this.allTableNames){
			String pkIdentifier = "pk:"+table;

			Map<String, String> tmpMap = new LinkedHashMap<String, String>();
			tmpMap.put("type", "PK");

			boolean isTableInPast = this.tableData.get(table).get("past").equals("1");
			boolean isTableInPresent = this.tableData.get(table).get("present").equals("1");
			boolean isTableInFuture = this.tableData.get(table).get("future").equals("1");

			if(isTableInPast){
				Map<String, String> tmpPkData = Db.filterForRow("table_name", table, pastPkList);
				if(tmpPkData != null){
					tmpMap.put("past-name", tmpPkData.get("index_name"));
					tmpMap.put("past-column-list", tmpPkData.get("column_list"));
				}
			}

			if(isTableInPast){
				Map<String, String> tmpPkData = Db.filterForRow("table_name", table, presentPkList);
				if(tmpPkData != null){
					tmpMap.put("present-name", tmpPkData.get("index_name"));
					tmpMap.put("present-column-list", tmpPkData.get("column_list"));
				}
			}

			if(isTableInPast){
				Map<String, String> tmpPkData = Db.filterForRow("table_name", table, futurePkList);
				if(tmpPkData != null){
					tmpMap.put("future-name", tmpPkData.get("index_name"));
					tmpMap.put("future-column-list", tmpPkData.get("column_list"));
				}
			}

			String status = null;
			String statusCode = null;

			if(isTableInPresent && isTableInFuture){
				if(tmpMap.get("present-column-list") == null && tmpMap.get("future-column-list") == null){
					status = "No PK in present or future table.";
					statusCode = "no-pks";
				}
				else if(tmpMap.get("present-column-list") == null && tmpMap.get("future-column-list") != null){
					status = "PK ony exist in future. Missing in present";
					statusCode = "pk-future-only";
				}
				else if(tmpMap.get("present-column-list") != null && tmpMap.get("future-column-list") == null){
					status = "PK ony exist in present. Missing in future";
					statusCode = "pk-present-only";
					Debug.expose(tmpMap);
					Debug.die("not coded yet");
				}
				else if(tmpMap.get("present-column-list").equals(tmpMap.get("future-column-list"))){
					status = "PK is the same for present and future";
					statusCode = "pk-match";
				}
				else if(!tmpMap.get("present-column-list").equals(tmpMap.get("future-column-list"))){
					if(tmpMap.get("present-column-list").equals(tmpMap.get("past-column-list"))){
						status = "The future defaul PK is different from the past defaul PK";
						statusCode = "pk-defaul-update";
					}
					else{
						String tmpLookup = pkIdentifier+"|"+tmpMap.get("present-name")+"|"+tmpMap.get("present-column-list");
						if(this.customSettings.presentCustomPrimaryKeys.contains(tmpLookup)){
							status = "present PK is custom";
							statusCode = "custom-pk";
						}
						else{
							Debug.expose(this.customSettings.presentCustomPrimaryKeys);
							status = "present PK doesnt match past or future";
							statusCode = "pk-mismatch";
							Debug.die("PKs dont match");
						}
					}
				}
				else{
					Debug.expose(tmpMap);
					Debug.die("not coded yet");
				}
			}
			else if(isTableInPresent && !isTableInFuture){
				status = "Custom Table";
				statusCode = "custom-table";
			}
			else if(!isTableInPresent && isTableInFuture){
				status = "New Table";
				statusCode = "new-table";
			}
			else{
				Debug.expose(tmpMap);
				Debug.die("Not coded yet.");
			}

			tmpMap.put("status", status);
			tmpMap.put("status-code", statusCode);

			String action = null;

			// search the columns for ones that are getting altered - will need to drop those constraints
			if(tmpMap.get("present-name") == null)
				action = "none";
			else{
				String[] primaryKeyCols = tmpMap.get("present-column-list").split(",");

				for(String primaryKeyCol : primaryKeyCols){
					String fullColumnName = table+"."+primaryKeyCol;

					// see if this column is being altered
					if(this.columnsToBeAltered.contains(fullColumnName)){
						switch(statusCode){
						case "pk-match":
							action = "drop";
							break;
						case "pk-present-only":
							//action = "save-then-drop";
							Debug.die("not coded yet");
							break;
						case "pk-future-only":
							action = "none";
							break;
						default:
							Debug.pr(statusCode);
							Debug.die("not coded yet");
							break;
						}
					}
					else
						action = "none";

					if(action.equals("drop")){
						// can break out of the loop since the action has already been determined
						break;
					}
				}
			}

			tmpMap.put("action", action);

			this.constraintData.put(pkIdentifier, tmpMap);
		}
	}

	private void setDefaultConstraintData(){
		String sql =
				"SELECT \n" +
				"	default_constraints.name, \n" +
				"	default_constraints.definition, \n" +
				"	CONCAT(tables.name, '.',columns.name) full_column_name  \n" +
				"FROM \n" +
				"	sys.default_constraints \n" +
				"JOIN \n" +
				"	sys.schemas ON ( \n" +
				"		schemas.schema_id = default_constraints.schema_id) \n" +
				"JOIN \n" +
				"	sys.tables ON ( \n" +
				"		tables.object_id = default_constraints.parent_object_id AND \n"+
				"     	tables.name IN "+Db.escStrJoin(this.allTableNames)+") \n" +
				"JOIN \n" +
				"	sys.columns ON ( \n" +
				"		columns.object_id = tables.object_id AND \n" +
				"		columns.column_id = default_constraints.parent_column_id)";
		this.runSelectRowsAllDbs(sql);

		for(String fullColumnName: this.allColumnNames){
			String dcIdentifier = "dc:"+fullColumnName;

			Map<String, String> tmpMap = new LinkedHashMap<String, String>();

			// check for default values in each env
			Map<String, String> pastDcData= Db.filterForRow("full_column_name", fullColumnName, this.latestResults.get("past"));
			Map<String, String> presentDcData= Db.filterForRow("full_column_name", fullColumnName, this.latestResults.get("present"));
			Map<String, String> futureDcData= Db.filterForRow("full_column_name", fullColumnName, this.latestResults.get("future"));

			String pastDefaultValue = null;
			String presentDefaultValue = null;
			String futureDefaultValue = null;

			//@todo-combine
			if(pastDcData != null){
				tmpMap.put("past-name", pastDcData.get("name"));
				pastDefaultValue = pastDcData.get("definition").trim();
				tmpMap.put("past-value", pastDefaultValue);
			}

			if(presentDcData != null){
				tmpMap.put("present-name", presentDcData.get("name"));
				presentDefaultValue = presentDcData.get("definition").trim();
				tmpMap.put("present-value", presentDefaultValue);
			}

			if(futureDcData != null){
				tmpMap.put("future-name", futureDcData.get("name"));
				futureDefaultValue = futureDcData.get("definition").trim();
				tmpMap.put("future-value", futureDefaultValue);
			}

			if(tmpMap.size() > 0){
				tmpMap.put("type", "DC");

				this.constraintData.put(dcIdentifier, tmpMap);
			}
		}
	}

	private void setDefaultConstraintStatus(){
		Debug.pr("Upgrade.setDefaultConstraintStatus()");
		// search for defaults whose column is being altered

		for(String fullColumnName : this.columnsToBeAltered){
			String status = null;
			String statusCode = null;
			String action = null;

			Map<String, String> dcConstraintData = this.constraintData.get("dc:"+fullColumnName);

			if(dcConstraintData != null){
				String pastDefaultValue = dcConstraintData.get("past-value");
				String presentDefaultValue = dcConstraintData.get("present-value");
				String futureDefaultValue = dcConstraintData.get("future-value");

				if(pastDefaultValue != null)
					pastDefaultValue = pastDefaultValue.trim();

				if(presentDefaultValue != null)
					presentDefaultValue = presentDefaultValue.trim();

				if(futureDefaultValue != null)
					futureDefaultValue = futureDefaultValue.trim();

				if(presentDefaultValue != null){
					Debug.pr("This column has a default value.");
					if(Upgrade.isEqualDefaultValue(presentDefaultValue, futureDefaultValue)){
						status = "This column is being altered has the same default as the future. Dropping ";
						statusCode = "drop-future-match";
						action = "drop";
					}
					else if(futureDefaultValue == null && Upgrade.isEqualDefaultValue(pastDefaultValue, presentDefaultValue)){
						Debug.pr("Past and present have same value, and future has no value");
						status = "Future default has been dropped. Will drop in present as well.";
						statusCode = "future-drop";
						action = "drop";
					}
					else{
						Debug.expose(fullColumnName);
						Debug.expose(pastDefaultValue);
						Debug.expose(presentDefaultValue);
						Debug.expose(futureDefaultValue);
						Debug.die("not coded yet");
					}
				}
			}

			if(action != null){
				dcConstraintData.put("status", status);
				dcConstraintData.put("status-code", statusCode);
				dcConstraintData.put("action", action);
			}
		}
	}

	private void setIndexData(){
		String sql =
		"SELECT \n" +
		"	indexes.index_id, \n" +
		"	indexes.name index_name, \n" +
		"	tables.name table_name, \n" +
		"	columns.name column_name, \n" +
		"	indexes.is_disabled \n" +
		"FROM \n" +
		"	sys.indexes \n" +
		"JOIN \n" +
		"	sys.tables ON ( \n" +
		"		tables.object_id = indexes.object_id) \n" +
		"JOIN \n" +
		"	sys.schemas ON ( \n" +
		"		schemas.schema_id = tables.schema_id) \n" +
		"JOIN \n" +
		"	sys.index_columns ON ( \n" +
		"		index_columns.index_id = indexes.index_id AND \n" +
		"		index_columns.object_id = tables.object_id) \n" +
		"JOIN \n" +
		"	sys.columns ON ( \n" +
		"		columns.object_id = tables.object_id AND \n" +
		"		columns.column_id = index_columns.column_id)  \n" +
		"WHERE \n" +
		"	tables.name IN "+Db.escStrJoin(this.allTableNames)+" AND \n" +
		"   indexes.is_primary_key = 0 \n " + // PKs are handled separately
		"ORDER BY \n" +
		"	tables.name, \n" +
		"	indexes.name, \n" +
		"	columns.name";

		this.runSelectRowsAllDbs(sql);

		List<Map<String, String>> pastIndexList = this.latestResults.get("past");
		List<Map<String, String>> presentIndexList = this.latestResults.get("present");
		List<Map<String, String>> futureIndexList = this.latestResults.get("future");

		String lastIndexName = null;
		String indexIdentifier = null;

		Map<String, String> tmpMap = null;

		for(Map<String, String> tmpIndexData: presentIndexList){
			String table = tmpIndexData.get("table_name");
			String column = tmpIndexData.get("column_name");
			String indexName = tmpIndexData.get("index_name");
			String isDisabled = tmpIndexData.get("is_disabled");

			if(lastIndexName == null){
				indexIdentifier = "idx:"+table+"."+column;
				tmpMap = new LinkedHashMap<String, String>();
				tmpMap.put("table", table);
				tmpMap.put("present-name", indexName);
				tmpMap.put("present-is-disabled", isDisabled);
			}
			else if(indexName.equals(lastIndexName)){
				// same index name as last time - add column name to ID
				indexIdentifier += ","+column;
			}
			else{
				this.indexData.put(indexIdentifier, tmpMap);
				indexIdentifier = "idx:"+tmpIndexData.get("table_name")+"."+tmpIndexData.get("column_name");
				tmpMap = new LinkedHashMap<String, String>();
				tmpMap.put("table", table);
				tmpMap.put("present-name", indexName);
				tmpMap.put("present-is-disabled", isDisabled);
			}

			lastIndexName = indexName;
		}

		this.indexData.put(indexIdentifier, tmpMap);
	}

	private void disableIndexes(){
		Debug.pr("Upgrade.disableIndexes()");
	    Iterator it = this.indexData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry indexEntry = (Map.Entry)it.next();

	    	String tmpIndexId = indexEntry.getKey().toString();
	    	tmpIndexId = tmpIndexId.replace("idx:", "");
	    	String[] parts = tmpIndexId.split("\\.");
	    	String table = parts[0];
	    	String columnList = parts[1];

	    	String[] columns = columnList.split(",");

	    	Map tmpIndexData = (Map)indexEntry.getValue();

	    	String indexName  = tmpIndexData.get("present-name").toString();
	    	boolean isDisabled = tmpIndexData.get("present-is-disabled").equals("1");

	    	for(String column : columns){
	    		String fullColumnName = table+"."+column;
	    		if(this.columnsToBeAltered.contains(fullColumnName) && !isDisabled){
	    			Debug.pr(fullColumnName);

	    			String applySql = "ALTER INDEX "+indexName+" ON "+table+" DISABLE;";
	    			String inverseSql = "ALTER INDEX "+indexName+" ON "+table+" REBUILD;";

	    			this.saveDdl(applySql, inverseSql);
	    		}
	    	}
	    }
	}

	private void dropIndexes(){
	    Iterator it = this.indexData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry indexEntry = (Map.Entry)it.next();
	    	String tmpIndexId = indexEntry.getKey().toString();
	    	tmpIndexId = tmpIndexId.replace("idx:", "");
	    	String[] parts = tmpIndexId.split("\\.");
	    	String table = parts[0];
	    	String columnList = parts[1];
	    	String[] columns = columnList.split(",");
	    	Map tmpIndexData = (Map)indexEntry.getValue();

	    	String indexName  = tmpIndexData.get("present-name").toString();
	    	boolean isDisabled = tmpIndexData.get("present-is-disabled").equals("1");
	    	boolean dropIndex = false;

	    	for(String column : columns){
	    		String fullColumnName = table+"."+column;

	    		if(this.columnsToBeAltered.contains(fullColumnName)){
	    			dropIndex = true;
	    			break;
	    		}
	    	}
	    	if(dropIndex){
	    		String applySql = "DROP INDEX "+table+"."+indexName+";";
	    		String inverseSql = "CREATE INDEX "+indexName+" ON "+table+" ("+columnList+"); ";
	    		this.saveDdl(applySql, inverseSql);
	    	}
	    }
	}

	public static boolean isEqualDefaultValue(String valueA, String valueB){
		if(valueA.equals(valueB))
			return true;

		boolean isEqual = false;

		return isEqual;
	}

	private boolean isForeignKeyLinkedToAlteredColumn(String fkIdentifier){
		Map<String, String> foreignKeyData = this.constraintData.get(fkIdentifier);
		List<String> allLinkedColumns = new ArrayList<String>();

		// get list of all columns linked to this constraint - parent and ref
		String parentTable = foreignKeyData.get("parent-table");
		String[] parentColumns = foreignKeyData.get("parent-column-list").split(",");

		for(String parentColumn : parentColumns)
			allLinkedColumns.add(parentTable+"."+parentColumn);

		String refTable = foreignKeyData.get("ref-table");
		String[] refColumns = foreignKeyData.get("ref-column-list").split(",");

		for(String refColumn : refColumns)
			allLinkedColumns.add(refTable+"."+refColumn);

		for(String linkedColumn: allLinkedColumns){
			if(this.columnsToBeAltered.contains(linkedColumn)){
				return true;
			}
		}

		return false;
	}

	private void applyColumnChanges(){
		this.alterColumns();
		this.fixIdentityColumns();
		this.addColumns();
	}

	private void alterColumns(){
	    for(String fullColumnName : this.columnsToBeAltered){
	    	Debug.pr(fullColumnName);
	    	String[] parts = fullColumnName.split("\\.");
	    	String table  = parts[0];
	    	String column = parts[1];

	    	this.compareColumnStructure(table, column, true);
	    }
	}

	private void fixIdentityColumns(){
		// parse through all columns for mismatches in is_identity
	    Iterator it = this.columnData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry colEntry = (Map.Entry)it.next();
	    	String fullColumnName = (String)colEntry.getKey();
	    	//Debug.pr(fullColumnName);
	    	Map colData = (Map)colEntry.getValue();
	    	Map<String, String> presentData = (Map<String, String>) colData.get("present");
	    	Map<String, String> futureData = (Map<String, String>) colData.get("future");
			if(presentData == null || futureData == null)
				continue;

			String presentIsIdentity = presentData.get("is_identity");
			String futureIsIdentity = futureData.get("is_identity");

			if(!presentIsIdentity.equals(futureIsIdentity)){
		    	String[] parts = fullColumnName.split("\\.");
		    	String table  = parts[0];
		    	String column = parts[1];
				if(futureIsIdentity.equals("1")){ // column needs to be set to IDENTITY in present
					this.renameColumn(table, column, "zz_"+column);
					this.addColumn(table, column);
					//determine if there is any data to move
					String sql =
					"SELECT \n" +
					"	COUNT(*) count \n" +
					"FROM \n" +
					"	"+table+" \n" +
					"WHERE \n" +
					"	"+fullColumnName+" IS NOT NULL";

					Integer count = Integer.parseInt(this.dbMap.get("present").selectItem(sql));
					if(count > 0)
						Debug.die("not coded yet - need to move identity column data ");

					sql = "ALTER TABLE "+table+" DROP COLUMN zz_"+column+";";
					this.saveDdl(sql);
				}
			}
	    }
	}

	private void renameColumn(String table, String currentName, String newName){
		String sql = "sp_rename '"+table+"."+currentName+"', '"+newName+"', 'COLUMN';";
		this.saveDdl(sql);
	}

	private void addColumns(){
	    for(String fullColumnName : this.columnsToBeAdded){
	    	Debug.pr(fullColumnName);
	    	String[] parts = fullColumnName.split("\\.");
	    	String table  = parts[0];
	    	String column = parts[1];
	    	this.addColumn(table, column);
	    }
	}

	public boolean compareTables(boolean autoFix){
		Debug.pr("Upgrade.udpateTables()");
		return this.compareTableData("tbls", autoFix);
	}

	public String getPrimaryKey(String table, MyDb myDb){
		String primaryKey = null;

		List<String> primaryKeys = myDb.getPrimaryKeys(table);
		return primaryKeys.get(0);
	}

	// assume we want it from the future
	public String[] getPrimaryKeyColumns(String table){
		for (Entry<String, Map<String, String>> entry : this.constraintData.entrySet()){

      Map<String, String> constraintMap = entry.getValue();

      if(constraintMap.get("type").equals("PK")){
      	String constraintName = entry.getKey();
      	String constraintTable = constraintName.replace("pk:", "");
      	if(table.equals(constraintTable)){
      		String columnList = constraintMap.get("future-column-list");
      		return columnList.split(",");
      	}
      }
		}

		return null;
	}

	public boolean compareTableData(String table, boolean autoFix){
		String presentPrimaryKey = this.getPrimaryKey(table, this.dbMap.get("present"));
		String futurePrimaryKey = this.getPrimaryKey(table, this.dbMap.get("future"));

		boolean match = true;

		if(!presentPrimaryKey.equals(futurePrimaryKey)){
			Debug.pr("The primary keys dont match for present and future DBs for table: "+table);
			Debug.pr("present: "+ presentPrimaryKey);
			Debug.pr("future:" +futurePrimaryKey);
			match = false;
			Debug.die();
		}


		this.runSelectRowsAllDbs(sql);

		for (Map<String, String> futureRow : this.latestResults.get("future")){
			Debug.pr(futureRow.get(futurePrimaryKey));
			Map<String, String> presentRow = Db.filterForRow(futurePrimaryKey, futureRow.get(futurePrimaryKey), this.latestResults.get("present"));
			if(presentRow == null){
				Debug.pr("The present database does not have a row where  "+table+"."+futurePrimaryKey+ "=" +futureRow.get(futurePrimaryKey));
				match = false;
			}
			else{
				// the column exists in both the future and present dbs
				if(!this.compareRowValues(futureRow,  presentRow, table, futurePrimaryKey, futureRow.get(futurePrimaryKey), autoFix))
					match = false;
			}
		}

		return match;
	}

	private void addTables(){
	    Iterator it = this.tableData.entrySet().iterator();

	    // need a list of tables that exist in present
	    Set<String> existingTables = new HashSet<String>();//(this.allTableNames);
	    Set<String> missingTables = new HashSet<String>();

	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry tmpTableData = (Map.Entry)it.next();
	    	String table = tmpTableData.getKey().toString();
	    	Map tmpMap = (Map)tmpTableData.getValue();

	    	boolean isInPresent = tmpMap.get("present").toString().equals("1");
	    	boolean isInFuture = tmpMap.get("future").toString().equals("1");

	    	if(isInPresent)
	    		existingTables.add(table);

	    	if(isInFuture && !isInPresent)
	    		missingTables.add(table);
	    }

	    Integer loopIterationCount = 0;
	    while(true){
	    	loopIterationCount++;
	    	Debug.pr("***************************** New Loop iteration: "+loopIterationCount);
	    	Debug.pr("Num Missing Tables: "+missingTables.size());
	    	Debug.pr("Num Existing Tables: "+existingTables.size());
	    	Debug.pr("Num Total Tables: "+(existingTables.size() + missingTables.size()));
	    	Set<String> readyToBeAddedTables = new HashSet<String>();
	    	for(String missingTable : missingTables){
	    		List<String> requiredTables = this.getRequiredTables(missingTable, existingTables);
	    		if(requiredTables.size() == 0)
	    			readyToBeAddedTables.add(missingTable);
	    		else{
	    			Debug.pr("table: "+missingTable+" cant be added yet because of: ");
	    			Debug.expose(requiredTables);
	    			Debug.expose(requiredTables.contains(missingTable));
	    		}
	    	}

	    	if(readyToBeAddedTables.size() == 0){
	    		Debug.pr("no tables ready to be added");
		    	Debug.pr("Num Missing Tables: "+missingTables.size());
		    	Debug.pr("Num Existing Tables: "+existingTables.size());
	    	}
	    	else{
				for(String readyTable : readyToBeAddedTables){
					this.addTable(readyTable);
					existingTables.add(readyTable);
					missingTables.remove(readyTable);
				}
	    	}
	    	 if(missingTables.size() == 0)
	    		break;
	    }
	}

	// assumes this is an existing table in future that is needed in present
	private void addTable(String table){
		table = table.trim();
		Debug.pr("Upgrade.addTable("+table+")");

		// get ddl from future table
		String sql = "exec sp_GetDDL "+table+"'";
		Map<String, String> row = this.dbMap.get("future").selectRow(sql);
		String applySql = row.get("Item")+"; \n\n";
		String inverseSql = "DROP TABLE "+table+";";
		this.saveDdl(applySql, inverseSql);
	}

	//@todo - but this in the original data strucuture import
	// for now assume this just works for the future db
	private List<String> getRequiredTables(String table ){
		return this.getRequiredTables(table, new HashSet<String>(this.allTableNames));
	}

	private List<String> getRequiredTables(String table, Set<String> existingTables){
		Debug.pr("Upgrade.getRequiredTables("+table+")");
		List<String> ret = new ArrayList<String>();

		String sql  =
		"SELECT DISTINCT \n" +
		"	ref_tables.name ref_table \n" +
		"FROM \n" +
		" 	sys.foreign_keys \n" +
		"JOIN \n" +
		" 	sys.schemas ON ( \n" +
		" 		schemas.schema_id = foreign_keys.schema_id) \n" +
		"JOIN \n" +
		" 	sys.foreign_key_columns ON ( \n" +
		" 		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n" +
		"JOIN \n" +
		" 	sys.tables parent_tables ON ( \n" +
		" 		parent_tables.object_id = foreign_keys.parent_object_id AND \n" +
		" 		parent_tables.name = "+Db.escStr(table)+") \n" +
		"JOIN \n" +
		" 	sys.all_columns parent_cols ON ( \n" +
		" 		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n" +
		" 		parent_cols.column_id = foreign_key_columns.parent_column_id) \n" +
		"JOIN \n" +
		"	sys.tables ref_tables ON ( \n" +
		" 		ref_tables.object_id = foreign_keys.referenced_object_id AND \n" +
		"       ref_tables.name <> parent_tables.name AND \n" +
		"       ref_tables.name NOT IN "+Db.escStrJoin(existingTables)+") \n" +
		"JOIN \n" +
		" 	sys.all_columns ref_cols ON ( \n" +
		"		ref_cols.object_id =  foreign_key_columns.referenced_object_id AND \n" +
		" 		ref_cols.column_id = foreign_key_columns.referenced_column_id) \n" +
		"ORDER BY \n" +
		"   ref_tables.name";

		ret = this.dbMap.get("future").selectList(sql);

		return ret;
	}

	// take a list of tables and return them in order based on which ones have prereq tables
	// necessary for determining order of table creating and data insertion
	private List<String> getOrderedTables(String table, Set<String> existingTables){
		Debug.pr("Upgrade.getRequiredTables("+table+")");
		List<String> ret = new ArrayList<String>();

		String sql  =
		"SELECT DISTINCT \n" +
		"	ref_tables.name ref_table \n" +
		"FROM \n" +
		" 	sys.foreign_keys \n" +
		"JOIN \n" +
		" 	sys.schemas ON ( \n" +
		" 		schemas.schema_id = foreign_keys.schema_id) \n" +
		"JOIN \n" +
		" 	sys.foreign_key_columns ON ( \n" +
		" 		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n" +
		"JOIN \n" +
		" 	sys.tables parent_tables ON ( \n" +
		" 		parent_tables.object_id = foreign_keys.parent_object_id AND \n" +
		" 		parent_tables.name = "+Db.escStr(table)+") \n" +
		"JOIN \n" +
		" 	sys.all_columns parent_cols ON ( \n" +
		" 		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n" +
		" 		parent_cols.column_id = foreign_key_columns.parent_column_id) \n" +
		"JOIN \n" +
		"	sys.tables ref_tables ON ( \n" +
		" 		ref_tables.object_id = foreign_keys.referenced_object_id AND \n" +
		"       ref_tables.name <> parent_tables.name AND \n" +
		"       ref_tables.name NOT IN "+Db.escStrJoin(existingTables)+") \n" +
		"JOIN \n" +
		" 	sys.all_columns ref_cols ON ( \n" +
		"		ref_cols.object_id =  foreign_key_columns.referenced_object_id AND \n" +
		" 		ref_cols.column_id = foreign_key_columns.referenced_column_id) \n" +
		"ORDER BY \n" +
		"   ref_tables.name";

		ret = this.dbMap.get("future").selectList(sql);

		return ret;
	}

	private String getColumnDefinition(String table, String column, MyDb thisDb,  Map<String, String> changes, String mode){
		Map<String, String> structure = this.getColumnStructure(table, column, thisDb.name);

		String dataType = structure.get("data_type");
		Boolean isNullable = structure.get("is_nullable").equals("1");
		String defaultVal = structure.get("default_val");
		Integer precision =  Integer.parseInt(structure.get("precision"));
		Integer scale =  Integer.parseInt(structure.get("scale"));
		Integer maxLength =  Integer.parseInt(structure.get("max_length"));
		Boolean isIdentity = structure.get("is_identity").equals("1");
		if(defaultVal == null){
			// attempt to get default val from future flds
			defaultVal = this.getFieldValue(table, column, "dflt_val", thisDb);
		}

		if(changes.size() > 0){
		    Iterator it = changes.entrySet().iterator();
		    while (it.hasNext()) {
		    	// for now assume we want to trim both values
		    	Map.Entry pair = (Map.Entry)it.next();
		    	String key = pair.getKey().toString();
		    	String val = (String)pair.getValue();
		    	if(val != null)
		    		val = val.toString();

		    	switch(key){
		    	case "data_type":
		    		dataType = val;
		    		break;
		    	case "max_length":
		    		maxLength = Integer.parseInt(val);
		    		break;
		    	case "default_val":
		    		defaultVal = val;
		    		break;
		    	case "is_nullable":
		    		isNullable = val.equals("1");
		    		break;
		    	case "scale":
		    		scale = Integer.parseInt(val);
		    		break;
		    	case "precision":
		    		precision = Integer.parseInt(val);
		    		break;
		    	case "is_identity":
		    		isIdentity = val.equals("1");
		    		break;
		    	default:
		    		Debug.die("not coded yet: "+key);
		    	}
		    }
		}

		if(defaultVal != null){
			defaultVal = defaultVal.trim();
			if(defaultVal.equals("AUTOINCREMENT"))
				isIdentity = true;
		}

		String columnType;
		String columnNull;
		String columnDefault;
		String columnIdentity;

		switch(dataType){
		case "char":
		case "varchar":
			columnType = " "+dataType.toUpperCase()+"("+maxLength+") ";
			break;
		case "numeric":
			columnType = " NUMERIC("+precision+", "+scale+") ";
			break;
		case "int":
		case "tinyint":
		case "smallint":
		case "bigint":
		case "datetime":
		case "image": //?? need to test
			columnType = " "+dataType.toUpperCase()+" ";
			break;
		default:
			columnType = "";
			Debug.die("Unknown column type: "+dataType);
			break;
		}

		if(isNullable)
			columnNull =  " NULL ";
		else
			columnNull = " NOT NULL ";

		if(defaultVal == null)
			columnDefault =  "";
		else
			columnDefault = this.getFormattedDefault(table, column, defaultVal, dataType);

		// adding identitiy can only be done a column is being created - cant be editied for existing columns
		if(isIdentity && mode.equals("add")){

			String maxId = this.presentDb.selectItem(sql);
			columnIdentity = " IDENTITY ("+maxId+",1)";
		}
		else
			columnIdentity = "";

		if(mode.equals("alter")) // in sql server defaults for existing columns are set in a separate statement
			columnDefault = "";

		return column+columnType+columnIdentity+columnNull+columnDefault;
	}

	private List<String> getAlterColumnSqlList(String table, String column, MyDb thisDb,  Map<String, String> changes){
		String colDefinition = this.getColumnDefinition(table, column, thisDb, changes, "alter");
		List<String> retSql = new ArrayList<String>();

		// skip this step if default_val is the only change
		if(!(changes.size() == 1 && changes.containsKey("default_val")))
			retSql.add("ALTER TABLE "+table+" ALTER COLUMN "+colDefinition+";");

		if(changes.containsKey("default_val")){
			Debug.pr("Changes to default value");

			String dataType = thisDb.getColumnDataType(table, column);
			retSql.add("ALTER TABLE "+table+" ADD "+this.getFormattedDefault(table, column, changes.get("default_val"), dataType)+" FOR "+column+";");
		}

		return retSql;
	}

	private List<Map<String,String>> getForeignKeyData(String table, String column){
		return this.getForeignKeyData(table, column, this.dbMap.get("present"));
	}

	public static List<Map<String,String>> getForeignKeyData(String table, String column, MyDb thisDb){
		String sql = Upgrade.getForeignKeySql(table, column);
		List<Map<String, String>> rows = thisDb.selectRows(sql);
		return rows;
	}

	public static String getForeignKeySql(String table, String column){
		return Db.getForeignKeySql(table, column);
	}

	public static String getForeignKeysSql(String table, String status){

		table = table.trim();
		int isDisabled;
		if(status.equals("enabled"))
			isDisabled = 0;
		else
			isDisabled = 1;
		String sql =
		"SELECT \n"+
		"	foreign_keys.name \n"+
		"FROM \n"+
		"	sys.foreign_keys \n"+
		"JOIN \n"+
		"	sys.all_objects AS parent_objects ON ( \n"+
		"		parent_objects.object_id = foreign_keys.parent_object_id) \n"+
		"JOIN \n"+
		"	sys.foreign_key_columns ON ( \n"+
		"		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n"+
		"JOIN \n"+
		"	sys.all_columns parent_cols ON ( \n"+
		"		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n"+
		"		parent_cols.column_id = foreign_key_columns.parent_column_id) \n"+
		"WHERE \n"+
		"	parent_objects.name = "+Db.escStr(table)+" AND \n"+
		"	foreign_keys.is_disabled = "+Db.escInt(isDisabled)+" \n"+
		"ORDER BY \n" +
		"   foreign_keys.name;";

		return sql;
	}

	private void disableAllForeignKeyConstraintsxx( MyDb thisDb){
		List<String> retSql = new ArrayList<String>();

		List<String> tables = this.getTables(thisDb);
		List<String> disableSql = new ArrayList<String>();

		for(String table : tables){
			table = table.trim();
			String sql = this.getForeignKeysSql(table, "enabled");
			List<String> tmpForeignKeys = thisDb.selectList(sql);
			for(String foreignKey : tmpForeignKeys){
				foreignKey = foreignKey.trim();
				disableSql.add("ALTER TABLE "+table+" NOCHECK CONSTRAINT "+foreignKey+";");
			}
		}
		this.saveDdl(disableSql);
	}

	private void dropConstraints(){
		Debug.pr("Upgrade.dropConstraints()");
		this.dropForeignKeyConstraints();
		//@todo - check for FKs that need to be dropped before PKs can be dropped
		this.dropPrimaryKeyConstraints();
		this.dropDefaultConstraints();
	}

	private void dropForeignKeyConstraints(){
		Debug.pr("Upgrade.dropForeignKeyConstraints()");
	    Iterator it = this.constraintData.entrySet().iterator();
	    while (it.hasNext()) {

	    	// for now assume we want to trim both values
	    	Map.Entry constraintEntry = (Map.Entry)it.next();

	    	String tmpConstraintId = constraintEntry.getKey().toString();
	    	Map tmpConstraintData = (Map)constraintEntry.getValue();

	    	if(tmpConstraintData.get("type").equals("FK")){
		    	if(tmpConstraintData.get("action").equals("drop")){
			    	String presentName = tmpConstraintData.get("present-name").toString();
			    	String parentTable = tmpConstraintData.get("parent-table").toString();
			    	String parentCols = tmpConstraintData.get("parent-column-list").toString();
			    	String refTable = tmpConstraintData.get("ref-table").toString();
			    	String refCols = tmpConstraintData.get("ref-column-list").toString();

				    String applySql = "ALTER TABLE "+parentTable+" DROP CONSTRAINT "+presentName+";";
					  String inverseSql = "ALTER TABLE "+parentTable+" ADD CONSTRAINT "+presentName+" FOREIGN KEY ("+parentCols+") REFERENCES "+refTable+"("+refCols+");";

					this.saveDdl(applySql, inverseSql);
		    	}
	    	}
	    }
	}

	//@todo - add logic to also drop any PK that is related to a field that will require an IDENTITY change - aka rename->add->drop column
	private void dropPrimaryKeyConstraints(){
		Debug.pr("Upgrade.dropPrimaryKeyConstraints()");
	  Iterator it = this.constraintData.entrySet().iterator();
	  while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry constraintEntry = (Map.Entry)it.next();

	    	String tmpConstraintId = constraintEntry.getKey().toString();
	    	Map tmpConstraintData = (Map)constraintEntry.getValue();

	    	if(tmpConstraintData.get("type").equals("PK")){
		    	if(tmpConstraintData.get("action").equals("drop")){
		    		String presentName = tmpConstraintData.get("present-name").toString();
		    		String[] parts = tmpConstraintId.split(":");
		    		String table = parts[1];
		    		String columnsList = tmpConstraintData.get("present-column-list").toString();

					String applySql = "ALTER TABLE "+table+" DROP CONSTRAINT "+presentName+";";
					String inverseSql = "ALTER TABLE "+table+" ADD CONSTRAINT "+presentName+" PRIMARY KEY ("+columnsList+");";

					this.saveDdl(applySql, inverseSql);
		    	}
	    	}
	    }
	}

	private void dropDefaultConstraints(){
	    Iterator it = this.constraintData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry constraintEntry = (Map.Entry)it.next();
	    	String dcIdentifier = constraintEntry.getKey().toString();
	    	Map tmpConstraintData = (Map) constraintEntry.getValue();

	    	if(tmpConstraintData.get("type").equals("DC")){
	    		String action = (String) tmpConstraintData.get("action");

	    		if(action != null){
	    			switch(action){
	    			case "drop":
	    		    	String[] parts = dcIdentifier.replace("dc:", "").split("\\.");

	    		    	String table = parts[0];
	    		    	String column = parts[1];
				    	String presentName = tmpConstraintData.get("present-name").toString();

						String applySql = "ALTER TABLE "+table+" DROP CONSTRAINT "+presentName+";";

						this.saveDdl(applySql);
	    				break;
	    			default:
	    				Debug.die("not coded yet");
	    			}
	    		}
	    	}
	    }
	}

	private void addConstraints(){
		this.addPrimaryKeyConstraints();
		this.addCustomPrimaryKeyConstraints();
		this.addForeignKeyConstraints();
		this.addCustomForeignKeyConstraints();
		this.addDefaultConstraints();
	}

	private void addPrimaryKeyConstraints(){
	    Iterator it = this.constraintData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry constraintEntry = (Map.Entry)it.next();
	    	String constraintId = constraintEntry.getKey().toString();
	    	Debug.pr(constraintId);
	    	Map constraintMap = (Map)constraintEntry.getValue();

	    	if(constraintMap.get("type").equals("PK")){
	    		String statusCode = constraintMap.get("status-code").toString();
	    		switch(statusCode){
	    		case "pk-match":
	    		case "custom-table":
	    		case "new-table":
	    		case "custom-pk": // will handle separetely
	    			// do nothing
	    			break;
	    		case "pk-future-only":
	    			this.addPrimaryKeyConstraint(constraintId);
	    			break;
	    		default:
	    			Debug.pr(statusCode);
	    			Debug.die("not coded yet");
	    		}
	    	}
	    }
	}

	private void addPrimaryKeyConstraint(String pkIdentifier){
		Map<String, String> constraintMap = this.constraintData.get(pkIdentifier);
		String table = pkIdentifier.replace("pk:", "");
		String constraintName = constraintMap.get("future-name");
		String columnList = constraintMap.get("future-column-list");
		String applySql = "ALTER TABLE "+table+" ADD CONSTRAINT "+constraintName+" PRIMARY KEY ("+columnList+"); ";
		this.saveDdl(applySql);
	}

	private void addCustomPrimaryKeyConstraints(){

		for(String customPk : this.customSettings.presentCustomPrimaryKeys){
			String[] parts = customPk.split("\\|");
			String pkIdentifier = parts[0];
			String table = pkIdentifier.replace("pk:", "");
			String constraintName = parts[1];
			String columnList = parts[2];
			String applySql = "ALTER TABLE "+table+" ADD CONSTRAINT "+constraintName+" PRIMARY KEY ("+columnList+"); ";
			this.saveDdl(applySql);
		}
	}

	private void addForeignKeyConstraints(){

	    Iterator it = this.constraintData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry constraintEntry = (Map.Entry)it.next();
	    	String constraintId = constraintEntry.getKey().toString();
	    	Debug.pr(constraintId);
	    	Map constraintMap = (Map)constraintEntry.getValue();

	    	if(constraintMap.get("type").equals("FK")){

		    	String statusCode = (String)constraintMap.get("status-code");

		    	if(statusCode == null)
		    		statusCode = "null";

	    		switch(statusCode){
	    		case "null":
	    			// check the present name - if it is set - move on
	    			if(constraintMap.get("present-name") == null)
	    				this.addForeignKeyConstraint(constraintId);
	    			break;

	    		case "custom-fk-drop": // dont add these back
	    		case "custom-fk": // these will be handle separately
	    		case "all-three":
	    			break;
	    		default:
	    			Debug.pr(statusCode);
	    			Debug.expose(constraintMap);
	    			Debug.die("not coded yet");
	    		}
	    	}
	    }
	}

	private void addForeignKeyConstraint(String constriantId){
		Debug.pr("Upgrade.addForeignKeyConstraint("+constriantId+")");
		Map<String, String> constraintMap = this.constraintData.get(constriantId);

		String parentTable = constraintMap.get("parent-table");
		String parentColumns = constraintMap.get("parent-column-list");
		String constraintName = constraintMap.get("future-name");
		String refTable = constraintMap.get("ref-table");
		String refColumns = constraintMap.get("ref-column-list");
		String applySql = "ALTER TABLE "+parentTable+" ADD CONSTRAINT "+constraintName+" FOREIGN KEY ("+parentColumns+") REFERENCES "+refTable+"("+refColumns+");";

		this.saveDdl(applySql);
	}

	private void addCustomForeignKeyConstraints(){
		Debug.pr("Upgrade.addCustomForeignKeyConstraints()");
		for(String constraintId : this.customSettings.presentOnlyForeignKeys){
			String[] parts = constraintId.split("\\|");
			String parentTable = parts[1];
			String parentColumns = parts[2];
			String refTable = parts[3];
			String refColumns = parts[4];
			String applySql = "ALTER TABLE "+parentTable+" ADD FOREIGN KEY ("+parentColumns+") REFERENCES "+refTable+"("+refColumns+");";

			this.saveDdl(applySql);
		}
	}

	private void addDefaultConstraints(){
	    Iterator it = this.constraintData.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map.Entry constraintEntry = (Map.Entry)it.next();
	    	String constraintId = constraintEntry.getKey().toString();
	    	Map constraintMap = (Map)constraintEntry.getValue();

	    	if(constraintMap.get("type").equals("DC")){
	    		this.addDefaultConstraint(constraintId);
	    	}
	    }
	}

	private void addDefaultConstraint(String constraintId){
		Map<String, String> constraintMap = this.constraintData.get(constraintId);
		Debug.expose(constraintMap);
		Debug.die("not coded yet");
	}

	private void deleteDataFromNewTables(){
		Collections.reverse(this.customSettings.newTables);
		for(String table : this.customSettings.newTables){
			String sql =
			"DELETE FROM \n" +
			"   "+table +";";

			this.testSql(sql);
		}
		// set order back to normal
		Collections.reverse(this.customSettings.newTables);

	}

	private void copyDataIntoNewTables(){
		this.copyNewTablesIntoTables();
		this.disableAllIdentityInsert();
		// todo - need a list of all newly created table names
		// for now using hard coded list
		for(String newTable : this.customSettings.newTables){
			Debug.pr(newTable);
			if(this.identityTables.contains(newTable)){
				Debug.pr("new table is an identity table");
				this.enableIdentityInsert(newTable);
				this.copyFutureRows(newTable, "1=1");
				this.disableIdentityInsert(newTable);
			}
			else
				this.copyFutureRows(newTable, "1=1");
		}

	}

	private void copyDataIntoExistingTables(){
		this.copyDataIntoExistingTable("conn_flds");
		this.copyDataIntoExistingTable("cost_index_values");
		this.copyDataIntoExistingTable("helpdesk_sla_request");
		this.copyDataIntoExistingTable("helpdesk_sla_response");
	}

	private void copyDataIntoExistingTable(String table){
		// get pk column(s)
		String[] primaryKeyColumns = this.getPrimaryKeyColumns(table);
		List<String> nonIdentityColumns = this.getNonIdentityColumns(table);

		String sql =
		"SELECT \n" +
		"   "+String.join(",", primaryKeyColumns)+" \n" +
		"FROM \n" +
		"   "+table;

		this.runSelectRowsAllDbs(sql);
		// find all records that are in future that are not in present

		//List<Map<String, String>> pastRows = this.latestResults.get("past");
		List<Map<String, String>> presentRows = this.latestResults.get("present");
		List<Map<String, String>> futureRows = this.latestResults.get("future");

		Integer matchCount = 0;
		for(Map<String, String> futureRow : futureRows){
			// check to see if this row is in the present
			Map<String, String> presentMatch = Db.filterForRow(futureRow, presentRows);
			if(presentMatch == null){
				Debug.pr("this row does not exist in the present");
				Debug.expose(futureRow);
				List<String> criteria = new ArrayList<String>();

				for (Map.Entry<String,String> entry : futureRow.entrySet()){
					String column  = entry.getKey();
					String value = entry.getValue();
					criteria.add(column+" = "+ Db.escStr(value));
				}
				this.copyFutureRows(table, String.join(" AND ", criteria));
			}else{
				matchCount++;
				Debug.pr("this row exists in the present and future database");
			}
		}
	}

	private void copyNewTablesIntoTables(){
		for (Entry<String, Map<String, String>> entry : this.tableData.entrySet()){
			String table = entry.getKey();
			Map<String, String> tableData = entry.getValue();
			String statusCode = tableData.get("status-code");

			switch(statusCode){
			case "all-three":
			case "present-only":
			case "present-and-future":
				break;
			case "future-only":
				Debug.expose(table);
				Debug.expose(tableData);
				this.copyFutureRows("tbls", "table_name = "+Db.escStr(table));
				break;
			default:
				Debug.expose(table);
				Debug.expose(tableData);
				Debug.die("not coded yet");
				break;
			}
		}
	}

	private String getFormattedDefault(String table, String column, String defaultVal, String dataType){
		String ret;

		if(defaultVal == null)
			defaultVal = "NULL";

		defaultVal = defaultVal.trim();

		if(dataType.equals("datetime") && defaultVal.equals("CURRENT"))
			ret = " DEFAULT GETDATE() ";
		else if(defaultVal.equals("AUTOINCREMENT")){
			Debug.pr("AUTOINCREMENT col");
			ret = "";
		}
		else if(defaultVal.equals("NULL"))
			ret = " DEFAULT NULL ";
		else
			ret = " DEFAULT "+Db.escDynamic(defaultVal, dataType)+" ";

		return ret;

	}

	private String getAddColumnDefinition(String table, String column, MyDb thisDb){
		return this.getColumnDefinition(table, column, thisDb, new LinkedHashMap<String, String>(), "add");
	}

	private void addColumn(String table, String column){
		String colDefinition = this.getAddColumnDefinition(table, column, this.dbMap.get("future"));

		List<String> sqlList = new ArrayList<String>();
		sqlList.add("ALTER TABLE "+table+" ADD "+colDefinition+";");
		this.saveDdl(sqlList);
	}

	private void alterColumn(String table, String column, Map<String, String> changes){
		List<String> applySqlList = this.getAlterColumnSqlList(table, column, this.dbMap.get("present"), changes);
		Map<String, String> inverseChanges = new LinkedHashMap<String, String>();
		if(changes.containsKey("default_val"))
			inverseChanges.put("default_val", this.columnData.get(table+"."+column).get("present").get("default_val"));

		List<String> inverseSqlList = this.getAlterColumnSqlList(table, column, this.dbMap.get("present"), inverseChanges);
		this.saveDdl(applySqlList, inverseSqlList);
	}

	private void saveDdl(String applySql){
		this.ddlPrinter.writeLine(applySql);
	}

	private void saveDdl(String applySql, String inverseSql){
		this.ddlPrinter.writeLine(applySql);
		this.ddlPrinterInverse.writeLine(inverseSql);
	}

	private void saveDdl(List<String> applySqlList, List<String> inverseSqlList){
		for(String applySql : applySqlList )
			this.ddlPrinter.writeLine(applySql);

		for(String inverseSql : inverseSqlList )
			this.ddlPrinterInverse.writeLine(inverseSql);
	}

	private void saveDdl(List<String> applySqlList){
		for(String applySql : applySqlList )
			this.ddlPrinter.writeLine(applySql);
	}

	private void saveDml(String applySql){
		this.dmlPrinter.writeLine(applySql);
	}

	private List<String> getDropColumnSql(String table, String column){
		List<String> ret = new ArrayList<>();

		String sql =
		"SELECT \n" +
		"	default_constraints.name constraint_name \n" +
		"FROM \n" +
		"	sys.columns \n" +
		"JOIN \n" +
		"	sys.default_constraints ON ( \n" +
		"		default_constraints.parent_object_id = columns.object_id AND \n" +
		"		default_constraints.parent_column_id = columns.column_id) \n" +
		"WHERE \n" +
		"	columns.name = "+Db.escStr(column)+" AND \n" +
		"	columns.object_id = OBJECT_ID("+Db.escStr(table)+")";

		List<Map<String,String>> rows = this.dbMap.get("present").selectRows(sql);

		for(Map<String,String> row : rows)
			ret.add("DROP CONSTRANT "+row.get("constraint_name"));

		ret.add("ALTER TABLE "+table+" DROP COLUMN "+column+";");

		return ret;
	}

	private void testSql(String sql){
		this.dbMap.get("present").execute(sql);
	}

	private void testDdl(String applySql){
		this.dbMap.get("present").execute(applySql);
	}

	private void testDdl(String applySql, String inverseSql){
		Debug.pr("testDdl()");
		Debug.pr("apply");
		this.dbMap.get("present").execute(applySql);

		Debug.pr("inverse");
		this.dbMap.get("present").execute(inverseSql);
	}

	private void testDdl(List<String> applySqlList){
		for(String applySql : applySqlList )
			this.dbMap.get("present").execute(applySql);
	}

	private void testDdl(List<String> applySqlList, List<String> inverseSqlList){
		Debug.pr("testDdl()");
		Debug.pr("apply");
		for(String applySql : applySqlList )
			this.dbMap.get("present").execute(applySql);

		Debug.pr("inverse");
		for(String inverseSql : inverseSqlList )
			this.dbMap.get("present").execute(inverseSql);
	}

	private MyDb getMyDb(String type){
		MyDb thisDb = null;

		switch(type){
		case "past":
			thisDb = this.dbMap.get("past");
			break;
		case "present":
			thisDb = this.dbMap.get("present");
			break;
		case "future":
			thisDb = this.dbMap.get("future");
			break;
		}

		return thisDb;
	}

	private Map<String,String> getColumnStructure(String table, String column, String dbType){
		return this.columnData.get(table+"."+column).get(dbType);
	}

	private List<String> getNonIdentityColumns(String table){
		Debug.pr("Upgrade.getNonIdentityColumns("+table+")");
		List<String> ret = new ArrayList<String>();

	    Iterator it = this.columnData.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry colEntry = (Map.Entry)it.next();
	    	String fullColumnName = (String)colEntry.getKey();
	    	Map<String, Map<String, String>> columnMap = (Map<String, Map<String, String>>)colEntry.getValue();

	    	String[] parts = fullColumnName.split("\\.");

	    	if(table.equals(parts[0])){
	    		Map<String, String> futureData = columnMap.get("future");
	    		if(futureData.get("is_identity").equals("0"))
	    			ret.add(parts[1]); //column name
	    	}
	    }
		return ret;
	}

	private void copyFutureRows(String table, String criteria){
		Debug.pr("Upgrade.copyFutureRows("+table+")");
		// inserts dont seem to work when using an IDENTITY column - adding data for all other columns
		List<String> nonIdentityColumns = this.getNonIdentityColumns(table);

		String sql =
		"SELECT \n" +
		"   "+String.join(",", nonIdentityColumns)+" \n" +
		"FROM \n" +
		"   "+table +" \n" +
		"WHERE \n" +
		"    "+criteria;

		List<Map<String, String>> rows = this.dbMap.get("future").selectRows(sql);
		List<String> cols = new ArrayList<String>();
		List<String> vals = new ArrayList<String>();

		for(Map<String, String> row : rows){
			cols = new ArrayList<String>();
			vals = new ArrayList<String>();
		    Iterator it = row.entrySet().iterator();
		    while (it.hasNext()) {

		    	// for now assume we want to trim both values
		    	Map.Entry pair = (Map.Entry)it.next();
		    	cols.add(pair.getKey().toString());
		    	Object val = pair.getValue();
		    	if(val == null)
		    		val = "null";
		    	else
		    		val = Db.escStr(pair.getValue().toString().trim());
		    	vals.add(val.toString());
		    }

			sql = "INSERT INTO "+table + "\n("+StringUtils.join(cols, ", ")+") VALUES \n("+StringUtils.join(vals, ", ")+");";
			this.dmlPrinter.writeLine(sql);
		}

		sql = "DELETE FROM "+table+ " WHERE "+criteria+";";
		this.dmlPrinterInverse.writeLine(sql);
	}

	// return true if they are the same
	private boolean compareTableStructure(String table, boolean autoFix){
		// get all columns in future table
		boolean match = true;
		List<String> columns = this.dbMap.get("future").getTableColumns(table);
		for(String column : columns){
			column = column.toLowerCase();
			if(!this.compareColumnStructure(table, column, autoFix))
				match = false;
		}

		return match;
	}

	private boolean compareColumnStructure(String table, String column, boolean autoFix){
		String fullColumnName = table+"."+column;
		Debug.pr("Upgrade.compareColumnStructure("+fullColumnName+")");
		Map<String, String> pastStructure = this.columnData.get(fullColumnName).get("past");
		Map<String, String> presentStructure = this.columnData.get(fullColumnName).get("present");
		Map<String, String> futureStructure = this.columnData.get(fullColumnName).get("future");

		boolean match = true;

		if(futureStructure == null){
			Debug.pr("Column: "+table+"."+column+" does not exist in future database.");
			return false;
		}

		if(presentStructure == null){
			Debug.pr("Column: "+table+"."+column+" does not exist in present database.");
			if(autoFix)
				this.addColumn(table, column);
			return false;
		}

		// check for cases when both the past and future match but present is differnet - custom alter
		Map<String, Map<String, String>> differences = new LinkedHashMap<String, Map<String, String>>();

	    Iterator it = futureStructure.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map<String, String> pastValMap = new LinkedHashMap<String, String>();
	    	Map<String, String> presentValMap = new LinkedHashMap<String, String>();
	    	Map<String, String> futureValMap = new LinkedHashMap<String, String>();

	    	Map.Entry futurePair = (Map.Entry)it.next();
	        String key = futurePair.getKey().toString();

	        String pastVal = null;
	        if(pastStructure != null){
		        if(pastStructure.get(key) != null)
		        	pastVal = pastStructure.get(key).toString().trim();
	        }

	        String presentVal = null;
	        if(presentStructure.get(key) != null)
	        	presentVal = presentStructure.get(key).toString().trim();

	        String futureVal = null;
	        if(futurePair.getValue() != null)
	        	futureVal = futurePair.getValue().toString().trim();

	        boolean tmpMatch = true;

	        if(futureVal == null && presentVal == null)
	        	tmpMatch = true;
	        else if(pastVal != null && pastVal.equals(futureVal) && !presentVal.equals(futureVal)){
	        	tmpMatch = true; // @todo setting this to true for now - would like to move this logic at some point
	        }
	        else if(futureVal == null || presentVal == null)
	        	tmpMatch = false;
	        else if(!futureVal.equals(presentVal))
	        	tmpMatch = false;

	        if(!tmpMatch){
	        	Map<String, String>tmpMap = new LinkedHashMap<String,String>();
	        	tmpMap.put("future", futureVal);
	        	tmpMap.put("present", presentVal);
	        	differences.put(key, tmpMap);
	        }
	    }

	    if(differences.size() > 0){
	    	Map<String, String> changes = new LinkedHashMap<String, String>();

	    	it = differences.entrySet().iterator();
		    while (it.hasNext()) {
		    	Map.Entry differenceEntry = (Map.Entry)it.next();
		    	String key = differenceEntry.getKey().toString();
		    	Map<String, String> differenceVals = (Map<String, String>)differenceEntry.getValue();
		    	String futureVal = differenceVals.get("future");
		    	String presentVal = differenceVals.get("present");
		    	Debug.pr("future has a value of: "+futureVal+" and present has a value of "+presentVal+" for "+key);

        		switch(key){
        		case "max_length":
        		case "precision":
        		case "scale":
        			Integer futureIntVal = Integer.parseInt(futureVal);
        			Integer presentIntVal = Integer.parseInt(presentVal);
        			if(futureIntVal > presentIntVal)
        				changes.put(key, futureVal);
        			else
        				Debug.pr("present "+key+" for "+table+"."+column+" is longer than future - ignoring difference");
        			break;
        		case "data_type":
        			if(futureVal.equals("char") && presentVal.equals("varchar")){
        				Debug.pr("present data_type for "+table+"."+column+" is varchar and future is char -- ignore difference");
        			}
        			else if(futureVal.equals("varchar") && presentVal.equals("char")){
        				Debug.pr("present data_type for "+table+"."+column+" is char and future is varchar -- changing present to varchar");
        				changes.put("data_type", "varchar");
        			}
        			else if(futureVal.equals("smallint") && presentVal.equals("char")){
        				Debug.pr("not sure if this will work - need to test");
        				Debug.pr("present data_type for "+table+"."+column+" is "+presentVal+" and future is "+futureVal+" -- changing present to match future");
        				changes.put("data_type", futureVal);
        			}
        			else if(futureVal.equals("numeric") && presentVal.equals("varchar")){
        				Debug.pr("not sure if this will work - need to test");
        				Debug.pr("present data_type for "+table+"."+column+" is "+presentVal+" and future is "+futureVal+" -- changing present to match future");
        				changes.put("data_type", futureVal);
        				changes.put("precision", futureStructure.get("precision"));
        				changes.put("scale", futureStructure.get("scale"));
        			}
        			else if(futureVal.equals("smallint") && presentVal.equals("int")){
        				Debug.pr("not sure if this will work - need to test");
        				Debug.pr("present data_type for "+table+"."+column+" is "+presentVal+" and future is "+futureVal+" -- ignoring difference");
        			}
        			else
        				Debug.die("not coded yet");
        			break;
        		case "is_nullable":
        			if(futureVal.equals("0") && presentVal.equals("1")){
        				Debug.pr("future version no longer allows nulls");
        				//@todo - this is a bit of duplicated code - perhaps isolate to a function

        				String defaultVal = futureStructure.get("default_val");

        				if(defaultVal == null){
        					// attempt to get default val from future flds
        					defaultVal = this.getFieldValue(table, column, "dflt_val", this.dbMap.get("future"));
        				}

        				if(defaultVal == null){
        					Debug.pr("no default value for non nullable column");
        					defaultVal = "NULL";
        				}

        				changes.put("is_nullable", "0");
        				changes.put("default_val", defaultVal);
        			}
        			break;
        		case "is_primary_key":
        			Debug.pr("pk status doesnt match - will handle that separately");
        			break;
        		case "is_identity":
        			changes.put("is_identity", futureVal);
        			break;
        		case "column_id":
        			// ignoring column_id differences
        			break;
        		default:
        			Debug.die("not coded yet");
        		}
        	}
		    if(changes.size() > 0){
		    	match = false;
		    	if(autoFix)
		    		this.alterColumn(table, column, changes);
			}
	    }

        if(!match)
        	Debug.pr("The two rows are not a match.");
        else
        	Debug.pr("The two rows are a match");

        return match;
	}

	private void rebuildIndexes(){
		Debug.pr("Upgrade.rebuildIndex()");
	}

	// compare two actual db table rows
	public boolean compareRowValues(Map<String, String> futureRow, Map<String, String> presentRow, String table, String pkCol, String pkVal, boolean autoFix){
		pkVal = pkVal.trim();
		// row in this case refers to a result row - not an actual table rows
		// generally it is assumed that these two rows have the same PK - just checking rest of columns
		// assumes the same keys in both rows
		boolean match = true;
	    Iterator it = futureRow.entrySet().iterator();
	    while (it.hasNext()) {
	    	// for now assume we want to trim both values
	    	Map.Entry pair = (Map.Entry)it.next();
	        String key = pair.getKey().toString();
	        String futureVal = null;
	        if(pair.getValue() != null)
	        	futureVal = pair.getValue().toString().trim();

	        String presentVal = null;
	        if(presentRow.get(key) != null)
	        	presentVal = presentRow.get(key).toString().trim();

	        if(futureVal == null && presentVal == null){
	        	//Debug.pr("both are null");
	        }
	        else if(futureVal != null && presentVal == null){
	        	match = false;
	        	Debug.pr("future has a value of: "+futureVal+" and present is null for "+key);

        		switch(key){
        		case "comments":
        			if(autoFix){
	        			this.dmlPrinter.writeLine("-- Ignoring differences in comments for "+table+"."+key+" Present Val: "+Util.stripLineBreaks(presentVal)+" Future Val: "+Util.stripLineBreaks(presentVal));
	        			this.dmlPrinter.writeLine("");
        			}
        			break;
        		default:
        			if(autoFix)
        				this.updateValue(table, pkCol, pkVal, key, futureVal, presentVal);
        			break;
        		}
	        }
	        else if(futureVal == null && presentVal != null){
	        	match = false;
	        	Debug.pr("present has a value of: "+presentVal+" and future is null for "+table+"."+key);
	        	switch(table+"."+key){
		        	case "tbls.comments":
		        			if(autoFix){
		        				this.dmlPrinter.writeLine("-- Ignoring differences in comments for "+table+"."+key+" Present Val: "+Util.stripLineBreaks(presentVal)+" Future Val: "+Util.stripLineBreaks(presentVal));
		        				this.dmlPrinter.writeLine("");
		        			}
		        		break;
		        	default:
		        		Debug.die("not coded yet");
		        		break;
	        	}
	        }
	        else if(!futureVal.equals(presentVal)){
	        	match = false;
	        	Debug.pr("future has a value of: "+futureVal+" and present has a value of "+presentVal+" for "+key);

	        	switch(key){
	        	case "comments":
	        		this.dmlPrinter.writeLine("-- Ignoring differences in comments for "+table+"."+key+" Present Val: "+Util.stripLineBreaks(presentVal)+" Future Val: "+Util.stripLineBreaks(presentVal));
	        		this.dmlPrinter.writeLine("");
	        		break;
	        	default:
	        		if(autoFix)
	        			this.updateValue(table, pkCol, pkVal, key, futureVal, presentVal);
	        	}
	        }
	    }

	    return match;
	}

	private void updateValue(String table, String pkCol, String pkVal, String col, String newVal, String oldVal){

		uniqUpdateFields.add(table+"."+col);
		// assume we want to trim all vals
		newVal = newVal.trim();
		pkVal = pkVal.trim();

		newVal = Db.escStr(newVal);

		this.dmlPrinter.writeLine("--- Updating "+table+"."+col+" PK: "+pkCol+" = "+pkVal+ " New Val: "+Util.stripLineBreaks(newVal)+" Old Val: "+Util.stripLineBreaks(oldVal));
		pkVal = Db.escStr(pkVal);
		String sql = "UPDATE "+table+" SET "+col+" = "+newVal+" WHERE "+pkCol+" = "+pkVal+";";
		this.dmlPrinter.writeLine(sql);
		this.dmlPrinter.writeLine("");

		if(oldVal != null)
			oldVal = Db.escStr(oldVal);

		sql = "UPDATE "+table+" SET "+col+" = "+oldVal+" WHERE "+pkCol+" = "+pkVal+";";
		this.dmlPrinterInverse.writeLine(sql);
	}

	public void initDatabases(){
		Map<String,String> creds = (Map<String,String>)this.customSettings.dbCreds.get("past");

		this.dbMap.put("past", new MyDb((Map<String,String>)this.customSettings.dbCreds.get("past")));
		this.dbMap.put("present",new MyDb((Map<String,String>)this.customSettings.dbCreds.get("present")));
		this.dbMap.put("future", new MyDb((Map<String,String>)this.customSettings.dbCreds.get("future")));

		String sql =
		"SELECT 	\n" +
		"  user_name 	\n" +
		"FROM \n" +
		"  users \n" +
		"WHERE \n" +
		"  users.user_name LIKE 'TEST%'";

		sql = "SELECT \n"+
			 "	foreign_keys.name fk_name, \n"+
			 "	parent_tables.name parent_table, \n"+
			 "	parent_cols.name parent_column, \n"+
			 "	ref_tables.name ref_table, \n"+
			 "	ref_cols.name ref_column \n"+
			 "FROM \n"+
			" 	sys.foreign_keys \n"+
			" JOIN \n"+
			" 	sys.schemas ON ( \n"+
			" 		schemas.schema_id = foreign_keys.schema_id) \n"+
			" JOIN \n"+
			" 	sys.foreign_key_columns ON ( \n"+
			" 		foreign_key_columns.constraint_object_id = foreign_keys.object_id) \n"+
			" JOIN \n"+
			" 	sys.tables parent_tables ON ( \n"+
			" 		parent_tables.object_id = foreign_keys.parent_object_id AND \n"+
			" 		parent_tables.name IN ('em')) \n"+
			" JOIN \n"+
			" 	sys.all_columns parent_cols ON ( \n"+
			" 		parent_cols.object_id =  foreign_key_columns.parent_object_id AND \n"+
			" 		parent_cols.column_id = foreign_key_columns.parent_column_id) \n"+
			" JOIN \n"+
			" 	sys.tables ref_tables ON ( \n"+
			" 		ref_tables.object_id = foreign_keys.referenced_object_id) \n"+
			" JOIN \n"+
			" 	sys.all_columns ref_cols ON ( \n"+
			" 		ref_cols.object_id =  foreign_key_columns.referenced_object_id AND \n"+
			" 		ref_cols.column_id = foreign_key_columns.referenced_column_id) \n"+
			"ORDER BY \n"+
			"   foreign_keys.name, \n"+
			"   parent_tables.name, \n"+
			"   parent_cols.name";
	}

	private void printStats(){
		Debug.expose(this.uniqUpdateFields);
	}

	private void die(String msg){
		this.closeFiles();
		Debug.die(msg);
	}
	private void die(){
		this.die("");
	}

	//public Map<String, List<Map<String, String>>> SelectRowsAllDbs(String sql){
	public void runSelectRowsAllDbs(String sql){
		this.latestResults.put("past", this.dbMap.get("past").selectRows(sql));
		this.latestResults.put("present", this.dbMap.get("present").selectRows(sql));
		this.latestResults.put("future", this.dbMap.get("future").selectRows(sql));
	}
}
