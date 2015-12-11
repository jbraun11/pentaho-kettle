/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.di.trans.steps.dimensionlookup;

import java.sql.*;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.row.RowMetaInterface;

/**
 * @author John
 * @since 3-dec-2015
 */
public class DimensionCache
{
	private DimensionLookup dl;
	
    public PreparedStatement prepStatementLookup;
    public PreparedStatement prepStatementInsert;
    public PreparedStatement prepStatementUpdatePrevVersion;
    public PreparedStatement prepStatementDimensionUpdate;
    public PreparedStatement prepStatementPunchThrough;
    
    public RowMetaInterface insertRowMeta;
    public RowMetaInterface updateRowMeta;
    public RowMetaInterface dimensionUpdateRowMeta;
    public RowMetaInterface punchThroughRowMeta;
	
	public boolean initialLoad;
	
	public Database db;
	
	public DimensionCache(DimensionLookup dimensionLookup, DimensionLookupMeta meta, DimensionLookupData data) throws SQLException, KettleDatabaseException
	{
		this.dl = dimensionLookup;
		
		DatabaseMeta dbMeta = new DatabaseMeta();
		dbMeta.setDatabaseType("H2");
		dbMeta.setName("cache_db");
		dbMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
		dbMeta.setDBName("mem:");
		dbMeta.setSupportsTimestampDataType(true);
		dbMeta.setSupportsBooleanDataType(true);
		
		db = new Database(dimensionLookup, dbMeta);	      
	    db.connect();
	    
	    // create schema ddl
	    String createSchema = "CREATE SCHEMA " + data.realSchemaName;
	    
	    // create table ddl
	    String tableDDL = db.getDDL(data.schemaTable.replace("\"", ""), meta.getTableFields(), meta.getKeyField(), 
				false, meta.getKeyField(), false);
	    
	    // run create schema statement, do not fail if schema already exists
	    try
	    {
	    	db.getConnection().createStatement().execute(createSchema);
	    }
	    catch (Exception e) {
	    	e.getMessage();
	    }
	    
	    // create table in h2 memory db
	    db.getConnection().createStatement().execute(tableDDL);
	    
	    // create index ddl
	    String indexDDL = db.getCreateIndexStatement(data.realSchemaName, data.realTableName, "natural key index"
	    			, meta.getKeyLookup(), false, false, false, false).replace("INDEX", "HASH INDEX");
	    
	    // create index on cache table
	    db.getConnection().createStatement().execute(indexDDL);
	}
	
	public void setRowCache(ResultSet rowSet, DimensionLookupData data) throws KettleDatabaseException, SQLException
	{
		boolean stop = false;
		
		if (rowSet != null) 
		{
			int i = 0;
			int j = 1;
			long startTime = System.currentTimeMillis();
			
			while (!stop) 
			{
				Object[] row = data.db.getRow(rowSet, null, data.returnRowMeta);
				if (row != null)
				{
					db.setValues(insertRowMeta, row, prepStatementInsert);
					db.insertRow(prepStatementInsert, true, false);
					
					i++;					
					long endTime = System.currentTimeMillis();
					long totalTime = endTime - startTime;
					
					if ((float)totalTime/(float)60000 > j)
					{
						dl.logBasic("Current rows in cache: " + i);
						j++;
					}
				}
				else
				{
					stop = true;
				}
			}
			
			long endTime = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			dl.logBasic("Caching complete...");
			dl.logBasic("Total rows added to cached :" + i);
			dl.logBasic("Total time taken: " + (float)totalTime/(float)1000/(float)60);
			
			if (i == 1)
			{
				initialLoad = true;
			}
		}
	}
}
