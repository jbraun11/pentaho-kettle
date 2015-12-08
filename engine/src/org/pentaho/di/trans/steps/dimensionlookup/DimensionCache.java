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
import java.util.Arrays;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;

/**
 * @author John
 * @since 3-dec-2015
 */
public class DimensionCache
{
	private DimensionLookupMeta meta;
	private DimensionLookupData data;
	private DimensionLookup dl;
	
    public PreparedStatement prepStatementLookup;
    public PreparedStatement prepStatementInsert;
    public PreparedStatement prepStatementUpdatePrevVersion;
    public PreparedStatement prepStatementDimensionUpdate;
    public PreparedStatement prepStatementPunchThrough;
	
	public boolean initialLoad;
	
	public Database db;
	
	public DimensionCache(DimensionLookup dimensionLookup, DimensionLookupMeta meta, DimensionLookupData data) throws SQLException, KettleDatabaseException
	{
		this.dl = dimensionLookup;
		this.meta = meta;
		this.data = data;
		
		DatabaseMeta dbMeta = new DatabaseMeta();
		dbMeta.setDatabaseType("DIMCACHE");
		dbMeta.setName("cache_db");
		dbMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
		dbMeta.setDBName("mem:");
		dbMeta.setSupportsTimestampDataType(true);
		dbMeta.setSupportsBooleanDataType(true);
		
		db = new Database(dimensionLookup, dbMeta);	      
	    db.connect();
	    
	    String createSchema = "CREATE SCHEMA " + data.realSchemaName;
	    
	    String ddl = db.getDDL(data.schemaTable.replace("\"", ""), meta.getTableFields(), meta.getKeyField(), 
				(meta.getTechKeyCreation() == DimensionLookupMeta.CREATION_METHOD_AUTOINC), meta.getKeyField(), false);
	    try
	    {
	    	db.getConnection().createStatement().execute(createSchema);
	    }
	    catch (Exception e) {
	    	e.getMessage();
	    }

	    db.getConnection().createStatement().execute(ddl);
	}
	
	public void setRowCache(ResultSet rowSet) throws KettleDatabaseException
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
					if (meta.getTechKeyCreation().equals(DimensionLookupMeta.CREATION_METHOD_AUTOINC))
					{
						row = Arrays.copyOfRange(row, 1, row.length);
					}
					db.setValues(data.insertRowMeta, row, prepStatementInsert);
					db.insertRow(prepStatementInsert, !(meta.getTechKeyCreation().equals(DimensionLookupMeta.CREATION_METHOD_AUTOINC)), false);
				}
				else
				{
					stop = true;
				}
				
				i++;
				long endTime = System.currentTimeMillis();
				long totalTime = endTime - startTime;
				
				if ((float)totalTime/(float)60000 > j)
				{
					dl.logBasic("Current rows in cache: " + i);
					j++;
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
	
	public Object[] lookupRow(Object[] inputRow)
	{
		Object[] returnRow = null;
		
		return returnRow;
	}
}
