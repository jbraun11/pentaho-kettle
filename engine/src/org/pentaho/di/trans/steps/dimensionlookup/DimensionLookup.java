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

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.MySQLDatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseBatchException;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Manages a slowly changing dimension (lookup or update)
 * 
 * @author John
 * @since 8-april-2015
 */
@SuppressWarnings("deprecation")
public class DimensionLookup extends BaseStep implements StepInterface {
	private static Class<?> PKG = DimensionLookupMeta.class; // for i18n
																// purposes,
																// needed by
																// Translator2!!
																// $NON-NLS-1$

	private final static int CREATION_METHOD_AUTOINC = 1;
	private final static int CREATION_METHOD_SEQUENCE = 2;
	private final static int CREATION_METHOD_TABLEMAX = 3;

	private int techKeyCreation;
	
	private int commitcounter;
	private boolean insert;
	private boolean identical;
	private boolean punch;
	
	private DimensionLookupMeta meta;
	private DimensionLookupData data;
	private DimensionCache cache;

	int[] columnLookupArray = null;

	public DimensionLookup(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	protected void setMeta( DimensionLookupMeta meta ) {
	    this.meta = meta;
	}

	protected void setData( DimensionLookupData data ) {
	    this.data = data;
	}


	private void setTechKeyCreation(int method) {
		techKeyCreation = method;
	}

	private int getTechKeyCreation() {
		return techKeyCreation;
	}

	private void determineTechKeyCreation() {
		String keyCreation = meta.getTechKeyCreation();
		if (meta.getDatabaseMeta().supportsAutoinc()
				&& DimensionLookupMeta.CREATION_METHOD_AUTOINC
						.equals(keyCreation)) {
			setTechKeyCreation(CREATION_METHOD_AUTOINC);
		} else if (meta.getDatabaseMeta().supportsSequences()
				&& DimensionLookupMeta.CREATION_METHOD_SEQUENCE
						.equals(keyCreation)) {
			setTechKeyCreation(CREATION_METHOD_SEQUENCE);
		} else {
			setTechKeyCreation(CREATION_METHOD_TABLEMAX);
		}
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {
		meta = (DimensionLookupMeta) smi;
		data = (DimensionLookupData) sdi;

		Object[] r = getRow(); // Get row from input rowset & set row busy!
		if (r == null) // no more input to be expected...
		{
			setOutputDone(); // signal end to receiver(s)
			return false;
		}	

		if (first) {
			first = false;

			data.setSchemaTable();

			data.inputRowMeta = getInputRowMeta().clone();
			data.outputRowMeta = getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this,
					repository, metaStore);

			// Get the fields that need conversion to normal storage...
			// Modify the storage type of the input data...
			//
			data.lazyList = new ArrayList<Integer>();
			for (int i = 0; i < data.inputRowMeta.size(); i++) {
				ValueMetaInterface valueMeta = data.inputRowMeta
						.getValueMeta(i);
				if (valueMeta.isStorageBinaryString()) {
					data.lazyList.add(i);
					valueMeta
							.setStorageType(ValueMetaInterface.STORAGE_TYPE_NORMAL);
				}
			}

			// The start date value column (if applicable)
			//
			data.startDateFieldIndex = -1;
			if (data.startDateChoice == DimensionLookupMeta.START_DATE_ALTERNATIVE_COLUMN_VALUE) {
				data.startDateFieldIndex = data.inputRowMeta.indexOfValue(meta
						.getStartDateFieldName());
				if (data.startDateFieldIndex < 0) {
					throw new KettleStepException(
							BaseMessages
									.getString(
											PKG,
											"DimensionLookup.Exception.StartDateValueColumnNotFound",
											meta.getStartDateFieldName()));
				}
			}

			// Lookup values
			data.keynrs = new int[meta.getKeyStream().length];
			for (int i = 0; i < meta.getKeyStream().length; i++) {
				// logDetailed("Lookup values key["+i+"] --> "+key[i]+", row==null?"+(row==null));
				data.keynrs[i] = data.inputRowMeta.indexOfValue(meta
						.getKeyStream()[i]);
				if (data.keynrs[i] < 0) // couldn't find field!
				{
					throw new KettleStepException(BaseMessages.getString(PKG,
							"DimensionLookup.Exception.KeyFieldNotFound",
							meta.getKeyStream()[i]));
				}
			}

			// Return values
			data.fieldnrs = new int[meta.getFieldStream().length];
			for (int i = 0; meta.getFieldStream() != null
					&& i < meta.getFieldStream().length; i++) {
				if (!DimensionLookupMeta.isUpdateTypeWithoutArgument(
						meta.isUpdate(), meta.getFieldUpdate()[i])) {
					data.fieldnrs[i] = data.outputRowMeta.indexOfValue(meta
							.getFieldStream()[i]);
					if (data.fieldnrs[i] < 0) {
						throw new KettleStepException(BaseMessages.getString(
								PKG,
								"DimensionLookup.Exception.KeyFieldNotFound",
								meta.getFieldStream()[i]));
					}
				} else {
					data.fieldnrs[i] = -1;
				}
			}
			
			// configure the cache db
			try {
				configureCache();
			} catch (SQLException ex) {
				throw new KettleDatabaseException(
						"Unable to configure dimension cache", ex);
			}
			
			setDimLookup(data.outputRowMeta);

			if (data.cacheKeyRowMeta==null)
            {
            	// KEY : the natural key(s)
                //
                data.cacheKeyRowMeta = new RowMeta();
                for (int i=0;i<data.keynrs.length;i++)
                {
                    ValueMetaInterface key = data.inputRowMeta.getValueMeta(data.keynrs[i]);
                    data.cacheKeyRowMeta.addValueMeta( key.clone());
                }
            }

			if (!Const.isEmpty(meta.getDateField())) {
				data.datefieldnr = data.inputRowMeta.indexOfValue(meta
						.getDateField());
			} else {
				data.datefieldnr = -1;
			}

			// Initialize the start date value in case we don't have one in the
			// input rows
			//
			data.valueDateNow = determineDimensionUpdatedDate(r);

			determineTechKeyCreation();

			data.notFoundTk = new Long(meta.getDatabaseMeta().getNotFoundTK(
					isAutoIncrement()));
			// if (meta.getKeyRename()!=null && meta.getKeyRename().length()>0)
			// data.notFoundTk.setName(meta.getKeyRename());

			if (getCopy() == 0)
				checkDimZero();
			
			if (meta.isPreloadingCache())
				preloadCache();
		}

		// convert row to normal storage...
		//
		for (int lazyFieldIndex : data.lazyList) {
			ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta(
					lazyFieldIndex);
			r[lazyFieldIndex] = valueMeta
					.convertToNormalStorageType(r[lazyFieldIndex]);
		}

		try {
			Object[] outputRow = lookupValues(r); // add new
																		// values
																		// to
																		// the
																		// row
																		// in
																		// rowset[0].
			putRow(data.outputRowMeta, outputRow); // copy row to output
													// rowset(s);

			if (checkFeedback(getLinesRead())) {
				if (log.isBasic())
					logBasic(BaseMessages.getString(PKG,
							"DimensionLookup.Log.LineNumber") + getLinesRead());
			}
		} catch (Exception e) {
			logError(BaseMessages.getString(PKG,
					"DimensionLookup.Log.StepCanNotContinueForErrors",
					e.getMessage()));
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
			setOutputDone(); // signal end to receiver(s)
			return false;
		}
		
		if (commitcounter >= meta.getCommitSize() || (meta.getCacheSize() > 0 && commitcounter >= meta.getCacheSize()) || meta.getCacheSize() < 0) {
			executeBatch(data.prepStatementInsert);
			executeBatch(data.prepStatementUpdatePrevVersion);
			executeBatch(data.prepStatementDimensionUpdate);
			executeBatch(data.prepStatementPunchThrough);
			data.db.commit();
			
			commitcounter = 0;
		}
		
		cache.checkCacheSize();

		return true;
	}

	private void executeBatch(PreparedStatement prepStatement) throws KettleException {
		try {
			if (prepStatement != null) {
				prepStatement.executeBatch();				
				prepStatement.clearBatch();
			}
			
		} catch (BatchUpdateException ex) {
			KettleDatabaseBatchException kdbe = new KettleDatabaseBatchException("Error updating batch", ex);
		    kdbe.setUpdateCounts(ex.getUpdateCounts());
            List<Exception> exceptions = new ArrayList<Exception>();
            
            // 'seed' the loop with the root exception
            SQLException nextException = ex;
            do 
            {
                exceptions.add(nextException);
                // while current exception has next exception, add to list
            } 
            while ((nextException = nextException.getNextException())!=null);            
            kdbe.setExceptionsList(exceptions);
		    throw kdbe;
		}
		catch(SQLException ex) 
		{
			throw new KettleDatabaseException("Error inserting row", ex);
		}
		catch(Exception ex)
		{
			throw new KettleDatabaseException("Unexpected error inserting row", ex);
		}
		
	}

	private Date determineDimensionUpdatedDate(Object[] row)
			throws KettleException {
		if (data.datefieldnr < 0) {
			return getTrans().getCurrentDate(); // start of transformation...
		} else {
			// Date field in the input row
			Date rtn = data.inputRowMeta.getDate(row, data.datefieldnr);
			if (rtn != null) {
				return rtn;
			} else {
				// Fix for PDI-4816
				String inputRowMetaStringMeta = null;
				try {
					inputRowMetaStringMeta = data.inputRowMeta.toStringMeta();
				} catch (Exception ex) {
					inputRowMetaStringMeta = "No row input meta";
				}
				throw new KettleStepException(BaseMessages.getString(PKG,
						"DimensionLookup.Exception.NullDimensionUpdatedDate",
						inputRowMetaStringMeta));
			}
		}
	}
	
	/**
	 * Configure the cache by creating a table in memory with H2
	 * 
	 * @throws KettleException
	 *             in case there is a database or cache problem.
	 * @throws SQLException 
	 */
	private void configureCache() throws KettleException, SQLException
	{
		cache = new DimensionCache(this, this.meta, this.data);
	}

	/**
	 * Pre-load the cache by reading the whole dimension table from disk...
	 * 
	 * @throws KettleException
	 *             in case there is a database or cache problem.
	 */
	private void preloadCache() throws KettleException 
	{
		createDimInsertStatement();
		
		try {
			DatabaseMeta databaseMeta = meta.getDatabaseMeta();
			String sql;
	
			// tk, version, from, to, natural keys, retrieval fields...
			//
			sql = "SELECT "
					+ databaseMeta.quoteField(meta.getKeyField());

			// add all natural keys
			for (int i = 0; i < meta.getKeyLookup().length; i++) {
				sql += ", " + meta.getKeyLookup()[i]; // the natural key field
														// in the table
			}
			// add all lookup fields
			for (int i = 0; i < meta.getFieldLookup().length; i++) {
				sql += ", " + meta.getFieldLookup()[i]; // the extra fields to
														// retrieve...
			}
			
			// only had version if updating the dimension
			if (meta.isUpdate())
			{
				sql += ", " + databaseMeta.quoteField(meta.getVersionField());
			}
			// add date from and to fields
			sql += ", " + databaseMeta.quoteField(meta.getDateFrom()); 
			sql += ", " + databaseMeta.quoteField(meta.getDateTo()); 

			// add from statement
			sql += " FROM " + data.schemaTable;
			
			logDetailed("Pre-loading cache by reading from database with: "
					+ Const.CR + sql + Const.CR);

			// get rows from database
			ResultSet rset = data.db.openQuery(sql);
			data.returnRowMeta = data.db.getReturnRowMeta();

			data.preloadKeyIndexes = new int[meta.getKeyLookup().length];
			for (int i = 0; i < data.preloadKeyIndexes.length; i++) {
				data.preloadKeyIndexes[i] = data.returnRowMeta.indexOfValue(meta
						.getKeyLookup()[i]); // the field in the table
			}
			
			if (data.cacheKeyRowMeta==null)
            {
            	// KEY : the natural key(s)
                //
                data.cacheKeyRowMeta = new RowMeta();
                for (int i=0;i<data.keynrs.length;i++)
                {
                    ValueMetaInterface key = data.inputRowMeta.getValueMeta(data.keynrs[i]);
                    data.cacheKeyRowMeta.addValueMeta( key.clone());
                }
            }

			//create and add all rows to cache
			
			logBasic("Adding rows to cache...");
			cache.setRowCache(rset, data);

			// Also see what indexes to take to populate the lookup row...
			// We only ever compare indexes and the lookup date in the cache,
			// the rest is not needed...
			//
			data.preloadIndexes = new ArrayList<Integer>();
			for (int i = 0; i < meta.getKeyStream().length; i++) {
				int index = data.inputRowMeta
						.indexOfValue(meta.getKeyStream()[i]);
				if (index < 0) {
					// Just to be safe...
					//
					throw new KettleStepException(BaseMessages.getString(PKG,
							"DimensionLookup.Exception.KeyFieldNotFound",
							meta.getFieldStream()[i]));
				}
				data.preloadIndexes.add(index);
			}

			// This is all for now...
		} catch (Exception e) {
			throw new KettleException(
					"Error encountered during cache pre-load", e);
		}
	}

	private synchronized Object[] lookupValues(Object[] row) throws Exception 
	{
		Object[] outputRow = new Object[data.outputRowMeta.size()];
		
		// update row to set all strings to lowercase - fix for case matching

		RowMetaInterface lookupRowMeta;
		Object[] lookupRow;

		Object[] returnRow = null;

		Long technicalKey;
		Long valueVersion;
		Date valueDateFrom = null;
		Date valueDateTo = null;
		Long returnRowTechnicalKey;

		// Determine the lookup date ("now") if we have a field that carries
		// said
		// date.
		// If not, the system date is taken.
		//
		valueDateFrom = determineDimensionUpdatedDate(row);
		
	    // Perform the lookup...
	    //
	    lookupRow = new Object[data.lookupRowMeta.size()];
        lookupRowMeta = data.lookupRowMeta;

        // Construct the lookup row...
        //
        for (int i = 0; i < meta.getKeyStream().length; i++) {
          try {
            lookupRow[i] = row[data.keynrs[i]];
          } catch (Exception e) // TODO : remove exception??
          {
            throw new KettleStepException(BaseMessages.getString(PKG, "DimensionLookup.Exception.ErrorDetectedInGettingKey", i + "", data.keynrs[i] + "/" + data.inputRowMeta.size(), data.inputRowMeta.getString(row)));   //$NON-NLS-3$ //$NON-NLS-4$
          }
        }

        lookupRow[meta.getKeyStream().length] = valueDateFrom; // ? >= date_from
        lookupRow[meta.getKeyStream().length + 1] = valueDateFrom; // ? < date_to
        
        cache.db.setValues(data.lookupRowMeta, lookupRow, cache.prepStatementLookup);
        returnRow = cache.db.getLookup(cache.prepStatementLookup);
        
		// Nothing found in the cache?
        if (returnRow == null && !meta.isPreloadingCache()) 
	    {
	        data.db.setValues(data.lookupRowMeta, lookupRow, data.prepStatementLookup);
	        returnRow = data.db.getLookup(data.prepStatementLookup);
	        data.returnRowMeta = data.db.getReturnRowMeta();
	        
	        if (returnRow != null && meta.getCacheSize() >= 0) 
	        {
	        	
	        }
	        
	        incrementLinesInput();
	    }        

		// This next block of code handles the dimension key LOOKUP ONLY.
		// We handle this case where "update = false" first for performance
		// reasons
		//
		if (!meta.isUpdate()) {
			if (returnRow == null) {
				returnRow = new Object[data.returnRowMeta.size()];
				returnRow[0] = data.notFoundTk;

				if (meta.getCacheSize() >= 0) // need -oo to +oo as well...
				{
					returnRow[returnRow.length - 2] = data.min_date;
					returnRow[returnRow.length - 1] = data.max_date;
				}
			} else {
				// We found the return values in row "add".
				// Throw away the version nr...
				// add.removeValue(1);

				// Rename the key field if needed. Do it directly in the row...
				// if (meta.getKeyRename()!=null &&
				// meta.getKeyRename().length()>0)
				// add.getValue(0).setName(meta.getKeyRename());
			}
		} else
		// This is the "update=true" case where we update the dimension table...
		// It is an "Insert - update" algorithm for slowly changing dimensions
		//
		{
			// The dimension entry was not found, we need to add it!
			//
			if (returnRow == null) {
				if (log.isRowLevel()) {
					logRowlevel(BaseMessages.getString(PKG,
							"DimensionLookup.Log.NoDimensionEntryFound")
							+ lookupRowMeta.getString(lookupRow) + ")");
				}

				// Date range: ]-oo,+oo[
				//

				if (data.startDateChoice != DimensionLookupMeta.START_DATE_ALTERNATIVE_SYSDATE) {
					valueDateFrom = data.min_date;
				}

				valueDateTo = data.max_date;
				valueVersion = new Long(1L); // Versions always start at 1.

				// get a new value from the sequence generator chosen.
				//
				technicalKey = null;
				switch (getTechKeyCreation()) {
				case CREATION_METHOD_TABLEMAX:
					// What's the next value for the technical key?
					technicalKey = data.db.getNextValue(getTrans()
							.getCounters(), data.realSchemaName,
							data.realTableName, meta.getKeyField());
					break;
				case CREATION_METHOD_AUTOINC:
					technicalKey = null; // Set to null to flag auto-increment usage
					break;
				case CREATION_METHOD_SEQUENCE:
					technicalKey = data.db.getNextSequenceValue(
							data.realSchemaName, meta.getSequenceName(),
							meta.getKeyField());
					if (technicalKey != null && log.isRowLevel())
						logRowlevel(BaseMessages.getString(PKG,
								"DimensionLookup.Log.FoundNextSequence")
								+ technicalKey.toString());
					break;
				default:
					break;
				}

				/*
				 * INSERT INTO table(version, datefrom, dateto, fieldlookup)
				 * VALUES(valueVersion, valueDateFrom, valueDateTo,
				 * row.fieldnrs) ;
				 */

				technicalKey = dimInsert(row, technicalKey,
						true, valueVersion, valueDateFrom, valueDateTo, null);

				incrementLinesOutput();
				returnRow = new Object[data.returnRowMeta.size()];
				int returnIndex = 0;

				returnRow[returnIndex] = technicalKey;
				returnIndex++;

				// See if we need to store this record in the cache as well...
				/*
				 * TODO: we can't really assume like this that the cache layout
				 * of the incoming rows (below) is the same as the stored data.
				 * Storing this in the cache gives us data/metadata collision
				 * errors. (class cast problems etc) Perhaps we need to convert
				 * this data to the target data types. Alternatively, we can use
				 * a separate cache in the future. Reference: PDI-911
				 * 
				 * if (meta.getCacheSize()>=0) { Object[] values =
				 * getCacheValues(rowMeta, row, technicalKey, valueVersion,
				 * valueDateFrom, valueDateTo);
				 * 
				 * // put it in the cache... if (values!=null) {
				 * addToCache(lookupRow, values); } }
				 */

				if (log.isRowLevel())
					logRowlevel(BaseMessages.getString(PKG,
							"DimensionLookup.Log.AddedDimensionEntry")
							+ data.returnRowMeta.getString(returnRow));
			} else
			//
			// The entry was found: do we need to insert, update or both?
			//
			{
				if (log.isRowLevel())
					logRowlevel(BaseMessages.getString(PKG,
							"DimensionLookup.Log.DimensionEntryFound")
							+ data.returnRowMeta.getString(returnRow));				
				
				// What's the key? The first value of the return row
				technicalKey = (Long) returnRow[data.techKeyIndex];
				valueVersion = (Long) returnRow[data.versionIndex];
				valueDateTo = (Date) returnRow[data.toDateIndex];
				returnRowTechnicalKey = (Long) returnRow[data.techKeyIndex];
				
				// If everything is the same: don't do anything
				// If one of the fields is different: insert or update
				// If all changed fields have update = Y, update
				// If one of the changed fields has update = N, insert

				insert = false;
				identical = true;
				punch = false;

				compareFieldValues(row, returnRow, valueDateFrom);

				// After comparing the record in the database and the data in
				// the input
				// and taking into account the rules of the slowly changing
				// dimension,
				// we found out whether or not to perform an insert or an
				// update.
				//
				if (!insert) // Just an update of row at key = valueKey
				{
					if (!identical) {
						if (log.isRowLevel())
							logRowlevel(BaseMessages.getString(PKG,
									"DimensionLookup.Log.UpdateRowWithValues")
									+ data.inputRowMeta.getString(row));
						/*
						 * UPDATE d_customer SET fieldlookup[] =
						 * row.getValue(fieldnrs) WHERE returnkey = dimkey
						 */
						dimUpdate(data.inputRowMeta, row, technicalKey);
						incrementLinesUpdated();
					} else {
						if (log.isRowLevel())
							logRowlevel(BaseMessages.getString(PKG,
									"DimensionLookup.Log.SkipLine"));
						// Don't do anything, everything is file in de
						// dimension.
						incrementLinesSkipped();
					}
				} else {
					if (log.isRowLevel())
						logRowlevel(BaseMessages.getString(PKG,
								"DimensionLookup.Log.InsertNewVersion")
								+ technicalKey.toString());				

					Long valueNewVersion = valueVersion + 1;

					// First try to use an AUTOINCREMENT field
					if (meta.getDatabaseMeta().supportsAutoinc()
							&& isAutoIncrement()) {
						technicalKey = null; // value to accept new key...
					} else
					// Try to get the value by looking at a SEQUENCE (oracle
					// mostly)
					if (meta.getDatabaseMeta().supportsSequences()
							&& meta.getSequenceName() != null
							&& meta.getSequenceName().length() > 0) {
						technicalKey = data.db.getNextSequenceValue(
								data.realSchemaName, meta.getSequenceName(),
								meta.getKeyField());
						if (technicalKey != null && log.isRowLevel())
							logRowlevel(BaseMessages.getString(PKG,
									"DimensionLookup.Log.FoundNextSequence2")
									+ technicalKey.toString());
					} else
					// Use our own sequence here...
					{
						// What's the next value for the technical key?
						technicalKey = data.db.getNextValue(getTrans()
								.getCounters(), data.realSchemaName,
								data.realTableName, meta.getKeyField());
					}

					// update our technicalKey with the return of the insert
					technicalKey = dimInsert(row, technicalKey, false,
							valueNewVersion, valueDateFrom, valueDateTo, returnRowTechnicalKey);
					incrementLinesOutput();
				}
				if (punch) // On of the fields we have to punch through has changed!
				{
					/*
					 * This means we have to update all versions:
					 * 
					 * UPDATE dim SET punchf1 = val1, punchf2 = val2, ... WHERE
					 * fieldlookup[] = ? ;
					 * 
					 * --> update ALL versions in the dimension table.
					 */
					dimPunchThrough(row);					
					incrementLinesUpdated();
				}

				returnRow = new Object[data.returnRowMeta.size()];
				returnRow[0] = technicalKey;
				if (log.isRowLevel())
					logRowlevel(BaseMessages.getString(PKG,
							"DimensionLookup.Log.TechnicalKey") + technicalKey);
			}
		}
		
		if (log.isRowLevel())
			logRowlevel(BaseMessages.getString(PKG,
					"DimensionLookup.Log.AddValuesToRow")
					+ data.returnRowMeta.getString(returnRow));

		// Copy the results to the output row...
		//
		// First copy the input row values to the output..
		//
		for (int i = 0; i < data.inputRowMeta.size(); i++)
			outputRow[i] = row[i];

		int outputIndex = data.inputRowMeta.size();
		int inputIndex = 0;

		// Then the technical key...
		//
		if (data.returnRowMeta.getValueMeta(0).isBigNumber()
				&& returnRow[0] instanceof Long) {
			if (log.isDebug()) {
				log.logDebug("Changing the type of the technical key from TYPE_BIGNUMBER to an TYPE_INTEGER");
			}
			ValueMetaInterface tkValueMeta = data.returnRowMeta.getValueMeta(0);
			data.returnRowMeta.setValueMeta(0, ValueMetaFactory.cloneValueMeta(
					tkValueMeta, ValueMetaInterface.TYPE_INTEGER));
		}

		outputRow[outputIndex++] = data.returnRowMeta.getInteger(returnRow,
				inputIndex++);

		// skip the version in the input
		inputIndex++;

		// Then get the "extra fields"...
		// don't return date from-to fields, they can be returned when
		// explicitly
		// specified in lookup fields.
		while (inputIndex < returnRow.length && outputIndex < outputRow.length) {
			outputRow[outputIndex] = returnRow[inputIndex];
			outputIndex++;
			inputIndex++;
		}

		// Finaly, check the date range!
		/*
		 * TODO: WTF is this??? [May be it makes sense to keep the return date
		 * from-to fields within min/max range, but even then the code below is
		 * wrong]. Value date; if (data.datefieldnr>=0) date =
		 * row.getValue(data.datefieldnr); else date = new Value("date", new
		 * Date()); // system date
		 * 
		 * if (data.min_date.compare(date)>0) data.min_date.setValue(
		 * date.getDate() ); if (data.max_date.compare(date)<0)
		 * data.max_date.setValue( date.getDate() );
		 */

		return outputRow;
	}
	
	private void compareFieldValues(Object[] row, Object[] returnRow, Date valueDateFrom) throws KettleStepException, KettleValueException
	{
		// The other values, we compare with
		int cmp;
		
		// Column lookup array - initialize to all -1
		if (columnLookupArray == null) {
			columnLookupArray = new int[meta.getFieldStream().length];
			for (int i = 0; i < columnLookupArray.length; i++) {
				columnLookupArray[i] = -1;
			}
		}
		// Integer returnRowColNum = null;
		int returnRowColNum = -1;
		String findColumn = null;
		boolean ignoreNulls = false;
		for (int i = 0; i < meta.getFieldStream().length; i++) {
			if (data.fieldnrs[i] >= 0) {
				// Only compare real fields, not last updated row, last
				// version, etc
				//
				ValueMetaInterface v1 = data.outputRowMeta
						.getValueMeta(data.fieldnrs[i]);
				Object valueData1 = row[data.fieldnrs[i]];
				findColumn = meta.getFieldLookup()[i];
				ignoreNulls = meta.getFieldIgnoreNulls()[i];
				// find the returnRowMeta based on the field in the
				// fieldLookup list
				ValueMetaInterface v2 = null;
				Object valueData2 = null;
				// Fix for PDI-8122
				// See if it's already been computed.
				returnRowColNum = columnLookupArray[i];
				if (returnRowColNum == -1) {
					// It hasn't been found yet - search the list and
					// make sure we're comparing
					// the right column to the right column.
					for (int j = 0; j < data.returnRowMeta.size(); j++) { 
						v2 = data.returnRowMeta.getValueMeta(j);
						// is this the right column?
						if ((v2.getName() != null) && (v2.getName().equalsIgnoreCase(findColumn))) { 
							columnLookupArray[i] = j; // yes - record
														// the "j" into
														// the
														// columnLookupArray
														// at [i] for
														// the next time
														// through the
														// loop
							valueData2 = returnRow[j]; // get the
														// valueData2
														// for
														// comparison
							returnRowColNum = j;
							break; // get outta here.
						} else {
							// Reset to null because otherwise, we'll
							// get a false finding at the end.
							// This could be optimized to use a
							// temporary variable to avoid the repeated
							// set if necessary
							// but it will never be as slow as the
							// database lookup anyway
							v2 = null;
						}
					}
				} else {
					// We have a value in the columnLookupArray - use
					// the value stored there.
					v2 = data.returnRowMeta
							.getValueMeta(returnRowColNum);
					valueData2 = returnRow[returnRowColNum];
				}
				if (v2 == null) {
					// If we made it here, then maybe someone tweaked
					// the XML in the transformation
					// and we're matching a stream column to a column
					// that doesn't really exist. Throw an exception.
					throw new KettleStepException(
							BaseMessages
									.getString(
											PKG,
											"DimensionLookup.Exception.ErrorDetectedInComparingFields",
											meta.getFieldStream()[i]));
				}
				
				if (valueData1 !=  null && valueData1 instanceof String) {
					valueData1 = ((String) valueData1).toLowerCase();
				}
				if (valueData2 !=  null && valueData2 instanceof String) {
					valueData2 = ((String) valueData2).toLowerCase();
				}

				try {
					if (ignoreNulls)
					{
						if (valueData1 == null && valueData2 != null)
						{
							valueData1 = valueData2;
							row[data.fieldnrs[i]] = returnRow[returnRowColNum];
						}
					}
					
					cmp = v1.compare(valueData1, v2, valueData2);					
				} catch (ClassCastException e) {
					throw e;
				}

				// Not the same and update = 'N' --> insert
				if (cmp != 0)
					identical = false;

				// Field flagged for insert: insert
				if (cmp != 0
						&& meta.getFieldUpdate()[i] == DimensionLookupMeta.TYPE_UPDATE_DIM_INSERT && valueData2 != null) {
					insert = true;
				}

				// Field flagged for punchthrough
				if (cmp != 0
						&& meta.getFieldUpdate()[i] == DimensionLookupMeta.TYPE_UPDATE_DIM_PUNCHTHROUGH) {
					punch = true;
				}

				if (log.isRowLevel())
					logRowlevel(BaseMessages
							.getString(
									PKG,
									"DimensionLookup.Log.ComparingValues", "" + v1, "" + v2, String.valueOf(cmp), String.valueOf(identical), String.valueOf(insert), String.valueOf(punch))); //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
			}
		}
	}

	/**
	 * table: dimension table keys[]: which dim-fields do we use to look up key?
	 * retval: name of the key to return datefield: do we have a datefield?
	 * datefrom, dateto: date-range, if any.
	 */
	private void setDimLookup(RowMetaInterface rowMeta)
			throws KettleDatabaseException {
		DatabaseMeta databaseMeta = meta.getDatabaseMeta();

		data.lookupRowMeta = new RowMeta();

		/*
		 * DEFAULT, SYSDATE, START_TRANS, COLUMN_VALUE :
		 * 
		 * SELECT <tk>, <version>, ... , FROM <table> WHERE key1=keys[1] AND
		 * key2=keys[2] ... AND <datefrom> <= <datefield> AND <dateto> >
		 * <datefield> ;
		 * 
		 * NULL :
		 * 
		 * SELECT <tk>, <version>, ... , FROM <table> WHERE key1=keys[1] AND
		 * key2=keys[2] ... AND ( <datefrom> is null OR <datefrom> <=
		 * <datefield> ) AND <dateto> >= <datefield>
		 */

		// tk, version, from, to, natural keys, retrieval fields...
		//
		String sql = "SELECT "
				+ databaseMeta.quoteField(meta.getKeyField());
		// sql+=", "+databaseMeta.quoteField(meta.getVersionField());
		for (int i = 0; i < meta.getKeyLookup().length; i++) {
			sql += ", " + meta.getKeyLookup()[i]; // the natural key field
													// in the table
		}
		for (int i = 0; i < meta.getFieldLookup().length; i++) {
			sql += ", " + meta.getFieldLookup()[i]; // the extra fields to
													// retrieve...
		}
		sql += ", " + databaseMeta.quoteField(meta.getVersionField());
		sql += ", " + databaseMeta.quoteField(meta.getDateFrom()); // extra
																	// info
																	// in
																	// cache
		sql += ", " + databaseMeta.quoteField(meta.getDateTo()); // extra
																	// info
																	// in
																	// cache

		sql += " FROM " + data.schemaTable + " WHERE ";

		for (int i = 0; i < meta.getKeyLookup().length; i++) {
			if (i != 0)
				sql += " AND ";
			sql += databaseMeta.quoteField(meta.getKeyLookup()[i]) + " = ? ";
			data.lookupRowMeta.addValueMeta(rowMeta
					.getValueMeta(data.keynrs[i]));
		}

		String dateFromField = databaseMeta.quoteField(meta.getDateFrom());
		String dateToField = databaseMeta.quoteField(meta.getDateTo());

		if (meta.isUsingStartDateAlternative()
				&& (meta.getStartDateAlternative() == DimensionLookupMeta.START_DATE_ALTERNATIVE_NULL)
				|| (meta.getStartDateAlternative() == DimensionLookupMeta.START_DATE_ALTERNATIVE_COLUMN_VALUE)) {
			// Null as a start date is possible...
			//
			sql += " AND ( " + dateFromField + " IS NULL OR " + dateFromField
					+ " <= ? )" + Const.CR;
			sql += " AND " + dateToField + " > ?" + Const.CR;

			data.lookupRowMeta.addValueMeta(new ValueMeta(meta.getDateFrom(),
					ValueMetaInterface.TYPE_DATE));
			data.lookupRowMeta.addValueMeta(new ValueMeta(meta.getDateTo(),
					ValueMetaInterface.TYPE_DATE));
		} else {
			// Null as a start date is NOT possible
			//
			sql += " AND ? >= " + dateFromField + Const.CR;
			sql += " AND ? < " + dateToField + Const.CR;

			data.lookupRowMeta.addValueMeta(new ValueMeta(meta.getDateFrom(),
					ValueMetaInterface.TYPE_DATE));
			data.lookupRowMeta.addValueMeta(new ValueMeta(meta.getDateTo(),
					ValueMetaInterface.TYPE_DATE));
		}
		
		

		try {
			logDetailed("Dimension Lookup setting preparedStatement to [" + sql
					+ "]");
			data.prepStatementLookup = data.db.getConnection()
					.prepareStatement(databaseMeta.stripCR(sql));
			cache.prepStatementLookup = cache.db.getConnection()
					.prepareStatement(databaseMeta.stripCR(sql));

			data.returnRowMeta = data.db.getMetaFromRow(null, data.prepStatementLookup.getMetaData());
			
			data.techKeyIndex = data.returnRowMeta.indexOfValue(meta.getKeyField());
			data.versionIndex = data.returnRowMeta.indexOfValue(meta.getVersionField());
			data.fromDateIndex = data.returnRowMeta.indexOfValue(meta.getDateFrom());
			data.toDateIndex = data.returnRowMeta.indexOfValue(meta.getDateTo());
			
			if (databaseMeta.supportsSetMaxRows()) {
				data.prepStatementLookup.setMaxRows(1); // alywas get only 1
														// line back!
			}
			if (databaseMeta.getDatabaseInterface() instanceof MySQLDatabaseMeta) {
				data.prepStatementLookup.setFetchSize(0); // Make sure to
															// DISABLE Streaming
															// Result sets
			}
			logDetailed("Finished preparing dimension lookup statement.");
		} catch (SQLException ex) {
			throw new KettleDatabaseException(
					"Unable to prepare dimension lookup", ex);
		}
	}

	protected boolean isAutoIncrement() {
		return techKeyCreation == CREATION_METHOD_AUTOINC;
	}

	/**
	 * This inserts new record into dimension Optionally, if the entry already
	 * exists, update date range from previous version of the entry.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public Long dimInsert(Object[] row, Long technicalKey, boolean newEntry,
		    Long versionNr, Date dateFrom, Date dateTo, Long returnRowTechnicalKey ) throws KettleException {
		// data.prepStatementInsert == null && data.prepStatementUpdate == null)
		// first time: construct prepared statement
		if (data.prepStatementInsert == null || data.prepStatementUpdatePrevVersion ==  null) {
			createDimInsertStatement();
		}

		Object[] insertRow = new Object[data.insertRowMeta.size()];
		int insertIndex = 0;
		
		// add technical key value
		if (!isAutoIncrement()) {
			insertRow[insertIndex++] = technicalKey;
		}

		// add natural key values
		for (int i = 0; i < data.keynrs.length; i++) {
			insertRow[insertIndex++] = row[data.keynrs[i]];
		}
		
		// add field values
		for (int i = 0; i < data.fieldnrs.length; i++) {
			if (data.fieldnrs[i] >= 0) {
				// Ignore last_version, last_updated, etc. These are handled
				// below...
				//
				insertRow[insertIndex++] = row[data.fieldnrs[i]];
			}
		}
		
		// The special update field values
		//
		for (int i = 0; i < meta.getFieldUpdate().length; i++) {
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSERTED:
				insertRow[insertIndex++] = new Date();
				break;
			case DimensionLookupMeta.TYPE_UPDATE_LAST_VERSION:
				insertRow[insertIndex++] = Boolean.TRUE;
				break;
			default:
				break;
			}
		}
		
		// add version number
		insertRow[insertIndex++] = versionNr;
		
		// add date_from
		switch (data.startDateChoice) {
		case DimensionLookupMeta.START_DATE_ALTERNATIVE_NONE:
			insertRow[insertIndex++] = dateFrom;
			break;
		case DimensionLookupMeta.START_DATE_ALTERNATIVE_SYSDATE:
			// use the time the step execution begins as the date from (passed
			// in as dateFrom).
			// before, the current system time was used. this caused an
			// exclusion of the row in the
			// lookup portion of the step that uses this 'valueDate' and not the
			// current time.
			// the result was multiple inserts for what should have been 1
			// [PDI-4317]
			insertRow[insertIndex++] = dateFrom;
			break;
		case DimensionLookupMeta.START_DATE_ALTERNATIVE_START_OF_TRANS:
			insertRow[insertIndex++] = getTrans().getStartDate();
			break;
		case DimensionLookupMeta.START_DATE_ALTERNATIVE_NULL:
			insertRow[insertIndex++] = null;
			break;
		case DimensionLookupMeta.START_DATE_ALTERNATIVE_COLUMN_VALUE:
			insertRow[insertIndex++] = data.inputRowMeta.getDate(row,
					data.startDateFieldIndex);
			break;
		default:
			throw new KettleStepException(BaseMessages.getString(PKG,
					"DimensionLookup.Exception.IllegalStartDateSelection",
					Integer.toString(data.startDateChoice)));
		}

		// add date_to
		insertRow[insertIndex++] = dateTo;
		
		// INSERT NEW VALUE! AND DO UPDATES!
		data.db.setValues(data.insertRowMeta, insertRow, data.prepStatementInsert);
		data.db.insertRow(data.prepStatementInsert, !isAutoIncrement(), false);
		
		// check for logging level
		if (log.isDebug())
			logDebug("Row inserted!");
		
		// get generated key if auto increment is true
		if (isAutoIncrement()) {
			try 
			{
				RowMetaAndData keys = data.db.getGeneratedKeys(data.prepStatementInsert);
				if (keys.getRowMeta().size() > 0) 
				{
					technicalKey = keys.getRowMeta().getInteger(keys.getData(), 0);
				} 
				else 
				{
					throw new KettleDatabaseException("Unable to retrieve value of auto-generated technical key : no value found!");
				}
			} catch (Exception e) {
				throw new KettleDatabaseException(
						"Unable to retrieve value of auto-generated technical key : unexpected error: ",
						e);
			}
		}
		
		// INSERT NEW VALUE INTO CACHE!
		if (meta.getCacheSize() >= 0) 
		{
			Object[] cacheInsertRow = new Object[cache.insertRowMeta.size()];
			if (isAutoIncrement())
			{
				cacheInsertRow[0] = technicalKey;
				System.arraycopy(insertRow, 0, cacheInsertRow, 1, insertRow.length);
			} else cacheInsertRow = insertRow;
			
			cache.db.setValues(cache.insertRowMeta, cacheInsertRow, cache.prepStatementInsert);
			cache.db.insertRow(cache.prepStatementInsert, true, false);
		}

		if (!newEntry) // we have to update the previous version in the dimension!
		{
			/*
			 * UPDATE d_customer SET dateto = val_datfrom , last_updated = <now>
			 * , last_version = false WHERE keylookup[] = keynrs[] AND
			 * techkey = returnrowtechkey;
			 */
			 
			Object[] updateRow = new Object[data.updateRowMeta.size()];
			int updateIndex = 0;
			
			switch (data.startDateChoice) {
			case DimensionLookupMeta.START_DATE_ALTERNATIVE_NONE:
				updateRow[updateIndex++] = dateFrom;
				break;
			case DimensionLookupMeta.START_DATE_ALTERNATIVE_SYSDATE:
				updateRow[updateIndex++] = new Date();
				break;
			case DimensionLookupMeta.START_DATE_ALTERNATIVE_START_OF_TRANS:
				updateRow[updateIndex++] = getTrans().getCurrentDate();
				break;
			case DimensionLookupMeta.START_DATE_ALTERNATIVE_NULL:
				updateRow[updateIndex++] = null;
				break;
			case DimensionLookupMeta.START_DATE_ALTERNATIVE_COLUMN_VALUE:
				updateRow[updateIndex++] = data.inputRowMeta.getDate(row,
						data.startDateFieldIndex);
				break;
			default:
				throw new KettleStepException(BaseMessages.getString(
						"DimensionLookup.Exception.IllegalStartDateSelection",
						Integer.toString(data.startDateChoice)));
			}

			// The special update fields...
			//
			for (int i = 0; i < meta.getFieldUpdate().length; i++) {
				switch (meta.getFieldUpdate()[i]) {
				case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
					updateRow[updateIndex++] = new Date();
					break;
				case DimensionLookupMeta.TYPE_UPDATE_LAST_VERSION:
					updateRow[updateIndex++] = Boolean.FALSE;
					break; // Never the last version on this update
				case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
					updateRow[updateIndex++] = new Date();
					break;
				default:
					break;
				}
			}

			updateRow[updateIndex++] = returnRowTechnicalKey;

			if (log.isRowLevel())
				logRowlevel("UPDATE using rupd="
						+ data.updateRowMeta.getString(updateRow));
			
			data.db.setValues(data.updateRowMeta, updateRow, data.prepStatementUpdatePrevVersion);
			data.db.insertRow(data.prepStatementUpdatePrevVersion, !isAutoIncrement(), false);
			
			// UPDATE EXISTING RECORD IN CACHE!
			if (meta.getCacheSize() >= 0) 
			{
				cache.db.setValues(cache.updateRowMeta, updateRow, cache.prepStatementUpdatePrevVersion);
				cache.db.insertRow(cache.prepStatementUpdatePrevVersion, true, false);
			}
		}
		
		if (log.isDebug())
			logDebug("rins, size=" + data.insertRowMeta.size() + ", values="
					+ data.insertRowMeta.getString(insertRow));

		commitcounter++;

		return technicalKey;
	}

	public void dimUpdate(RowMetaInterface rowMeta, Object[] row, Long dimkey) throws KettleDatabaseException 
	{
		if (data.prepStatementDimensionUpdate == null) // first time: construct
														// prepared statement
		{
			data.dimensionUpdateRowMeta = new RowMeta();

			// Construct the SQL statement...
			/*
			 * UPDATE d_customer SET fieldlookup[] = row.getValue(fieldnrs) ,
			 * last_updated = <now> WHERE returnkey = dimkey ;
			 */

			String sql = "UPDATE " + data.schemaTable + Const.CR + "SET ";
			boolean comma = false;
			for (int i = 0; i < meta.getFieldLookup().length; i++) {
				if (!DimensionLookupMeta.isUpdateTypeWithoutArgument(
						meta.isUpdate(), meta.getFieldUpdate()[i])) {
					if (comma)
						sql += ", ";
					else
						sql += "  ";
					comma = true;
					sql += meta.getDatabaseMeta().quoteField(
							meta.getFieldLookup()[i])
							+ " = ?" + Const.CR;
					data.dimensionUpdateRowMeta.addValueMeta(rowMeta
							.getValueMeta(data.fieldnrs[i]));
				}
			}

			// The special update fields...
			//
			for (int i = 0; i < meta.getFieldUpdate().length; i++) {
				ValueMetaInterface valueMeta = null;
				switch (meta.getFieldUpdate()[i]) {
				case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
				case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
					valueMeta = new ValueMeta(meta.getFieldLookup()[i],
							ValueMetaInterface.TYPE_DATE);
					break;
				default:
					break;
				}
				if (valueMeta != null) {
					if (comma)
						sql += ", ";
					else
						sql += "  ";
					comma = true;
					sql += meta.getDatabaseMeta().quoteField(
							valueMeta.getName())
							+ " = ?" + Const.CR;
					data.dimensionUpdateRowMeta.addValueMeta(valueMeta);
				}
			}

			sql += "WHERE  "
					+ meta.getDatabaseMeta().quoteField(meta.getKeyField())
					+ " = ?";
			data.dimensionUpdateRowMeta.addValueMeta(new ValueMeta(meta
					.getKeyField(), ValueMetaInterface.TYPE_INTEGER));
			sql += "; ";

			try {
				if (log.isDebug())
					logDebug("Preparing statement: [" + sql + "]");
				data.prepStatementDimensionUpdate = data.db.getConnection()
						.prepareStatement(meta.getDatabaseMeta().stripCR(sql));
				cache.prepStatementDimensionUpdate = cache.db.getConnection()
						.prepareStatement(meta.getDatabaseMeta().stripCR(sql));
			} catch (SQLException ex) {
				throw new KettleDatabaseException(
						"Couldn't prepare statement :" + Const.CR + sql, ex);
			}
		}

		// Assemble information
		// New
		Object[] dimensionUpdateRow = new Object[data.dimensionUpdateRowMeta
				.size()];
		int updateIndex = 0;
		for (int i = 0; i < data.fieldnrs.length; i++) {
			// Ignore last_version, last_updated, etc. These are handled
			// below...
			//
			if (data.fieldnrs[i] >= 0) {
				dimensionUpdateRow[updateIndex++] = row[data.fieldnrs[i]];
			}
		}
		for (int i = 0; i < meta.getFieldUpdate().length; i++) {
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
				dimensionUpdateRow[updateIndex++] = new Date(); //getTrans().getCurrentDate();
				break;
			default:
				break;
			}
		}
		dimensionUpdateRow[updateIndex++] = dimkey;

		data.db.setValues(data.dimensionUpdateRowMeta, dimensionUpdateRow, data.prepStatementDimensionUpdate);
		data.db.insertRow(data.prepStatementDimensionUpdate, !isAutoIncrement(), false);
		
		if (meta.getCacheSize() > 0)
		{
			cache.db.setValues(data.dimensionUpdateRowMeta, dimensionUpdateRow, cache.prepStatementDimensionUpdate);
			cache.db.insertRow(cache.prepStatementDimensionUpdate, true, false);
		}
		commitcounter++;
	}

	// This updates all versions of a dimension entry.
	//
	public void dimPunchThrough(Object[] row)
			throws KettleDatabaseException {
		if (data.prepStatementPunchThrough == null) // first time: construct
													// prepared statement
		{
			DatabaseMeta databaseMeta = meta.getDatabaseMeta();
			data.punchThroughRowMeta = new RowMeta();

			/*
			 * UPDATE table SET punchv1 = fieldx, ... , last_updated = <now>
			 * WHERE keylookup[] = keynrs[] ;
			 */

			String sql_upd = "UPDATE " + data.schemaTable + Const.CR;
			sql_upd += "SET ";
			boolean first = true;
			for (int i = 0; i < meta.getFieldLookup().length; i++) {
				if (meta.getFieldUpdate()[i] == DimensionLookupMeta.TYPE_UPDATE_DIM_PUNCHTHROUGH) {
					if (!first)
						sql_upd += ", ";
					else
						sql_upd += "  ";
					first = false;
					sql_upd += databaseMeta
							.quoteField(meta.getFieldLookup()[i])
							+ " = ?"
							+ Const.CR;
					data.punchThroughRowMeta.addValueMeta(data.inputRowMeta
							.getValueMeta(data.fieldnrs[i]));
				}
			}
			// The special update fields...
			//
			for (int i = 0; i < meta.getFieldUpdate().length; i++) {
				ValueMetaInterface valueMeta = null;
				switch (meta.getFieldUpdate()[i]) {
				case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
				case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
					valueMeta = new ValueMeta(meta.getFieldLookup()[i],
							ValueMetaInterface.TYPE_DATE);
					break;
				default:
					break;
				}
				if (valueMeta != null) {
					sql_upd += ", "
							+ databaseMeta.quoteField(valueMeta.getName())
							+ " = ?" + Const.CR;
					data.punchThroughRowMeta.addValueMeta(valueMeta);
				}
			}

			sql_upd += "WHERE ";
			for (int i = 0; i < meta.getKeyLookup().length; i++) {
				if (i > 0)
					sql_upd += "AND   ";
				sql_upd += databaseMeta.quoteField(meta.getKeyLookup()[i])
						+ " = ?" + Const.CR;
				data.punchThroughRowMeta.addValueMeta(data.inputRowMeta
						.getValueMeta(data.keynrs[i]));
			}
			sql_upd += "; ";

			try {
				data.prepStatementPunchThrough = data.db.getConnection().prepareStatement(
								meta.getDatabaseMeta().stripCR(sql_upd));
				cache.prepStatementPunchThrough = cache.db.getConnection().prepareStatement(
						meta.getDatabaseMeta().stripCR(sql_upd));
			} catch (SQLException ex) {
				throw new KettleDatabaseException(
						"Unable to prepare dimension punchThrough update statement : "
								+ Const.CR + sql_upd, ex);
			}
		}

		Object[] punchThroughRow = new Object[data.punchThroughRowMeta.size()];
		int punchIndex = 0;

		for (int i = 0; i < meta.getFieldLookup().length; i++) {
			if (meta.getFieldUpdate()[i] == DimensionLookupMeta.TYPE_UPDATE_DIM_PUNCHTHROUGH) {
				punchThroughRow[punchIndex++] = row[data.fieldnrs[i]];
			}
		}
		for (int i = 0; i < meta.getFieldUpdate().length; i++) {
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
				punchThroughRow[punchIndex++] = new Date();
				break;
			default:
				break;
			}
		}
		for (int i = 0; i < data.keynrs.length; i++) {
			punchThroughRow[punchIndex++] = row[data.keynrs[i]];
		}

		// UPDATE VALUES
		data.db.setValues(data.punchThroughRowMeta, punchThroughRow, data.prepStatementPunchThrough); // set values for update
		data.db.insertRow(data.prepStatementPunchThrough, !isAutoIncrement(), false); // do the actual
															// punch through
															// update
		
		if (meta.getCacheSize()>0)
		{
			cache.db.setValues(data.punchThroughRowMeta, punchThroughRow, cache.prepStatementPunchThrough); // set values for update
			cache.db.insertRow(cache.prepStatementPunchThrough, true, false);
		}
		
		commitcounter++;
	}

	public void checkDimZero() throws KettleException {
		// Don't insert anything when running in lookup mode.
		//
		if (!meta.isUpdate())
			return;

		DatabaseMeta databaseMeta = meta.getDatabaseMeta();
		int start_tk = databaseMeta.getNotFoundTK(isAutoIncrement());

		if (meta.isAutoIncrement()) {
			// See if there are rows in the table
			// If so, we can't insert the unknown row anymore...
			//
			String sql = "SELECT count(*) FROM " + data.schemaTable + " WHERE "
					+ databaseMeta.quoteField(meta.getKeyField()) + " = "
					+ start_tk;
			RowMetaAndData r = data.db.getOneRow(sql);
			Long count = r.getRowMeta().getInteger(r.getData(), 0);
			if (count.longValue() != 0) {
				return; // Can't insert below the rows already in there...
			}
		}

		String sql = "SELECT count(*) FROM " + data.schemaTable + " WHERE "
				+ databaseMeta.quoteField(meta.getKeyField()) + " = "
				+ start_tk;
		RowMetaAndData r = data.db.getOneRow(sql);
		Long count = r.getRowMeta().getInteger(r.getData(), 0);
		if (count.longValue() == 0) {
			String isql = null;
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
			try {
				if (!databaseMeta.supportsAutoinc() || !isAutoIncrement()) {
					isql = "insert into " + data.schemaTable + "("
							+ databaseMeta.quoteField(meta.getKeyField())
							+ ", "
							+ databaseMeta.quoteField(meta.getVersionField())
							+ ", "
							+ databaseMeta.quoteField(meta.getDateFrom())
							+ ", "
							+ databaseMeta.quoteField(meta.getDateTo())
							+ ") values (0, 1, '" + df.format(meta.getMinDate()) + "','" + df.format(meta.getMaxDate()) + "')";
				} else {
					isql = databaseMeta.getSQLInsertAutoIncUnknownDimensionRow(
							data.schemaTable,
							databaseMeta.quoteField(meta.getKeyField()),
							databaseMeta.quoteField(meta.getVersionField()));
				}

				data.db.execStatement(databaseMeta.stripCR(isql));
			} catch (KettleException e) {
				throw new KettleDatabaseException(
						"Error inserting 'unknown' row in dimension ["
								+ data.schemaTable + "] : " + isql, e);
			}
		}
	}

	public void createDimInsertStatement() throws KettleDatabaseException 
	{
		DatabaseMeta databaseMeta = meta.getDatabaseMeta();
		RowMetaInterface insertRowMeta = new RowMeta();
		RowMetaInterface inputRowMeta = data.inputRowMeta;
		
		RowMetaInterface dimInsertRowMeta = new RowMeta();
		RowMetaInterface cacheInsertRowMeta = new RowMeta();

		/*
		 * Construct the SQL statement...
		 * 
		 * INSERT INTO d_customer(keyfield, versionfield, datefrom, dateto,
		 * key[], fieldlookup[], last_updated, last_inserted, last_version)
		 * VALUES (val_key ,val_version , val_datfrom, val_datto, keynrs[],
		 * fieldnrs[], last_updated, last_inserted, last_version) ;
		 */

		String sql = "INSERT INTO " + data.schemaTable + "( ";
		
		// add placeholder for technical key
		sql += " %s ";
		
		// add natural keys
		for (int i = 0; i < meta.getKeyLookup().length; i++) {
			if (i > 0)
			{
				sql += ", ";
			}
			sql += databaseMeta.quoteField(meta.getKeyLookup()[i]);
			insertRowMeta.addValueMeta(new ValueMeta(meta.getKeyLookup()[i],
					inputRowMeta.getValueMeta(data.keynrs[i]).getType()));
		}
		
		// add fields
		for (int i = 0; i < meta.getFieldLookup().length; i++) {
			// Ignore last_version, last_updated etc, they are handled below
			// (at the
			// back of the row).
			//
			if (!DimensionLookupMeta.isUpdateTypeWithoutArgument(
					meta.isUpdate(), meta.getFieldUpdate()[i])) {
				sql += ", "
						+ databaseMeta.quoteField(meta.getFieldLookup()[i]);
				insertRowMeta.addValueMeta(inputRowMeta.getValueMeta(data.fieldnrs[i]));
			}
		}
		
		// add special update fields
		for (int i = 0; i < meta.getFieldUpdate().length; i++) 
		{
			ValueMetaInterface valueMeta = null;
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSERTED:
				valueMeta = new ValueMeta(meta.getFieldLookup()[i],
						ValueMetaInterface.TYPE_DATE);
				break;
			case DimensionLookupMeta.TYPE_UPDATE_LAST_VERSION:
				valueMeta = new ValueMeta(meta.getFieldLookup()[i],
						ValueMetaInterface.TYPE_BOOLEAN);
				break;
			default:
				break;
			}
			if (valueMeta != null) {
				sql += ", " + databaseMeta.quoteField(valueMeta.getName());
				insertRowMeta.addValueMeta(valueMeta);
			}
		}

		// add version field
		if (meta.isUpdate()) {
			sql += ", " + databaseMeta.quoteField(meta.getVersionField());
			insertRowMeta.addValueMeta(new ValueMeta(meta.getVersionField(),
					ValueMetaInterface.TYPE_INTEGER));
		}
		
		// add from and to date fields
		sql += ", " + databaseMeta.quoteField(meta.getDateFrom())
				+ ", " + databaseMeta.quoteField(meta.getDateTo());		
		insertRowMeta.addValueMeta(new ValueMeta(meta.getDateFrom(),
				ValueMetaInterface.TYPE_DATE));
		insertRowMeta.addValueMeta(new ValueMeta(meta.getDateTo(),
				ValueMetaInterface.TYPE_DATE));

		sql += ") VALUES (";
		
		// add placeholder for technical key
		sql += " %s ";
		
		// add placeholders for natural keys
		for (int i = 0; i < data.keynrs.length; i++) {
			if (i > 0)
			{
				sql += ", ";
			}
			sql += "?";
		}

		// add placeholders for all fields
		for (int i = 0; i < meta.getFieldLookup().length; i++) {
			// Ignore last_version, last_updated, etc. These are handled
			// below...
			//
			if (!DimensionLookupMeta.isUpdateTypeWithoutArgument(
					meta.isUpdate(), meta.getFieldUpdate()[i])) {
				sql += ", ?";
			}
		}
		
		// add placeholders for special update fields
		for (int i = 0; i < meta.getFieldUpdate().length; i++) 
		{
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSERTED:
			case DimensionLookupMeta.TYPE_UPDATE_LAST_VERSION:
				sql += ", ?";
				break;
			default:
				break;
			}
		}

		// add placeholder for version
		if (meta.isUpdate()) {
			sql += ", ?";
		}
		// add placeholders for date_from, date_to
		sql += ", ?, ?";

		sql += " ); ";
		
		// create insert sql for dim and cache
		String dim_insert_sql = "";
		String cache_insert_sql = "";
		
		// add technical key to cache insert sql
		cache_insert_sql = String.format(sql, databaseMeta.quoteField(meta.getKeyField()) + ", ", "?, ");
		cacheInsertRowMeta.addValueMeta(data.outputRowMeta
				.getValueMeta(inputRowMeta.size()));
		cacheInsertRowMeta.addRowMeta(insertRowMeta);
		
		// add technical key to dim insert sql
		if (!isAutoIncrement()) {
			dim_insert_sql = String.format(sql, databaseMeta.quoteField(meta.getKeyField()) + ", ", "?, "); // NO AUTOINCREMENT
			dimInsertRowMeta.addValueMeta(data.outputRowMeta
					.getValueMeta(inputRowMeta.size())); // the first return
															// value after
															// the input
		} else if (databaseMeta.needsPlaceHolder()) {
			dim_insert_sql = String.format(sql, "0, ", ""); // placeholder on informix!
		} else {
			dim_insert_sql = String.format(sql, "", "");
		}
		
		dimInsertRowMeta.addRowMeta(insertRowMeta);

		/*
		 * UPDATE d_customer SET dateto = val_datnow, datefrom = originaldatefrom or returnrowupdatedate, 
		 * last_updated = <now>, last_version = false WHERE techkey = returnrowtechkey
		 */
		RowMetaInterface updateRowMeta = new RowMeta();

		String upd_sql = " UPDATE " + data.schemaTable + Const.CR;

		// The end of the date range
		//
		upd_sql += "SET " + databaseMeta.quoteField(meta.getDateTo())
				+ " = ?" + Const.CR;
		updateRowMeta.addValueMeta(new ValueMeta(meta.getDateTo(),
				ValueMetaInterface.TYPE_DATE));		

		// The special update fields...
		//
		for (int i = 0; i < meta.getFieldUpdate().length; i++) {
			ValueMetaInterface valueMeta = null;
			switch (meta.getFieldUpdate()[i]) {
			case DimensionLookupMeta.TYPE_UPDATE_DATE_INSUP:
			case DimensionLookupMeta.TYPE_UPDATE_DATE_UPDATED:
				valueMeta = new ValueMeta(meta.getFieldLookup()[i],
						ValueMetaInterface.TYPE_DATE);
				break;
			case DimensionLookupMeta.TYPE_UPDATE_LAST_VERSION:
				valueMeta = new ValueMeta(meta.getFieldLookup()[i],
						ValueMetaInterface.TYPE_BOOLEAN);
				break;
			default:
				break;
			}
			if (valueMeta != null) {
				upd_sql += ", "
						+ databaseMeta.quoteField(valueMeta.getName())
						+ " = ?" + Const.CR;
				updateRowMeta.addValueMeta(valueMeta);
			}
		}

		upd_sql += "WHERE " + databaseMeta.quoteField(meta.getKeyField()) + " = ? ";
		updateRowMeta.addValueMeta(new ValueMeta(meta.getKeyField(),
				ValueMetaInterface.TYPE_INTEGER));
		
		upd_sql += "; ";
		
		try {
			if (getTechKeyCreation() == CREATION_METHOD_AUTOINC) {
				logDetailed("SQL w/ return keys=[" + dim_insert_sql + "]");
				data.prepStatementInsert = data.db.getConnection().prepareStatement(databaseMeta.stripCR(dim_insert_sql), Statement.RETURN_GENERATED_KEYS);
				data.prepStatementUpdatePrevVersion = data.db.getConnection().prepareStatement(databaseMeta.stripCR(upd_sql));
			} else {
				logDetailed("SQL=[" + dim_insert_sql + "]");
				data.prepStatementInsert = data.db.getConnection().prepareStatement(databaseMeta.stripCR(dim_insert_sql));
				data.prepStatementUpdatePrevVersion = data.db.getConnection().prepareStatement(databaseMeta.stripCR(upd_sql));
			}
			
			cache.prepStatementInsert = cache.db.getConnection().prepareStatement(databaseMeta.stripCR(cache_insert_sql));
			cache.prepStatementUpdatePrevVersion = cache.db.getConnection().prepareStatement(databaseMeta.stripCR(upd_sql));
		} catch (SQLException ex) {
			throw new KettleDatabaseException(
					"Unable to prepare dimension insert :" + Const.CR + sql + Const.CR + upd_sql,
					ex);
		}
		
		data.insertRowMeta = dimInsertRowMeta;
		data.updateRowMeta = updateRowMeta;
		
		cache.insertRowMeta = cacheInsertRowMeta;
		cache.updateRowMeta = updateRowMeta;
	}
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (DimensionLookupMeta) smi;
		data = (DimensionLookupData) sdi;

		if (super.init(smi, sdi)) {
			data.min_date = meta.getMinDate();
			data.max_date = meta.getMaxDate();

			data.realSchemaName = environmentSubstitute(meta.getSchemaName());
			data.realTableName = environmentSubstitute(meta.getTableName());

			data.startDateChoice = DimensionLookupMeta.START_DATE_ALTERNATIVE_NONE;
			if (meta.isUsingStartDateAlternative())
				data.startDateChoice = meta.getStartDateAlternative();
			if (meta.getDatabaseMeta() == null) {
				logError(BaseMessages
						.getString(PKG,
								"DimensionLookup.Init.ConnectionMissing",
								getStepname()));
				return false;
			}
			data.db = new Database(this, meta.getDatabaseMeta());
			data.db.shareVariablesWith(this);
			try {
				if (getTransMeta().isUsingUniqueConnections()) {
					synchronized (getTrans()) {
						data.db.connect(getTrans().getTransactionId(),
								getPartitionID());
					}
				} else {
					data.db.connect(getPartitionID());
				}

				if (log.isDetailed())
					logDetailed(BaseMessages.getString(PKG,
							"DimensionLookup.Log.ConnectedToDB"));
				data.db.setCommit(meta.getCommitSize());

				return true;
			} catch (KettleException ke) {
				logError(BaseMessages.getString(PKG,
						"DimensionLookup.Log.ErrorOccurredInProcessing")
						+ ke.getMessage());
			}
		}
		return false;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (DimensionLookupMeta) smi;
		data = (DimensionLookupData) sdi;
		if (data.db != null) {
			try {
				if (!data.db.isAutoCommit()) {
					if (getErrors() == 0) {
						executeBatch(data.prepStatementInsert);
						executeBatch(data.prepStatementUpdatePrevVersion);
						executeBatch(data.prepStatementDimensionUpdate);
						executeBatch(data.prepStatementPunchThrough);
						data.db.commit();
						
						cache.db.disconnect();
					} else {
						data.db.rollback();
					}
				}
			} catch (KettleException e) {
				logError(BaseMessages.getString(PKG,
						"DimensionLookup.Log.ErrorOccurredInProcessing")
						+ e.getMessage());
			} finally {
				data.db.disconnect();
			}
		}
		super.dispose(smi, sdi);
	}

}
