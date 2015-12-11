/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.Test;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.core.util.Assert;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepPartitioningMeta;

public class DimensionCacheTest {
	
  private DatabaseMeta databaseMeta;

  private StepMeta stepMeta;

  private DimensionLookup dimensionLookup, dimensionLookupSpy;
  private DimensionLookupMeta dimensionLookupMeta;
  private DimensionLookupData dimensionLookupData;

  @Test
  public void testCompareDateInterval() throws KettleDatabaseException, SQLException {
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaTimestamp( "DATE_FROM" ) );
    rowMeta.addValueMeta( new ValueMetaTimestamp( "DATE_TO" ) );
    int[] keyIndexes = new int[] {};
    int fromDateIndex = 0;
    int toDateIndex = 1;
    
    databaseMeta = mock( DatabaseMeta.class );
    doReturn( "" ).when( databaseMeta ).quoteField( anyString() );

    dimensionLookupMeta = mock( DimensionLookupMeta.class );
    doReturn( databaseMeta ).when( dimensionLookupMeta ).getDatabaseMeta();
    doReturn( new String[]{} ).when( dimensionLookupMeta ).getKeyLookup();
    doReturn( new String[]{} ).when( dimensionLookupMeta ).getFieldLookup();
    doReturn( new int[]{} ).when( dimensionLookupMeta ).getFieldUpdate();

    stepMeta = mock( StepMeta.class );
    doReturn( "step" ).when( stepMeta ).getName();
    doReturn( mock( StepPartitioningMeta.class ) ).when( stepMeta ).getTargetStepPartitioningMeta();
    doReturn( dimensionLookupMeta ).when( stepMeta ).getStepMetaInterface();

    Database db = mock( Database.class );
    doReturn( mock( Connection.class ) ).when( db ).getConnection();

    dimensionLookupData = mock( DimensionLookupData.class );
    dimensionLookupData.db = db;
    dimensionLookupData.keynrs = new int[] { };
    dimensionLookupData.fieldnrs = new int[] { };

    TransMeta transMeta = mock( TransMeta.class );
    doReturn( stepMeta ).when( transMeta ).findStep( anyString() );

    dimensionLookup = new DimensionLookup( stepMeta, dimensionLookupData, 1, transMeta, mock( Trans.class ) );
    dimensionLookup.setData( dimensionLookupData );
    dimensionLookup.setMeta( dimensionLookupMeta );
    dimensionLookupSpy = spy( dimensionLookup );
    doReturn( stepMeta ).when( dimensionLookupSpy ).getStepMeta();
    doReturn( false ).when( dimensionLookupSpy ).isRowLevel();
    doReturn( false ).when( dimensionLookupSpy ).isDebug();
    doReturn( true ).when( dimensionLookupSpy ).isAutoIncrement();
    doNothing().when( dimensionLookupSpy ).logDetailed( anyString() );
    DimensionCache dc = new DimensionCache( dimensionLookup, dimensionLookupMeta, dimensionLookupData );

    long t0 = 1425300000000L; // (3/2/15 4:40 PM)
    final Date D1 = new Timestamp( t0 );
    final Date D2 = new Timestamp( t0 + 3600000L );
    final Date D3 = new Timestamp( t0 + 3600000L * 2 );
    final Date D4 = new Timestamp( t0 + 3600000L * 3 );
    final Date D5 = new Timestamp( t0 + 3600000L * 4 );

    // [PDI-13508] NPE in DimensionCache class after update to Java 1.7u76
    // fix prevents NullPointerException in the combinations marked "NPE"

    assertCompareDateInterval( dc, null, null, null, null, 0 );
    assertCompareDateInterval( dc, null, null, D1, null, -1 );

    assertCompareDateInterval( dc, D2, null, null, null, 1 );
    assertCompareDateInterval( dc, D2, null, D1, null, 1 );
    assertCompareDateInterval( dc, D2, null, D2, null, 0 );
    assertCompareDateInterval( dc, D2, null, D3, null, -1 );

    assertCompareDateInterval( dc, D2, D4, null, null, 1 ); // NPE
    assertCompareDateInterval( dc, D2, D4, D1, null, 1 );
    assertCompareDateInterval( dc, D2, D4, D2, null, 0 );
    assertCompareDateInterval( dc, D2, D4, D3, null, 0 );
    assertCompareDateInterval( dc, D2, D4, D4, null, -1 );
    assertCompareDateInterval( dc, D2, D4, D5, null, -1 );

    assertCompareDateInterval( dc, null, D4, null, null, 0 ); // NPE
    assertCompareDateInterval( dc, null, D4, D3, null, 0 );
    assertCompareDateInterval( dc, null, D4, D4, null, -1 ); // NPE
    assertCompareDateInterval( dc, null, D4, D5, null, -1 ); // NPE
  }

  private static void assertCompareDateInterval( DimensionCache dc, Object from1, Object to1, Object from2, Object to2,
      int expectedValue ) {

    final int actualValue = 0; // dc.compare( new Object[] { from1, to1 }, new Object[] { from2, to2 } );

    boolean success = ( expectedValue == 0 && actualValue == 0 ) //
        || ( expectedValue < 0 && actualValue < 0 ) //
        || ( expectedValue > 0 && actualValue > 0 );
    Assert.assertTrue( success, "{0} expected, {1} actual. compare( [({2}), ({3})], [({4}), ({5})] )", //
        expectedValue, actualValue, from1, to1, from2, to2 );
  }

}
