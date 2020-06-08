/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;


public class ViewTest1 extends CQLTester
{
    ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    private void updateView(String query, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) Stage.VIEW_MUTATION.executor()).getPendingTaskCount() == 0
                && ((SEPExecutor) Stage.VIEW_MUTATION.executor()).getActiveTaskCount() == 0))
        {
            Thread.sleep(1);
        }
    }

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage(String.format("Materialized view '%s.view_does_not_exist' doesn't exist", KEYSPACE), "DROP MATERIALIZED VIEW " + KEYSPACE + ".view_does_not_exist");
        assertInvalidMessage("Materialized view 'keyspace_does_not_exist.view_does_not_exist' doesn't exist", "DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist");

        execute("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".view_does_not_exist");
        execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist");
    }

    @Test
    public void testExistingRangeTombstoneWithFlush() throws Throwable
    {
        testExistingRangeTombstone(true);
    }

    @Test
    public void testExistingRangeTombstoneWithoutFlush() throws Throwable
    {
        testExistingRangeTombstone(false);
    }

    public void testExistingRangeTombstone(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k1, c1, c2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1",
                   "CREATE MATERIALIZED VIEW view1 AS SELECT * FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (k1, c2, c1)");

        updateView("DELETE FROM %s USING TIMESTAMP 10 WHERE k1 = 1 and c1=1");

        if (flush)
            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 0, 0, 0) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 1, 0, 1) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 0, 1, 0) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 1, 1, 1) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 2, 1, 2) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 3, 1, 3) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 2, 0, 2, 0) USING TIMESTAMP 5; " +
                "APPLY BATCH");

        assertRowsIgnoringOrder(execute("select * from %s"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
        assertRowsIgnoringOrder(execute("select k1,c1,c2,v1,v2 from view1"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
    }

    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1, c1, val FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from view1").size());
    }

    @Test
    public void createMvWithUnrestrictedPKParts() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT val, k1, c1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1, c1, val FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from view1").size());
    }

    @Test
    public void testPrimaryKeyIsNotNull() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must include both when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        dropTable("DROP TABLE %s");

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY(k, asciival))");
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must still include both even when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "sval text static, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)");
            Assert.fail("Use of static column in a MV primary key should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)");
            Assert.fail("Explicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");
            Assert.fail("Implicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

        Assert.assertEquals(2, execute("select * from %s").size());

        assertRows(execute("SELECT sval from %s"), row("bar99"), row("bar99"));

        Assert.assertEquals(2, execute("select * from mv_static").size());

        assertInvalid("SELECT sval from mv_static");
    }


    @Test
    public void testOldTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_tstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv_tstest").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"), row(0));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(1));
    }

    @Test
    public void testRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_rctstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(1, 0, 1));

        updateView("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);

        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(2, 0, 2));
        assertRows(execute("SELECT c, k, val FROM mv_rctstest limit 1"), row(2, 0, 2));
    }

    @Test
    public void testCountersTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "count counter)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_counter", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)");
            Assert.fail("MV on counter should fail");
        }
        catch (InvalidQueryException e)
        {
        }
    }

    @Test
    public void testDurationsTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "result duration)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_duration", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)");
            Assert.fail("MV on duration should fail");
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("duration type is not supported for PRIMARY KEY column 'result'", e.getMessage());
        }
    }

    @Test
    public void complexTimestampUpdateTestWithFlush() throws Throwable
    {
        complexTimestampUpdateTest(true);
    }

    @Test
    public void complexTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexTimestampUpdateTest(false);
    }

    public void complexTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        //Set initial values TS=0, leaving e null and verify view
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        //update c's timestamp TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0));

        if(flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }


        //change c's value back to 1 with TS=4, check we can see d
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }

        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));


        //Add e value @ TS=1
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=3
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));


        //Tombstone c
        executeNet(protocolVersion, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRows(execute("SELECT d from mv"));

        //Add back without D
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));


        //New partition
        // insert a row with timestamp 0
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet(protocolVersion, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, 0, 0)
        );


    }

    @Test
    public void testBuilderWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "PRIMARY KEY (k, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());


        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval) VALUES (?, ?, ?)", 0, i, 0);

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");


        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv"))
            Thread.sleep(1000);

        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));
        assertRows(execute("SELECT count(*) from mv WHERE intval = ?", 0), row(1024L));
    }

}
