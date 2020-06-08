package org.apache.cassandra.cql3;

import com.google.common.base.Objects;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ViewComplexTest2 extends CQLTester
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
        updateViewWithFlush(query, false, params);
    }

    private void updateViewWithFlush(String query, boolean flush, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) Stage.VIEW_MUTATION.executor()).getPendingTaskCount() == 0
                && ((SEPExecutor) Stage.VIEW_MUTATION.executor()).getActiveTaskCount() == 0))
        {
            Thread.sleep(1);
        }
        if (flush)
            Keyspace.open(keyspace()).flush();
    }

    @Test
    public void testUpdateWithColumnTimestampSmallerThanPkWithFlush() throws Throwable
    {
        testUpdateWithColumnTimestampSmallerThanPk(true);
    }

    @Test
    public void testUpdateWithColumnTimestampSmallerThanPkWithoutFlush() throws Throwable
    {
        testUpdateWithColumnTimestampSmallerThanPk(false);
    }

    public void testUpdateWithColumnTimestampSmallerThanPk(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // reset value
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 6;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 3, 6L));
        // increase pk's timestamp to 20
        updateView("Insert into %s (p) values (3) using timestamp 20;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 3, 6L));
        // change v1's to 2 and remove existing view row with ts7
        updateView("UPdate %s using timestamp 7 set v1 = 2 where p = 3;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, 3, 6L));
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv limit 1"), row(2, 3, 3, 6L));
        // change v1's to 1 and remove existing view row with ts8
        updateView("UPdate %s using timestamp 8 set v1 = 1 where p = 3;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 3, 6L));
    }

    @Test
    public void testExpiredLivenessLimitWithFlush() throws Throwable
    {
        // CASSANDRA-13883
        testExpiredLivenessLimit(true);
    }

    @Test
    public void testExpiredLivenessLimitWithoutFlush() throws Throwable
    {
        // CASSANDRA-13883
        testExpiredLivenessLimit(false);
    }

    private void testExpiredLivenessLimit(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int);");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a);");
        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k);");
        ks.getColumnFamilyStore("mv1").disableAutoCompaction();
        ks.getColumnFamilyStore("mv2").disableAutoCompaction();

        for (int i = 1; i <= 100; i++)
            updateView("INSERT INTO %s(k, a, b) VALUES (?, ?, ?);", i, i, i);
        for (int i = 1; i <= 100; i++)
        {
            if (i % 50 == 0)
                continue;
            // create expired liveness
            updateView("DELETE a FROM %s WHERE k = ?;", i);
        }
        if (flush)
        {
            ks.getColumnFamilyStore("mv1").forceBlockingFlush();
            ks.getColumnFamilyStore("mv2").forceBlockingFlush();
        }

        for (String view : Arrays.asList("mv1", "mv2"))
        {
            // paging
            assertEquals(1, executeNetWithPaging(protocolVersion, String.format("SELECT k,a,b FROM %s limit 1", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(protocolVersion, String.format("SELECT k,a,b FROM %s limit 2", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(protocolVersion, String.format("SELECT k,a,b FROM %s", view), 1).all().size());
            assertRowsNet(protocolVersion, executeNetWithPaging(protocolVersion, String.format("SELECT k,a,b FROM %s ", view), 1),
                          row(50, 50, 50),
                          row(100, 100, 100));
            // limit
            assertEquals(1, execute(String.format("SELECT k,a,b FROM %s limit 1", view)).size());
            assertRowsIgnoringOrder(execute(String.format("SELECT k,a,b FROM %s limit 2", view)),
                                    row(50, 50, 50),
                                    row(100, 100, 100));
        }
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(true);
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithoutFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(false);
    }

    public void testUpdateWithColumnTimestampBiggerThanPk(boolean flush) throws Throwable
    {
        // CASSANDRA-11500 able to shadow old view row with column ts greater tahn pk's ts and re-insert the view row
        String baseTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int);");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();
        updateView("DELETE FROM %s USING TIMESTAMP 0 WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // sstable-1, Set initial values TS=1
        updateView("INSERT INTO %s(k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 1));
        updateView("UPDATE %s USING TIMESTAMP 10 SET b = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        updateView("UPDATE %s USING TIMESTAMP 2 SET a = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 2, 2));
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 2, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv limit 1"), row(1, 2, 2));
        updateView("UPDATE %s USING TIMESTAMP 11 SET a = 1 WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, 1, 2));

        // set non-key base column as tombstone, view row is removed with shadowable
        updateView("UPDATE %s USING TIMESTAMP 12 SET a = null WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, null, 2));

        // column b should be alive
        updateView("UPDATE %s USING TIMESTAMP 13 SET a = 1 WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, 1, 2));

        assertInvalidMessage(String.format("Cannot drop column a on base table %s with materialized views", baseTable), "ALTER TABLE %s DROP a");
    }

    @Test
    public void testNonBaseColumnInViewPkWithFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(true);
    }

    @Test
    public void testNonBaseColumnInViewPkWithoutFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(true);
    }

    public void testNonBaseColumnInViewPk(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key (p1,p2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p1 is not null and p2 is not null primary key (p2, p1)"
                           + " with gc_grace_seconds=5;");
        ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
        cfs.disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 1 set v1 =1 where p1 = 1 AND p2 = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, 1, null));

        updateView("UPDATE %s USING TIMESTAMP 2 set v1 = null, v2 = 1 where p1 = 1 AND p2 = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, 1));

        updateView("UPDATE %s USING TIMESTAMP 2 set v2 = null where p1 = 1 AND p2 = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"));

        updateView("INSERT INTO %s (p1,p2) VALUES(1,1) USING TIMESTAMP 3;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, null));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, null));

        updateView("DELETE FROM %s USING TIMESTAMP 4 WHERE p1 =1 AND p2 = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"));

        updateView("UPDATE %s USING TIMESTAMP 5 set v2 = 1 where p1 = 1 AND p2 = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, 1));
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(true);
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithoutFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(false);
    }

    private void testCellTombstoneAndShadowableTombstones(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable 1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable 2
        updateView("UPdate %s using timestamp 2 set v2 = null where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3),
                                row(null, null));
        // sstable 3
        updateView("UPdate %s using timestamp 3 set v1 = 2 where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, null, null));
        // sstable 4
        updateView("UPdate %s using timestamp 4 set v1 = 1 where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 3;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
            List<String> sstables = cfs.getLiveSSTables()
                                       .stream()
                                       .sorted(Comparator.comparingInt(s -> s.descriptor.generation))
                                       .map(s -> s.getFilename())
                                       .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(2)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
        }
        // cell-tombstone in sstable 4 is not compacted away, because the shadowable tombstone is shadowed by new row.
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv limit 1"), row(1, 3, null, null));
    }

    @Test
    public void complexTimestampDeletionTestWithFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(true);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(true);
    }

    @Test
    public void complexTimestampDeletionTestWithoutFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(false);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(false);
    }

    private void complexTimestampWithbasePKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key(p1, p2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv2",
                   "create materialized view %s as select * from %%s where p1 is not null and p2 is not null primary key (p2, p1);");
        ks.getColumnFamilyStore("mv2").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p1 = 1 and p2 = 2;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv2"));
        // insert PK with TS=3
        updateView("Insert into %s (p1, p2) values (1, 2) using timestamp 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));

        ks.getColumnFamilyStore("mv2").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));

        // reset values
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 10;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 10L));

        updateView("UPDATE %s using timestamp 20 SET v2 = 5 WHERE p1 = 1 and p2 = 2");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 5, 20L));

        updateView("DELETE FROM %s using timestamp 10 WHERE p1 = 1 and p2 = 2");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(null, 5, 20L));
    }

    public void complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(5, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p = 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv"));
        // insert PK with TS=3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 2;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));
        assertRowsIgnoringOrder(execute("SELECT * from mv limit 1"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET v2 = ? WHERE p = ?", 4, 3);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));

        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));
        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv limit 1"), row(1, 3, 4, 3L));
    }

    @Test
    public void testMVWithDifferentColumnsWithFlush() throws Throwable
    {
        testMVWithDifferentColumns(true);
    }

    @Test
    public void testMVWithDifferentColumnsWithoutFlush() throws Throwable
    {
        testMVWithDifferentColumns(false);
    }

    private void testMVWithDifferentColumns(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY(a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        List<String> viewNames = new ArrayList<>();
        List<String> mvStatements = Arrays.asList(
                                                  // all selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // unselected e,f
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // no selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // all selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // unselected e,f, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // no selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)");

        Keyspace ks = Keyspace.open(keyspace());

        for (int i = 0; i < mvStatements.size(); i++)
        {
            String name = "mv" + i;
            viewNames.add(name);
            createView(name, mvStatements.get(i));
            ks.getColumnFamilyStore(name).disableAutoCompaction();
        }

        // insert
        updateViewWithFlush("INSERT INTO %s (a,b,c,d,e,f) VALUES(1,1,1,1,1,1) using timestamp 1", flush);
        assertBaseViews(row(1, 1, 1, 1, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET c=0, d=0 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 0, 0, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET e=0, f=0 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 0, 0, 0, 0), viewNames);

        updateViewWithFlush("DELETE FROM %s using timestamp 2 WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        // partial update unselected, selected
        updateViewWithFlush("UPDATE %s using timestamp 3 SET f=1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = 1, f=null WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, 1, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET d = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, 1, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 7 SET d = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = 1 WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, 1), viewNames);

        // view row still alive due to c=1@6
        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = null WHERE a=1 AND b=1", flush);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = null WHERE a=1 AND b=1", flush);
        assertBaseViews(null, viewNames);
    }

    private void assertBaseViews(Object[] row, List<String> viewNames) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s");
        if (row == null)
            assertRowsIgnoringOrder(result);
        else
            assertRowsIgnoringOrder(result, row);
        for (int i = 0; i < viewNames.size(); i++)
            assertBaseView(result, execute(String.format("SELECT * FROM %s", viewNames.get(i))), viewNames.get(i));
    }

    private void assertBaseView(UntypedResultSet base, UntypedResultSet view, String mv)
    {
        List<ColumnSpecification> baseMeta = base.metadata();
        List<ColumnSpecification> viewMeta = view.metadata();

        Iterator<UntypedResultSet.Row> iter = base.iterator();
        Iterator<UntypedResultSet.Row> viewIter = view.iterator();

        List<UntypedResultSet.Row> baseData = com.google.common.collect.Lists.newArrayList(iter);
        List<UntypedResultSet.Row> viewData = com.google.common.collect.Lists.newArrayList(viewIter);

        if (baseData.size() != viewData.size())
            fail(String.format("Mismatch number of rows in view %s: <%s>, in base <%s>",
                               mv,
                               makeRowStrings(view),
                               makeRowStrings(base)));
        if (baseData.size() == 0)
            return;
        if (viewData.size() != 1)
            fail(String.format("Expect only one row in view %s, but got <%s>",
                               mv,
                               makeRowStrings(view)));

        UntypedResultSet.Row row = baseData.get(0);
        UntypedResultSet.Row viewRow = viewData.get(0);

        Map<String, ByteBuffer> baseValues = new HashMap<>();
        for (int j = 0; j < baseMeta.size(); j++)
        {
            ColumnSpecification column = baseMeta.get(j);
            ByteBuffer actualValue = row.getBytes(column.name.toString());
            baseValues.put(column.name.toString(), actualValue);
        }
        for (int j = 0; j < viewMeta.size(); j++)
        {
            ColumnSpecification column = viewMeta.get(j);
            String name = column.name.toString();
            ByteBuffer viewValue = viewRow.getBytes(name);
            if (!baseValues.containsKey(name))
            {
                fail(String.format("Extra column: %s with value %s in view", name, column.type.compose(viewValue)));
            }
            else if (!Objects.equal(baseValues.get(name), viewValue))
            {
                fail(String.format("Non equal column: %s, expected <%s> but got <%s>",
                                   name,
                                   column.type.compose(baseValues.get(name)),
                                   column.type.compose(viewValue)));
            }
        }
    }
}
