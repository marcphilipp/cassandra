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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnselectedColumnsTTLTest extends CQLTester
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
    public void testUnselectedColumnsTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUnselectedColumnsTTL(true);
    }

    @Test
    public void testUnselectedColumnsTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUnselectedColumnsTTL(false);
    }

    private void testUnselectedColumnsTTL(boolean flush) throws Throwable
    {
        // CASSANDRA-13127 not ttled unselected column in base should keep view row alive
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM %%s WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (0, 0) USING TTL 3;", flush);

        updateViewWithFlush("UPDATE %s USING TTL 1000 SET v = 0 WHERE p = 0 and c = 0;", flush);

        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        Thread.sleep(3000);

        UntypedResultSet.Row row = execute("SELECT v, ttl(v) from %s WHERE c = ? AND p = ?", 0, 0).one();
        assertTrue("row should have value of 0", row.getInt("v") == 0);
        assertTrue("row should have ttl less than 1000", row.getInt("ttl(v)") < 1000);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateViewWithFlush("DELETE FROM %s WHERE p = 0 and c = 0;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (0, 0) ", flush);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        // already have a live row, no need to apply the unselected cell ttl
        updateViewWithFlush("UPDATE %s USING TTL 3 SET v = 0 WHERE p = 0 and c = 0;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (1, 1) USING TTL 3", flush);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 1, 1), row(1, 1));

        Thread.sleep(4000);

        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 1, 1));

        // unselected should keep view row alive
        updateViewWithFlush("UPDATE %s SET v = 0 WHERE p = 1 and c = 1;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 1, 1), row(1, 1));

    }
}
