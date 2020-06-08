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

public class UpdateColumnInViewPKWithTTLTest extends CQLTester
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
    public void testUpdateColumnInViewPKWithTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(true);
    }

    @Test
    public void testUpdateColumnInViewPKWithTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(false);
    }

    private void testUpdateColumnInViewPKWithTTL(boolean flush) throws Throwable
    {
        // CASSANDRA-13657 if base column used in view pk is ttled, then view row is considered dead
        createTable("create table %s (k int primary key, a int, b int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s SET a = 1 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null));

        updateView("DELETE a FROM %s WHERE k = 1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        updateView("INSERT INTO %s (k) VALUES (1);");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from mv"));

        updateView("UPDATE %s USING TTL 5 SET a = 10 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 10, null));
        assertRows(execute("SELECT * from mv"), row(10, 1, null));

        updateView("UPDATE %s SET b = 100 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 10, 100));
        assertRows(execute("SELECT * from mv"), row(10, 1, 100));

        Thread.sleep(5000);

        // 'a' is TTL of 5 and removed.
        assertRows(execute("SELECT * from %s"), row(1, null, 100));
        assertEmpty(execute("SELECT * from mv"));
        assertEmpty(execute("SELECT * from mv WHERE k = ? AND a = ?", 1, 10));

        updateView("DELETE b FROM %s WHERE k=1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from mv"));

        updateView("DELETE FROM %s WHERE k=1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));
    }
}
