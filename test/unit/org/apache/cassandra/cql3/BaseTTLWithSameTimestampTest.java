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

public class BaseTTLWithSameTimestampTest extends CQLTester
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
    public void testBaseTTLWithSameTimestampTest() throws Throwable
    {
        // CASSANDRA-13127 when liveness timestamp tie, greater localDeletionTime should win if both are expiring.
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) using timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        // reversed order
        execute("truncate %s;");

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

    }
}
