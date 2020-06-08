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

public class StrictLivenessTombstoneTest extends CQLTester
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
    public void testStrictLivenessTombstone() throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p)"
                           + " with gc_grace_seconds=5;");
        ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
        cfs.disableAutoCompaction();

        updateView("Insert into %s (p, v1, v2) values (1, 1, 1) ;");
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        updateView("Update %s set v1 = null WHERE p = 1");
        FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"));

        cfs.forceMajorCompaction(); // before gc grace second, strict-liveness tombstoned dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());

        Thread.sleep(6000);
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());

        updateView("Update %s using ttl 5 set v1 = 1 WHERE p = 1");
        FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        cfs.forceMajorCompaction(); // before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        Thread.sleep(5500); // after expired, before gc_grace_second
        cfs.forceMajorCompaction();// before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"));

        Thread.sleep(5500); // after expired + gc_grace_second
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());
    }

}
