package org.apache.cassandra.db.index;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.SSTableIndex;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.memory.IndexMemtable;
import org.apache.cassandra.db.index.search.plan.QueryPlan;
import org.apache.cassandra.db.index.search.tokenization.AbstractTokenizer;
import org.apache.cassandra.db.index.search.tokenization.NoOpTokenizer;
import org.apache.cassandra.db.index.search.tokenization.StandardTokenizer;
import org.apache.cassandra.db.index.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.SSTableWriterListener.Source;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.*;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.index.search.OnDiskSABuilder.Mode;

/**
 * Note: currently does not work with cql3 tables (unless 'WITH COMPACT STORAGE' is declared when creating the table).
 *
 * ALos, makes the assumption this will be the only index running on the table as part of the query.
 * SIM tends to shoves all indexed columns into one PerRowSecondaryIndex
 */
public class SuffixArraySecondaryIndex extends PerRowSecondaryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(SuffixArraySecondaryIndex.class);

    private static final String INDEX_MODE_OPTION = "mode";
    private static final String INDEX_ANALYZED_OPTION = "analyzed";

    private static final Set<AbstractType<?>> TOKENIZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
        add(UTF8Type.instance);
        add(AsciiType.instance);
    }};

    private static final String FILE_NAME_FORMAT = "SI_%s.db";
    private static final ThreadPoolExecutor INDEX_FLUSHER_MEMTABLE;
    private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL;

    static
    {
        INDEX_FLUSHER_GENERAL = new JMXEnabledThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                 new NamedThreadFactory("SuffixArrayBuilder-General"),
                                                                 "internal");
        INDEX_FLUSHER_GENERAL.allowCoreThreadTimeOut(true);

        INDEX_FLUSHER_MEMTABLE = new JMXEnabledThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                 new NamedThreadFactory("SuffixArrayBuilder-Memtable"),
                                                                 "internal");
        INDEX_FLUSHER_MEMTABLE.allowCoreThreadTimeOut(true);
    }

    private final BiMap<ByteBuffer, Component> columnDefComponents;
    private final ConcurrentMap<ByteBuffer, Pair<ColumnDefinition, IndexMode>> indexedColumns;

    private final ConcurrentMap<ByteBuffer, SADataTracker> intervalTrees;

    private final ConcurrentMap<Descriptor, CopyOnWriteArrayList<SSTableIndex>> currentIndexes;

    private final AtomicReference<IndexMemtable> globalMemtable = new AtomicReference<>(new IndexMemtable(this));

    private AbstractType<?> keyComparator;
    private boolean isInitialized;

    public SuffixArraySecondaryIndex()
    {
        columnDefComponents = HashBiMap.create();
        intervalTrees = new ConcurrentHashMap<>();
        currentIndexes = new ConcurrentHashMap<>();
        indexedColumns = new NonBlockingHashMap<>();
    }

    public void init()
    {
        if (!(StorageService.getPartitioner() instanceof Murmur3Partitioner))
            throw new UnsupportedOperationException("SASI supported only with Murmur3Partitioner.");

        isInitialized = true;

        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        addComponent(columnDefs);

        baseCfs.getDataTracker().subscribe(new DataTrackerConsumer());
    }

    @VisibleForTesting
    public void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);

        Map<String, String> indexOptions = columnDef.getIndexOptions();

        Mode mode = indexOptions.get(INDEX_MODE_OPTION) == null ? Mode.ORIGINAL : Mode.mode(indexOptions.get(INDEX_MODE_OPTION));
        boolean isAnalyzed = indexOptions.get(INDEX_ANALYZED_OPTION) == null ? false : Boolean.valueOf(indexOptions.get(INDEX_ANALYZED_OPTION));

        indexedColumns.put(columnDef.name, Pair.create(columnDef, new IndexMode(mode, isAnalyzed)));
        addComponent(Collections.singleton(columnDef));
    }

    private void addComponent(Set<ColumnDefinition> defs)
    {
        // if SI hasn't been initialized that means that this instance
        // was created for validation purposes only, so we don't have do anything here
        if (!isInitialized)
            return;

        // only reason baseCfs would be null is if coming through the CFMetaData.validate() path, which only
        // checks that the SI 'looks' legit, but then throws away any created instances - fml, this 2I api sux
        if (baseCfs == null)
            return;
        else if (keyComparator == null)
            keyComparator = this.baseCfs.metadata.getKeyValidator();

        INDEX_FLUSHER_GENERAL.setCorePoolSize(columnDefs.size());
        INDEX_FLUSHER_GENERAL.setMaximumPoolSize(columnDefs.size() * 2);
        INDEX_FLUSHER_MEMTABLE.setMaximumPoolSize(columnDefs.size());

        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, def.getIndexName());
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));

            // on restart, sstables are loaded into DataTracker before 2I are hooked up (and init() invoked),
            // so we need to grab the sstables here
            if (!intervalTrees.containsKey(def.name))
                intervalTrees.put(def.name, new SADataTracker(def.name, baseCfs.getDataTracker().getSSTables()));
        }
    }

    public Pair<ColumnDefinition, IndexMode> getIndexDefinition(ByteBuffer columnName)
    {
        return indexedColumns.get(columnName);
    }

    private void addToIntervalTree(Collection<SSTableReader> readers, Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition columnDefinition : defs)
        {
            SADataTracker dataTracker = intervalTrees.get(columnDefinition.name);
            if (dataTracker == null)
            {
                SADataTracker newDataTracker = new SADataTracker(columnDefinition.name, Collections.<SSTableReader>emptySet());
                dataTracker = intervalTrees.putIfAbsent(columnDefinition.name, newDataTracker);
                if (dataTracker == null)
                    dataTracker = newDataTracker;
            }
            dataTracker.update(Collections.<SSTableReader>emptyList(), readers);
        }
    }

    private void removeFromIntervalTree(Collection<SSTableReader> readers)
    {
        for (Map.Entry<ByteBuffer, SADataTracker> entry : intervalTrees.entrySet())
        {
            entry.getValue().update(readers, Collections.<SSTableReader>emptyList());
        }
    }

    private ColumnDefinition getColumnDef(Component component)
    {
        //ugly hack to get validator via the columnDefs
        ByteBuffer colName = columnDefComponents.inverse().get(component);
        if (colName != null)
            return getColumnDefinition(colName);
        return null;
    }

    public boolean isIndexBuilt(ByteBuffer columnName)
    {
        return true;
    }

    public void validateOptions() throws ConfigurationException
    {
        // nop
    }

    public String getIndexName()
    {
        return "RowLevel_SuffixArrayIndex_" + baseCfs.getColumnFamilyName();
    }

    public ColumnFamilyStore getIndexCfs()
    {
        return null;
    }

    public long getLiveSize()
    {
        return globalMemtable.get().estimateSize();
    }

    public void reload()
    {
        invalidateMemtable();
    }

    public void index(ByteBuffer key, ColumnFamily cf)
    {
        globalMemtable.get().index(key, cf);
    }

    /**
     * parent class to eliminate the index rebuild
     *
     * @return a future that does and blocks on nothing
     */
    public Future<?> buildIndexAsync()
    {
        return Futures.immediateCheckedFuture(null);
    }

    @Override
    public void buildIndexes(Collection<SSTableReader> sstables, Set<String> indexNames)
    {
        SortedSet<ByteBuffer> indexes = new TreeSet<>(baseCfs.getComparator());
        for (ColumnDefinition columnDef : columnDefs)
        {
            Iterator<String> iterator = indexNames.iterator();

            while (iterator.hasNext())
            {
                String indexName = iterator.next();
                if (columnDef.getIndexName().equals(indexName))
                {
                    dropIndexData(columnDef.name, System.currentTimeMillis());
                    indexes.add(columnDef.name);
                    iterator.remove();
                    break;
                }
            }
        }

        if (indexes.isEmpty())
            return;

        for (SSTableReader sstable : sstables)
        {
            try
            {
                FBUtilities.waitOnFuture(CompactionManager.instance.submitIndexBuild(new IndexBuilder(sstable, indexes)));
            }
            catch (Exception e)
            {
                logger.error("Failed index build task for " + sstable, e);
            }
        }
    }

    public void candidatesForIndexing(Collection<SSTableReader> initialSstables)
    {
        for (SSTableReader sstable : initialSstables)
        {
            SortedSet<ByteBuffer> missingIndexes = new TreeSet<>();
            for (ColumnDefinition def : getColumnDefs())
            {
                File indexFile = new File(sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, def.getIndexName())));
                if (!indexFile.exists())
                    missingIndexes.add(def.name);
            }

            if (!missingIndexes.isEmpty())
            {
                logger.info("submitting rebuild of missing indexes, count = {}, for sttable {}", missingIndexes.size(), sstable);
                CompactionManager.instance.submitIndexBuild(new IndexBuilder(sstable, missingIndexes));
            }
        }
    }

    public void delete(DecoratedKey key)
    {
        // called during 'nodetool cleanup' - can punt on impl'ing this
    }

    public void removeIndex(ByteBuffer columnName)
    {
        removeIndex(columnName, FBUtilities.timestampMicros());
    }

    public void removeIndex(ByteBuffer columnName, long truncateUntil)
    {
        columnDefComponents.remove(columnName);
        dropIndexData(columnName, truncateUntil);
    }

    private void dropIndexData(ByteBuffer columnName, long truncateUntil)
    {
        SADataTracker dataTracker = intervalTrees.get(columnName);
        if (dataTracker != null)
            dataTracker.dropData(truncateUntil);
    }

    public void invalidate()
    {
        invalidate(true, FBUtilities.timestampMicros());
    }

    public void invalidate(boolean invalidateMemtable, long truncateUntil)
    {
        if (invalidateMemtable)
            invalidateMemtable();

        for (ByteBuffer colName : new ArrayList<>(columnDefComponents.keySet()))
            dropIndexData(colName, truncateUntil);
    }

    private void invalidateMemtable()
    {
        globalMemtable.getAndSet(new IndexMemtable(this));
    }

    public void truncateBlocking(long truncatedAt)
    {
        invalidate(false, truncatedAt);
    }

    public void forceBlockingFlush()
    {
        //nop, as this 2I will flush with the owning CF's sstable, so we don't need this extra work
    }

    public Collection<Component> getIndexComponents()
    {
        return ImmutableList.<Component>builder().addAll(columnDefComponents.values()).build();
    }

    public SSTableWriterListener getWriterListener(Descriptor descriptor, Source source)
    {
        return new PerSSTableIndexWriter(descriptor, source);
    }

    public IndexMemtable getMemtable()
    {
        return globalMemtable.get();
    }

    public SAView getView(ByteBuffer columnName)
    {
        SADataTracker dataTracker = intervalTrees.get(columnName);
        return dataTracker != null ? dataTracker.view.get() : null;
    }

    protected class PerSSTableIndexWriter implements SSTableWriterListener
    {
        private final Descriptor descriptor;
        private final Source source;

        // need one entry for each term we index
        private final Map<ByteBuffer, ColumnIndex> indexPerColumn;

        private DecoratedKey currentKey;
        private long currentKeyPosition;
        private boolean isComplete;

        public PerSSTableIndexWriter(Descriptor descriptor, Source source)
        {
            this.descriptor = descriptor;
            this.source = source;
            this.indexPerColumn = new HashMap<>();
        }

        public void begin()
        {
            // nop
        }

        public void startRow(DecoratedKey key, long curPosition)
        {
            currentKey = key;
            currentKeyPosition = curPosition;
        }

        public void nextColumn(Column column)
        {
            Component component = columnDefComponents.get(column.name());
            if (component == null)
                return;

            ColumnIndex index = indexPerColumn.get(column.name());
            if (index == null)
            {
                String outputFile = descriptor.filenameFor(component);
                indexPerColumn.put(column.name(), (index = new ColumnIndex(getColumnDef(component), outputFile)));
            }

            index.add(column.value().duplicate(), currentKey, currentKeyPosition);
        }

        public void complete()
        {
            if (isComplete)
                return;
            currentKey = null;

            try
            {
                logger.info("about to submit for concurrent SA'ing");
                final CountDownLatch latch = new CountDownLatch(indexPerColumn.size());

                // first, build up a listing per-component (per-index)
                for (final ColumnIndex index : indexPerColumn.values())
                {
                    logger.info("Submitting {} for concurrent SA'ing", index.outputFile);
                    ThreadPoolExecutor executor = source == Source.MEMTABLE ? INDEX_FLUSHER_MEMTABLE : INDEX_FLUSHER_GENERAL;
                    executor.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                long flushStart = System.nanoTime();
                                index.blockingFlush();
                                logger.info("Flushing SA index to {} took {} ms.",
                                        index.outputFile,
                                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - flushStart));
                            }
                            finally
                            {
                                latch.countDown();
                            }
                        }
                    });
                }

                Uninterruptibles.awaitUninterruptibly(latch, 10, TimeUnit.MINUTES);
            }
            finally
            {
                // drop this data ASAP
                indexPerColumn.clear();
                isComplete = true;
            }
        }

        public int hashCode()
        {
            return descriptor.hashCode();
        }

        public boolean equals(Object o)
        {
            return !(o == null || !(o instanceof PerSSTableIndexWriter)) && descriptor.equals(((PerSSTableIndexWriter) o).descriptor);
        }

        private class ColumnIndex
        {
            private final ColumnDefinition column;
            private final String outputFile;
            private final AbstractTokenizer tokenizer;
            private final Map<ByteBuffer, TokenTreeBuilder> keysPerTerm;

            // key range of the per-column index
            private DecoratedKey min, max;

            public ColumnIndex(ColumnDefinition column, String outputFile)
            {
                this.column = column;
                this.outputFile = outputFile;
                this.tokenizer = getTokenizer(indexedColumns.get(column.name));
                this.tokenizer.init(column.getIndexOptions());
                this.keysPerTerm = new HashMap<>();
            }

            private void add(ByteBuffer term, DecoratedKey key, long keyPosition)
            {
                final Long keyToken = ((LongToken) key.getToken()).token;

                if (term.remaining() == 0)
                    return;

                if (term.remaining() >= Short.MAX_VALUE)
                {
                    logger.error("Rejecting value (size {}, maximum {} bytes) for column {} (analyzed {}) at {} SSTable.",
                                 term.remaining(),
                                 Short.MAX_VALUE,
                                 baseCfs.getComparator().getString(column.name),
                                 indexedColumns.get(column.name).right.isAnalyzed,
                                 descriptor);
                    return;
                }

                boolean isAdded = false;

                tokenizer.reset(term);
                while (tokenizer.hasNext())
                {
                    ByteBuffer token = tokenizer.next();

                    if (!TypeUtil.isValid(token, column.getValidator()))
                    {
                        int size = token.remaining();
                        if ((token = TypeUtil.tryUpcast(token, column.getValidator())) == null)
                        {
                            logger.error("({}) Failed to add {} to index for key: {}, value size was {} bytes, validator is {}.",
                                         outputFile,
                                         baseCfs.getComparator().getString(column.name),
                                         keyComparator.getString(key.key),
                                         size,
                                         column.getValidator());
                            continue;
                        }
                    }

                    TokenTreeBuilder keys = keysPerTerm.get(token);
                    if (keys == null)
                        keysPerTerm.put(token, (keys = new TokenTreeBuilder(org.apache.cassandra.db.index.search.Descriptor.CURRENT)));

                    keys.add(Pair.create(keyToken, keyPosition));
                    isAdded = true;
                }

                if (!isAdded)
                    return; // non of the generated tokens were added to the index

                /* calculate key range (based on actual key values) for current index */

                min = (min == null || keyComparator.compare(min.key, currentKey.key) > 0) ? currentKey : min;
                max = (max == null || keyComparator.compare(max.key, currentKey.key) < 0) ? currentKey : max;
            }

            public void blockingFlush()
            {
                Mode mode = getMode(column.name);
                if (mode == null || keysPerTerm.size() == 0)
                    return;

                OnDiskSABuilder builder = new OnDiskSABuilder(column.getValidator(), mode);

                for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : keysPerTerm.entrySet())
                    builder.add(e.getKey(), e.getValue());

                // since everything added to the builder, it's time to drop references to the data
                keysPerTerm.clear();

                builder.finish(Pair.create(min, max), new File(outputFile));
            }
        }
    }

    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new LocalSecondaryIndexSearcher(baseCfs.indexManager, columns);
    }

    protected class LocalSecondaryIndexSearcher extends SecondaryIndexSearcher
    {
        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
        }

        public List<Row> search(ExtendedFilter filter)
        {
            try
            {
                return filter != null && !filter.getClause().isEmpty()
                        ? new QueryPlan(SuffixArraySecondaryIndex.this, filter).execute()
                        : Collections.<Row>emptyList();
            }
            catch(Exception e)
            {
                logger.info("error occurred while searching suffix array indexes; ignoring", e);
                return Collections.emptyList();
            }
        }

        public boolean isIndexing(List<IndexExpression> clause)
        {
            // this is a bit weak, currently just checks for the success of one column, not all
            // however, parent SIS.isIndexing only cares if one predicate is covered ... grrrr!!
            for (IndexExpression expression : clause)
            {
                if (columnDefComponents.keySet().contains(expression.column_name))
                    return true;
            }
            return false;
        }
    }

    /** a pared-down version of DataTracker and DT.View. need one for each index of each column family */
    private class SADataTracker
    {
        // by using using DT.View, we do get some baggage fields (memtable, compacting, and so on)
        // but always pass in empty list for those fields, we should be ok
        private final AtomicReference<SAView> view = new AtomicReference<>();

        public SADataTracker(ByteBuffer name, Set<SSTableReader> ssTables)
        {
            view.set(new SAView(name, ssTables));
        }

        public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
        {
            SAView currentView, newView;
            do
            {
                currentView = view.get();

                if (currentView.keyIntervalTree.intervalCount() == 0 && oldSSTables.size() > 0)
                    return;

                newView = currentView.update(oldSSTables, newSSTables);
                if (newView == null)
                    break;
            }
            while (!view.compareAndSet(currentView, newView));

            for (SSTableReader sstable : oldSSTables)
            {
                CopyOnWriteArrayList<SSTableIndex> indexes = currentIndexes.remove(sstable.descriptor);
                if (indexes == null)
                    continue;

                for (SSTableIndex index : indexes)
                {
                    // reference count has already been decremented by marking as obsolete
                    if (!index.isObsolete())
                        index.release();
                }
            }
        }

        public void dropData(long truncateUntil)
        {
            SAView currentView = view.get();
            if (currentView == null)
                return;

            Set<SSTableReader> toRemove = new HashSet<>();
            for (SSTableIndex index : currentView)
            {
                if (index.getSSTable().getMaxTimestamp() > truncateUntil)
                    continue;

                index.markObsolete();
                toRemove.add(index.getSSTable());
            }

            update(toRemove, Collections.<SSTableReader>emptyList());
        }
    }

    public class SAView implements Iterable<SSTableIndex>
    {
        private final ByteBuffer col;
        private final AbstractType<?> validator;
        private ByteBuffer minSuffix, maxSuffix;

        private final IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> termIntervalTree;
        private final IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> keyIntervalTree;

        public SAView(ByteBuffer col, Set<SSTableReader> sstables)
        {
            this(null, col, Collections.<SSTableReader>emptyList(), sstables);
        }

        private SAView(SAView previous, ByteBuffer col, final Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            this.col = col;
            this.validator = previous == null
                                ? baseCfs.metadata.getColumnDefinitionFromColumnName(col).getValidator()
                                : previous.validator;

            String name = baseCfs.getComparator().getString(col);

            Predicate<Interval<ByteBuffer, SSTableIndex>> predicate = new Predicate<Interval<ByteBuffer, SSTableIndex>>()
            {
                public boolean apply(Interval<ByteBuffer, SSTableIndex> interval)
                {
                    return !toRemove.contains(interval.data.getSSTable());
                }
            };

            List<Interval<ByteBuffer, SSTableIndex>> termIntervals = new ArrayList<>();
            List<Interval<ByteBuffer, SSTableIndex>> keyIntervals = new ArrayList<>();

            // reuse entries from the previously constructed view to avoid reloading from disk or keep (yet another) cache of the sstreader -> intervals
            if (previous != null)
            {
                for (Interval<ByteBuffer, SSTableIndex> interval : Iterables.filter(previous.keyIntervalTree, predicate))
                    keyIntervals.add(interval);

                for (Interval<ByteBuffer, SSTableIndex> interval : Iterables.filter(previous.termIntervalTree, predicate))
                {
                    termIntervals.add(interval);
                    updateRange(interval.data);
                }
            }

            for (SSTableReader sstable : toAdd)
            {
                String columnName = getColumnDefinition(col).getIndexName();
                File indexFile = new File(sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName)));
                if (!indexFile.exists())
                    continue;

                SSTableIndex index = null;

                try
                {
                    index = new SSTableIndex(col, indexFile, sstable);
                    logger.info("Interval.create(column: {}, minSuffix: {}, maxSuffix: {}, minKey: {}, maxKey: {}, sstable: {})",
                                name,
                                validator.getString(index.minSuffix()),
                                validator.getString(index.maxSuffix()),
                                keyComparator.getString(index.minKey()),
                                keyComparator.getString(index.maxKey()),
                                sstable);
                }
                catch (Exception e)
                {
                    logger.error("Can't open index file at " + indexFile.getAbsolutePath() + ", skipping.", e);

                    if (index != null)
                        index.release();

                    continue;
                }

                CopyOnWriteArrayList<SSTableIndex> openIndexes = currentIndexes.get(sstable.descriptor);
                if (openIndexes == null)
                {
                    CopyOnWriteArrayList<SSTableIndex> newList = new CopyOnWriteArrayList<>();
                    openIndexes = currentIndexes.putIfAbsent(sstable.descriptor, newList);
                    if (openIndexes == null)
                        openIndexes = newList;
                }

                openIndexes.add(index);

                termIntervals.add(Interval.create(index.minSuffix(), index.maxSuffix(), index));
                keyIntervals.add(Interval.create(index.minKey(), index.maxKey(), index));

                updateRange(index);
            }

            termIntervalTree = IntervalTree.build(termIntervals, new Comparator<ByteBuffer>()
            {
                @Override
                public int compare(ByteBuffer a, ByteBuffer b)
                {
                    return validator.compare(a, b);
                }
            });

            keyIntervalTree = IntervalTree.build(keyIntervals, new Comparator<ByteBuffer>()
            {
                @Override
                public int compare(ByteBuffer a, ByteBuffer b)
                {
                    return keyComparator.compare(a, b);
                }
            });
        }

        public void updateRange(SSTableIndex index)
        {
            minSuffix = minSuffix == null || validator.compare(minSuffix, index.minSuffix()) > 0 ? index.minSuffix() : minSuffix;
            maxSuffix = maxSuffix == null || validator.compare(maxSuffix, index.maxSuffix()) < 0 ? index.maxSuffix() : maxSuffix;
        }

        public Set<SSTableIndex> match(Expression expression)
        {
            if (minSuffix == null) // no data in this view
                return Collections.emptySet();

            ByteBuffer min = expression.lower == null ? minSuffix : expression.lower.value;
            ByteBuffer max = expression.upper == null ? maxSuffix : expression.upper.value;

            if (validator.compare(min, minSuffix) < 0)
                min = minSuffix;

            if (validator.compare(max, minSuffix) < 0)
                max = minSuffix;

            return new HashSet<>(termIntervalTree.search(Interval.create(min, max, (SSTableIndex) null)));
        }

        public List<SSTableIndex> match(ByteBuffer minKey, ByteBuffer maxKey)
        {
            return keyIntervalTree.search(Interval.create(minKey, maxKey, (SSTableIndex) null));
        }

        public SAView update(Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            int newKeysSize = assertCorrectSizing(keyIntervalTree, toRemove, toAdd);
            int newTermsSize = assertCorrectSizing(termIntervalTree, toRemove, toAdd);
            assert newKeysSize == newTermsSize : String.format("mismatched sizes for intervals tree for keys vs terms: %d != %d", newKeysSize, newTermsSize);

            return new SAView(this, col, toRemove, toAdd);
        }

        private int assertCorrectSizing(IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> tree,
                                        Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newReaders)
        {
            int newSSTablesSize = tree.intervalCount() - oldSSTables.size() + newReaders.size();
            assert newSSTablesSize >= newReaders.size() :
                String.format("Incoherent new size %d replacing %s by %s, sstable size %d", newSSTablesSize, oldSSTables, newReaders, tree.intervalCount());
            return newSSTablesSize;
        }

        public Iterator<SSTableIndex> iterator()
        {
            return Iterators.transform(keyIntervalTree.iterator(), new Function<Interval<ByteBuffer, SSTableIndex>, SSTableIndex>()
            {
                public SSTableIndex apply(Interval<ByteBuffer, SSTableIndex> i)
                {
                    return i.data;
                }
            });
        }
    }

    private class DataTrackerConsumer implements INotificationConsumer
    {
        public void handleNotification(INotification notification, Object sender)
        {
            // unfortunately, we can only check the type of notification via instaceof :(
            if (notification instanceof SSTableAddedNotification)
            {
                SSTableAddedNotification notif = (SSTableAddedNotification) notification;
                addToIntervalTree(Collections.singletonList(notif.added), getColumnDefs());
            }
            else if (notification instanceof SSTableListChangedNotification)
            {
                SSTableListChangedNotification notif = (SSTableListChangedNotification) notification;
                for (SSTableReader reader : notif.added)
                    addToIntervalTree(Collections.singletonList(reader), getColumnDefs());
                for (SSTableReader reader : notif.removed)
                    removeFromIntervalTree(Collections.singletonList(reader));
            }
            else if (notification instanceof SSTableDeletingNotification)
            {
                SSTableDeletingNotification notif = (SSTableDeletingNotification) notification;
                removeFromIntervalTree(Collections.singletonList(notif.deleting));
            }
            else if (notification instanceof MemtableRenewedNotification)
            {
                invalidateMemtable();
            }
        }
    }

    private class IndexBuilder extends IndexBuildTask
    {
        private final KeyIterator keys;

        private final SSTableReader sstable;
        private final SortedSet<ByteBuffer> indexNames;
        private final Collection<ColumnDefinition> indexes;
        private final PerSSTableIndexWriter indexWriter;

        public IndexBuilder(SSTableReader sstable, SortedSet<ByteBuffer> indexesToBuild)
        {
            this.keys = new KeyIterator(sstable.descriptor);
            this.sstable = sstable;

            if (!sstable.acquireReference())
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);

            this.indexWriter = new PerSSTableIndexWriter(sstable.descriptor.asTemporary(true), SSTableWriterListener.Source.COMPACTION);
            this.indexNames = indexesToBuild;
            this.indexes = new ArrayList<ColumnDefinition>()
            {{
                for (ByteBuffer name : indexNames)
                    add(getColumnDefinition(name));
            }};
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(baseCfs.metadata,
                                      org.apache.cassandra.db.compaction.OperationType.INDEX_BUILD,
                                      keys.getBytesRead(),
                                      keys.getTotalBytes());
        }

        public void build()
        {
            try
            {
                while (keys.hasNext())
                {
                    if (isStopRequested())
                        throw new CompactionInterruptedException(getCompactionInfo());

                    DecoratedKey key = keys.next();

                    indexWriter.startRow(key, keys.getKeyPosition());

                    SSTableNamesIterator columns = new SSTableNamesIterator(sstable, key, indexNames);

                    while (columns.hasNext())
                    {
                        OnDiskAtom atom = columns.next();

                        if (atom != null && atom instanceof Column)
                            indexWriter.nextColumn((Column) atom);
                    }
                }
            }
            finally
            {
                sstable.releaseReference();
            }

            indexWriter.complete();

            for (ColumnDefinition columnDef : indexes)
            {
                String indexName = String.format(FILE_NAME_FORMAT, columnDef.getIndexName());

                File tmpIndex = new File(indexWriter.descriptor.filenameFor(indexName));
                if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                    continue;

                FileUtils.renameWithConfirm(tmpIndex, new File(sstable.descriptor.filenameFor(indexName)));
            }

            addToIntervalTree(Collections.singletonList(sstable), indexes);
        }
    }

    public Mode getMode(ByteBuffer columnName)
    {
        Pair<ColumnDefinition, IndexMode> definition = indexedColumns.get(columnName);
        return definition == null ? null : definition.right.mode;
    }

    public static AbstractTokenizer getTokenizer(Pair<ColumnDefinition, IndexMode> column)
    {
        assert column != null;
        return column.right.isAnalyzed && TOKENIZABLE_TYPES.contains(column.left.getValidator()) ? new StandardTokenizer() : new NoOpTokenizer();
    }

    public static class IndexMode
    {
        public final Mode mode;
        public final boolean isAnalyzed;

        public IndexMode(Mode mode, boolean isAnalyzed)
        {
            this.mode = mode;
            this.isAnalyzed = isAnalyzed;
        }
    }
}
