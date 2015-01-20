package org.apache.cassandra.db.index.search.container;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;

public class TokenTreeBuilder
{
    // note: ordinal positions are used here, do not change order
    enum EntryType
    {
        SIMPLE, FACTORED, PACKED, OVERFLOW
    }

    public static final int BLOCK_BYTES = 4096;
    public static final int BLOCK_HEADER_BYTES = 64;
    public static final int OVERFLOW_TRAILER_BYTES = 64;
    public static final int OVERFLOW_TRAILER_CAPACITY = OVERFLOW_TRAILER_BYTES / 8;
    public static final int TOKENS_PER_BLOCK = 248; // TODO (jwest): calculate using other constants
    public static final long MAX_OFFSET = (1L << 47) - 1; // 48 bits for (signed) offset

    // TODO (jwest): merge iterator would be better here but bc duplicate keys just use SortedMap.putAll? how can we coalesce duplicates?
    // TODO (jwest): use a comparator for DK
    private final SortedMap<Long, LongSet> tokens = new TreeMap<>();
    private int numBlocks;


    private Node root;
    private InteriorNode rightmostParent;
    private Leaf leftmostLeaf;
    private Leaf rightmostLeaf;
    private long tokenCount = 0;
    private TokenRange treeTokenRange = new TokenRange();

    public TokenTreeBuilder(SortedMap<Long, LongSet> data)
    {
        add(data);
    }

    public void add(SortedMap<Long, LongSet> data)
    {
        for (Map.Entry<Long, LongSet> newEntry : data.entrySet())
        {
            LongSet found = tokens.get(newEntry.getKey());
            if (found != null)
                for (LongCursor offset : newEntry.getValue())
                    found.add(offset.value);
            else
                tokens.put(newEntry.getKey(), newEntry.getValue());
        }
    }

    public TokenTreeBuilder finish()
    {
        maybeBulkLoad();
        return this;
    }

    public int serializedSize()
    {
        if (numBlocks == 1)
            return (BLOCK_HEADER_BYTES + ((int) tokenCount * 16));
        else
            return numBlocks * BLOCK_BYTES;
    }

    public void write(DataOutput out) throws IOException
    {
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;

        while (levelIterator != null)
        {

            Node firstChild = null;
            while (levelIterator.hasNext())
            {
                Node block = levelIterator.next();

                if (firstChild == null && !block.isLeaf())
                    firstChild = ((InteriorNode) block).children.get(0);

                block.serialize(childBlockIndex, blockBuffer);
                flushBuffer(blockBuffer, out, numBlocks != 1);

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    public Iterator<Pair<Long, LongSet>> iterator()
    {
        return new TokenIterator(leftmostLeaf.levelIterator());
    }

    private void maybeBulkLoad()
    {
        if (root == null)
            bulkLoad();
    }

    private void flushBuffer(ByteBuffer buffer, DataOutput o, boolean align) throws IOException
    {
        // seek to end of last block before flushing
        if (align)
            alignBuffer(buffer, BLOCK_BYTES);

        buffer.flip();
        ByteBufferUtil.write(buffer, o);
        buffer.clear();
    }

    private static void alignBuffer(ByteBuffer buffer, int blockSize)
    {
        long curPos = buffer.position();
        if ((curPos & (blockSize - 1)) != 0) // align on the block boundary if needed
            buffer.position((int) FBUtilities.align(curPos, blockSize));
    }

    private void bulkLoad()
    {
        tokenCount = tokens.size();
        treeTokenRange.setRange(tokens.firstKey(), tokens.lastKey());
        numBlocks = 1;

        // special case the tree that only has a single block in it (so we don't create a useless root)
        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new Leaf(tokens);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            int i = 0;
            Leaf lastLeaf = null;
            Long firstToken = tokens.firstKey();
            Long finalToken = tokens.lastKey();
            Long lastToken;
            for (Long token : tokens.keySet())
            {
                if (i == 0 || (i % TOKENS_PER_BLOCK != 0 && i != (tokenCount - 1)))
                {
                    i++;
                    continue;
                }

                lastToken = token;
                Leaf leaf = (i != (tokenCount - 1) || token.equals(finalToken)) ?
                        new Leaf(tokens.subMap(firstToken, lastToken)) : new Leaf(tokens.tailMap(firstToken));

                if (i == TOKENS_PER_BLOCK)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = leaf;
                rightmostLeaf = leaf;
                firstToken = lastToken;
                i++;
                numBlocks++;

                if (token.equals(finalToken))
                {
                    Leaf finalLeaf = new Leaf(tokens.tailMap(token));
                    lastLeaf.next = finalLeaf;
                    rightmostParent.add(finalLeaf);
                    rightmostLeaf = finalLeaf;
                    numBlocks++;
                }
            }

        }
    }

    private abstract class Node
    {
        protected InteriorNode parent;
        protected Node next;
        protected TokenRange tokenRange = new TokenRange();

        public abstract void serialize(long childBlockIndex, ByteBuffer buf);
        public abstract int childCount();
        public abstract int tokenCount();
        public abstract Long smallestToken();

        public Iterator<Node> levelIterator()
        {
            return new LevelIterator(this);
        }

        public boolean isLeaf()
        {
            return (this instanceof Leaf);
        }

        protected boolean isLastLeaf()
        {
            return this == rightmostLeaf;
        }

        protected boolean isRoot()
        {
            return this == root;
        }

        protected void serializeHeader(ByteBuffer buf)
        {
            Header header;
            if (isRoot())
                header = new RootHeader();
            else if (!isLeaf())
                header = new InteriorNodeHeader();
            else
                header = new LeafHeader();

            header.serialize(buf);
            alignBuffer(buf, BLOCK_HEADER_BYTES);
        }

        private abstract class Header
        {
            public void serialize(ByteBuffer buf)
            {
                buf.put(infoByte())                        // info byte
                        .putShort((short) (tokenCount())) // block token count
                        .putLong(tokenRange.minToken)      // min token in root block
                        .putLong(tokenRange.maxToken);     // max token in root block

            }

            protected abstract byte infoByte();
        }

        private class RootHeader extends Header
        {
            @Override
            public void serialize(ByteBuffer buf)
            {
                super.serialize(buf);
                buf.putLong(tokenCount)                    // total number of tokens in tree
                        .putLong(treeTokenRange.minToken)  // min token in tree
                        .putLong(treeTokenRange.maxToken);  // max token in tree
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return (byte) ((isLeaf()) ? 3 : 0);
            }
        }

        private class InteriorNodeHeader extends Header
        {
            // bit 0 (leaf indicator) & bit 1 (last leaf indicator) cleared
            protected byte infoByte()
            {
                return 0;
            }
        }

        // TODO (jwest): add collision information
        private class LeafHeader extends Header
        {
            // bit 0 set as leaf indicator
            // bit 1 set if this is last leaf of data
            protected byte infoByte()
            {
                byte infoByte = 1;
                infoByte |= (isLastLeaf()) ? (1 << 1) : 0; // TODO (jwest): dont hardcode indicator pos

                return infoByte;
            }
        }

    }

    private class Leaf extends Node
    {
        private final SortedMap<Long, LongSet> tokens;
        private LongArrayList overflowCollisions;

        // TODO (jwest): enforce toks length <= TOKENS_PER_BLOCK;
        Leaf(SortedMap<Long, LongSet> data)
        {
            tokenRange.setRange(data.firstKey(), data.lastKey());
            tokens = data;
        }

        public Long largestToken()
        {
            return tokenRange.maxToken;
        }

        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeData(buf);
            serializeOverflowCollisions(buf);
        }

        public int childCount()
        {
            return 0;
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokenRange.minToken;
        }

        public Iterator<Map.Entry<Long, LongSet>> tokenIterator()
        {
            return tokens.entrySet().iterator();
        }

        private void serializeData(ByteBuffer buf)
        {
            for (Map.Entry<Long, LongSet> entry : tokens.entrySet())
                createEntry(entry.getKey(), entry.getValue()).serialize(buf);
        }

        private void serializeOverflowCollisions(ByteBuffer buf)
        {
            if (overflowCollisions != null)
                for (LongCursor offset : overflowCollisions)
                    buf.putLong(offset.value);
        }


        private LeafEntry createEntry(final long tok, final LongSet offsets)
        {
            int offsetCount = offsets.size();
            switch (offsetCount)
            {
                case 0:
                    throw new AssertionError("no offsets for token " + tok);
                case 1:
                    long offset = offsets.toArray()[0];
                    if (offset > MAX_OFFSET)
                        throw new AssertionError("offset " + offset + " cannot be greater than " + MAX_OFFSET);
                    else if (offset <= Integer.MAX_VALUE)
                        return new SimpleLeafEntry(tok, offset);
                    else
                        return new FactoredOffsetLeafEntry(tok, offset);
                case 2:
                    long[] rawOffsets = offsets.toArray();
                    if (rawOffsets[0] <= Integer.MAX_VALUE && rawOffsets[1] <= Integer.MAX_VALUE &&
                            (rawOffsets[0] <= Short.MAX_VALUE || rawOffsets[1] <= Short.MAX_VALUE))
                        return new PackedCollisionLeafEntry(tok, rawOffsets);
                    else
                        return createOverflowEntry(tok, offsetCount, offsets);
                default:
                    return createOverflowEntry(tok, offsetCount, offsets);
            }
        }

        private LeafEntry createOverflowEntry(final long tok, final int offsetCount, final LongSet offsets)
        {
            if (overflowCollisions == null)
                overflowCollisions = new LongArrayList();

            LeafEntry entry = new OverflowCollisionLeafEntry(tok, (short) overflowCollisions.size(), (short) offsetCount);
            for (LongCursor o : offsets) {
                if (overflowCollisions.size() == OVERFLOW_TRAILER_CAPACITY)
                    throw new AssertionError("cannot have more than " + OVERFLOW_TRAILER_CAPACITY + " overflow collisions per leaf");
                else
                    overflowCollisions.add(o.value);
            }
            return entry;
        }

        private abstract class LeafEntry
        {
            protected final long token;

            abstract public EntryType type();
            abstract public int offsetData();
            abstract public short offsetExtra();

            public LeafEntry(final long tok)
            {
                token = tok;
            }

            public void serialize(ByteBuffer buf)
            {
                buf.putShort((short) type().ordinal())
                        .putShort(offsetExtra())
                        .putLong(token)
                        .putInt(offsetData());
            }

        }


        // assumes there is a single offset and the offset is <= Integer.MAX_VALUE
        private class SimpleLeafEntry extends LeafEntry
        {
            private final long offset;

            public SimpleLeafEntry(final long tok, final long off)
            {
                super(tok);
                offset = off;
            }

            public EntryType type()
            {
                return EntryType.SIMPLE;
            }

            public int offsetData()
            {
                return (int) offset;
            }

            public short offsetExtra()
            {
                return 0;
            }
        }

        // assumes there is a single offset and Integer.MAX_VALUE < offset <= MAX_OFFSET
        // take the middle 32 bits of offset (or the top 32 when considering offset is max 48 bits)
        // and store where offset is normally stored. take bottom 16 bits of offset and store in entry header
        private class FactoredOffsetLeafEntry extends LeafEntry
        {
            private final long offset;

            public FactoredOffsetLeafEntry(final long tok, final long off)
            {
                super(tok);
                offset = off;
            }

            public EntryType type()
            {
                return EntryType.FACTORED;
            }

            public int offsetData()
            {
                return (int) (offset >>> Short.SIZE);
            }

            public short offsetExtra()
            {
                return (short) offset;
            }
        }

        // holds an entry with two offsets that can be packed in an int & a short
        // the int offset is stored where offset is normally stored. short offset is
        // stored in entry header
        private class PackedCollisionLeafEntry extends LeafEntry
        {
            private short smallerOffset;
            private int largerOffset;

            public PackedCollisionLeafEntry(final long tok, final long[] offs)
            {
                super(tok);

                smallerOffset = (short) Math.min(offs[0], offs[1]);
                largerOffset = (int) Math.max(offs[0], offs[1]);
            }

            public EntryType type()
            {
                return EntryType.PACKED;
            }

            public int offsetData()
            {
                return largerOffset;
            }

            public short offsetExtra()
            {
                return smallerOffset;
            }
        }

        // holds an entry with three or more offsets, or two offsets that cannot
        // be packed into an int & a short. the index into the overflow list
        // is stored where the offset is normally stored. the number of overflowed offsets
        // for the entry is stored in the entry header
        private class OverflowCollisionLeafEntry extends LeafEntry
        {
            private final short startIndex;
            private final short count;

            public OverflowCollisionLeafEntry(final long tok, final short collisionStartIndex, final short collisionCount)
            {
                super(tok);
                startIndex = collisionStartIndex;
                count = collisionCount;
            }

            public EntryType type()
            {
                return EntryType.OVERFLOW;
            }

            public int offsetData()
            {
                return startIndex;
            }

            public short offsetExtra()
            {
                return count;
            }

        }

    }

    private class InteriorNode extends Node
    {
        private List<Long> tokens = new ArrayList<>(TOKENS_PER_BLOCK);
        private List<Node> children = new ArrayList<>(TOKENS_PER_BLOCK + 1);
        private int position = 0; // TODO (jwest): can get rid of this and use array size


        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);
        }

        public int childCount()
        {
            return children.size();
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokens.get(0);
        }

        protected void add(Long token, InteriorNode leftChild, InteriorNode rightChild)
        {
            int pos = tokens.size();
            if (pos == TOKENS_PER_BLOCK)
            {
                InteriorNode sibling = split();
                sibling.add(token, leftChild, rightChild);

            }
            else {
                if (leftChild != null)
                    children.add(pos, leftChild);

                if (rightChild != null)
                {
                    children.add(pos + 1, rightChild);
                    rightChild.parent = this;
                }

                tokenRange.updateRange(token);
                tokens.add(pos, token);
            }
        }

        protected void add(Leaf node)
        {

            if (position == (TOKENS_PER_BLOCK + 1))
            {
                rightmostParent = split();
                rightmostParent.add(node);
            }
            else
            {

                node.parent = this;
                children.add(position, node);
                position++;

                // the first child is referenced only during bulk load. we don't take a value
                // to store into the tree, one is subtracted since position has already been incremented
                // for the next node to be added
                if (position - 1 == 0)
                    return;


                // tokens are inserted one behind the current position, but 2 is subtracted because
                // position has already been incremented for the next add
                Long smallestToken = node.smallestToken();
                tokenRange.updateRange(smallestToken);
                tokens.add(position - 2, smallestToken);
            }

        }

        protected InteriorNode split()
        {
            Pair<Long, InteriorNode> splitResult = splitBlock();
            Long middleValue = splitResult.left;
            InteriorNode sibling = splitResult.right;
            InteriorNode leftChild = null;

            // create a new root if necessary
            if (parent == null)
            {
                parent = new InteriorNode();
                root = parent;
                sibling.parent = parent;
                leftChild = this;
                numBlocks++;
            }

            parent.add(middleValue, leftChild, sibling);

            return sibling;
        }

        protected Pair<Long, InteriorNode> splitBlock()
        {
            final int splitPosition = TOKENS_PER_BLOCK - 2;
            InteriorNode sibling = new InteriorNode();
            sibling.parent = parent;
            next = sibling;

            Long middleValue = tokens.get(splitPosition);

            for (int i = splitPosition; i < TOKENS_PER_BLOCK; i++)
            {
                if (i != TOKENS_PER_BLOCK && i != splitPosition)
                {
                    long token = tokens.get(i);
                    sibling.tokenRange.updateRange(token);
                    sibling.tokens.add(token);
                }

                Node child = children.get(i + 1);
                child.parent = sibling;
                sibling.children.add(child);
                sibling.position++;
            }

            for (int i = TOKENS_PER_BLOCK; i >= splitPosition; i--)
            {
                if (i != TOKENS_PER_BLOCK)
                    tokens.remove(i);

                if (i != splitPosition)
                    children.remove(i);
            }

            tokenRange.setRange(smallestToken(), tokens.get(tokens.size() - 1));
            numBlocks++;

            return Pair.create(middleValue, sibling);
        }

        protected boolean isFull()
        {
            return (position >= TOKENS_PER_BLOCK + 1);
        }

        private void serializeTokens(ByteBuffer buf)
        {
            for (Long token : tokens)
                buf.putLong(token);
        }


        private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf)
        {
            for (int i = 0; i < children.size(); i++)
                buf.putLong((childBlockIndex + i) * BLOCK_BYTES);
        }
    }

    public static class LevelIterator extends AbstractIterator<Node>
    {
        private Node currentNode;

        LevelIterator(Node first)
        {
            currentNode = first;
        }

        public Node computeNext()
        {
            if (currentNode == null)
                return endOfData();

            Node returnNode = currentNode;
            currentNode = returnNode.next;

            return returnNode;
        }


    }

    public static class TokenIterator extends AbstractIterator<Pair<Long, LongSet>>
    {
        private Iterator<Node> levelIterator;
        private Iterator<Map.Entry<Long, LongSet>> currentIterator;

        TokenIterator(Iterator<Node> level)
        {
            levelIterator = level;
            if (levelIterator.hasNext())
                currentIterator = ((Leaf) levelIterator.next()).tokenIterator();
        }

        @Override
        public Pair<Long, LongSet> computeNext()
        {
            if (currentIterator != null && currentIterator.hasNext())
            {
                Map.Entry<Long, LongSet> next = currentIterator.next();
                return Pair.create(next.getKey(), next.getValue());
            }
            else
            {
                if (!levelIterator.hasNext())
                    return endOfData();
                else
                {
                    currentIterator = ((Leaf) levelIterator.next()).tokenIterator();
                    return computeNext();
                }
            }

        }
    }
}