package apoc.algo;

import apoc.path.CandidatePath;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.cypher.internal.runtime.ClosingIterator;
import org.neo4j.graphalgo.EvaluationContext;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphalgo.impl.util.PathImpl.Builder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.NestingResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import apoc.path.CFGValidationHelper.RelTypes;
import apoc.path.CFGValidationHelper.NodeLabel;

public class CFGShortestPath {

    public final int NULL = -1;
    private final int maxDepth;
    private final int maxResultCount;
    private final PathExpander expander;
    private Metadata lastMetadata;
    private ShortestPathPredicate predicate;
    private final EvaluationContext context;
    private DataMonitor dataMonitor;
    private MemoryTracker memoryTracker;
    private static final long DIRECTION_DATA_SHALLOW_SIZE = shallowSizeOfInstance( DirectionData.class );

    public interface ShortestPathPredicate
    {
        boolean test( Path path );
    }

    /**
     * Constructs a new shortest path algorithm.
     * @param maxDepth the maximum depth for the traversal. Returned paths
     * will never have a greater {@link Path#length()} than {@code maxDepth}.
     * @param expander the {@link PathExpander} to use for deciding
     * which relationships to expand for each {@link Node}.
     */
    public CFGShortestPath( EvaluationContext context, int maxDepth, PathExpander expander )
    {
        this( context, maxDepth, expander, Integer.MAX_VALUE, EmptyMemoryTracker.INSTANCE );
    }

    public CFGShortestPath( EvaluationContext context, int maxDepth, PathExpander expander, ShortestPathPredicate predicate, MemoryTracker memoryTracker )
    {
        this( context, maxDepth, expander, Integer.MAX_VALUE, memoryTracker );
        this.predicate = predicate;
    }

    public CFGShortestPath( EvaluationContext context, int maxDepth, PathExpander expander, int maxResultCount )
    {
        this( context, maxDepth, expander, maxResultCount, EmptyMemoryTracker.INSTANCE );
    }

    /**
     * Constructs a new shortest path algorithm.
     * @param maxDepth the maximum depth for the traversal. Returned paths
     * will never have a greater {@link Path#length()} than {@code maxDepth}.
     * @param expander the {@link PathExpander} to use for deciding
     * which relationships to expand for each {@link Node}.
     * @param maxResultCount the maximum number of hits to return. If this number
     * of hits are encountered the traversal will stop.
     * @param memoryTracker tracks the memory used by the algorithm
     */
    public CFGShortestPath( EvaluationContext context, int maxDepth, PathExpander expander, int maxResultCount, MemoryTracker memoryTracker )
    {
        this.context = context;
        this.maxDepth = maxDepth;
        this.expander = expander;
        this.maxResultCount = maxResultCount;
        this.memoryTracker = new ScopedMemoryTracker( memoryTracker );
    }

    //@Override
    public Iterable<Path> findAllPaths( Node start, Node end, Relationship dataflowRel)
    {
        Node targetNode = dataflowRel.getEndNode();
        boolean filterVar = (targetNode.hasLabel(NodeLabel.cVariable) &&
                (dataflowRel.isType(RelTypes.varWrite) ||
                        dataflowRel.isType(RelTypes.parWrite)));
        return internalPaths( start, end, false, targetNode, filterVar);
    }

    /**
     * Finds all shortest paths and returns an auto closeable iterator. This method should
     * be called when a memoryTracker is used in order to keep track of the memory correctly.
     *
     * @param start start node
     * @param end end node
     * @return
     */
    public ClosingIterator<Path> findAllPathsAutoCloseableIterator( Node start, Node end,
                                                                    Relationship dataflowRel)
    {
        return new ClosingIterator()
        {
            Node targetNode = dataflowRel.getEndNode();
            boolean filterVar = (targetNode.hasLabel(NodeLabel.cVariable) &&
                    (dataflowRel.isType(RelTypes.varWrite) ||
                            dataflowRel.isType(RelTypes.parWrite)));
            Iterator<Path> inner = internalPaths( start, end, false, targetNode,
                    filterVar).iterator();

            @Override
            public void closeMore()
            {
                inner = null;
                memoryTracker.reset();
            }

            @Override
            public boolean innerHasNext()
            {
                return this.inner.hasNext();
            }

            @Override
            public Path next()
            {
                if ( inner == null )
                {
                    throw new NoSuchElementException();
                }
                return inner.next();
            }
        };
    }

    //@Override
    // Function for returning CFG path based on start and end nodes
    public Path findSinglePath( Node start, Node end, Relationship dataflowRel)
    {
        Node srcNode = dataflowRel.getStartNode();

        // filter conditions:
        //    - dataflowRel is not a pubVar relationship and the dataflowRel start node is either cVariable
        //        or cReturn
        boolean filterVar = (srcNode.hasLabel(NodeLabel.cVariable)) || (srcNode.hasLabel(NodeLabel.cReturn));
        filterVar = (!dataflowRel.isType(RelTypes.pubVar)) && filterVar;

        // run path finding algorithm
        Iterator<Path> paths = internalPaths( start, end, true, srcNode,
                filterVar).iterator();
        Path path = paths.hasNext() ? paths.next() : null;
        memoryTracker.reset();
        return path;
    }

    // currently not in use
    public Path findSinglePath(Node start, Node end, Node targetNode, boolean filterVar) {
        Iterator<Path> paths = internalPaths( start, end, true, targetNode,
                filterVar).iterator();
        Path path = paths.hasNext() ? paths.next() : null;
        memoryTracker.reset();
        return path;
    }

    private void resolveMonitor()
    {
        if ( dataMonitor == null )
        {
            GraphDatabaseService service = context.databaseService();
            Monitors monitors = ((GraphDatabaseFacade) service).getDependencyResolver().resolveDependency( Monitors.class );
            dataMonitor = monitors.newMonitor( DataMonitor.class );
        }
    }

    private Iterable<Path> internalPaths( Node start, Node end, boolean stopAsap,
                                          Node srcNode, boolean filterVar)
    {
        lastMetadata = new Metadata();
        if ( start.equals( end ) )
        {
            return filterPaths(Collections.singletonList( PathImpl.singular( start ) ));
        }
        Hits hits = new Hits();
        MutableInt sharedFrozenDepth = new MutableInt( NULL ); // ShortestPathLengthSoFar
        MutableBoolean sharedStop = new MutableBoolean();
        MutableInt sharedCurrentDepth = new MutableInt( 0 );
        try ( DirectionData startData = new DirectionData( start, sharedFrozenDepth, sharedStop,
                sharedCurrentDepth, expander, memoryTracker, filterVar, srcNode, start);
              DirectionData endData = new DirectionData( end, sharedFrozenDepth,
                      sharedStop, sharedCurrentDepth, expander.reverse(), memoryTracker,
                      filterVar, srcNode, start) )
        {
            while ( startData.hasNext() || endData.hasNext() )
            {
                goOneStep( startData, endData, hits, startData, stopAsap);
                goOneStep( endData, startData, hits, startData, stopAsap);
            }
            Collection<Hit> least = hits.least();
            return least != null ? filterPaths( hitsToPaths( least, start, end, stopAsap, maxResultCount, memoryTracker ) ) : Collections.emptyList();
        }
    }

    //@Override
    public TraversalMetadata metadata()
    {
        return lastMetadata;
    }

    // Few long-lived instances
    private static class Hit // TODO: Extend Measurable? Not if above comment about few is correct?
    {
        private final DirectionData start;
        private final DirectionData end;
        private final Node connectingNode;

        Hit( DirectionData start, DirectionData end, Node connectingNode )
        {
            this.start = start;
            this.end = end;
            this.connectingNode = connectingNode;
        }

        @Override
        public int hashCode()
        {
            return connectingNode.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            Hit o = (Hit) obj;
            return connectingNode.equals( o.connectingNode );
        }
    }

    /**private boolean validCFG(Node nextNode, Node targetNode, Node actualStart, boolean filterVar) {
        if (nextNode.equals(actualStart)) {
            return true;
        }

        Iterable<Relationship> overWriteConns = nextNode.getRelationships(Direction.INCOMING,
                RelTypes.varWriteDestination, RelTypes.retWriteDestination);
        for (Relationship overWriteConn : overWriteConns) {
            if (overWriteConn.getStartNode().equals(targetNode)) {
                return false;
            }
        }

        return true;
    }**/

    private void goOneStep( DirectionData directionData, DirectionData otherSide, Hits hits, DirectionData startSide,
                            boolean stopAsap)
    {
        if ( !directionData.hasNext() )
        {
            // We can not go any deeper from this direction. Possibly disconnected nodes.
            otherSide.finishCurrentLayerThenStop = true;
            return;
        }
        Node nextNode = directionData.next();
        LevelData otherSideHit = otherSide.visitedNodes.get( nextNode );
        if ( (otherSideHit != null) )
        {

            // This is a hit
            int depth = directionData.currentDepth + otherSideHit.depth;

            if ( directionData.sharedFrozenDepth.intValue() == NULL )
            {
                directionData.sharedFrozenDepth.setValue( depth );
            }
            if ( depth <= directionData.sharedFrozenDepth.intValue() )
            {
                directionData.haveFoundSomething = true;
                if ( depth < directionData.sharedFrozenDepth.intValue() )
                {
                    directionData.sharedFrozenDepth.setValue( depth );
                    // TODO Is it really ok to just stop the other side here?
                    // I'm basing that decision on that it was the other side
                    // which found the deeper paths (correct assumption?)
                    otherSide.stop = true;
                }
                // Add it to the list of hits
                DirectionData startSideData = directionData == startSide ? directionData : otherSide;
                DirectionData endSideData = directionData == startSide ? otherSide : directionData;
                Hit hit = new Hit( startSideData, endSideData, nextNode );
                Node start = startSide.startNode;
                Node end = (startSide == directionData) ? otherSide.startNode : directionData.startNode;
                //monitorData( startSide, (otherSide == startSide) ? directionData : otherSide, nextNode ); // Disabled. Needs GraphDatabaseService in context.

                // NOTE: Applying the filter-condition could give the wrong results with allShortestPaths,
                // so only use it for singleShortestPath
                // TODO: We don't need to create an intermediate array list just to check if list is empty after filtering
                if ( !stopAsap || !filterPaths( hitToPaths( hit, start, end, stopAsap ) ).isEmpty() )
                {
                    if ( hits.add( hit, depth ) >= maxResultCount )
                    {
                        directionData.stop = true;
                        otherSide.stop = true;
                        lastMetadata.paths++;
                    }
                    else if ( stopAsap )
                    {   // This side found a hit, but wait for the other side to complete its current depth
                        // to see if it finds a shorter path. (i.e. stop this side and freeze the depth).
                        // but only if the other side has not stopped, otherwise we might miss shorter paths
                        if ( otherSide.stop )
                        {
                            return;
                        }
                        directionData.stop = true;
                    }
                }
                else
                {
                    directionData.haveFoundSomething = false;
                    directionData.sharedFrozenDepth.setValue( NULL );
                    otherSide.stop = false;
                }
            }
        }
    }

    private void monitorData( DirectionData directionData, DirectionData otherSide, Node connectingNode )
    {
        resolveMonitor();
        if ( dataMonitor != null )
        {
            dataMonitor.monitorData( directionData.visitedNodes, directionData.nextNodes, otherSide.visitedNodes,
                    otherSide.nextNodes, connectingNode );
        }
    }

    private <T extends Path> Collection<T> filterPaths( Collection<T> paths )
    {
        if ( predicate == null )
        {
            return paths;
        }
        else
        {
            Collection<T> filteredPaths = new ArrayList<>();
            for ( T path : paths )
            {
                if ( predicate.test( path ) )
                {
                    filteredPaths.add( path );
                }
            }
            return filteredPaths;
        }
    }

    public interface DataMonitor
    {
        void monitorData( MutableMap<Node,LevelData> theseVisitedNodes, Iterable<Node> theseNextNodes,
                          MutableMap<Node,LevelData> thoseVisitedNodes, Iterable<Node> thoseNextNodes, Node connectingNode );
    }

    // Two long-lived instances
    private class DirectionData extends PrefetchingResourceIterator<Node>
    {
        private boolean finishCurrentLayerThenStop;
        private final Node startNode;
        private int currentDepth;
        private ResourceIterator<Relationship> nextRelationships;
        private final HeapTrackingArrayList<Node> nextNodes;
        private final HeapTrackingUnifiedMap<Node,LevelData> visitedNodes;
        private final DirectionDataPath lastPath;
        private final MutableInt sharedFrozenDepth;
        private final MutableBoolean sharedStop;
        private final MutableInt sharedCurrentDepth;
        private boolean haveFoundSomething;
        private boolean stop;
        private final PathExpander expander;
        private final boolean checkNode;
        private final Node srcNode;
        private final Node actualStart;

        DirectionData( Node startNode,
                       MutableInt sharedFrozenDepth,
                       MutableBoolean sharedStop,
                       MutableInt sharedCurrentDepth,
                       PathExpander expander,
                       MemoryTracker memoryTracker,
                       boolean filterVar,
                       Node srcNode,
                       Node actualStart)
        {
            this.startNode = startNode;
            this.visitedNodes = HeapTrackingCollections.newMap( memoryTracker );
            this.nextNodes = HeapTrackingArrayList.newArrayList( memoryTracker);
            memoryTracker.allocateHeap( LevelData.SHALLOW_SIZE + NodeEntity.SHALLOW_SIZE + DIRECTION_DATA_SHALLOW_SIZE );
            this.visitedNodes.put( startNode, new LevelData( null, 0 ) );
            this.nextNodes.add( startNode );
            this.sharedFrozenDepth = sharedFrozenDepth;
            this.sharedStop = sharedStop;
            this.sharedCurrentDepth = sharedCurrentDepth;
            this.expander = expander;
            this.lastPath = new DirectionDataPath( startNode );
            if ( sharedCurrentDepth.intValue() < maxDepth )
            {
                prepareNextLevel();
            }
            else
            {
                this.nextRelationships = Iterators.emptyResourceIterator();
            }


            this.srcNode = srcNode;                 // start variable of last relationship
            this.checkNode = filterVar;             // whether or not we are performing filtering
            this.actualStart = actualStart;         // start CFG node

        }

        private void prepareNextLevel()
        {
            HeapTrackingArrayList<Node> nodesToIterate = this.nextNodes.clone();
            this.nextNodes.clear();
            this.lastPath.setLength( currentDepth );
            closeRelationshipsIterator();
            this.nextRelationships = new NestingResourceIterator<>( nodesToIterate.autoClosingIterator() )
            {
                @Override
                protected ResourceIterator<Relationship> createNestedIterator( Node node )
                {
                    lastPath.setEndNode( node );
                    return asResourceIterator( expander.expand( lastPath, BranchState.NO_STATE ) );
                }
            };
            this.currentDepth++;
            this.sharedCurrentDepth.increment();
        }

        private void closeRelationshipsIterator()
        {
            if ( this.nextRelationships != null )
            {
                this.nextRelationships.close();
            }
        }

        @Override
        public void close()
        {
            nextNodes.close();
            visitedNodes.close();
            closeRelationshipsIterator();
        }

        @Override
        protected Node fetchNextOrNull()
        {
            while ( true )
            {
                Relationship nextRel = fetchNextRelOrNull();
                if ( nextRel == null )
                {
                    return null;
                }

                Node result = nextRel.getOtherNode( this.lastPath.endNode() );

                // perform check to determine whether or not we are filtering
                if (!validCFG(result)) {
                    continue;
                }

                if ( filterNextLevelNodes( result ) != null )
                {
                    lastMetadata.rels++;

                    LevelData levelData = this.visitedNodes.get( result );
                    if ( levelData == null )
                    {
                        // Instead of passing the memoryTracker to LevelData, which would require 2 calls to allocate memory,
                        // we make a single call to allocate memory here
                        memoryTracker.allocateHeap( LevelData.SHALLOW_SIZE + NodeEntity.SHALLOW_SIZE + HeapEstimator.sizeOfLongArray( 1 ) );
                        levelData = new LevelData( nextRel, this.currentDepth );
                        this.visitedNodes.put( result, levelData );
                        this.nextNodes.add( result );
                        return result;
                    }
                    else if ( this.currentDepth == levelData.depth )
                    {
                        memoryTracker.allocateHeap( Long.BYTES );
                        levelData.addRel( nextRel );
                    }
                }
            }
        }

        // helper function: check whether any in between variables in execution path overwritten some values
        //      focuses mostly on dataflow queries
        private boolean validCFG(Node cfgNode) {

            if (cfgNode.equals(this.actualStart)) {
                return true;
            }

            if (this.checkNode) {
                Iterable<Relationship> cfgConnections = cfgNode.getRelationships(Direction.INCOMING);
                for (Relationship cfgConnection : cfgConnections) {
                    // compare with next relationship - start node
                    String cfgType = cfgConnection.getType().toString();
                    // exclude vi and vif because they don't change dataflow values
                    if (cfgType.endsWith("Destination") && !cfgType.equals("vifDestination") &&
                            !cfgType.equals("viDestination") &&
                            cfgConnection.getStartNode().equals(this.srcNode)) {
                        return false;
                    }
                }
            }

            return true;
        }

        private boolean canGoDeeper()
        {
            return (this.sharedFrozenDepth.intValue() == NULL) && (this.sharedCurrentDepth.intValue() < maxDepth) &&
                    !finishCurrentLayerThenStop;
        }

        private Relationship fetchNextRelOrNull()
        {
            if ( this.stop || this.sharedStop.booleanValue() )
            {
                return null;
            }
            boolean hasComeTooFarEmptyHanded = (this.sharedFrozenDepth.intValue() != NULL) &&
                    (this.sharedCurrentDepth.intValue() > this.sharedFrozenDepth.intValue()) &&
                    !this.haveFoundSomething;
            if ( hasComeTooFarEmptyHanded )
            {
                return null;
            }
            if ( !this.nextRelationships.hasNext() )
            {
                if ( canGoDeeper() )
                {
                    prepareNextLevel();
                }
            }
            return this.nextRelationships.hasNext() ? this.nextRelationships.next() : null;
        }
    }

    // Two long-lived instances
    private static class DirectionDataPath implements Path
    {
        private final Node startNode;
        private Node endNode;
        private int length;

        DirectionDataPath( Node startNode )
        {
            this.startNode = startNode;
            this.endNode = startNode;
            this.length = 0;
        }

        void setEndNode( Node endNode )
        {
            this.endNode = endNode;
        }

        void setLength( int length )
        {
            this.length = length;
        }

        @Override
        public Node startNode()
        {
            return startNode;
        }

        @Override
        public Node endNode()
        {
            return endNode;
        }

        @Override
        public Relationship lastRelationship()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> relationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> reverseRelationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> nodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> reverseNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public Iterator<Entity> iterator()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected Node filterNextLevelNodes( Node nextNode )
    {
        // We need to be able to override this method from Cypher, so it must exist in this concrete class.
        // And we also need it to do nothing but still work when not overridden.
        return nextNode;
    }

    // Many long-lived instances
    public static class LevelData
    {
        public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( LevelData.class );
        private Relationship[] relsToHere;
        public final int depth;

        LevelData( Relationship relToHere, int depth )
        {
            if ( relToHere != null )
            {
                addRel( relToHere );
            }
            this.depth = depth;
        }

        void addRel( Relationship rel )
        {
            Relationship[] newRels;
            if ( relsToHere == null )
            {
                newRels = new Relationship[1];
            }
            else
            {
                newRels = new Relationship[relsToHere.length + 1];
                System.arraycopy( relsToHere, 0, newRels, 0, relsToHere.length );
            }
            newRels[newRels.length - 1] = rel;
            relsToHere = newRels;
        }
    }

    // One long lived instance
    private static class Hits
    {
        private final MutableIntObjectMap<Collection<Hit>> hits = new IntObjectHashMap<>(); // TODO: Heap tracking collection?
        private int lowestDepth;
        private int totalHitCount;

        int add( Hit hit, int atDepth )
        {
            Collection<Hit> depthHits = hits.getIfAbsentPut( atDepth, HashSet::new );
            if ( depthHits.add( hit ) )
            {
                totalHitCount++;
            }
            if ( lowestDepth == 0 || atDepth < lowestDepth )
            {
                lowestDepth = atDepth;
            }
            return totalHitCount;
        }

        Collection<Hit> least()
        {
            return hits.get( lowestDepth );
        }
    }

    // Methods for converting data representing paths to actual Path instances.
    // It's rather tricky just because this algo stores as little info as possible
    // required to build paths from hit information.
    private static class PathData
    {
        private final LinkedList<Relationship> rels;
        private final Node node;

        PathData( Node node, LinkedList<Relationship> rels )
        {
            this.rels = rels;
            this.node = node;
        }
    }

    private static Collection<Path> hitsToPaths( Collection<Hit> depthHits, Node start, Node end, boolean stopAsap, int maxResultCount,
                                                 MemoryTracker memoryTracker )
    {
        Set<Path> paths = HeapTrackingCollections.newSet( memoryTracker );
        for ( Hit hit : depthHits )
        {
            for ( PathImpl path : hitToPaths( hit, start, end, stopAsap ) )
            {
                memoryTracker.allocateHeap( path.estimatedHeapUsage() );
                paths.add( path );
                if ( paths.size() >= maxResultCount )
                {
                    break;
                }
            }
        }
        return paths;
    }

    private static Collection<PathImpl> hitToPaths( Hit hit, Node start, Node end, boolean stopAsap )
    {
        Collection<PathImpl> paths = new ArrayList<>();
        Iterable<List<Relationship>> startPaths = getPaths( hit.connectingNode, hit.start, stopAsap );
        Iterable<List<Relationship>> endPaths = getPaths( hit.connectingNode, hit.end, stopAsap );
        for ( List<Relationship> startPath : startPaths )
        {
            PathImpl.Builder startBuilder = toBuilder( start, startPath );
            for ( List<Relationship> endPath : endPaths )
            {
                PathImpl.Builder endBuilder = toBuilder( end, endPath );
                PathImpl path = startBuilder.build( endBuilder );
                paths.add( path );
            }
        }
        return paths;
    }

    private static Iterable<List<Relationship>> getPaths( Node connectingNode, DirectionData data, boolean stopAsap )
    {
        LevelData levelData = data.visitedNodes.get( connectingNode );
        if ( levelData.depth == 0 )
        {
            Collection<List<Relationship>> result = new ArrayList<>();
            result.add( new LinkedList<>() );
            return result;
        }
        Collection<PathData> set = new ArrayList<>();
        for ( Relationship rel : levelData.relsToHere )
        {
            set.add( new PathData( connectingNode, new LinkedList<>( Arrays.asList( rel ) ) ) );
            if ( stopAsap )
            {
                break;
            }
        }
        for ( int i = 0; i < levelData.depth - 1; i++ )
        {
            // One level
            Collection<PathData> nextSet = new ArrayList<>();
            for ( PathData entry : set )
            {
                // One path...
                Node otherNode = entry.rels.getFirst().getOtherNode( entry.node );
                LevelData otherLevelData = data.visitedNodes.get( otherNode );
                int counter = 0;
                for ( Relationship rel : otherLevelData.relsToHere )
                {
                    // ...may split into several paths
                    LinkedList<Relationship> rels = ++counter == otherLevelData.relsToHere.length ?
                            // This is a little optimization which reduces number of
                            // lists being copied
                            entry.rels
                            : new LinkedList<>( entry.rels );
                    rels.addFirst( rel );
                    nextSet.add( new PathData( otherNode, rels ) );
                    if ( stopAsap )
                    {
                        break;
                    }
                }
            }
            set = nextSet;
        }
        return new IterableWrapper<>( set )
        {
            @Override
            protected List<Relationship> underlyingObjectToObject( PathData object )
            {
                return object.rels;
            }
        };
    }

    private static Builder toBuilder( Node startNode, List<Relationship> rels )
    {
        PathImpl.Builder builder = new PathImpl.Builder( startNode );
        for ( Relationship rel : rels )
        {
            builder = builder.push( rel );
        }
        return builder;
    }

    private static class Metadata implements TraversalMetadata
    {
        private int rels;
        private int paths;

        @Override
        public int getNumberOfPathsReturned()
        {
            return paths;
        }

        @Override
        public int getNumberOfRelationshipsTraversed()
        {
            return rels;
        }
    }

}
