package apoc.dataflow;

import apoc.Pools;
import apoc.path.CFGValidationHelper;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.*;

public class ROSFindPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Pools pool;

    @Context
    public TerminationGuard terminationGuard;


    private DataflowHelper.DataflowType getCategory(Node startNode, Node endNode, Relationship startEdge,
                                                    Relationship endEdge) {

        // Determine which parital query we are working with
        if ((startNode != null) && (endNode != null)) {             // in-between
            return DataflowHelper.DataflowType.MIDDLE;
        } else if ((startEdge != null) && (endNode != null)) {      // prefix
            return DataflowHelper.DataflowType.PREFIX;
        } else if ((startNode != null) && (endEdge != null)) {      // suffix
            return DataflowHelper.DataflowType.SUFFIX;
        } else {
            return null;
        }
    }

    class DataflowCallable implements Callable<List<Path>> {

        private Relationship startEdge = null;
        private Relationship endEdge = null;
        private Node endNode = null;
        private Node startNode = null;
        private boolean cfgCheck = false;
        private Relationship pubVar = null;
        private Relationship pubTarget = null;
        private DataflowHelper.DataflowType category = null;

        public DataflowCallable(final Relationship startEdge, final Relationship endEdge,
                              final boolean cfgCheck) {

            if (startEdge.isType(DataflowHelper.RelTypes.pubTarget) &&
                    endEdge.isType(DataflowHelper.RelTypes.pubVar)) {
                // MIDDLE
                this.category = DataflowHelper.DataflowType.MIDDLE;
                this.startNode = startEdge.getEndNode();
                this.endNode = endEdge.getStartNode();
                this.pubVar = endEdge;
                this.pubTarget = startEdge;
            } else if (startEdge.isType(DataflowHelper.RelTypes.varWrite) &&
                    endEdge.isType(DataflowHelper.RelTypes.pubVar)) {
                // PREFIX
                this.category = DataflowHelper.DataflowType.PREFIX;
                this.startEdge = startEdge;
                this.endNode = endEdge.getStartNode();
                this.pubVar = endEdge;
            } else if (startEdge.isType(DataflowHelper.RelTypes.pubTarget) &&
                    (endEdge.isType(DataflowHelper.RelTypes.varInfFunc) ||
                            endEdge.isType(DataflowHelper.RelTypes.varInfluence) )) {
                // SUFFIX
                this.category = DataflowHelper.DataflowType.SUFFIX;
                this.startNode = startEdge.getEndNode();
                this.pubTarget = startEdge;
                this.endEdge = endEdge;
            }

            this.cfgCheck = cfgCheck;
        }

        @Override
        public List<Path> call() throws Exception {
            //terminationGuard.check();
            return rosAllShortestMulti(this.startNode, this.endNode, this.startEdge, this.endEdge,
                    this.pubVar, this.pubTarget, this.category, this.cfgCheck);
        }
    }

    @UserFunction
    @Description("apoc.dataflow.rosDataflow")
    public List<Path> rosDataflow(@Name("startEdges") List<Relationship> startEdges,
                                          @Name("endEdges") List<Relationship> endEdges,
                                          @Name("cfgCheck") boolean cfgCheck,
                                            @Name("numThreads") long numThreads) {


        ArrayList<Path> returnedPath = new ArrayList<>();
        List<Callable<List<Path>>> findPaths = new ArrayList<>();
        Queue<Future<List<Path>>> list = new LinkedList<>();

        int threads = (int) numThreads;
        //ExecutorService es = pool.getDefaultExecutorService();
        //CompletionService<List<Path>> executorCompletionService= new ExecutorCompletionService<>(es);
        ExecutorService es = Executors.newFixedThreadPool(threads);
        int numTask = 0;

        //int threads =  Runtime.getRuntime().availableProcessors();
        for (Relationship startEdge : startEdges) {
            for (Relationship endEdge : endEdges) {
                numTask += 1;

                /**final FutureTask<List<Path>> futureTask = new FutureTask<>(
                        new DataflowCallable(startEdge, endEdge, cfgCheck)
                );
                findPaths.add(new DataflowCallable(startEdge, endEdge, cfgCheck));**/

                Future<List<Path>> ftr = es.submit(
                        new DataflowCallable(startEdge, endEdge, cfgCheck)
                );
                list.add(ftr);


                /**executorCompletionService.submit(
                        new DataflowCallable(startEdge, endEdge, cfgCheck)
                );**/

            }

        }


        while (!list.isEmpty()) {
            Future<List<Path>> ftr = list.remove();
            try {
                final List<Path> result = ftr.get();
                if ((result != null) && (!result.isEmpty())) {
                    returnedPath.addAll(result);
                }
            } catch (InterruptedException e) {
                // If an InterruptedException or ExecutionException is thrown, print the exception message
                es.shutdown();
                throw new RuntimeException("Interruption error executing in parallel: ", e);
            } catch (ExecutionException e) {
                es.shutdown();
                throw new RuntimeException("Execution error executing in parallel: ", e);
            }

            ftr = null;
            Runtime.getRuntime().gc();
        }

        es.shutdown();
        try {
            es.awaitTermination(10, TimeUnit.SECONDS);
            es.shutdownNow();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException("Interruption error executing in parallel: ", e);
        }





        /**try {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }**/



        return returnedPath;

    }

    public List<Path> rosAllShortestMulti(@Name("startNode") Node startNode,
                                                        @Name("endNode") Node endNode,
                                     @Name("startEdge") Relationship startEdge,
                                     @Name("endEdge") Relationship endEdge,
                                     @Name("pubVar") Relationship pubVar,
                                     @Name("pubTarget") Relationship pubTarget,
                                     @Name("category") DataflowHelper.DataflowType category,
                                     @Name("cfgCheck") boolean cfgCheck) {

        // path finding data structures
        HashSet<Relationship> visitedRels = new HashSet<>();
        HashSet<Relationship> visitedRel = new HashSet<>();
        Queue<EdgeInfo> queueEdge = new LinkedList<>();
        ArrayList<Path> returnedPath = new ArrayList<>();
        EdgeInfo foundPath = null;
        ArrayList<ArrayList<Relationship>> retCovered = new ArrayList<>();
        int pathLen = -1;

        // path finding variables
        //DataflowHelper.DataflowType category = getCategory(startNode, endNode, startEdge, endEdge);
        if (category == null) {return returnedPath;}

        // Check if path finding is necessary
        Node start = (category == DataflowHelper.DataflowType.PREFIX) ? startEdge.getEndNode() : startNode;
        Node end = (category == DataflowHelper.DataflowType.SUFFIX) ? endEdge.getStartNode() : endNode;
        if (start.getId() == end.getId()) {
            // not necessary
            PathImpl.Builder builder;
            builder = (category == DataflowHelper.DataflowType.PREFIX) ?
                    new PathImpl.Builder(startEdge.getStartNode()) :
                    new PathImpl.Builder(pubTarget.getStartNode());
            builder = (category == DataflowHelper.DataflowType.PREFIX) ?
                     builder.push(startEdge) : builder.push(pubTarget);
            //builder = (endEdge != null) ? builder.push(endEdge) : builder;
            builder = (category == DataflowHelper.DataflowType.SUFFIX) ?
                    builder.push(endEdge) : builder.push(pubVar);
            return List.of(builder.build());
        }

        // Add first edges to queue before beginning search
        if (category != DataflowHelper.DataflowType.PREFIX) {
            Iterable<Relationship> nextRels = DataflowHelper.getNextRels(startNode);
            for (Relationship nextRel : nextRels) {
                visitedRels.add(nextRel);
                queueEdge.add(new EdgeInfo(nextRel, null));
            }
        } else {
            visitedRels.add(startEdge);
            queueEdge.add(new EdgeInfo(startEdge, null));
        }


        while (!queueEdge.isEmpty()) {

            EdgeInfo curEdge = queueEdge.remove();
            Relationship curRel = curEdge.getCurRel();

            if (foundPath != null) {
                if ((!curEdge.compareRetNodes(foundPath))) {
                    continue;
                } else {
                    if (retCovered.contains(curEdge.getRetWrites())) {
                        continue;
                    }
                }
            }

            // validate or get the corresponding CFG
            if ((!cfgCheck) || getCFGPath(curEdge)) {

                visitedRel.add(curRel);

                if (curRel.getEndNode().getId() == end.getId()) {
                    if (category == DataflowHelper.DataflowType.SUFFIX) {
                        EdgeInfo vifEdge = new EdgeInfo(endEdge, curEdge);
                        if ((!cfgCheck) || getCFGPath(vifEdge)) {
                            returnedPath.add(recursiveConstructPath(vifEdge, pubTarget).build());
                            foundPath = vifEdge;
                            visitedRels.addAll(visitedRel);
                            retCovered.addAll(curEdge.getRetComp());
                            continue;
                            //return List.of(constructPath(curEdge));
                        }
                    } else {
                        PathImpl.Builder b = recursiveConstructPath(curEdge, pubTarget);
                        b = (pubVar != null) ? b.push(pubVar) : b;
                        returnedPath.add(b.build());
                        foundPath = curEdge;
                        visitedRels.addAll(visitedRel);
                        retCovered.addAll(curEdge.getRetComp());
                        continue;
                        //return List.of(constructPath(curEdge));
                    }
                }

                Iterable<Relationship> nextRels = DataflowHelper.getNextRels(curRel.getEndNode());
                for (Relationship nextRel : nextRels) {
                    if (!visitedRels.contains(nextRel)) {
                        queueEdge.add(new EdgeInfo(nextRel, curEdge));
                    }
                }
            }

        }

        return returnedPath;
    }

    @UserFunction
    @Description("apoc.dataflow.rosAllShortest()")
    public List<Path> rosAllShortest(@Name("startNode") Node startNode,
                                     @Name("endNode") Node endNode,
                                     @Name("startEdge") Relationship startEdge,
                                     @Name("endEdge") Relationship endEdge,
                                     @Name("cfgCheck") boolean cfgCheck) {

        // path finding data structures
        HashSet<Relationship> visitedRels = new HashSet<>();
        HashSet<Relationship> visitedRel = new HashSet<>();
        Queue<EdgeInfo> queueEdge = new LinkedList<>();
        ArrayList<Path> returnedPath = new ArrayList<>();
        EdgeInfo foundPath = null;
        ArrayList<ArrayList<Relationship>> retCovered = new ArrayList<>();
        int pathLen = -1;

        // path finding variables
        DataflowHelper.DataflowType category = getCategory(startNode, endNode, startEdge, endEdge);
        if (category == null) {return null;}

        // Check if path finding is necessary
        Node start = (category == DataflowHelper.DataflowType.PREFIX) ? startEdge.getEndNode() : startNode;
        Node end = (category == DataflowHelper.DataflowType.SUFFIX) ? endEdge.getStartNode() : endNode;
        if (start.getId() == end.getId()) {
            // not necessary
            PathImpl.Builder builder = (startNode != null) ? new PathImpl.Builder(startNode):
                    new PathImpl.Builder(startEdge.getStartNode());
            builder = (startEdge != null) ? builder.push(startEdge) : builder;
            builder = (endEdge != null) ? builder.push(endEdge) : builder;
            return List.of(builder.build());
        }

        // Add first edges to queue before beginning search
        if (category != DataflowHelper.DataflowType.PREFIX) {
            Iterable<Relationship> nextRels = DataflowHelper.getNextRels(startNode);
            for (Relationship nextRel : nextRels) {
                visitedRels.add(nextRel);
                queueEdge.add(new EdgeInfo(nextRel, null));
            }
        } else {
            visitedRels.add(startEdge);
            queueEdge.add(new EdgeInfo(startEdge, null));
        }


        while (!queueEdge.isEmpty()) {

            EdgeInfo curEdge = queueEdge.remove();
            Relationship curRel = curEdge.getCurRel();

            if (foundPath != null) {
                if ((!curEdge.compareRetNodes(foundPath))) {
                    continue;
                } else {
                    if (retCovered.contains(curEdge.getRetWrites())) {
                        continue;
                    }
                }
            }

            // validate or get the corresponding CFG
            if ((!cfgCheck) || getCFGPath(curEdge)) {

                visitedRels.add(curRel);

                if (curRel.getEndNode().getId() == end.getId()) {
                    if (category == DataflowHelper.DataflowType.SUFFIX) {
                        curEdge = new EdgeInfo(endEdge, curEdge);
                        if ((!cfgCheck) || getCFGPath(curEdge)) {
                            returnedPath.add(constructPath(curEdge));
                            foundPath = curEdge;
                            visitedRels.addAll(visitedRel);
                            retCovered.addAll(curEdge.getRetComp());
                            //return List.of(constructPath(curEdge));
                        }
                    } else {
                        returnedPath.add(constructPath(curEdge));
                        foundPath = curEdge;
                        visitedRels.addAll(visitedRel);
                        retCovered.addAll(curEdge.getRetComp());
                        //return List.of(constructPath(curEdge));
                    }
                }

                Iterable<Relationship> nextRels = DataflowHelper.getNextRels(curRel.getEndNode());
                for (Relationship nextRel : nextRels) {
                    if (!visitedRels.contains(nextRel)) {
                        queueEdge.add(new EdgeInfo(nextRel, curEdge));
                    }
                }
            }

        }

        return returnedPath;
    }

    private boolean getCFGPath(EdgeInfo curEdge) {

        HashMap<List<Node>, Relationship> curCFG = DataflowHelper.getConnectionNodes(curEdge.getCurRel());
        ArrayList<Node> prevCFG = curEdge.getPrevRelCFG();

        ShortestPath shortestPath = new ShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));

        ArrayList<Node> acceptedCFGNode = new ArrayList<>();

        for (List<Node> endCFG : curCFG.keySet()) {
            if (curEdge.getPathLength() == 1) {
                acceptedCFGNode.add(endCFG.get(1));
                continue;
            }

            for (Node startCFG : prevCFG) {
                try {
                    Path cfgPath = shortestPath.findSinglePath(startCFG, endCFG.get(0));
                    if (cfgPath != null) {
                        acceptedCFGNode.add(endCFG.get(0));
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }

        curEdge.updateCfgNodes(acceptedCFGNode);
        return (acceptedCFGNode.isEmpty()) ? false : true;

    }

    public Path constructPath(EdgeInfo edge) {
        if (edge == null) {
            return null;
        }
        PathImpl.Builder builder = recursiveConstructPath(edge);
        return builder.build();
    }

    public PathImpl.Builder recursiveConstructPath(EdgeInfo edge) {
        PathImpl.Builder builder;
        if (edge.getPrevEdge() == null) {
            builder = new PathImpl.Builder(edge.getCurRel().getStartNode());
        } else {
            builder = recursiveConstructPath(edge.getPrevEdge());
        }
        builder = builder.push(edge.getCurRel());
        return builder;
    }

    public PathImpl.Builder recursiveConstructPath(EdgeInfo edge, Relationship pubTarget) {
        PathImpl.Builder builder;
        if (edge.getPrevEdge() == null) {
            if (pubTarget != null) {
                builder = new PathImpl.Builder(pubTarget.getStartNode());
                builder = builder.push(pubTarget);
            } else {
                builder = new PathImpl.Builder(edge.getCurRel().getStartNode());
            }
        } else {
            builder = recursiveConstructPath(edge.getPrevEdge(), pubTarget);
        }
        builder = builder.push(edge.getCurRel());
        return builder;
    }

}
