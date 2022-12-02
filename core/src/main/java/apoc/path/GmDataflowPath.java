package apoc.path;

import apoc.algo.CFGShortestPath;
import org.checkerframework.checker.units.qual.C;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import apoc.path.CFGValidationHelper.DataflowType;
import apoc.path.CFGValidationHelper.RelTypes;

import java.util.*;


public class GmDataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;


    @UserFunction
    @Description("apoc.path.gmDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a gm dataflow path using forward propagation")
    public Path gmDataflowPath(@Name("startNode") Node startNode,
                                @Name("endNode") Node endNode,
                                @Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                             @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        Node end;
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath();

        if ((startEdge != null) && (endNode != null)) {         // prefix: startEdge, endNode
            category = DataflowType.PREFIX;
            start = startEdge.getEndNode();
            end = endNode;

            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else if ((startNode != null) && (endNode != null)) {      // intra: startNode, endNode
            category = DataflowType.INTRA;
            start = startNode;
            end = endNode;

            curPath = new CandidatePath(startNode);
        } else if ((startNode != null) && (endEdge != null)) {      // suffix: startNode, endEdge
            category = DataflowType.SUFFIX;
            start = startNode;
            end = endEdge.getStartNode();

            curPath = new CandidatePath(startNode);
        } else {
            return null;
        }

        // PREFIX - startEdge to (endNode -pwSource)
        // INTRA - (startNode pwDestination) to (endNode - pwSource)
        // SUFFIX - (startNode pwDestination) to endEdge
        // Adding first CFGs to Candidate path
        if (cfgCheck) {
            HashMap<List<Node>, Relationship> firstCFGs = (category == DataflowType.PREFIX) ?
                    CFGValidationHelper.getConnectionNodes(startEdge, curPath, true, false) :
                    CFGValidationHelper.getParWriteConnectionNodes(start, curPath, true);
            CFGValidationHelper.addCFGToCandidatePath(curPath, firstCFGs, false);
        }

        // check for already found values
        if (start.equals(end)) {

            CandidatePath path = (category == DataflowType.SUFFIX) ? new CandidatePath(curPath, endEdge) :
                    new CandidatePath(curPath);
            if ((!cfgCheck) || (gmGetCFGPath(path,
                    (category != DataflowType.PREFIX),
                    (category != DataflowType.SUFFIX)))) {
                PathImpl.Builder builder = (startEdge != null) ?
                        new PathImpl.Builder(startEdge.getStartNode()) :
                        new PathImpl.Builder(start);
                builder = (startEdge != null) ? builder.push(startEdge) : builder;
                builder = (endEdge != null) ? builder.push(endEdge) : builder;
                return builder.build();
            } else {
                return null;
            }
        }


        if (category != DataflowType.PREFIX) {
            dataflowRels = CFGValidationHelper.getNextRels(start, false);
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    CandidatePath candPath = new CandidatePath(curPath, dataflowRel);
                    queuePath.add(candPath);
                }
            }
        }


        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.poll();

            // isStartPW: category != PREFIX, len == 2
            boolean isStartPW = ((category != DataflowType.PREFIX) &&
                    (curPath.getPathSize() == 1));

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (gmGetCFGPath(curPath,isStartPW, false))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    CandidatePath returnPath = (category == DataflowType.SUFFIX) ?
                            new CandidatePath(curPath, endEdge) :
                            new CandidatePath(curPath);
                    isStartPW = ((category != DataflowType.PREFIX)
                            && (curPath.getPathSize() == 1));
                    boolean isEndPW = (category != DataflowType.SUFFIX);

                    if ((!cfgCheck) || (gmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                        return returnPath.buildPath();
                    }
                }

                dataflowRels = CFGValidationHelper.getNextRels(curPath.getEndNode(), false);
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdge.contains(dataflowRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, dataflowRel);
                        queuePath.add(newCandidatePath);
                    }
                }

            }

        }


        return null;

    }

    @UserFunction
    @Description("apoc.path.allGmDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all shortest path dataflow path using forward propagation")
    public List<Path> allGmDataflowPaths(@Name("startNode") Node startNode,
                                @Name("endNode") Node endNode,
                                @Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                               @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        Node end;
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath();

        if ((startEdge != null) && (endNode != null)) {         // prefix
            category = DataflowType.PREFIX;
            start = startEdge.getEndNode();
            end = endNode;

            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else if ((startNode != null) && (endNode != null)) {      // intra
            category = DataflowType.INTRA;
            start = startNode;
            end = endNode;

            curPath = new CandidatePath(end);
        } else if ((startNode != null) && (endEdge != null)) {      // suffix
            category = DataflowType.SUFFIX;
            start = startNode;
            end = endEdge.getStartNode();

            curPath = new CandidatePath(end);
        } else {
            return null;
        }

        // PREFIX - startEdge to (endNode -pwSource)
        // INTRA - (startNode pwDestination) to (endNode - pwSource)
        // SUFFIX - (startNode pwDestination) to endEdge
        // Adding first CFGs to Candidate path
        if (cfgCheck) {
            HashMap<List<Node>, Relationship> firstCFGs = (category != DataflowType.PREFIX) ?
                    CFGValidationHelper.getParWriteConnectionNodes(start, curPath, true) :
                    CFGValidationHelper.getConnectionNodes(startEdge, curPath,
                            true, false);
            CFGValidationHelper.addCFGToCandidatePath(curPath, firstCFGs, false);
        }

        // check for already found values
        if (start.equals(end)) {
            curPath = (category == DataflowType.SUFFIX) ? new CandidatePath(curPath, endEdge) :
                    curPath;
            if ((!cfgCheck) || (gmGetCFGPath(curPath,
                    (category != DataflowType.PREFIX),
                    (category != DataflowType.SUFFIX)))) {
                PathImpl.Builder builder = (startEdge != null) ?
                        new PathImpl.Builder(startEdge.getStartNode()) :
                        new PathImpl.Builder(start);
                builder = (startEdge != null) ? builder.push(startEdge) : builder;
                builder = (endEdge != null) ? builder.push(endEdge) : builder;
                return List.of(builder.build());
            }
        }


        if (category != DataflowType.PREFIX) {
            dataflowRels = CFGValidationHelper.getNextRels(start, false);
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdges.contains(dataflowRel)) {
                    CandidatePath candPath = new CandidatePath(curPath, dataflowRel);
                    queuePath.add(candPath);
                }
            }
        }

        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        int pathLen = -1;
        boolean foundPath = false;


        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.poll();

            int curLen = curPath.getPathSize();

            if (foundPath && curLen > pathLen) {
                // if path has been found and current path is longer than found path, can break
                break;
            }

            if (curLen > pathLen) {
                // add all relationships found at previous path length to visitedRels
                visitedEdges.addAll(visitedEdge);
                visitedEdge = new HashSet<Relationship>();
            }
            pathLen = curLen;

            // boolean variables indicating whether we are doing a start or end check
            boolean isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 1));
            boolean isEndPW = false;

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (gmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    CandidatePath returnPath = new CandidatePath(curPath, endEdge);
                    isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 1));
                    isEndPW = (category != DataflowType.SUFFIX);

                    if ((!cfgCheck) || (gmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                        foundPath = true;
                        returnCandidates.add(returnPath);
                    }
                }

                dataflowRels = CFGValidationHelper.getNextRels(curPath.getEndNode(), false);
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdges.contains(dataflowRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, dataflowRel);
                        queuePath.add(newCandidatePath);
                    }
                }

            }

        }

        List<Path> returnPaths = new ArrayList<Path>();
        for (CandidatePath returnCandidate : returnCandidates) {
            if (returnCandidate.getPathSize() > 0) {
                returnPaths.add(returnCandidate.buildPath());
            }
        }


        return returnPaths;

    }

    @UserFunction
    @Description("apoc.path.allGmDataflowPathsV2(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all shortest path dataflow path using forward propagation")
    public List<Path> allGmDataflowPathsV2(@Name("startNode") Node startNode,
                                         @Name("endNode") Node endNode,
                                         @Name("startEdge") Relationship startEdge,
                                         @Name("endEdge") Relationship endEdge,
                                         @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        Node end;
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath();

        if ((startEdge != null) && (endNode != null)) {         // prefix: startEdge, endNode
            category = DataflowType.PREFIX;
            start = startEdge.getEndNode();
            end = endNode;

            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else if ((startNode != null) && (endNode != null)) {      // intra: startNode, endNode
            category = DataflowType.INTRA;
            start = startNode;
            end = endNode;

            curPath = new CandidatePath(endNode);
        } else if ((startNode != null) && (endEdge != null)) {      // suffix: startNode, endEdge
            category = DataflowType.SUFFIX;
            start = startNode;
            end = endEdge.getStartNode();

            curPath = new CandidatePath(endNode);
        } else {
            return null;
        }

        // PREFIX - startEdge to (endNode -pwSource)
        // INTRA - (startNode pwDestination) to (endNode - pwSource)
        // SUFFIX - (startNode pwDestination) to endEdge
        // Adding first CFGs to Candidate path
        if (cfgCheck) {
            HashMap<List<Node>, Relationship> firstCFGs = (category != DataflowType.PREFIX) ?
                    CFGValidationHelper.getParWriteConnectionNodes(start, curPath, true) :
                    CFGValidationHelper.getConnectionNodes(startEdge, curPath,
                            true, false);
            CFGValidationHelper.addCFGToCandidatePath(curPath, firstCFGs, false);
        }

        // check for already found values
        if (start.equals(end)) {
            curPath = (category == DataflowType.SUFFIX) ? new CandidatePath(curPath, endEdge) :
                    curPath;
            if ((!cfgCheck) || (gmGetCFGPath(curPath,
                    (category != DataflowType.PREFIX),
                    (category != DataflowType.SUFFIX)))) {
                PathImpl.Builder builder = (startEdge != null) ?
                        new PathImpl.Builder(startEdge.getStartNode()) :
                        new PathImpl.Builder(start);
                builder = (startEdge != null) ? builder.push(startEdge) : builder;
                builder = (endEdge != null) ? builder.push(endEdge) : builder;
                return List.of(builder.build());
            }
        }


        if (category != DataflowType.PREFIX) {
            dataflowRels = CFGValidationHelper.getNextRels(start, false);
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    CandidatePath candPath = new CandidatePath(curPath, dataflowRel);
                    queuePath.add(candPath);
                }
            }
        }

        //HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        CandidatePath foundCandidatePath = null;
        ArrayList<ArrayList<Relationship>> retCovered = new ArrayList<>();


        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.remove();

            if (foundCandidatePath != null) {
                if ((!curPath.compareRetNodes(foundCandidatePath))) {
                    continue;
                } else {
                    if (retCovered.contains(curPath.retRel)) {
                        continue;
                    }
                }
            }

            // boolean variables indicating whether we are doing a start or end check
            boolean isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 1));
            boolean isEndPW = false;

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (gmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    CandidatePath returnPath = new CandidatePath(curPath, endEdge);
                    isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 1));
                    isEndPW = (category != DataflowType.SUFFIX);

                    if ((!cfgCheck) || (gmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                        foundCandidatePath = returnPath;
                        retCovered.addAll(returnPath.getRetComp());
                        returnCandidates.add(returnPath);
                        continue;
                    }
                }

                dataflowRels = CFGValidationHelper.getNextRels(curPath.getEndNode(), false);
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdge.contains(dataflowRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, dataflowRel);
                        queuePath.add(newCandidatePath);
                    }
                }

            }

        }

        List<Path> returnPaths = new ArrayList<Path>();
        for (CandidatePath returnCandidate : returnCandidates) {
            if (returnCandidate.getPathSize() > 0) {
                returnPaths.add(returnCandidate.buildPath());
            }
        }


        return returnPaths;

    }


    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean gmGetCFGPath(CandidatePath candidatePath, boolean isStartPW,
                              boolean isEndPW) {

        // obtain cfg nodes and relationships associated with r1
        HashSet<Node> startNodes = candidatePath.validCFGs;

        // for CFG path - comparison
        Node targetNode = (isEndPW) ? candidatePath.getEndNode() : candidatePath.getStartNode();
        Relationship nextRel = (isEndPW) ? null : candidatePath.getLastRel();
        Relationship curRel = (isEndPW) ? candidatePath.getLastRel() : candidatePath.getSecondLastRel();
        boolean filterVar = (curRel != null) && (!curRel.isType(RelTypes.parWrite)) &&
                (targetNode.hasLabel(CFGValidationHelper.NodeLabel.cVariable));


        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"),
                (int) Integer.MAX_VALUE
        );

        HashMap<List<Node>, Relationship> endCFGs = (isEndPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(targetNode, candidatePath, false) :
                CFGValidationHelper.getConnectionNodes(nextRel, candidatePath, false, false);

        CFGShortestPath shortestPath = new CFGShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));
        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        for (Node srcNode : startNodes) {
            for (List<Node> endCFG : endCFGs.keySet()) {
                Node dstNode = endCFG.get(0);
                Path cfgPath = shortestPath.findSinglePath(srcNode, dstNode, targetNode, filterVar);
                if (cfgPath != null) {
                    acceptedCFGEnd.add(endCFG.get(1));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGEnd);

        return !acceptedCFGEnd.isEmpty();

    }


}
