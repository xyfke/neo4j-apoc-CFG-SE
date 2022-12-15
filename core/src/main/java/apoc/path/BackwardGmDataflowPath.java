package apoc.path;

import apoc.algo.CFGShortestPath;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import apoc.path.CFGValidationHelper.DataflowType;
import apoc.path.CFGValidationHelper.RelTypes;

import java.util.*;

public class BackwardGmDataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserFunction
    @Description("apoc.path.backwardGmDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a gm dataflow path using backward propagation")
    public Path backwardGmDataflowPath(@Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                               @Name("cfgCheck") boolean cfgCheck) {

        Node start = startEdge.getEndNode();
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath(endEdge);
        queuePath.add(curPath);


        if ((startEdge.isType(RelTypes.varWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            category = DataflowType.PREFIX;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            category = DataflowType.INTRA;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.varInfFunc))) {
            category = DataflowType.SUFFIX;
        } else {
            return null;
        }

        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.poll();

            // isStartPW: category != PREFIX, len == 2
            boolean isStartPW = false;
            boolean isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 2));

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (backwardGmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getStartNode().equals(start)) {

                    CandidatePath returnPath = new CandidatePath(curPath, startEdge);
                    isStartPW = (category != DataflowType.PREFIX);
                    isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 2));

                    if ((!cfgCheck) || (backwardGmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                        return returnPath.backwardBuildPath();
                    }
                }

                dataflowRels = CFGValidationHelper.getPrevRels(curPath.getStartNode(), false);
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
    @Description("apoc.path.allBackwardGmDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all gm shortest dataflow paths using backward propagation")
    public List<Path> allBackwardGmDataflowPaths(@Name("startEdge") Relationship startEdge,
                                       @Name("endEdge") Relationship endEdge,
                                       @Name("cfgCheck") boolean cfgCheck) {

        Node start = startEdge.getEndNode();
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath(endEdge);
        queuePath.add(curPath);


        if ((startEdge.isType(RelTypes.varWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            category = DataflowType.PREFIX;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            category = DataflowType.INTRA;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.varInfFunc))) {
            category = DataflowType.SUFFIX;
        } else {
            return null;
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;
        int pathLen = -1;
        boolean foundPath = false;

        // keep track of visited relationships at current length
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();

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

            // isStartPW: category != PREFIX, len == 2
            boolean isStartPW = false;
            boolean isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 2));

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (backwardGmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getStartNode().equals(start)) {

                    CandidatePath returnPath = new CandidatePath(curPath, startEdge);
                    isStartPW = (category != DataflowType.PREFIX);
                    isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 2));

                    if ((!cfgCheck) || (backwardGmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                        foundPath = true;
                        returnCandidates.add(returnPath);
                    }
                }

                dataflowRels = CFGValidationHelper.getPrevRels(curPath.getStartNode(), false);
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
                returnPaths.add(returnCandidate.backwardBuildPath());
            }
        }

        return returnPaths;

    }

    @UserFunction
    @Description("apoc.path.allBackwardGmDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all gm shortest dataflow paths using backward propagation")
    public List<Path> allBackwardGmDataflowPathsV2(@Name("startNode") Node startNode,
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

            curPath = new CandidatePath(endNode);
            //queuePath.add(curPath);
        } else if ((startNode != null) && (endNode != null)) {      // intra: startNode, endNode
            category = DataflowType.INTRA;
            start = startNode;
            end = endNode;

            curPath = new CandidatePath(startNode);
        } else if ((startNode != null) && (endEdge != null)) {      // suffix: startNode, endEdge
            category = DataflowType.SUFFIX;
            start = startNode;
            end = endEdge.getStartNode();

            curPath = new CandidatePath(endEdge);
            queuePath.add(curPath);
        } else {
            return null;
        }

        // PREFIX - startEdge to (endNode -pwSource)
        // INTRA - (startNode pwDestination) to (endNode - pwSource)
        // SUFFIX - (startNode pwDestination) to endEdge
        // Adding first CFGs to Candidate path
        if (cfgCheck) {
            HashMap<List<Node>, Relationship> firstCFGs = (category != DataflowType.SUFFIX) ?
                    CFGValidationHelper.getParWriteConnectionNodes(end, curPath, false) :
                    CFGValidationHelper.getConnectionNodes(endEdge, curPath,
                            false, false);
            CFGValidationHelper.addCFGToCandidatePath(curPath, firstCFGs, true);
        }

        // check for already found values
        if (start.equals(end)) {
            curPath = (category == DataflowType.PREFIX) ? new CandidatePath(curPath, startEdge) :
                    curPath;
            if ((!cfgCheck) || (backwardGmGetCFGPath(curPath,
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

        if (category != DataflowType.SUFFIX) {
            dataflowRels = CFGValidationHelper.getPrevRels(endNode, false);
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    CandidatePath candPath = new CandidatePath(curPath, dataflowRel);
                    queuePath.add(candPath);
                }
            }
        }

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
            boolean isStartPW = false;
            boolean isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 1));

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (backwardGmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getStartNode().equals(start)) {

                    if (category == DataflowType.PREFIX) {
                        CandidatePath returnPath = new CandidatePath(curPath, startEdge);
                        isStartPW = (category != DataflowType.PREFIX);
                        isEndPW = ((category != DataflowType.SUFFIX) && (curPath.getPathSize() == 1));

                        if ((!cfgCheck) || (backwardGmGetCFGPath(returnPath, isStartPW, isEndPW))) {
                            foundCandidatePath = returnPath;
                            retCovered.addAll(returnPath.getRetComp());
                            returnCandidates.add(returnPath);
                            continue;
                        }
                    } else {
                        foundCandidatePath = curPath;
                        retCovered.addAll(curPath.getRetComp());
                        returnCandidates.add(curPath);
                        continue;
                    }


                }

                dataflowRels = CFGValidationHelper.getPrevRels(curPath.getStartNode(), false);
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
                returnPaths.add(returnCandidate.backwardBuildPath());
            }
        }


        return returnPaths;

    }


    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean backwardGmGetCFGPath(CandidatePath candidatePath, boolean isStartPW,
                                boolean isEndPW) {

        // obtain cfg nodes and relationships associated with r1
        HashSet<Node> endNodes = candidatePath.validCFGs;

        // for CFG path - comparison
        // a <- varWrite - b <- varWrite - c
        // [a <- varWrite - b, b <- varWrite - c]
        // endNode(curRel) == startNode(nextRel)
        Node targetNode = (isStartPW) ? candidatePath.getStartNode() : candidatePath.getEndNode();
        Relationship curRel = candidatePath.getLastRel();
        Relationship nextRel = (isStartPW) ? null : candidatePath.getSecondLastRel();
        boolean filterVar = (curRel != null) && (!curRel.isType(RelTypes.parWrite)) &&
                (targetNode.hasLabel(CFGValidationHelper.NodeLabel.cVariable));


        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"),
                (int) Integer.MAX_VALUE
        );

        HashMap<List<Node>, Relationship> startCFGs = (isStartPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(targetNode, candidatePath, true) :
                CFGValidationHelper.getConnectionNodes(curRel, candidatePath, true, false);

        CFGShortestPath shortestPath = new CFGShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));
        HashSet<Node> acceptedCFGStart = new HashSet<>();

        for (Node dstNode : endNodes) {
            for (List<Node> startCFG : startCFGs.keySet()) {
                Node srcNode = startCFG.get(1);
                Path cfgPath = shortestPath.findSinglePath(srcNode, dstNode, targetNode, filterVar);
                if (cfgPath != null) {
                    acceptedCFGStart.add(startCFG.get(0));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGStart);

        return !acceptedCFGStart.isEmpty();

    }

}
