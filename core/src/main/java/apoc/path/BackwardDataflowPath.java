package apoc.path;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import apoc.path.CFGValidationHelper.DataflowType;

import java.util.*;

public class BackwardDataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserFunction
    @Description("apoc.path.backwardDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path using backward propagation")
    public Path backwardDataflowPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                             @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                             @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        //Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;


        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            start = startNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            start = startNode;
            category = DataflowType.SUFFIX;
            curPath = new CandidatePath(endEdge);
            queuePath.add(curPath);
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            start = startEdge.getEndNode();
            category = DataflowType.PREFIX;
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.SUFFIX) {
            dataflowRels = CFGValidationHelper.getPrevRels(endNode, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    CandidatePath candidatePath = new CandidatePath(dataflowRel);
                    queuePath.add(candidatePath);
                }
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.poll();

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (backwardGetCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getStartNode().equals(start)) {

                    if (category == DataflowType.PREFIX) {
                        CandidatePath varPath = new CandidatePath(curPath, startEdge);
                        if ((!cfgCheck) || (backwardGetCFGPath(varPath))) {
                            // build path, and return (exit)
                            return varPath.backwardBuildPath();
                        }
                    } else {
                        // build path, and return (exit)
                        return curPath.backwardBuildPath();
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
    @Description("apoc.path.allBackwardDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all shortest dataflow paths using backward propagation")
    public List<Path> allBackwardDataflowPaths(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                                     @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                                     @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        //Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;

        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();

        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            start = startNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            start = startEdge.getEndNode();
            category = DataflowType.SUFFIX;
            curPath = new CandidatePath(endEdge);
            queuePath.add(curPath);
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            start = endNode;
            category = DataflowType.PREFIX;
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.SUFFIX) {
            dataflowRels = CFGValidationHelper.getPrevRels(endNode, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdges.contains(dataflowRel)) {
                    CandidatePath candidatePath = new CandidatePath(dataflowRel);
                    queuePath.add(candidatePath);
                }
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;
        int pathLen = -1;
        boolean foundPath = false;

        // keep track of visited relationships at current length
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();

        while (!queuePath.isEmpty()) {

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

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (backwardGetCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getStartNode().equals(start)) {

                    if (category == DataflowType.PREFIX) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge);
                        if ((!cfgCheck) || (backwardGetCFGPath(vifPath))) {
                            // build path, and return (exit)
                            foundPath = true;
                            returnCandidates.add(vifPath);
                        }
                    } else {
                        // build path, and return (exit)
                        foundPath = true;
                        returnCandidates.add(curPath);
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

    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean backwardGetCFGPath(CandidatePath candidatePath) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship nextRel = candidatePath.getSecondLastRel();
        Relationship curRel = candidatePath.getLastRel();

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<List<Node>, Relationship> startCFGs = CFGValidationHelper.getConnectionNodes(curRel,
                candidatePath, true, true);
        HashMap<List<Node>, Relationship> endCFGs = CFGValidationHelper.getConnectionNodes(nextRel,
                candidatePath, false, true);

        Path cfgPath = null;
        Node n1 = null;
        Node n2 = null;

        if ((startCFGs.isEmpty()) || (endCFGs.isEmpty())) {
            return false;
        }

        HashSet<Node> acceptedCFGStart = new HashSet<>();

        // if we can find a path from the cfg node associated with r1 to the cfg node associated
        // with r2, then there exists a cfg path
        for (List<Node> listStartCFG : startCFGs.keySet()) {
            Node startCFGNode = listStartCFG.get(0);
            Relationship nextCFGBlockEdge = startCFGs.get(listStartCFG);
            for (List<Node> listEndCFG : endCFGs.keySet()) {
                cfgPath = algo.findSinglePath(startCFGNode, listEndCFG.get(0));
                if (cfgPath != null) {
                    acceptedCFGStart.add(listStartCFG.get(1));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGStart);

        return !acceptedCFGStart.isEmpty();

    }



}