package apoc.path;

import com.google.common.collect.Iterables;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import apoc.path.CFGValidationHelper.DataflowType;
import apoc.algo.CFGShortestPath;

import java.util.*;
import java.util.stream.StreamSupport;

public class DataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserFunction
    @Description("apoc.path.dataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path using forward propagation")
    public Path dataflowPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                           @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                           @Name("cfgCheck") boolean cfgCheck) {

        //Node start;
        Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;


        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            end = endNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            end = endEdge.getStartNode();
            category = DataflowType.SUFFIX;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            end = endNode;
            category = DataflowType.PREFIX;
            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.PREFIX) {
            dataflowRels = CFGValidationHelper.getNextRels(startNode, false);

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
            if ((!cfgCheck) || (getCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    if (category == DataflowType.SUFFIX) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge);
                        if ((!cfgCheck) || (getCFGPath(vifPath))) {
                            // build path, and return (exit)
                            return vifPath.buildPath();
                        }
                    } else {
                        // build path, and return (exit)
                        return curPath.buildPath();
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
    @Description("apoc.path.allDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds all shortest dataflow paths using forward propagation")
    public List<Path> allDataflowPaths(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                                 @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                                 @Name("cfgCheck") boolean cfgCheck) {

        //Node start;
        Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;

        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();

        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            end = endNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            end = endEdge.getStartNode();
            category = DataflowType.SUFFIX;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            end = endNode;
            category = DataflowType.PREFIX;
            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.PREFIX) {
            dataflowRels = CFGValidationHelper.getNextRels(startNode, false);

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
            if ((!cfgCheck) || (getCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    if (category == DataflowType.SUFFIX) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge);
                        if ((!cfgCheck) || (getCFGPath(vifPath))) {
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


    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean getCFGPath(CandidatePath candidatePath) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship curRel = candidatePath.getSecondLastRel();
        Relationship nextRel = candidatePath.getLastRel();

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<List<Node>, Relationship> startCFGs = CFGValidationHelper.getConnectionNodes(curRel,
                candidatePath, true, false);
        HashMap<List<Node>, Relationship> endCFGs = CFGValidationHelper.getConnectionNodes(nextRel,
                candidatePath, false, false);

        CFGShortestPath cfgShortestPath = new CFGShortestPath(tx);
        List<Path> validPaths = cfgShortestPath.findPath(startCFGs, endCFGs, candidatePath);
        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        if (validPaths.isEmpty()) {
            return false;
        }

        for (Path validPath : validPaths) {
            acceptedCFGEnd.add(validPath.endNode());
        }

        if (acceptedCFGEnd.isEmpty()) {
            return false;
        }
        candidatePath.updateCFG(acceptedCFGEnd);

        return true;

    }

}
