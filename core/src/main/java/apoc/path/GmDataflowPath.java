package apoc.path;

import apoc.algo.CFGShortestPath;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
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
    public Path gmDataflowPath(@Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                             @Name("cfgCheck") boolean cfgCheck) {

        //Node start = startEdge.getEndNode();
        Node end = endEdge.getStartNode();
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath(startEdge);
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
            boolean isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 2));
            boolean isEndPW = false;

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (gmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    CandidatePath returnPath = new CandidatePath(curPath, endEdge);
                    isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 2));
                    isEndPW = (category != DataflowType.SUFFIX);

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
    public List<Path> allGmDataflowPaths(@Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                               @Name("cfgCheck") boolean cfgCheck) {

        //Node start = startEdge.getEndNode();
        Node end = endEdge.getStartNode();
        DataflowType category;
        Iterable<Relationship> dataflowRels;

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();

        // add first edge to path
        CandidatePath curPath = new CandidatePath(startEdge);
        queuePath.add(curPath);

        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();


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

            // boolean variables indicating whether we are doing a start or end check
            boolean isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 2));
            boolean isEndPW = false;

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (gmGetCFGPath(curPath,isStartPW, isEndPW))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    CandidatePath returnPath = new CandidatePath(curPath, endEdge);
                    isStartPW = ((category != DataflowType.PREFIX) && (curPath.getPathSize() == 2));
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


    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean gmGetCFGPath(CandidatePath candidatePath, boolean isStartPW,
                              boolean isEndPW) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship curRel = candidatePath.getSecondLastRel();
        Relationship nextRel = candidatePath.getLastRel();

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"),
                (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<List<Node>, Relationship> startCFGs = (isStartPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(curRel, candidatePath, true) :
                CFGValidationHelper.getConnectionNodes(curRel, candidatePath, true, false);
        HashMap<List<Node>, Relationship> endCFGs = (isEndPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(nextRel, candidatePath, false) :
                CFGValidationHelper.getConnectionNodes(nextRel, candidatePath, false, false);

        CFGShortestPath shortestPath = new CFGShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));
        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        for (List<Node> startCFG : startCFGs.keySet()) {
            Node srcNode = startCFG.get(1);
            for (List<Node> endCFG : endCFGs.keySet()) {
                Node dstNode = endCFG.get(0);
                Path cfgPath = shortestPath.findSinglePath(srcNode, dstNode, curRel);
                if (cfgPath != null) {
                    acceptedCFGEnd.add(dstNode);
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGEnd);

        return !acceptedCFGEnd.isEmpty();

    }


}
