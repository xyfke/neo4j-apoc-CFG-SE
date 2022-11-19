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



    // helper function: find and verify CFG path
    // returns:
    //      - boolean: indicating candidatePath is feasible
    public boolean backwardGmGetCFGPath(CandidatePath candidatePath, boolean isStartPW,
                                boolean isEndPW) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship nextRel = candidatePath.getSecondLastRel();
        Relationship curRel = candidatePath.getLastRel();

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"),
                (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<List<Node>, Relationship> startCFGs = (isStartPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(curRel.getStartNode(), candidatePath, true) :
                CFGValidationHelper.getConnectionNodes(curRel, candidatePath, true, true);
        HashMap<List<Node>, Relationship> endCFGs = (isEndPW) ?
                CFGValidationHelper.getParWriteConnectionNodes(nextRel.getEndNode(), candidatePath, false) :
                CFGValidationHelper.getConnectionNodes(nextRel, candidatePath, false, true);

        Path cfgPath = null;
        Node n1 = null;
        Node n2 = null;

        if ((startCFGs.isEmpty()) || (endCFGs.isEmpty())) {
            return false;
        }

        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        // if we can find a path from the cfg node associated with r1 to the cfg node associated
        // with r2, then there exists a cfg path
        for (List<Node> listStartCFG : startCFGs.keySet()) {
            Node startCFGNode = listStartCFG.get(0);
            for (List<Node> listEndCFG : endCFGs.keySet()) {
                cfgPath = algo.findSinglePath(startCFGNode, listEndCFG.get(0));
                if (cfgPath != null) {
                    acceptedCFGEnd.add(listEndCFG.get(1));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGEnd);

        return !acceptedCFGEnd.isEmpty();

    }

}
