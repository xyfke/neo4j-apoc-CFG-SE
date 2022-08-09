package apoc.path;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class BackwardDataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    // define used relationship types
    private enum RelTypes implements RelationshipType
    {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        nextCFGBlock, pubVar, pubTarget;
    }

   /*
    Full system analysis
        - dataflowPath (NCFG)
        - dataflowPath (NOTF)
        - dataflowPathOTF (OTF)
    */

    @UserFunction
    @Description("apoc.path.backwarddataflowPath(topicS, topicT) - returns a dataflowPath from start topic to end topic without CFG validation")
    public Path backwarddataflowPath(@Name("startTopic") Node startTopic, @Name("endTopic") Node endTopic) {
        Path dataflowPath = backwardfindDataflowPath(startTopic, endTopic, false);
        return dataflowPath;
    }

    @UserFunction
    @Description("apoc.path.dataflowPathNOTF(topicS, topicT) - returns a dataflowPath from a topic to another topic")
    public Path backwarddataflowPathNOTF(@Name("startTopic") Node startTopic, @Name("endTopic") Node endTopic) {
        // incomplete
        return null;
    }

    @UserFunction
    @Description("apoc.path.dataflowPathOTF(topicS, topicT) - returns a dataflowPath from start topic to end topic with CFG validation using on the fly method")
    public Path backwarddataflowPathOTF(@Name("startTopic") Node startTopic, @Name("endTopic") Node endTopic) {
        Path dataflowPath = backwardfindDataflowPath(startTopic, endTopic, true);
        return dataflowPath;
    }

    // helper function to look for dataflow paths from startTopic to endTopic
    // cfgCheck indicates whether or not CFG is needed
    public Path backwardfindDataflowPath(Node startTopic, Node endTopic, boolean cfgCheck) {

        // if startTopic equals endTopic, no need to look further
        if (startTopic.equals(endTopic)) {
            return null;
        }

        // keep track of already visited topic
        HashSet<Node> visitedTopic  = new HashSet<Node>();
        visitedTopic.add(startTopic);

        // look for need to visit path and add to queue
        List<Path> newPath = backwardvarParPathV2(startTopic, cfgCheck);
        Queue<Path> queuePath = new LinkedList<>();

        for (Path p : newPath) {
            if (p.endNode().equals(endTopic)) {
                return p;
            } else{
                if (!visitedTopic.contains(p.endNode())) {
                    visitedTopic.add(p.endNode());
                    queuePath.add(p);
                }
            }
        }

        // goes through each path to construct BFS tree
        while (!queuePath.isEmpty()) {

            Path curPath = queuePath.poll();
            Node newStart = curPath.endNode();

            newPath = backwardvarParPathV2(newStart, true);

            for (Path p : newPath) {
                if (p.endNode().equals(endTopic)) {
                    Path combinePath = backwardcombine(curPath, p);
                    return combinePath;
                }
                else {
                    if (!visitedTopic.contains(p.endNode())) {
                        visitedTopic.add(p.endNode());
                        Path combinePath = backwardcombine(curPath, p);
                        queuePath.add(combinePath);
                    }
                }
            }

        }


        return null;
    }

    // helper function to obtain all possible varWrite/parWrite Path from startTopic to any newTopics
    // cfgCheck indicates whether or not cfg validation is needed
    public ArrayList<Path> backwardvarParPathV2(Node startTopic, boolean cfgCheck) {

        // define search variables
        HashSet<Node> visitedVar = new HashSet<Node>();
        HashSet<Node> visitedTopic = new HashSet<Node>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = null;
        ArrayList<Path> listPath = new ArrayList<Path>();

        visitedTopic.add(startTopic);

        Iterable<Relationship> pubTargetRels = startTopic.getRelationships(Direction.OUTGOING, RelTypes.pubTarget);
        for (Relationship pubTargetRel : pubTargetRels) {
            Node firstVar = pubTargetRel.getEndNode();
            visitedVar.add(firstVar);

            // look for connected topics
            Iterable<Relationship> pubVarRels = firstVar.getRelationships(Direction.OUTGOING,
                RelTypes.pubVar);
            for (Relationship pubVarRel : pubVarRels) {
                if (!visitedTopic.contains(pubVarRel.getEndNode())) {
                    visitedTopic.add(pubVarRel.getEndNode());
                    PathImpl.Builder builder = new PathImpl.Builder(startTopic);
                    builder = builder.push(pubTargetRel);
                    builder = builder.push(pubVarRel);
                    listPath.add(builder.build());
                }
            }

            // look for continual vars
            Iterable<Relationship> firstVarParRels = firstVar.getRelationships(Direction.OUTGOING,
                    RelTypes.parWrite, RelTypes.varWrite);
            for (Relationship firstVarParRel : firstVarParRels) {
                if (!visitedVar.contains(firstVarParRel.getEndNode())) {
                    ArrayList<Relationship> relList = new ArrayList<Relationship>();
                    relList.add(pubTargetRel);
                    relList.add(firstVarParRel);
                    queuePath.add(relList);
                }
            }

        }

        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {

            curRels = queuePath.poll();

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node curNode = curRel.getStartNode();
            Node nextNode = curRel.getEndNode();

            if (curRels.size() == 2) {
                // look for topic
                Iterable<Relationship> pubVarRels = nextNode.getRelationships(Direction.OUTGOING,
                        RelTypes.pubVar);
                for (Relationship pubVarRel : pubVarRels) {
                    if (!visitedTopic.contains(pubVarRel.getEndNode())) {
                        visitedTopic.add(pubVarRel.getEndNode());
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(pubVarRel);
                        listPath.add(backwardbuildPath(newCurRels.get(0).getStartNode(), newCurRels));
                    }
                }

                // look for continual varPar relationships
                Iterable<Relationship> varParRels = nextNode.getRelationships(Direction.OUTGOING, RelTypes.parWrite,
                        RelTypes.varWrite);
                for (Relationship varParRel : varParRels) {
                    if (!visitedVar.contains(varParRel.getEndNode())) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(varParRel);
                        queuePath.add(newCurRels);
                    }
                }

                continue;
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship prevRel = curRels.get(curRels.size()-2);
            Node prevNode = prevRel.getStartNode();

            if (cfgCheck) {
                cfgPath = backwardgetCFGPath(prevRel, curRel);
            } else{
                cfgPath = new ArrayList<Relationship>();
            }

            if (cfgPath != null) {

                // add the node
                visitedVar.add(nextNode);

                // look for topic
                Iterable<Relationship> pubVarRels = nextNode.getRelationships(Direction.OUTGOING, RelTypes.pubVar);
                for (Relationship pubVarRel : pubVarRels) {
                    if (!visitedTopic.contains(pubVarRel.getEndNode())) {
                        visitedTopic.add(pubVarRel.getEndNode());
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(pubVarRel);
                        listPath.add(backwardbuildPath(newCurRels.get(0).getStartNode(), newCurRels));
                    }
                }

                // look for continual varPar relationships
                Iterable<Relationship> varParRels = nextNode.getRelationships(Direction.OUTGOING, RelTypes.parWrite,
                        RelTypes.varWrite);
                for (Relationship varParRel : varParRels) {
                    if (!visitedVar.contains(varParRel.getEndNode())) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(varParRel);
                        queuePath.add(newCurRels);
                    }
                }

            }


        }

        return listPath;

    }


    /*
    Two phased analysis
        - varParPath (OTF)
        - Neo4J Basic Query (NOTF + NCFG)
     */


    @UserFunction
    @Description("apoc.path.backwardvarParPathRels(start, end, isPrefix, cfgCheck) - finds all dataflow paths consisting of varWrites/parWrites/retWrites from one variable to another")
    public Path backwardvarParPathRels(@Name("start") Node start, @Name("end") Node end, @Name("isPrefix") boolean isPrefix,
                                             @Name("cfgCheck") boolean cfgCheck) {

        // terminates path if not exist
        if ((start == null) || (end == null)) {
            return null;
        }

        if ((start.equals(end))) {
            if (isPrefix) { return null; }
            else { return backwardbuildPath(start, null); }
        }

        // define needed variables
        HashSet<Relationship> visitedRels = new HashSet<Relationship>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = null;

        PathImpl.Builder builder = new PathImpl.Builder(start);
        Iterable<Relationship> varWriteRels;
        varWriteRels = getPrevRels(end, false);

        // add the relationships connected to start node
        for (Relationship varWriteRel : varWriteRels) {
            if (!visitedRels.contains(varWriteRel)) {
                ArrayList<Relationship> relList = new ArrayList<Relationship>();
                relList.add(varWriteRel);
                queuePath.add(relList);
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of varWrite-parWrite relationship

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node prevNode = curRel.getStartNode();

            // check size of existing path
            if (curRels.size() == 1) {
                // if start node matches and has varWrite relationship and length of path is 1
                // then return path without verifying CFG
                if (prevNode.equals(start) && curRel.isType(RelTypes.varWrite)) {
                    builder = builder.push(curRel);
                    return builder.build();
                    // otherwise, add the relationship connected to the next node to the queue
                    // then continue with the search
                } else {
                    varWriteRels = getPrevRels(prevNode, false);
                    if (varWriteRels == null) {
                        continue;
                    }
                    for (Relationship varWriteRel : varWriteRels) {
                        // only add not visited relationships
                        if (!visitedRels.contains(varWriteRel)) {
                            ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                            newCurRels.add(varWriteRel);
                            queuePath.add(newCurRels);
                        }
                    }
                    continue;
                }
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship nextRel = curRels.get(curRels.size()-2);

            if (cfgCheck) {
                cfgPath = backwardgetCFGPath(curRel, nextRel);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                visitedRels.add(nextRel);

                // if the prevNode happens to be equal to start node, then we found the path
                if (prevNode.equals(start) && curRel.isType(RelTypes.varWrite)) {
                    break;
                }

                // otherwise keep looking
                varWriteRels = getPrevRels(prevNode, false);
                if (varWriteRels == null) {
                    continue;
                }
                // only add not used relationships
                for (Relationship varWriteRel : varWriteRels) {
                    if (!visitedRels.contains(varWriteRel)) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(varWriteRel);
                        queuePath.add(newCurRels);
                    }
                }
            }
        }

        if ((curRels != null) && (curRels.size() > 0)) {
            Relationship firstRel = curRels.get(curRels.size() - 1);
            if ((firstRel.getStartNode().equals(start)) && firstRel.isType(RelTypes.varWrite) && (cfgPath != null)) {
                return backwardbuildPath(start, curRels);
            }
        }

        return null;

    }

    @UserFunction
    @Description("apoc.path.varParPaths(start, end, isPrefix, cfgCheck) - finds all dataflow paths consisting of varWrites/parWrites/retWrites from one variable to another")
    public List<Path> backwardvarParPaths(@Name("start") Node start, @Name("end") Node end, @Name("isPrefix") boolean isPrefix,
                           @Name("cfgCheck") boolean cfgCheck) {

        // terminates path if not exist
        if ((start == null) || (end == null)) {
            return null;
        }

        if ((start.equals(end))) {
            if (isPrefix) { return null; }
            else { return List.of(backwardbuildPath(start, null)); }
        }

        // define needed variables
        HashSet<Relationship> visitedRels = new HashSet<Relationship>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = null;

        List<List<Relationship>> returnRels = new ArrayList<List<Relationship>>();

        Iterable<Relationship> varWriteRels;
        varWriteRels = getPrevRels(end, false);

        // add the relationships connected to end node
        for (Relationship varWriteRel : varWriteRels) {
            if (!visitedRels.contains(varWriteRel)) {
                ArrayList<Relationship> relList = new ArrayList<Relationship>();
                relList.add(varWriteRel);
                queuePath.add(relList);
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;
        int pathLen = -1;
        boolean foundPath = false;
        // keep track of visited relationships at current length
        HashSet<Relationship> visitedRelsCurr = new HashSet<Relationship>();

        while (!queuePath.isEmpty()) {
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of varWrite-parWrite relationship
            int curLen = curRels.size();

            if (foundPath && curLen > pathLen) {
                // if path has been found and current path is longer than found path, can break
                break;
            }
            if (curLen > pathLen) {
                // add all relationships found at previous path length to visitedRels
                visitedRels.addAll(visitedRelsCurr);
                visitedRelsCurr = new HashSet<Relationship>();
            }
            pathLen = curLen;

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node prevNode = curRel.getStartNode();

            // check size of existing path
            if (curRels.size() == 1) {
                // if start node matches and length of path is 1 then return path without verifying CFG
                if (prevNode.equals(start) && curRel.isType(RelTypes.varWrite)) {
                    returnRels.add(curRels);
                    foundPath = true;
                    continue;
                    // otherwise, add the relationship connected to the next node to the queue
                    // then continue with the search
                } else {
                    varWriteRels = getPrevRels(prevNode, false);
                    if (varWriteRels == null) {
                        continue;
                    }
                    for (Relationship varWriteRel : varWriteRels) {
                        // only add unused relationships
                        if (!visitedRels.contains(varWriteRel)) {
                            ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                            newCurRels.add(varWriteRel);
                            queuePath.add(newCurRels);
                        }
                    }
                    continue;
                }
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship nextRel = curRels.get(curRels.size()-2);

            if (cfgCheck) {
                cfgPath = backwardgetCFGPath(curRel, nextRel);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                // since CFG has been verified, then nextRel is confirmed to be visited and exited
                visitedRelsCurr.add(nextRel);

                // if the nextNode happens to be equal to start node and has varWrite rel, then we found path
                if (prevNode.equals(start) && curRel.isType(RelTypes.varWrite)) {
                    returnRels.add(curRels);
                    foundPath = true;
                    continue;
                }

                // otherwise keep looking
                varWriteRels = getPrevRels(prevNode, false);
                if (varWriteRels == null) {
                    continue;
                }
                // only add not visited Nodes
                for (Relationship varWriteRel : varWriteRels) {
                    if (!visitedRels.contains(varWriteRel)) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(varWriteRel);
                        queuePath.add(newCurRels);
                    }
                }
            }
        }

        List<Path> returnPaths = new ArrayList<Path>();
        for (List<Relationship> rels : returnRels) {
            if ((rels != null) && (rels.size() > 0)) {
                returnPaths.add(backwardbuildPath(start, (ArrayList<Relationship>) rels));
            }
        }
        return returnPaths;

    }

    // helper function: verify cfgPath
    @UserFunction
    @Description("apoc.path.getCFGPath(r1, r2)")
    public List<Relationship> backwardgetCFGPath(@Name("r1") Relationship r1, @Name("r2") Relationship r2) {

        if ((r1 == null) || (r2 == null)) {
            return null;
        }

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<Node, ArrayList<Relationship>> startCFGGraph = backwardgetConnectionNodes(r1, true);
        HashMap<Node, ArrayList<Relationship>> endCFGGraph = backwardgetConnectionNodes(r2, false);

        Path cfgPath = null;
        Node n1 = null;
        Node n2 = null;

        if ((startCFGGraph.isEmpty()) || (endCFGGraph.isEmpty())) {
            return null;
        }


        // if we can find a path from the cfg node associated with r1 to the cfg node associated
        // with r2, then there exists a cfg path
        for (Node n1t : startCFGGraph.keySet()) {
            for (Node n2t : endCFGGraph.keySet()) {
                cfgPath = algo.findSinglePath(n1t, n2t);
                if (cfgPath != null) {
                    n1 = n1t; // store the cfg node for r1
                    n2 = n2t; // store the cfg node for r2
                    break;
                }
            }
            if (cfgPath != null) {
                break;
            }
        }

        // only return cfg graph if there exists one
        if ((cfgPath == null) || (n1 == null) || (n2 == null)) {
            return null;
        } else {
            Iterable<Relationship> cfgRels = cfgPath.relationships();
            ArrayList<Relationship> cfgGraph = startCFGGraph.get(n1);
            ArrayList<Relationship> endCFGRels = endCFGGraph.get(n2);


            if ((cfgGraph == null)) {
                return null;
            }

            if ((endCFGRels == null)) {
                return null;
            }

            // add relaitonships associated with n1
            for (Relationship endCFGRel : endCFGRels) {
                cfgGraph.add(endCFGRel);
            }

            // add connecting cfg nodes and relationships connecting n1 to n2
            if (cfgRels != null) {
                for (Relationship cfgRel : cfgRels) {
                    cfgGraph.add(cfgRel);
                }
            }

            return cfgGraph;
        }

    }

    // return varWrite and/or parWrite, retWrite edges directly connecting to end Node
    public Iterable<Relationship> getPrevRels(Node end, boolean isPrefix) {
        if (isPrefix) {
            return end.getRelationships(Direction.INCOMING, RelTypes.varWrite);
        } else {
            return end.getRelationships(Direction.INCOMING, RelTypes.varWrite, RelTypes.parWrite, RelTypes.retWrite);
        }
    }

    // merge two paths together
    public Path backwardcombine(@Name("first") Path first, @Name("second") Path second) {
        if (first == null) return second;
        if (second == null) return first;

        if (!first.endNode().equals(second.startNode()))
            throw new IllegalArgumentException("Paths don't connect on their end and start-nodes "+first+ " with "+second);

        PathImpl.Builder builder = new PathImpl.Builder(first.startNode());
        for (Relationship rel : first.relationships()) builder = builder.push(rel);
        for (Relationship rel : second.relationships()) builder = builder.push(rel);
        return builder.build();
    }

    // create a new path with start as the first node, and joined by the edges in listRels
    public Path backwardbuildPath(Node start, ArrayList<Relationship> listRels) {

        if (start == null) {
            return null;
        }

        PathImpl.Builder builder = new PathImpl.Builder(start);
        if (listRels == null) {
            return builder.build();
        }

        for (int i = listRels.size() - 1; i >= 0; i--) {
            builder = builder.push(listRels.get(i));
        }

        return builder.build();

    }

    // return start and end CFG nodes along with the connections
    public HashMap<Node, ArrayList<Relationship>> backwardgetConnectionNodes(Relationship r, boolean first) {

        //HashMap<Node, ArrayList<Relationship>> srcCFGConns = new HashMap<>();
        HashMap<Node, ArrayList<Relationship>> allCFGConns = new HashMap<>();


        if (r.isType(RelTypes.varWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwDestination);

            // get common nodes and store in vr1CFGConns
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    if (srcCFGNode.equals(dstCFGNode)) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        allCFGConns.put(srcCFGNode, cfgConnRels);
                    }
                }
            }
        } else if (r.isType(RelTypes.parWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        RelTypes.nextCFGBlock);
                HashMap<Node, Relationship> invokeCFGRel = new HashMap<>();

                // filter out the invoke CFG nodes
                for (Relationship nextCFGRel : nextCFGRels) {
                    boolean hasProp=nextCFGRel.hasProperty("cfgInvoke");
                    if ((hasProp) && (nextCFGRel.getProperty("cfgInvoke").equals("1"))) {
                        invokeCFGRel.put(nextCFGRel.getEndNode(), nextCFGRel);
                    }
                }

                // filter out nodes
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    Relationship invokeCFGConn = invokeCFGRel.get(dstCFGNode);
                    if (invokeCFGConn != null) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        cfgConnRels.add(invokeCFGConn);

                        // determine node to return as key
                        if (first) {
                            allCFGConns.put(dstCFGNode, cfgConnRels);
                        } else {
                            allCFGConns.put(srcCFGNode, cfgConnRels);
                        }
                    }
                }


            }

        } else if (r.isType(RelTypes.retWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        RelTypes.nextCFGBlock);
                HashMap<Node, Relationship> invokeCFGRel = new HashMap<>();

                // filter out the invoke CFG nodes
                for (Relationship nextCFGRel : nextCFGRels) {
                    boolean hasProp = nextCFGRel.hasProperty("cfgReturn");
                    if ((hasProp) && (nextCFGRel.getProperty("cfgReturn").equals("1"))) {
                        invokeCFGRel.put(nextCFGRel.getEndNode(), nextCFGRel);
                    }
                }

                // filter out nodes
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    Relationship invokeCFGConn = invokeCFGRel.get(dstCFGNode);
                    if (invokeCFGConn != null) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        cfgConnRels.add(invokeCFGConn);

                        // determine node to return as key
                        if (first) {
                            allCFGConns.put(dstCFGNode, cfgConnRels);
                        } else {
                            allCFGConns.put(srcCFGNode, cfgConnRels);
                        }
                    }
                }
            }
        }

        return allCFGConns;

    }

    private PathExpander<Double> buildPathExpander(String relationshipsAndDirections) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections
                .parse(relationshipsAndDirections)) {
            if (pair.first() == null) {
                if (pair.other() == null) {
                    builder = PathExpanderBuilder.allTypesAndDirections();
                } else {
                    builder = PathExpanderBuilder.allTypes(pair.other());
                }
            } else {
                if (pair.other() == null) {
                    builder = builder.add(pair.first());
                } else {
                    builder = builder.add(pair.first(), pair.other());
                }
            }
        }
        return builder.build();
    }

}
