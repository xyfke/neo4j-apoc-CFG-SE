package apoc.path;

import apoc.algo.CFGShortestPath;
import apoc.util.Util;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class CFGPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserFunction
    @Description("apoc.path.cfgValidatedPath(start, [settings])")
    public List<Path> cfgValidatedPath(@Name("start") Object start, @Name("config") Map<String,Object> config) {

        // configuration graph variables
        Node startNode = null;
        Relationship startEdge = null;
        Node endNode = (Node) config.getOrDefault("endNode", null);
        Relationship endEdge = (Relationship) config.getOrDefault("endEdge", null);

        // process starting points
        if (start == null) {
            return null;
        } else if (start instanceof Node) {
            startNode = (Node) start;
        } else if (start instanceof Relationship) {
            startEdge = (Relationship) start;
        } else {
            return null;
        }

        // other settings configuration
        boolean cfgCheck = Util.toBoolean(config.getOrDefault("cfgCheck", true));
        String relSequence = (String) config.getOrDefault("relSequence", null);
        boolean repeat = Util.toBoolean(config.getOrDefault("repeat", false));
        boolean allShortestPath = Util.toBoolean(config.getOrDefault("allShortestPath", false));
        List<Map<String, Object>> cfgConfigurationList =
                (List<Map<String,Object>>) config.getOrDefault("cfgConfiguration", null);
        HashMap<String, CFGSetting> cfgConfig = parseCFGConfiguration(cfgConfigurationList);
        RelExtension extension = new RelExtension(relSequence, repeat);
        HashSet<Label> acceptedNodes = filterNodes((String) config.getOrDefault("nodeFilter", null));

        return findPath(startNode, endNode, startEdge, endEdge, cfgConfig, extension, allShortestPath, cfgCheck,
                acceptedNodes);

    }

    // helper function: find path
    public List<Path> findPath(Node startNode, Node endNode, Relationship startEdge, Relationship endEdge,
                               HashMap<String, CFGSetting> cfgConfig, RelExtension extension,
                               boolean allShortestPath, boolean cfgCheck, HashSet<Label> acceptedNodes) {

        List<BasicCandidatePath> returnPaths = new ArrayList<>();
        HashSet<Relationship> visitedEdges = new HashSet<>();
        Queue<BasicCandidatePath> queuePath = new LinkedList<>();
        Node start = startNode;
        Node end = endNode;
        ArrayList<ArrayList<RelationshipType>> curType;
        Iterable<Relationship> nextRels;
        BasicCandidatePath curPath;

        if (startEdge != null) {
            start = startEdge.getEndNode();
            curPath = new BasicCandidatePath(startEdge, -1);
            if (cfgCheck) {updateFirstCFGNodes(curPath, cfgConfig);}
            queuePath.add(curPath);
            if (allShortestPath) {visitedEdges.add(startEdge);}
        }

        if (endEdge != null) {
            end = endEdge.getStartNode();
        }

        if (start == null) {
            return null;
        }


        // We only have the starting node
        if (startEdge == null) {

            // use relationship sequence 0
            curType = extension.constructTypes(0);
            int y = 0;

            // For each possible sequences, find next possible candidates
            for (ArrayList<RelationshipType> curT : curType) {
                nextRels = start.getRelationships(Direction.OUTGOING,
                        curT.toArray(RelationshipType[]::new));
                for (Relationship nextRel : nextRels) {
                    if ((acceptedNodes != null) &&
                            (!acceptedNodes.contains(nextRel.getEndNode().getLabels().iterator().next()))) {
                        continue;
                    }

                    // only create path if we are looking for all path or it is not in visited edges
                    if ((!allShortestPath) || (!visitedEdges.contains(nextRel))) {
                        curPath = new BasicCandidatePath(nextRel, y);
                        if (cfgCheck) {updateFirstCFGNodes(curPath, cfgConfig);}
                        queuePath.add(curPath);
                    }

                }
                y += 1;
            }


        }

        List<Relationship> cfgPath = null;
        BasicCandidatePath foundCandidatePath = null;
        ArrayList<ArrayList<Relationship>> retCovered = new ArrayList<>();

        while (!queuePath.isEmpty()) {
            curPath = queuePath.remove();

            if ((allShortestPath) && (foundCandidatePath != null)) {
                if ((!curPath.compareRetNodes(foundCandidatePath))) {
                    continue;
                } else {
                    if (retCovered.contains(curPath.retRel)) {
                        continue;
                    }
                }
            }

            // Make sure it passes the CFG test before proceeding to look further
            if ((!cfgCheck) || getCFGPath(curPath, cfgConfig)) {
                if (allShortestPath) {visitedEdges.add(curPath.getLastEdge()); }

                // Only record to valid path if it is consider end index
                if ((extension.isEndIndex(curPath.getPathIndex())) &&
                        ((end == null) || (curPath.getLastEdge().getEndNode().equals(end)))) {
                    if (endEdge != null) {
                        BasicCandidatePath tempPath = new BasicCandidatePath(curPath, endEdge, curPath.pathIndex);
                        if ((!cfgCheck) || getCFGPath(tempPath, cfgConfig)) {
                            returnPaths.add(tempPath);
                            if (allShortestPath) {
                                foundCandidatePath = curPath;
                                retCovered.addAll(tempPath.getRetComp());
                                continue;
                            }
                        }
                    } else {
                        returnPaths.add(curPath);
                        if (allShortestPath) {
                            foundCandidatePath = curPath;
                            retCovered.addAll(curPath.getRetComp());
                            continue;
                        }
                    }
                }

                int index = extension.nextIndex(curPath.getLastEdge().getType(), curPath.getPathIndex());
                if (index <= extension.lastIndex) {
                    curType = extension.constructTypes(index);
                    int i = 0;
                    for (ArrayList<RelationshipType> curT : curType) {
                        nextRels = curPath.getLastEdge().getEndNode().getRelationships(Direction.OUTGOING,
                                curT.toArray(RelationshipType[]::new));
                        for (Relationship nextRel : nextRels) {
                            if ((acceptedNodes != null) &&
                                    (!acceptedNodes.contains(nextRel.getEndNode().getLabels().iterator().next()))) {
                                continue;
                            }

                            boolean addPath = ((!allShortestPath) && (!curPath.getPath().contains(nextRel))) ||
                                    (!visitedEdges.contains(nextRel));

                            if (addPath) {
                                BasicCandidatePath newCandidatePath = new BasicCandidatePath(curPath, nextRel,
                                        index+i);
                                queuePath.add(newCandidatePath);
                            }
                        }
                        i += 1;
                    }

                }

            }

        }

        ArrayList<Path> paths = new ArrayList<>();
        for (BasicCandidatePath path : returnPaths) {
            paths.add(path.buildPath());
        }

        return paths;
    }

    private void updateFirstCFGNodes(BasicCandidatePath path, HashMap<String, CFGSetting> config) {
        HashSet<List<Node>> endCFGs = CFGValidationHelper.getConnectionNodesAll(path.getLastEdge(), config);
        HashSet<Node> endNodes = new HashSet<>();
        for (List<Node> endCFG : endCFGs) {
            endNodes.add(endCFG.get(1));
        }
        path.setValidCFGs(endNodes);
    }

    // helper function: parse CFG settings
    private HashMap<String, CFGSetting> parseCFGConfiguration(List<Map<String, Object>> cfgConfigList) {
        HashMap<String, CFGSetting> cfgConfig = new HashMap<>();

        for (Map<String, Object> cfgConfigItem : cfgConfigList) {
            String relType = (String) cfgConfigItem.getOrDefault("name", null);
            if (relType == null) {continue;} // skip if no name provided

            String startLabel = (String) cfgConfigItem.getOrDefault("startLabel", null);
            String endLabel = (String) cfgConfigItem.getOrDefault("endLabel", null);

            String attribute = (String) cfgConfigItem.getOrDefault("attribute", null);
            String length = (String) cfgConfigItem.getOrDefault("length", 0);
            cfgConfig.put(startLabel + relType + endLabel, new CFGSetting(length, attribute));
        }

        return cfgConfig;

    }

    public boolean getCFGPath(BasicCandidatePath path, HashMap<String, CFGSetting> config) {
        if (path.getPathSize() < 2) {
            return true;
        }

        //Relationship curEdge = path.getSecondLastEdge();
        Relationship nextEdge = path.getLastEdge();

        HashSet<Node> startCFGs = path.getValidCFGs();

        CFGShortestPath shortestPath = new CFGShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));

        HashSet<List<Node>> endCFGs = CFGValidationHelper.getConnectionNodesAll(nextEdge, config);
        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        for (Node startCFG : startCFGs) {
            for (List<Node> endCFG : endCFGs) {
                Node dstNode = endCFG.get(0);
                Path cfgPath = shortestPath.findSinglePath(startCFG, dstNode, nextEdge);
                if (cfgPath != null) {
                    acceptedCFGEnd.add(endCFG.get(1));
                }
            }
        }

        path.setValidCFGs(acceptedCFGEnd);
        return !acceptedCFGEnd.isEmpty();

    }

    // helper function
    public HashSet<Label> filterNodes(String acceptNodesStr) {
        if (acceptNodesStr == null) {
            return null;
        }

        HashSet<Label> acceptedNodes = new HashSet<>();

        for (String acceptedNodeStr : acceptNodesStr.split(",")) {
            Label acceptedNode = Label.label(acceptedNodeStr);
            acceptedNodes.add(acceptedNode);
        }

        return acceptedNodes;
    }

    // helper class: CFG setting private class
    public class CFGSetting {

        private int length;
        private String[] attribute;

        public CFGSetting(String length, String attribute) {
            this.attribute = (attribute == null) ? null : attribute.split(",");

            if (length.equals("*")) {
                this.length = -1;
            } else if (length.equals("+")) {
                this.length = -2;
            } else {
                this.length = Util.toInteger(length);
            }

        }

        public int getLength() {
            return this.length;
        }

        public String[] getAttribute() {
            return this.attribute;
        }

    }



    @UserFunction
    @Description("apoc.path.cfgValidatedPath")
    public List<Path> cfgValidatedPath1(@Name("startNode") Node startNode,
                                       @Name("endNode") Node endNode,
                                       @Name("startEdge") Relationship startEdge,
                                       @Name("endEdge") Relationship endEdge,
                                       @Name("relSequences") String relSquences) {


        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        RelExtension extension = new RelExtension(relSquences, false);
        Queue<CandidatePath> queuePath = new LinkedList<>();
        Node start = startNode;
        Node end = endNode;
        ArrayList<ArrayList<RelationshipType>> curType;
        Iterable<Relationship> nextRels;

        // Assumption: relationship has higher priority than nodes
        if (startEdge != null) {
            visitedEdge.add(startEdge);
            start = startEdge.getEndNode();
            queuePath.add(new CandidatePath(startEdge, -1));
        }
        if (endEdge != null) {
            end = endEdge.getStartNode();
        }

        // must have start and end Node to begin search
        if ((start == null)) {
            return null;
        }

        if (startEdge == null) {
            curType = extension.constructTypes(extension.getFirstType());
            nextRels = start.getRelationships(Direction.OUTGOING,
                    curType.toArray(RelationshipType[]::new));
            for (Relationship nextRel : nextRels) {
                queuePath.add(new CandidatePath(nextRel, 0));
            }
        }

        List<Relationship> cfgPath = null;
        CandidatePath foundCandidatePath = null;
        ArrayList<ArrayList<Relationship>> retCovered = new ArrayList<>();
        CandidatePath curPath = null;

        while (!queuePath.isEmpty()) {

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

            // continue searching only if does not require cfg check or cfg check passes
            if ((getCFGPath2(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                // path index
                if ((curPath.patternIndex == extension.lastIndex) &&
                        ((end == null) || (curPath.getEndNode().equals(end)))) {

                    if (endEdge != null) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge, curPath.patternIndex);
                        if ((getCFGPath2(vifPath))) {
                            // build path, and return (exit)
                            foundCandidatePath = curPath;
                            returnCandidates.add(vifPath);
                            retCovered.addAll(vifPath.getRetComp());
                            continue;
                        }
                    } else {
                        // build path, and return (exit)
                        foundCandidatePath = curPath;
                        returnCandidates.add(curPath);
                        retCovered.addAll(curPath.getRetComp());
                        continue;
                    }
                }

                int index = extension.nextIndex(curPath.getLastRel().getType(), curPath.patternIndex);
                curType = extension.constructTypes(index);
                nextRels = start.getRelationships(Direction.OUTGOING,
                        curType.toArray(RelationshipType[]::new));
                for (Relationship nextRel : nextRels) {
                    if (!visitedEdge.contains(nextRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, nextRel, index);
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
    public boolean getCFGPath2(CandidatePath candidatePath) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship curRel = candidatePath.getSecondLastRel();
        Relationship nextRel = candidatePath.getLastRel();

        // obtain cfg nodes and relationships associated with r1 and r2
        HashSet<Node> startCFGs = candidatePath.validCFGs;
        HashMap<List<Node>, Relationship> endCFGs = CFGValidationHelper.getConnectionNodes(nextRel,
                candidatePath, false, false);

        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        CFGShortestPath shortestPath = new CFGShortestPath(
                new BasicEvaluationContext(tx, db),
                (int) Integer.MAX_VALUE,
                CFGValidationHelper.buildPathExpander("nextCFGBlock>"));

        for (Node srcNode : startCFGs) {
            for (List<Node> endCFG : endCFGs.keySet()) {
                Node dstNode = endCFG.get(0);
                Path cfgPath = shortestPath.findSinglePath(srcNode, dstNode, curRel);
                if (cfgPath != null) {
                    acceptedCFGEnd.add(endCFG.get(1));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGEnd);

        return !acceptedCFGEnd.isEmpty();

    }



}
