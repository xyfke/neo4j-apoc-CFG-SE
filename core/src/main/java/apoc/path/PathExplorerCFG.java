package apoc.path;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;

public class PathExplorerCFG {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    public static Paths pathFunc = new Paths();

    @UserFunction("apoc.path.prefixDataflow")
    @Description("apoc.path.prefixDataflow(startEdge, endNode, cfgCheck, returnAll) - find prefix dataflow path made out of varWrites, parWrites, and retWrites")
    public Path prefixDataflow(@Name("startRel") Relationship startRel,
                                @Name("endNode") Node endNode,
                               @Name("cfgCheck") boolean cfgCheck) {


        if ((startRel == null) || (endNode == null)) {
            return null;
        } else {
            CFGEvaluator evaluator = new CFGEvaluator(startRel, endNode, cfgCheck);
            TraversalDescription td = cfgTraversalDescription();
            td = td.evaluator(evaluator);
            Traverser traverser = td.traverse(startRel.getEndNode());
            Iterator<Path> pdfs = traverser.iterator();

            if (pdfs.hasNext()) {
                return pdfs.next();
            } else {
                return null;
            }

        }

    }

    @UserFunction("apoc.path.intraDataflow")
    @Description("apoc.path.intraDataflow(startNode, endNode, cfgCheck, returnAll) - find intra-component dataflow path made out of varWrites, parWrites, and retWrites ")
    public Path intraDataflow(@Name("startNode") Node startNode,
                                             @Name("endNode") Node endNode,
                                             @Name("cfgCheck") boolean cfgCheck) {

        if ((startNode == null) || (endNode == null)) {
            return null;
        } else if (startNode == endNode) {
            return pathFunc.create(startNode, new ArrayList<Relationship>());
        } else {
            CFGEvaluator evaluator = new CFGEvaluator(startNode, endNode, cfgCheck);
            TraversalDescription td = cfgTraversalDescription();
            td = td.evaluator(evaluator);
            Traverser traverser = td.traverse(startNode);
            Iterator<Path> dfs = traverser.iterator();
            return dfs.next();
        }

    }

    @UserFunction("apoc.path.suffixDataflow")
    @Description("apoc.path.suffixDataflow(startNode, endEdge, cfgCheck) - find suffix dataflow path made out of varWrites, parWrites, and retWrites")
    public Path suffixDataflow(@Name("startNode") Node startNode,
                                          @Name("endRel") Relationship endRel,
                                          @Name("cfgCheck") boolean cfgCheck) {

        if ((startNode == null) || (endRel == null)) {
            return null;
        } else if (startNode == endRel) {
            return pathFunc.create(startNode, new ArrayList<Relationship>());
        } else {
            CFGEvaluator evaluator = new CFGEvaluator(startNode, endRel, cfgCheck);
            TraversalDescription td = cfgTraversalDescription();
            td = td.evaluator(evaluator);
            Traverser traverser = td.traverse(startNode);
            Iterator<Path> sdfs = traverser.iterator();
            return sdfs.next();
        }

    }

    private TraversalDescription cfgTraversalDescription() {
        RelationshipCFGExpander expander = new RelationshipCFGExpander();
        TraversalDescription td = tx.traversalDescription().breadthFirst()
                .expand(expander).uniqueness(Uniqueness.RELATIONSHIP_PATH);
        return td;
    }

    private ArrayList<Path> getListofPath(Iterator<Path> dataflowPaths, Relationship rel,
                                          CFGEvaluator.DataflowType dataflowType) {

        ArrayList<Path> finalPaths = new ArrayList<>();
        while (dataflowPaths.hasNext()) {
            Path tempPath = dataflowPaths.next();
            finalPaths.add(combinePath(tempPath, rel, dataflowType));
        }

        return finalPaths;

    }

    private Path combinePath(Path dataflowPath, Relationship rel, CFGEvaluator.DataflowType dataflowType) {

        if (dataflowPath == null) {
            return null;
        }

        PathImpl.Builder builder = null;
        int i = 0;

        if (dataflowType == CFGEvaluator.DataflowType.MIDDLE) {
            return dataflowPath;
        }

        if (dataflowType == CFGEvaluator.DataflowType.PREFIX) {
            builder = new PathImpl.Builder(rel.getStartNode());
            builder = builder.push(rel);
        }

        for (Relationship dataflowRel : dataflowPath.relationships()) {
            if ((i == 0) && (builder == null)) {
                builder = new PathImpl.Builder(dataflowRel.getStartNode());
            }
            builder = builder.push(dataflowRel);
        }

        if (dataflowType == CFGEvaluator.DataflowType.SUFFIX) {
            builder = builder.push(rel);
        }

        return builder.build();

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
