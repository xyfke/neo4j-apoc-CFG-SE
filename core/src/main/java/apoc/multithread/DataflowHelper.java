package apoc.multithread;

//import apoc.path.CFGValidationHelper;
import apoc.path.CFGValidationHelper;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;

public class DataflowHelper {

    @Context
    public static GraphDatabaseService db;

    @Context
    public static Transaction tx;

    public enum RelTypes implements RelationshipType
    {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock, pubVar, pubTarget;
    }

    public enum DataflowType {
        PREFIX, SUFFIX, MIDDLE;
    }

    // helper function: finds outgoing dataflow edge connected to current node
    // return: a list of these outgoing dataflow edge
    public static Iterable<Relationship> getNextRels(Node current) {
        return current.getRelationships(Direction.OUTGOING, RelTypes.varWrite,
                RelTypes.parWrite, RelTypes.retWrite);
    }

}
