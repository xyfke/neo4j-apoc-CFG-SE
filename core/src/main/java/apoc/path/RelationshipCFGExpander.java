package apoc.path;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.procedure.Context;



public class RelationshipCFGExpander implements PathExpander {


    protected Direction direction;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    public RelationshipCFGExpander(){
        this.direction = Direction.OUTGOING;
    }

    @Override
    public Iterable<Relationship> expand(final Path path, BranchState state) {

        final Node lastNode = path.endNode();

        return lastNode.getRelationships(this.direction, RelType.varWrite,
                RelType.parWrite, RelType.retWrite);

    }

    @Override
    public RelationshipCFGExpander reverse() {
        this.direction = this.direction.reverse();
        return this;
    }


    // enum indicating where dataflow path belongs
    public enum DataflowType {
        PREFIX, MIDDLE, SUFFIX
    }

    // enum for constructing path
    private enum RelType implements RelationshipType {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock
    }

}
