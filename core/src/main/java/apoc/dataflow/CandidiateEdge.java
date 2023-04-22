package apoc.dataflow;

import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class CandidiateEdge {

    Relationship curRel;
    Relationship prevRel;

    public CandidiateEdge(Relationship current, Relationship previous) {
        this.curRel = current;
        this.prevRel = previous;
    }

}
