package apoc.multithread;

import apoc.dataflow.EdgeInfo;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;

public class DataflowEdge {

    private DataflowEdge prevEdge;
    private Relationship curRel;
    private ArrayList<Long> retWriteIDs;
    private int pathLength;
    private ArrayList<Node> cfgNodes;

    public DataflowEdge(Relationship curRel, DataflowEdge prevEdge) {
        this.curRel = curRel;
        this.prevEdge = prevEdge;
        this.pathLength = (prevEdge == null) ? 1 : prevEdge.getPathLength() + 1;
        this.retWriteIDs = (prevEdge == null) ? new ArrayList<>() :
                new ArrayList<Long>(prevEdge.getRetWriteIDs());
        if (curRel.isType(DataflowHelper.RelTypes.retWrite)) {
            this.retWriteIDs.add(curRel.getId());
        }
    }

    public Relationship getCurRel() {
        return this.curRel;
    }

    public DataflowEdge getPrevEdge() {
        return this.prevEdge;
    }

    public Node getCurRelNode() {
        return this.curRel.getEndNode();
    }

    public int getPathLength() {
        return this.pathLength;
    }

    public ArrayList<Long> getRetWriteIDs() {
        return this.retWriteIDs;
    }


}
