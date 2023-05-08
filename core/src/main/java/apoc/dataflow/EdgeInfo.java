package apoc.dataflow;

import apoc.path.CandidatePath;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import javax.management.relation.Relation;
import java.util.ArrayList;

public class EdgeInfo {

    private EdgeInfo prevEdge;
    private Relationship curRel;
    private ArrayList<Node> cfgNodes;
    protected int pathLength;
    private ArrayList<Long> retWrites;

    public EdgeInfo(Relationship current, EdgeInfo previous) {
        this.curRel = current;
        this.prevEdge = previous;
        this.pathLength = (previous == null) ? 1 : previous.getPathLength() + 1;
        this.retWrites = (previous == null) ? new ArrayList<>() : new ArrayList<Long>(previous.getRetWrites());
        if (current.isType(DataflowHelper.RelTypes.retWrite)) {
            this.retWrites.add(current.getId());
        }
    }

    public ArrayList<Node> getCfgNodes() {
        return this.cfgNodes;
    }

    public int getPathLength() {
        return this.pathLength;
    }

    public void updateCfgNodes(ArrayList<Node> cfgNodes) {
        this.cfgNodes = cfgNodes;
    }

    public Relationship getCurRel() {
        return this.curRel;
    }

    public ArrayList<Node> getPrevRelCFG() {
        return (this.prevEdge == null) ? null : this.prevEdge.getCfgNodes();
    }

    public EdgeInfo getPrevEdge() {
        return this.prevEdge;
    }

    public ArrayList<Long> getRetWrites() {
        return this.retWrites;
    }

    public boolean compareRetNodes(EdgeInfo edgeInfo2) {

        if ((!edgeInfo2.getRetWrites().isEmpty()) && (!this.retWrites.isEmpty())) {
            return this.retWrites.get(0).equals(edgeInfo2.getRetWrites().get(0));
        } else {
            return false;
        }

    }

    public ArrayList<ArrayList<Long>> getRetComp() {

        ArrayList<ArrayList<Long>> visitedComps = new ArrayList<>();
        ArrayList<Long> comp = new ArrayList<>();

        for (Long retEdge : this.retWrites) {
            comp = new ArrayList<>(comp);
            comp.add(retEdge);
            visitedComps.add(comp);
        }

        return visitedComps;

    }
}
