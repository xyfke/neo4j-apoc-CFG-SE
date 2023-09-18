package apoc.dataflow;

import apoc.Pools;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.TerminationGuard;

import java.util.List;
import java.util.concurrent.Callable;

public class NonROSFindPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Pools pool;

    @Context
    public TerminationGuard terminationGuard;

    class DataflowCallable implements Callable<List<Path>> {

        private Relationship startEdge = null;
        private Relationship endEdge = null;
        private Node endNode = null;
        private Node startNode = null;
        private boolean cfgCheck = false;
        private Relationship pubVar = null;
        private Relationship pubTarget = null;
        private DataflowHelper.DataflowType category = null;

        public DataflowCallable(final Relationship startEdge, final Relationship endEdge,
                                final boolean cfgCheck) {

            if (startEdge.isType(DataflowHelper.RelTypes.pubTarget) &&
                    endEdge.isType(DataflowHelper.RelTypes.pubVar)) {
                // MIDDLE
                this.category = DataflowHelper.DataflowType.MIDDLE;
                this.startNode = startEdge.getEndNode();
                this.endNode = endEdge.getStartNode();
                this.pubVar = endEdge;
                this.pubTarget = startEdge;
            } else if (startEdge.isType(DataflowHelper.RelTypes.varWrite) &&
                    endEdge.isType(DataflowHelper.RelTypes.pubVar)) {
                // PREFIX
                this.category = DataflowHelper.DataflowType.PREFIX;
                this.startEdge = startEdge;
                this.endNode = endEdge.getStartNode();
                this.pubVar = endEdge;
            } else if (startEdge.isType(DataflowHelper.RelTypes.pubTarget) &&
                    (endEdge.isType(DataflowHelper.RelTypes.varInfFunc) ||
                            endEdge.isType(DataflowHelper.RelTypes.varInfluence) )) {
                // SUFFIX
                this.category = DataflowHelper.DataflowType.SUFFIX;
                this.startNode = startEdge.getEndNode();
                this.pubTarget = startEdge;
                this.endEdge = endEdge;
            }
        }

        @Override
        public List<Path> call() throws Exception {
            terminationGuard.check();
            return null;
            //return rosAllShortestMulti(this.startNode, this.endNode, this.startEdge, this.endEdge,
                    //this.pubVar, this.pubTarget, this.category, this.cfgCheck);
        }
    }

}
