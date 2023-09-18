package apoc.cfgPath;

import org.neo4j.graphdb.*;
import java.util.ArrayList;

public class RelExtension {
    // internal class for keeping track of each relationship sequence
    public class RelationSequence {

        public String relationTypeStr;
        public ArrayList<RelationshipType> relationType = new ArrayList<>();
        public boolean repeat;

        public RelationSequence(String relationTypeStr, boolean multiple) {
            this.relationTypeStr = relationTypeStr;
            this.repeat = multiple;

            String[] orRelations = relationTypeStr.split("\\|");
            for (String orRelation : orRelations) {
                RelationshipType k = RelationshipType.withName(orRelation);
                relationType.add(k);
            }

        }

    }

    public ArrayList<RelationSequence> relSequence = new ArrayList<>();
    public int lastIndex;
    public boolean loopBack;
    public int termIndexStart = -1;
    public int termIndexEnd = -1;

    public RelExtension(String pattern, boolean loopBack) {

        String[] sequenceList = pattern.split(",");
        this.loopBack = loopBack;

        // write, varWrite|parWrite|retWrite*


        for (String sequence : sequenceList) {

            // handle one or more
            if (sequence.endsWith("+")) {
                String stripMultiple = sequence.substring(0, sequence.length()-1);
                this.relSequence.add(new RelationSequence(stripMultiple, false));
                this.relSequence.add(new RelationSequence(stripMultiple, true));
                // handle zero or more
            } else if (sequence.endsWith("*")) {
                String stripMultiple = sequence.substring(0, sequence.length()-1);
                this.relSequence.add(new RelationSequence(stripMultiple, true));
            } else {
                this.relSequence.add(new RelationSequence(sequence, false));
            }
        }

        this.lastIndex = relSequence.size()-1;

        this.termIndexEnd = this.lastIndex;
        this.termIndexStart = this.lastIndex;
        while (this.termIndexStart >= 0) {
            if (relSequence.get(this.termIndexStart).repeat) {
                this.termIndexStart -= 1;
            } else {
                break;
            }
        }

    }

    // construct next possible sequences based on provided index
    public ArrayList<ArrayList<RelationshipType>> constructTypes(int startIndex) {

        // setup returning variable
        ArrayList<ArrayList<RelationshipType>> nextRelationshipType = new ArrayList<>();
        RelationSequence relSeq = relSequence.get(startIndex);

        while (relSeq.repeat) {
            nextRelationshipType.add(relSeq.relationType);
            startIndex += 1;
            if ((startIndex > this.lastIndex) && (this.loopBack)) {
                startIndex = 0;
            }

            if (startIndex > this.lastIndex) {
                break;
            }
            relSeq = relSequence.get(startIndex);
        }

        if (!relSeq.repeat) {
            nextRelationshipType.add(relSeq.relationType);
        }
        return nextRelationshipType;
    }

    public boolean isEndIndex(int curIndex) {
        return (curIndex >= termIndexStart) && (curIndex <= termIndexEnd);
    }

    public int getCurIndex(RelationshipType lastType, int curIndex) {
        // in case we start with a relationship not in the pattern
        if (curIndex == -1) {
            return 0;
        }

        // get current type
        RelationSequence relSeq = relSequence.get(curIndex);

        // check if current sequence contains last type
        // if it doesn't, then it means we moved on to the next sequence
        while (!relSeq.relationType.contains(lastType)) {
            curIndex += 1;
            relSeq = relSequence.get(curIndex);
        }

        return curIndex;

    }

    // determine next index, based on current index and type of edge
    public int nextIndex(RelationshipType lastType, int curIndex) {

        // in case we start with a relationship not in the pattern
        if (curIndex == -1) {
            return 0;
        }

        // get current type
        RelationSequence relSeq = relSequence.get(curIndex);

        // check if current sequence contains last type
        // if it doesn't, then it means we moved on to the next sequence
        while (!relSeq.relationType.contains(lastType)) {
            curIndex += 1;
            relSeq = relSequence.get(curIndex);
        }

        // if we are repeating the current pattern
        if (relSeq.repeat) {
            return curIndex;
        } else {
            if ((this.loopBack) && (curIndex == this.lastIndex)) {
                return 0;
            } else {
                return curIndex + 1;
            }
        }

    }

    public int getFirstType() {
        return (relSequence.isEmpty()) ? -1 : 0;
    }
}
