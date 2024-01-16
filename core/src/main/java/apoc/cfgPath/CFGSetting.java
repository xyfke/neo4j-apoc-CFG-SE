package apoc.cfgPath;

import apoc.util.Util;

// helper class: CFG setting - describes how source and destination CFG block relates to each other for
//      for a specific relationship
public class CFGSetting {
    private int length;
    private String[] attribute;

    public CFGSetting(String length, String attribute) {
        // must have attribute
        this.attribute = (attribute == null) ? null : attribute.split(",");

        // zero or more nextCFGBlock relation
        if (length.equals("*")) {
            this.length = -1;
            // one or more nextCFGBlock relation
        } else if (length.equals("+")) {
            this.length = -2;
            // default: whatever specified
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
