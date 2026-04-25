package com.lseg.ingest.plan;

public enum Target {
    ORGS("lseg_orgs", "entity_perm_id", "Entity_Perm_ID"),
    ASSETS("lseg_assets", "issue_perm_id", "Issue_Perm_ID"),
    QUOTES("lseg_quotes", "quote_perm_id", "Quote_Perm_ID");

    public final String table;
    public final String permIdColumn;          // db column name
    public final String permIdSourceHeader;    // file header column name

    Target(String table, String permIdColumn, String permIdSourceHeader) {
        this.table = table;
        this.permIdColumn = permIdColumn;
        this.permIdSourceHeader = permIdSourceHeader;
    }
}
