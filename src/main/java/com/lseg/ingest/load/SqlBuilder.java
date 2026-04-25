package com.lseg.ingest.load;

import com.lseg.ingest.plan.Target;

import java.util.List;
import java.util.stream.Collectors;

public class SqlBuilder {

    public static String upsert(Target target, List<TargetSchema.Column> cols) {
        String colList = cols.stream().map(TargetSchema.Column::dbColumn).collect(Collectors.joining(", "));
        String params = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        // Update every non-key column. The natural-key uniq index ensures the row matches.
        String onDup = cols.stream()
                .filter(c -> !c.dbColumn().equals(target.permIdColumn))
                .map(c -> c.dbColumn() + "=VALUES(" + c.dbColumn() + ")")
                .collect(Collectors.joining(", "));
        
        // Ensure row is un-deleted if re-introduced
        onDup += ", is_deleted=0";

        return "INSERT INTO " + target.table + " (" + colList + ") VALUES (" + params + ") ON DUPLICATE KEY UPDATE " + onDup;
    }

    public static String delete(Target target) {
        return "UPDATE " + target.table + " SET is_deleted = 1 WHERE " + target.permIdColumn + " = ?";
    }
}
