package com.lseg.ingest.load;

import com.lseg.ingest.plan.Target;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlBuilder {

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE for the given file-intersected column list.
     * Every column in cols is bound. The ON DUPLICATE clause updates every column EXCEPT
     * the table's unique-key columns (those define the row, no point overwriting with the
     * same value), and forces is_deleted=0 to undelete a previously soft-deleted row.
     *
     * Special logic for Target.PRICING:
     * Only keep the record with the most recent trade_date. We use an IF(...) wrapper
     * on every assignment and ensure trade_date is updated LAST so other columns
     * compare against the old value.
     */
    public static String upsert(Target target, List<TargetSchema.Column> cols) {
        Set<String> keyCols = new HashSet<>(target.uniqueKeyColumns);
        boolean isPricing = target == Target.PRICING && cols.stream().anyMatch(c -> "trade_date".equals(c.dbColumn()));

        String colList = cols.stream().map(TargetSchema.Column::dbColumn).collect(Collectors.joining(", "));
        String params = cols.stream().map(c -> "?").collect(Collectors.joining(", "));

        String onDup;
        if (isPricing) {
            List<String> updates = cols.stream()
                    .filter(c -> !keyCols.contains(c.dbColumn()) && !"trade_date".equals(c.dbColumn()))
                    .map(c -> c.dbColumn() + " = IF(VALUES(trade_date) >= trade_date, VALUES(" + c.dbColumn() + "), " + c.dbColumn() + ")")
                    .collect(Collectors.toCollection(ArrayList::new));

            updates.add("is_deleted = IF(VALUES(trade_date) >= trade_date, 0, is_deleted)");
            updates.add("trade_date = IF(VALUES(trade_date) >= trade_date, VALUES(trade_date), trade_date)");
            onDup = String.join(", ", updates);
        } else {
            onDup = cols.stream()
                    .filter(c -> !keyCols.contains(c.dbColumn()))
                    .map(c -> c.dbColumn() + "=VALUES(" + c.dbColumn() + ")")
                    .collect(Collectors.joining(", "));

            if (onDup.isEmpty()) {
                onDup = "is_deleted=0";
            } else {
                onDup += ", is_deleted=0";
            }
        }

        return "INSERT INTO " + target.table + " (" + colList + ") VALUES (" + params + ") ON DUPLICATE KEY UPDATE " + onDup;
    }

    /**
     * Soft-delete UPDATE keyed on the composite unique columns. Bind values in
     * the same order as target.uniqueKeyColumns.
     */
    public static String delete(Target target) {
        String where = target.uniqueKeyColumns.stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(" AND "));
        return "UPDATE " + target.table + " SET is_deleted = 1 WHERE " + where;
    }
}
