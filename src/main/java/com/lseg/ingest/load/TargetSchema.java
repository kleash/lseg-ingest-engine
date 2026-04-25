package com.lseg.ingest.load;

import com.lseg.ingest.plan.Target;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Describes the DB-side schema for a target table: ordered list of (sourceHeader, dbColumn, sqlType, valueParser).
 * Used to build prepared SQL once per (target, fileHeaderColumns) and to bind row values.
 */
public class TargetSchema {

    public interface ValueBinder {
        void bind(PreparedStatement ps, int idx, String raw) throws SQLException;
    }

    public record Column(String sourceHeader, String dbColumn, int sqlType, ValueBinder binder) {}

    private static final ValueBinder STRING = (ps, i, v) -> {
        if (v == null) ps.setNull(i, Types.VARCHAR);
        else ps.setString(i, v);
    };

    private static final Map<Target, List<Column>> SCHEMAS = new EnumMap<>(Target.class);

    static {
        SCHEMAS.put(Target.ORGS, List.of(
                col("Company_Legal_Domicile", "company_legal_domicile", Types.VARCHAR, STRING),
                col("Company_Short_Name", "company_short_name", Types.VARCHAR, STRING),
                col("Country_of_Incorporation", "country_of_incorporation", Types.VARCHAR, STRING),
                col("Dow_Jones_Industrial_Code", "dow_jones_industrial_code", Types.VARCHAR, STRING),
                col("Entity_ID", "entity_id", Types.VARCHAR, STRING),
                col("Entity_Perm_ID", "entity_perm_id", Types.VARCHAR, STRING),
                col("Finsbury_Company_Code", "finsbury_company_code", Types.VARCHAR, STRING),
                col("GICS_Industry_Code", "gics_industry_code", Types.VARCHAR, STRING),
                col("ICB_Code", "icb_code", Types.VARCHAR, STRING),
                col("ICB_Code_2019", "icb_code_2019", Types.VARCHAR, STRING),
                col("Issuer_LEI", "issuer_lei", Types.VARCHAR, STRING),
                col("Issuer_Name", "issuer_name", Types.VARCHAR, STRING),
                col("Issuer_OrgID", "issuer_orgid", Types.VARCHAR, STRING),
                col("Level", "level", Types.VARCHAR, STRING),
                col("Organization_Sub_Type", "organization_sub_type", Types.VARCHAR, STRING),
                col("Organization_Type", "organization_type", Types.VARCHAR, STRING),
                col("Reuters_Editorial_RIC", "reuters_editorial_ric", Types.VARCHAR, STRING),
                col("SICC_Sector_Code", "sicc_sector_code", Types.VARCHAR, STRING),
                col("Subscription_ID", "subscription_id", Types.VARCHAR, STRING),
                col("TRBC_Code", "trbc_code", Types.VARCHAR, STRING),
                col("Asset_ID", "asset_id", Types.VARCHAR, STRING),
                col("Issue_Perm_ID", "issue_perm_id", Types.VARCHAR, STRING),
                col("Quote_ID", "quote_id", Types.VARCHAR, STRING),
                col("Quote_Perm_ID", "quote_perm_id", Types.VARCHAR, STRING)));

        SCHEMAS.put(Target.ASSETS, List.of(
                col("Asset_ID", "asset_id", Types.VARCHAR, STRING),
                col("Entity_ID", "entity_id", Types.VARCHAR, STRING),
                col("Entity_Perm_ID", "entity_perm_id", Types.VARCHAR, STRING),
                col("Issue_Perm_ID", "issue_perm_id", Types.VARCHAR, STRING),
                col("Level", "level", Types.VARCHAR, STRING),
                col("Quote_ID", "quote_id", Types.VARCHAR, STRING),
                col("Quote_Perm_ID", "quote_perm_id", Types.VARCHAR, STRING),
                col("CUSIP", "cusip", Types.VARCHAR, STRING),
                col("IPO_Listing_Date", "ipo_listing_date", Types.VARCHAR, STRING),
                col("ISIN", "isin", Types.VARCHAR, STRING),
                col("RCS_Code", "rcs_code", Types.VARCHAR, STRING),
                col("Rights_Allocated", "rights_allocated", Types.VARCHAR, STRING),
                col("Security_Long_Description", "security_long_description", Types.VARCHAR, STRING),
                col("Settlement_Type", "settlement_type", Types.VARCHAR, STRING)));

        SCHEMAS.put(Target.QUOTES, List.of(
                col("Asset_ID", "asset_id", Types.VARCHAR, STRING),
                col("Entity_ID", "entity_id", Types.VARCHAR, STRING),
                col("Entity_Perm_ID", "entity_perm_id", Types.VARCHAR, STRING),
                col("Issue_Perm_ID", "issue_perm_id", Types.VARCHAR, STRING),
                col("Level", "level", Types.VARCHAR, STRING),
                col("Quote_ID", "quote_id", Types.VARCHAR, STRING),
                col("Quote_Perm_ID", "quote_perm_id", Types.VARCHAR, STRING),
                col("Asset_Category", "asset_category", Types.VARCHAR, STRING),
                col("Currency_Code", "currency_code", Types.VARCHAR, STRING),
                col("Exchange_Code", "exchange_code", Types.VARCHAR, STRING),
                col("Market_Segment_MIC", "market_segment_mic", Types.VARCHAR, STRING),
                col("RCS_Code", "rcs_code", Types.VARCHAR, STRING),
                col("RIC", "ric", Types.VARCHAR, STRING),
                col("Round_Lot_Size", "round_lot_size", Types.VARCHAR, STRING),
                col("SEDOL", "sedol", Types.VARCHAR, STRING),
                col("Strike_Price", "strike_price", Types.VARCHAR, STRING),
                col("Strike_Price_Multiplier", "strike_price_multiplier", Types.VARCHAR, STRING),
                col("Ticker", "ticker", Types.VARCHAR, STRING),
                col("Trading_Status", "trading_status", Types.VARCHAR, STRING),
                col("Exercise_Begin_Date", "exercise_begin_date", Types.VARCHAR, STRING),
                col("Expiration_Date", "expiration_date", Types.VARCHAR, STRING),
                col("Security_Description", "security_description", Types.VARCHAR, STRING),
                col("Warrant_Issue_Date", "warrant_issue_date", Types.VARCHAR, STRING)));
    }

    private static Column col(String src, String db, int type, ValueBinder b) {
        return new Column(src, db, type, b);
    }

    public static List<Column> columnsFor(Target t) { return SCHEMAS.get(t); }

    public static String schemaSummary(Target t) {
        return SCHEMAS.get(t).stream()
                .map(c -> c.sourceHeader() + " (" + c.dbColumn() + ")")
                .collect(Collectors.joining(", "));
    }

    /** Returns the subset of schema columns whose source-header name is present in the given file header set. */
    public static List<Column> intersect(Target t, Set<String> fileHeaders) {
        List<Column> all = SCHEMAS.get(t);
        List<Column> out = new ArrayList<>(all.size());
        for (Column c : all) if (fileHeaders.contains(c.sourceHeader())) out.add(c);
        return out;
    }
}
