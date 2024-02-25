package org.stjimmy.parser;

import com.google.api.services.bigquery.model.TableRow;

public class RowParserHelper {

    public static int getIntValue(TableRow row, String column) {
        Object value = row.get(column);
        if (value == null) {
            return 0;
        } else {
            Integer val = Integer.parseInt(value.toString());
            return val;
        }
    }

    public static long getLongValue(TableRow row, String column) {
        Object value = row.get(column);
        if (value == null) {
            return 0;
        } else {
            Long val = Long.parseLong(value.toString());
            return val;
        }
    }

    public static Double getDoubleValue(TableRow row, String column) {
        Object value = row.get(column);
        if (value == null) {
            return 0.0;
        } else {
            Double val = Double.parseDouble(value.toString());
            return val;
        }
    }


    public static String getStringValue(TableRow row, String column, boolean isNullable ) throws Exception {
        Object value = row.get(column);
        if (value == null) {
            if (isNullable) {
                return null;
            } else {
                throw new Exception("Not nullable value is NULL");
            }

        } else {
            return value.toString();
        }
    }
}
