package com.inf.infinity.database;

import com.inf.infinity.utility.Convert;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

public class DataRow extends HashMap<String, Object> {
    //region CONSTRUCTOR

    public DataRow() {}

    //endregion

    //region METHODS

    public Object Get(String fieldName) { return get(fieldName); }
    public String GetString(String fieldName) { return Convert.ToString(get(fieldName)); }
    public BigDecimal GetDecimal(String fieldName) { return Convert.ToBigDecimal(get(fieldName)); }
    public int GetInt(String fieldName) { return Convert.ToInt(get(fieldName)); }
    public double GetDouble(String fieldName) { return Convert.ToDouble(get(fieldName)); }
    public long GetLong(String fieldName) { return Convert.ToLong(get(fieldName)); }
    public boolean GetBool(String fieldName) { return Convert.ToBool(get(fieldName)); }
    public Date GetDate(String fieldName) { return Convert.ToDate(get(fieldName), "yyyy-MM-dd HH:mm:ss.SSS"); }
    public String GetDateFormat(String fieldName, String format) {
        return Convert.ToString(Convert.ToDate(get(fieldName),"yyyy-MM-dd HH:mm:ss.SSS"), format);
    }

    public String GetRoundCurrency(String curr, String fieldName) { return String.format(curr + " %,.0f", get(fieldName)); }
    public String GetCurrency(String curr, String fieldName) { return String.format(curr + " %,.2f", get(fieldName)); }

    //endregion
}
