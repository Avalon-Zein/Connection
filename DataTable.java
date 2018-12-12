package com.inf.infinity.database;

import com.google.gson.Gson;
import com.inf.infinity.utility.Convert;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DataTable {
    //region FIELDS AND VARIABLES

    private int realCount = 0;
    private int rowCount = 0;

    private List<Object[]> data = new ArrayList<>();
    public ArrayList<String> Columns = new ArrayList<String>();

    //endregion

    // region PROPERTIES

    public int GetRowCount() { return rowCount; }

    //endregion

    //region CONSTRUCTOR

    public DataTable() {}

    public DataTable(ResultSet rs) throws SQLException {
        Columns.clear();

        ResultSetMetaData meta = rs.getMetaData();

        for (int x = 0 ; x < meta.getColumnCount() ; x++) {
            Columns.add(meta.getColumnName(x + 1));
        }

        if (rs.last()) {
            rowCount = rs.getRow();
            rs.beforeFirst();
        }

        if (rowCount > 0) {
            int i = 0;
            while (rs.next()) {
                Object[] row = new Object[Columns.size()];

                for (int j = 0 ; j < Columns.size(); j++) {
                    row[j] = rs.getObject(j + 1);
                }

                data.add(row);
                i++;
            }
        }
    }

    public DataTable(android.database.Cursor cs) throws android.database.SQLException {
        Columns.clear();

        Columns = new ArrayList<>(Arrays.asList(cs.getColumnNames()));
        rowCount = cs.getCount();

        if (rowCount > 0) {
            int i = 0;
            while(cs.moveToNext()) {
                Object[] row = new Object[Columns.size()];

                for (int j = 0 ; j < Columns.size(); j++) {
                    row[j] = cs.getString(j);
                }

                data.add(row);
                i++;
            }
        }
    }

    public DataTable(JSONArray jsonArray, boolean isPublic) throws JSONException {
        Columns.clear();

        if (!isPublic) {
            JSONArray colList = jsonArray.getJSONArray(0);
            for (int x = 0 ; x < colList.length(); x++) {
                Columns.add(colList.getString(x));
            }

            rowCount = jsonArray.length() - 1;

            if (rowCount > 0) {
                for (int x = 1 ; x < jsonArray.length(); x++) {
                    Object[] row = new Object[Columns.size()];

                    JSONArray dataRow = jsonArray.getJSONArray(x);
                    for (int y = 0 ; y < dataRow.length(); y++) {
                        row[y] = dataRow.isNull(y) ? null : dataRow.get(y);
                    }

                    data.add(row);
                }
            }
        }
        else {
            JSONObject rowObj = jsonArray.getJSONObject(0);

            Iterator<?> keys = rowObj.keys();
            while(keys.hasNext()) {
                String key = (String)keys.next();
                Columns.add(key);
            }

            rowCount = jsonArray.length();

            for (int x = 0 ; x < jsonArray.length() ; x++) {
                Object[] row = new Object[Columns.size()];
                JSONObject arr = jsonArray.getJSONObject(x);

                for (int y = 0 ; y < Columns.size() ; y++) {
                    String column = Columns.get(y);
                    row[y] = arr.isNull(column) ? null : arr.get(column);
                }

                data.add(row);
            }
        }
    }

    public DataTable(JSONObject jsonObj) throws JSONException {
        Iterator<?> keys = jsonObj.keys();
        while(keys.hasNext()) {
            String key = (String)keys.next();
            if (key.equals("Count")) realCount = Convert.ToInt(jsonObj.get(key));
            else if (key.equals("Table")) {
                JSONArray arr = jsonObj.getJSONArray(key);

                for (int x = 0 ; x < arr.length(); x++) {
                    JSONArray arr2 = arr.getJSONArray(x);

                    if (x == 0) {
                        for (int y = 0 ; y < arr2.length(); y++) {
                            Columns.add(arr2.getString(y));
                        }
                    }
                    else {
                        Object[] row = new Object[Columns.size()];

                        for (int y = 0 ; y < Columns.size(); y++) {
                            String column = Columns.get(y);

                            row[y] = arr2.isNull(y) ? null : arr2.get(y);
                        }

                        data.add(row);
                    }
                }

                rowCount = arr.length() - 1;
            }
        }
    }

    //endregion

    //region METHODS

    public void AddColumn(String[] arr) {
        Columns.addAll(Arrays.asList(arr));
    }

    public void AddRow(Object[] row) {
        data.add(row);
        rowCount = data.size();
    }

    public Object[][] GetRows() {
        return data.toArray(new Object[data.size()][]);
    }

    public List<HashMap<String, Object>> GetRowsMapData() {
        List<HashMap<String, Object>> mapList = new ArrayList<>();

        for (Object[] n : data) {
            HashMap<String, Object> map = new HashMap<>();
            for (int x = 0 ; x < Columns.size() ; x++) {
                String colName = Columns.get(x);
                map.put(colName, n[Columns.indexOf(colName)]);
            }

            mapList.add(map);
        }

        return mapList;
    }

    public List<DataRow> GetRowsData() {
        List<DataRow> listRow = new ArrayList<>();

        for (Object[] n : data) {
            DataRow dr = new DataRow();

            for (int x = 0 ; x < Columns.size() ; x++) {
                String colName = Columns.get(x);
                dr.put(colName, n[Columns.indexOf(colName)]);
            }

            listRow.add(dr);
        }

        return listRow;
    }

    public Object[] GetRow(int index) {
        return data.get(index);
    }

    public void SetRow(int index, Object[] value) {
        data.set(index, value);
    }

    public Object GetValue(int index, String columnName) {
        return data.get(index)[Columns.indexOf(columnName)];
    }

    public void Clear() {
        data.clear();
        rowCount = 0;
    }

    public void RemoveAt(int index) {
        data.remove(index);
        rowCount = data.size();
    }

    public void SetJSONString(String json, boolean isPublic) {
        try {
            JSONArray jsonArray = new JSONArray(json);

            Columns.clear();
            data.clear();

            if (!isPublic) {
                JSONArray colList = jsonArray.getJSONArray(0);
                for (int x = 0 ; x < colList.length(); x++) {
                    Columns.add(colList.getString(x));
                }

                rowCount = jsonArray.length() - 1;

                if (rowCount > 0) {
                    for (int x = 1 ; x < jsonArray.length(); x++) {
                        Object[] row = new Object[Columns.size()];

                        JSONArray dataRow = jsonArray.getJSONArray(x);
                        for (int y = 0 ; y < dataRow.length(); y++) {
                            row[y] = dataRow.get(y);
                        }

                        data.add(row);
                    }
                }
            }
            else {
                JSONObject rowObj = jsonArray.getJSONObject(0);

                Iterator<?> keys = rowObj.keys();
                while(keys.hasNext()) {
                    String key = (String)keys.next();
                    Columns.add(key);
                }

                rowCount = jsonArray.length();

                for (int x = 0 ; x < jsonArray.length() ; x++) {
                    Object[] row = new Object[Columns.size()];
                    JSONObject arr = jsonArray.getJSONObject(x);

                    for (int y = 0 ; y < Columns.size() ; y++) {
                        row[y] = arr.get(Columns.get(y));
                    }

                    data.add(row);
                }
            }
        }
        catch (JSONException ex) {

        }
    }

    public String GetJSONString() {
        String jsonString = "[]";

        if (rowCount > 0) {
            List<HashMap<String, Object>> map = GetRowsMapData();

            Gson gson = new Gson();
            jsonString = gson.toJson(map);
        }

        return jsonString;
    }

    //endregion
}
