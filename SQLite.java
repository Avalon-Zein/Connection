package com.inf.infinity.database;

import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.inf.infinity.App;

import java.util.Arrays;
import java.util.HashMap;

public class SQLite extends BaseConnection {
    //region FIELDS AND REFERENCES

    private SQLiteDatabase dataConnection;

    //endregion

    //region CONNECTION

    @Override
    public boolean Connect(String connectionString) {
        try {
            dataConnection = App.Context.openOrCreateDatabase(connectionString, Context.MODE_PRIVATE, null);
            dataConnection.execSQL("PRAGMA foreign_keys=ON;");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean Connect(String serverPath, String databaseName, String userName, String userPassword, String serverPort, boolean authenticationType) {
        try {
            dataConnection = App.Context.openOrCreateDatabase(databaseName, Context.MODE_PRIVATE, null);
            dataConnection.execSQL("PRAGMA foreign_keys=ON;");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean Disconnect() {
        dataConnection.close();
        return true;
    }

    //endregion

    //region COMMAND

    @Override
    public String Command(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            if (paramValue != null) dataConnection.execSQL(sqlCommand, paramValue);
            else dataConnection.execSQL(sqlCommand);

            return "1";
        } catch (SQLException e) {
            return "-1";
        }
    }

    @Override
    public String CommandWithID(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            if (paramValue != null) dataConnection.execSQL(sqlCommand, paramValue);
            else dataConnection.execSQL(sqlCommand);

            return "1";
        } catch (SQLException e) {
            return "-1";
        }
    }

    public String BatchCommand(String sqlCommand, String splitCommand, boolean isSP, Object[] paramValue) {
        try {
            String[] sqlCol = sqlCommand.split(splitCommand);

            for (String x : sqlCol)
            {
                if (!x.trim().equals("")) {
                    if (paramValue != null) dataConnection.execSQL(x, paramValue);
                    else dataConnection.execSQL(x);
                }
            }

            return "1";
        } catch (SQLException e) {
            return "-1";
        }
    }

    //endregion

    //region SELECT SCALAR

    @Override
    public Object SelectScalar(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            Object scalarObj = null;
            if(c.moveToFirst()) scalarObj = c.getString(0);

            c.close();

            return scalarObj;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ARRAY

    @Override
    public DataTable SelectArray(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            DataTable dt = new DataTable(c);
            c.close();

            return dt;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ROW

    @Override
    public Object[] SelectRow(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            Object[] dataRow = new Object[c.getColumnCount()];

            if(c.moveToFirst())
            {
                for (int x = 0; x < c.getColumnCount() ; x++) {
                    dataRow[x] = c.getString(x);
                }
            }
            else dataRow = null;

            c.close();

            return dataRow;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ROW MAP

    @Override
    public HashMap<String, Object> SelectRowMap(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            HashMap<String, Object> map = new HashMap<>();

            if(c.moveToFirst())
            {
                for (int x = 0; x < c.getColumnCount() ; x++) {
                    map.put(c.getColumnName(x), c.getString(x));
                }
            }

            c.close();

            return map;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ROW DATA

    @Override
    public DataRow SelectRowData(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            DataRow row = new DataRow();

            if(c.moveToFirst())
            {
                for (int x = 0; x < c.getColumnCount() ; x++) {
                    row.put(c.getColumnName(x), c.getString(x));
                }
            }

            c.close();

            return row;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT COLUMN

    @Override
    public Object[] SelectColumn(String sqlCommand, int colIndex, boolean isSP, Object[] paramValue) {
        try {
            Cursor c = null;

            if (paramValue != null) {
                c = dataConnection.rawQuery(sqlCommand, Arrays.copyOf(paramValue, paramValue.length, String[].class));
            }
            else c = dataConnection.rawQuery(sqlCommand, null);

            Object[] dataCol = new Object[c.getCount()];

            int i = 0;
            while(c.moveToNext()) {
                dataCol[i] = c.getString(colIndex);
                i++;
            }

            c.close();

            return dataCol;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region TRANSACTION

    @Override
    public void BeginTrans() {
        dataConnection.beginTransaction();
    }

    @Override
    public void CommitTrans() {
        dataConnection.setTransactionSuccessful();
        dataConnection.endTransaction();
    }

    @Override
    public void RollBackTrans() {
        dataConnection.endTransaction();
    }

    //endregion

}
