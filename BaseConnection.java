package com.inf.infinity.database;

import java.io.InputStream;
import java.util.HashMap;

public abstract class BaseConnection {
    //region FIELDS AND REFERENCES

    public static DataSQL SQL = null;

    //endregion

    //region METHODS

    public void CreateSQL(InputStream stream) {
        if (SQL == null) {
            SQL = new DataSQL(stream);
        }
        else SQL.ParseXML(stream);
    }
    public String GetField(String sqlModule, String sqlName) {
        return SQL.GetSQL(sqlModule, sqlName);
    }

    //region CONNECTION

    public abstract boolean Connect(String connectionString);
    public abstract boolean Connect(String serverPath, String databaseName, String userName, String userPassword, String serverPort, boolean authenticationType);
    public abstract boolean Disconnect();

    //endregion

    //region COMMAND

    public abstract String Command(String sqlCommand, boolean isSP, Object[] paramValue);
    public String Command(String sqlModule, String sqlName, Object[] paramValue) {
        return Command(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }
    public String Command(String sqlModule, String sqlName, String[] paramString, Object[] paramValue) {
        return Command(String.format(SQL.GetSQL(sqlModule, sqlName), paramString), false, paramValue);
    }

    public abstract String CommandWithID(String sqlCommand, boolean isSP, Object[] paramValue);
    public String CommandWithID(String sqlModule, String sqlName, Object[] paramValue) {
        return CommandWithID(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT SCALAR

    public abstract Object SelectScalar(String sqlCommand, boolean isSP, Object[] paramValue);
    public Object SelectScalar(String sqlModule, String sqlName, Object[] paramValue) {
        return SelectScalar(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT ARRAY

    public abstract DataTable SelectArray(String sqlCommand, boolean isSP, Object[] paramValue);
    public DataTable SelectArray(String sqlModule, String sqlName, Object[] paramValue) {
        return SelectArray(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT ROW

    public abstract Object[] SelectRow(String sqlCommand, boolean isSP, Object[] paramValue);
    public Object[] SelectRow(String sqlModule, String sqlName, Object[] paramValue) {
        return SelectRow(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT ROW MAP

    public abstract HashMap<String, Object> SelectRowMap(String sqlCommand, boolean isSP, Object[] paramValue);
    public HashMap<String, Object> SelectRowMap(String sqlModule, String sqlName, Object[] paramValue) {
        return SelectRowMap(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT ROW DATA

    public abstract DataRow SelectRowData(String sqlCommand, boolean isSP, Object[] paramValue);
    public DataRow SelectRowData(String sqlModule, String sqlName, Object[] paramValue) {
        return SelectRowData(SQL.GetSQL(sqlModule, sqlName), false, paramValue);
    }

    //endregion

    //region SELECT COLUMN

    public abstract Object[] SelectColumn(String sqlCommand, int colIndex, boolean isSP, Object[] paramValue);
    public Object[] SelectColumn(String sqlModule, String sqlName, int colIndex, Object[] paramValue) {
        return SelectColumn(SQL.GetSQL(sqlModule, sqlName), colIndex, false, paramValue);
    }

    //endregion

    //region TRANSACTION

    public abstract void BeginTrans();
    public abstract void CommitTrans();
    public abstract void RollBackTrans();

    //endregion

    //endregion

    //endregion
}
