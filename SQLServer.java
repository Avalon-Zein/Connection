package com.inf.infinity.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public final class SQLServer extends BaseConnection {
    //region FIELDS AND REFERENCES

    private Connection dataConnection;

    //endregion

    //region METHODS

    //region CONNECTION

    @Override
    public boolean Connect(String connectionString) {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
            dataConnection = DriverManager.getConnection("jdbc:jtds:sqlserver://" + connectionString);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean Connect(String serverPath, String databaseName, String userName, String userPassword,
                           String serverPort, boolean authenticationType) {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
            dataConnection = DriverManager.getConnection("jdbc:jtds:sqlserver://" + serverPath + ":" +
                    serverPort + ";databaseName=" + databaseName + ";", userName, userPassword);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean Disconnect() {
        try {
            dataConnection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    //endregion

    //region COMMAND

    @Override
    public String Command(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            int resValue = p.executeUpdate();
            p.close();

            return String.valueOf(resValue);
        }
        catch (SQLException e) {
            return "-1";
        }
    }

    @Override
    public String CommandWithID(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            int resValue = p.executeUpdate();
            p.close();

            return String.valueOf(resValue);
        }
        catch (SQLException e) {
            return "-1";
        }
    }

    //endregion

    //region SELECT SCALAR

    @Override
    public Object SelectScalar(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            Object scalarObj = null;
            ResultSet rs = p.executeQuery();

            if (rs.next()) scalarObj = rs.getObject(1);
            rs.close();
            p.close();

            return scalarObj;
        }
        catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ARRAY

    @Override
    public DataTable SelectArray(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            ResultSet rs = p.executeQuery();
            DataTable dt = new DataTable(rs);

            rs.close();
            p.close();

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
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            ResultSet rs = p.executeQuery();
            int colCount = rs.getMetaData().getColumnCount();
            Object[] dataRow = new Object[colCount];

            if (rs.next()) {
                for (int x = 0 ; x < colCount ; x++) {
                    dataRow[x] = rs.getObject(x + 1);
                }
            }
            else dataRow = null;

            rs.close();
            p.close();

            return dataRow;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT ROW MAP

    @Override
    public HashMap<String,Object> SelectRowMap(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            ResultSet rs = p.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            HashMap<String,Object> map = new HashMap<String,Object>();

            if (rs.next()) {
                for (int x = 0 ; x < colCount ; x++) {
                    map.put(meta.getColumnName(x + 1), rs.getObject(x+1));
                }
            }

            rs.close();
            p.close();

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
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            ResultSet rs = p.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            DataRow dr = new DataRow();

            if (rs.next()) {
                for (int x = 0 ; x < colCount ; x++) {
                    dr.put(meta.getColumnName(x + 1), rs.getObject(x+1));
                }
            }

            rs.close();
            p.close();

            return dr;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region SELECT COLUMN

    @Override
    public Object[] SelectColumn(String sqlCommand, int colIndex, boolean isSP, Object[] paramValue) {
        try {
            NamedParameterStatement p = new NamedParameterStatement(dataConnection, sqlCommand, isSP);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    p.setObject(String.valueOf(x), paramValue[x]);
                }
            }

            ResultSet rs = p.executeQuery();
            int rowCount = 0;

            if (rs.last()) {
                rowCount = rs.getRow();
                rs.beforeFirst();
            }

            Object[] dataCol = new Object[rowCount];

            int i = 0;
            while (rs.next()) {
                dataCol[i] = rs.getObject(colIndex + 1);
                i++;
            }

            rs.close();
            p.close();

            return dataCol;
        } catch (SQLException e) {
            return null;
        }
    }

    //endregion

    //region TRANSACTION

    @Override
    public void BeginTrans() {
        try {
            dataConnection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void CommitTrans() {
        try {
            dataConnection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void RollBackTrans() {
        try {
            dataConnection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //endregion

    //endregion
}
