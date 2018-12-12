package com.inf.infinity.database;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class WebServer extends BaseConnection {
    //region FIELDS AND REFERENCES

    private URL url = null;
    private HttpURLConnection dataConnection = null;

    private int useTrans = 0;
    private String dataKey;

    private String tmpString, batchString;
    private boolean isPrepare = false, isBatch = false, isSimple;

    private int batchItem = 0;

    //endregion

    //region PROPERTIES

    public boolean GetIsPrepare() { return isPrepare; }
    public void SetIsPrepare(boolean value) { isPrepare = value; }

    public boolean GetIsBatch() { return isBatch; }
    public void SetIsBatch(boolean value) { isBatch = value; }

    //endregion

    //region METHODS

    public void SetKey(String key) {
        dataKey = key;
    }

    private String getPostDataString(HashMap<String, String> params) throws UnsupportedEncodingException {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for(Map.Entry<String, String> entry : params.entrySet()){
            if (first)
                first = false;
            else
                result.append("&");

            result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
        }

        return result.toString();
    }

    private String getParamString(Object[] paramValue) {
        String paramString = "";
        if (paramValue == null) return "";

        for (Object x : paramValue)
        {
            String objString = (x == null ? "" : x.toString());

            objString = objString.replace("|", "");
            objString = objString.replace("~", "");
            objString = objString.replace("`", "");
            objString = objString.replace("<>", "");

            paramString += objString + "|";
        }

        if (paramString != "") paramString = paramString.substring(0, paramString.length() - 1);
        return paramString;
    }

    // region CONNECTION

    @Override
    public boolean Connect(String connectionString) {
        try {
            url = new URL(connectionString);
        }
        catch(MalformedURLException error) {
            return false;
        }

        return true;
    }

    @Override
    public boolean Connect(String serverPath, String databaseName, String userName, String userPassword, String serverPort, boolean authenticationType) {
        return Connect(serverPath);
    }

    @Override
    public boolean Disconnect() {
        return true;
    }

    //endregion

    //region COMMAND

    @Override
    public String Command(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("UseTrans", String.valueOf(useTrans));
            map.put("Type", "Command");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            return sb.toString();
        }
        catch (IOException error) {
            return "-1";
        }
        finally {
            dataConnection.disconnect();
        }
    }

    @Override
    public String CommandWithID(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("UseTrans", String.valueOf(useTrans));
            map.put("Type", "CommandID");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            return sb.toString();
        }
        catch (IOException error) {
            return "-1";
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    //region BATCH COMMAND

    public void PrepareCommand() {
        tmpString = "";
        batchString = "";
        isPrepare = true;
    }

    public void SingleCommand(String sqlString, boolean isSP, Object[] paramValue) {
        tmpString += sqlString + "~" + String.valueOf(isSP ? 1 : 0) + "~" + getParamString(paramValue) + "<>";
    }

    public void SingleCommand(String moduleName, String sqlName, Object[] paramValue) {
        String sql = GetField(moduleName, sqlName);
        SingleCommand(sql, false, paramValue);
    }

    public void PrepareBatch(String moduleName, String sqlName) {
        batchString = GetField(moduleName, sqlName) + "~" + String.valueOf(0) + "~";
        isBatch = true;
        batchItem = 0;
    }

    public void InsertBatch(Object[] paramValue) {
        if (paramValue != null)
        {
            batchString += getParamString(paramValue) + "`";
            batchItem++;
        }
    }

    public void FinishBatch() {
        if (batchString != "")
        {
            batchString = batchString.substring(0, batchString.length() - 1);
            if (batchItem > 0) tmpString += batchString + "<>";
        }

        isBatch = false;
    }

    public String PerformCommand(boolean useID) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", "");
            map.put("IsSP", "0");
            map.put("Type", "BatchCommand" + (useID ? "ID" : ""));
            map.put("UseTrans", String.valueOf(useTrans));
            map.put("Key", dataKey);
            map.put("Data", tmpString.substring(0, tmpString.length() - 2));

            isPrepare = false;

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            return sb.toString();
        }
        catch (IOException error) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    //region SELECT SCALAR

    @Override
    public Object SelectScalar(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Scalar");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            if (dataConnection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = null;

                if ("gzip".equals(dataConnection.getContentEncoding())) {
                    reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
                }
                else {
                    reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
                }

                StringBuilder sb = new StringBuilder();
                String line = null;

                // Read Server Response
                while((line = reader.readLine()) != null)
                {
                    // Append server response in string
                    sb.append(line);
                }

                return sb.toString();
            }
            else return "GO ERROR";
        }
        catch (IOException error) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    //region SELECT ARRAY

    @Override
    public DataTable SelectArray(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Array");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            if (dataConnection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = null;

                if ("gzip".equals(dataConnection.getContentEncoding())) {
                    reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
                }
                else {
                    reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
                }

                StringBuilder sb = new StringBuilder();
                String line = null;

                // Read Server Response
                while((line = reader.readLine()) != null)
                {
                    // Append server response in string
                    sb.append(line);
                }

                String responseText = sb.toString();

                JSONArray obj = new JSONArray(responseText);

                DataTable dt = new DataTable(obj, true);
                return dt;
            }
            else {
                return null;
            }
        }
        catch (IOException error) {
            return null;
        } catch (JSONException e) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    public String SelectArrayTest(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Array");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            String addon = dataConnection.getContentEncoding();
            reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            String responseText = sb.toString();

            return addon + "|" + responseText;
        }
        catch (IOException error) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    public static String decompress(byte[] compressed) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();
        gis.close();
        bis.close();

        return sb.toString();
    }

    //endregion

    //region SELECT ROW

    @Override
    public Object[] SelectRow(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Row");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            String responseText = sb.toString();
            JSONArray obj = new JSONArray(responseText);
            Object[] dataRow = new Object[obj.length()];

            for (int x = 0 ; x < obj.length(); x++) {
                dataRow[x] = obj.isNull(x) ? null : obj.get(x);
            }

            return dataRow;
        }
        catch (IOException error) {
            return null;
        }
        catch (JSONException e) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    // region SELECT ROW MAP

    @Override
    public HashMap<String, Object> SelectRowMap(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Dictionary");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            String responseText = sb.toString();

            HashMap<String, Object> hash = new HashMap<>();
            JSONObject obj = new JSONObject(responseText);

            Iterator<?> keys = obj.keys();
            while(keys.hasNext()) {
                String key = (String)keys.next();

                hash.put(key, obj.isNull(key) ? null : obj.get(key));
            }

            return hash;
        }
        catch (IOException error) {
            return null;
        }
        catch (JSONException e) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    // region SELECT ROW DATA

    @Override
    public DataRow SelectRowData(String sqlCommand, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Dictionary");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            String responseText = sb.toString();

            DataRow dr = new DataRow();
            JSONObject obj = new JSONObject(responseText);

            Iterator<?> keys = obj.keys();
            while(keys.hasNext()) {
                String key = (String)keys.next();

                dr.put(key, obj.isNull(key) ? null : obj.get(key));
            }

            return dr;
        }
        catch (IOException error) {
            return null;
        }
        catch (JSONException e) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    //region SELECT COLUMN

    @Override
    public Object[] SelectColumn(String sqlCommand, int colIndex, boolean isSP, Object[] paramValue) {
        try {
            dataConnection = (HttpURLConnection)url.openConnection();
            dataConnection.setRequestMethod("POST");
            dataConnection.setRequestProperty("Accept-Encoding", "gzip");
            dataConnection.setDoOutput(true);

            HashMap<String, String> map = new HashMap<>();
            map.put("SQL", sqlCommand);
            map.put("IsSP", String.valueOf(isSP ? 1 : 0));
            map.put("Type", "Column");
            map.put("Key", dataKey);

            if (paramValue != null) {
                for (int x = 0 ; x < paramValue.length ; x++) {
                    map.put("p" + String.valueOf(x), paramValue[x] == null ? "" : paramValue[x].toString());
                }
            }

            OutputStream os = dataConnection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostDataString(map));
            writer.flush();
            writer.close();
            os.close();

            BufferedReader reader = null;

            if ("gzip".equals(dataConnection.getContentEncoding())) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(dataConnection.getInputStream())));
            }
            else {
                reader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
            }

            StringBuilder sb = new StringBuilder();
            String line = null;

            // Read Server Response
            while((line = reader.readLine()) != null)
            {
                // Append server response in string
                sb.append(line);
            }

            String responseText = sb.toString();
            JSONArray obj = new JSONArray(responseText);
            Object[] dataRow = new Object[obj.length()];

            for (int x = 0 ; x < obj.length(); x++) {
                dataRow[x] = obj.isNull(x) ? null : obj.get(x);
            }

            return dataRow;
        }
        catch (IOException error) {
            return null;
        }
        catch (JSONException e) {
            return null;
        }
        finally {
            dataConnection.disconnect();
        }
    }

    //endregion

    //region TRANSACTION

    @Override
    public void BeginTrans() {
        useTrans = 1;
    }

    @Override
    public void CommitTrans() {
        useTrans = 0;
    }

    @Override
    public void RollBackTrans() {
        useTrans = 0;
    }

    //endregion

    //endregion
}
