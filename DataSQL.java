package com.inf.infinity.database;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DataSQL {
    //region FIELDS AND REFERENCES

    private List<SQLRow> sqlData = new ArrayList<SQLRow>();

    //endregion

    //region CONSTRUCTOR

    public DataSQL() {}

    public DataSQL(InputStream stream) {
        ParseXML(stream);
    }

    //endregion

    //region METHODS

    public void ParseXML(InputStream input) {
        XmlPullParserFactory xmlFactoryObject;
        try {
            xmlFactoryObject = XmlPullParserFactory.newInstance();
            XmlPullParser myparser = xmlFactoryObject.newPullParser();
            myparser.setInput(input, null);

            int event = myparser.getEventType();
            String sqlModule = "";
            String sqlName = "";
            String sqlString = "";

            while (event != XmlPullParser.END_DOCUMENT)
            {
                String tagname = myparser.getName();
                switch (event){
                    case XmlPullParser.START_TAG:
                        if(tagname.equals("Database")){
                            sqlModule = myparser.getAttributeValue(null, "Module");
                        }
                        else if (tagname.equals("SQL")) {
                            sqlName = myparser.getAttributeValue(null, "Name");
                        }

                        break;
                    case XmlPullParser.TEXT:
                        sqlString = myparser.getText();
                        break;
                    case XmlPullParser.END_TAG:
                        if (tagname.equals("SQL")) {
                            SQLRow sqlRow = new SQLRow();
                            sqlRow.SQLModule = sqlModule;
                            sqlRow.SQLName = sqlName;
                            sqlRow.SQLString = sqlString;

                            sqlData.add(sqlRow);
                        }

                        break;
                }
                event = myparser.next();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void Clear() {
        sqlData.clear();
    }

    public String GetSQL(String sqlModule, String sqlName) {
        for (int x = 0 ; x < sqlData.size(); x++) {
            SQLRow sqlRow = sqlData.get(x);

            if (sqlRow.SQLModule.equalsIgnoreCase(sqlModule) && sqlRow.SQLName.equalsIgnoreCase(sqlName)) {
                return sqlRow.SQLString;
            }
        }

        return "";
    }

    //endregion

    //region CLASSES

    private class SQLRow {
        String SQLModule;
        String SQLName;
        String SQLString;
    }

    //endregion
}
