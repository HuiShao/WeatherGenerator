/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package weathergenerator;

import datasetjava.DataTable;
import datasetjava.Query;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shawn
 */
public class IMWEBsHydroClimate {

    private final String hydroClimateDir;
    private final double maxDistance;
    private final double maxElevation;
    private final boolean enableStationSearch;

    public IMWEBsHydroClimate(String path, double maxDist, double maxElev, boolean enableStationSearch) {
        hydroClimateDir = path;
        this.maxDistance = maxDist;
        this.maxElevation = maxElev;
        this.enableStationSearch = enableStationSearch;
    }

    Map<Integer, Map<InputType, List<String>>> otherStationsList;

    private double getClimateFromOtherStation(int stationID, InputType type, Date date, double noDataValue) {
        double out = noDataValue;
        if (otherStationsList == null) {
            otherStationsList = new HashMap();
        }
        try {
            String sql;
            ResultSet rs;

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            List<String> otherStations = new ArrayList();

            if (otherStationsList.containsKey(stationID) && otherStationsList.get(stationID).containsKey(type)) {
                otherStations = otherStationsList.get(stationID).get(type);
            } else {
                sql = String.format("SELECT %s, %s, %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                        WGNDatasetStructure.colLat, WGNDatasetStructure.colLong, WGNDatasetStructure.colElevation,
                        WGNDatasetStructure.tblStations,
                        WGNDatasetStructure.colID, (double) stationID,
                        WGNDatasetStructure.colType, type.toStationType());
                rs = Query.getDataTable(sql, hydroClimateDir);
                double lat = noDataValue, lon = noDataValue, elev = noDataValue;
                if (rs.next()) {
                    lat = rs.getDouble(1);
                    lon = rs.getDouble(2);
                    elev = rs.getDouble(3);
                } else {
                    return noDataValue;
                }

                sql = String.format("SELECT "
                        + "%s, "
                        + "3956 * 2 * ASIN(SQRT(POWER(SIN((%s - (%.2f)) * pi()/180 / 2), 2) + COS(%s * pi()/180) * COS((%.2f) * pi()/180) * POWER(SIN((%s - (%.2f)) * pi()/180 / 2), 2))) AS %s, "
                        + "%s "
                        + "FROM %s "
                        + "WHERE %s = \"%s\" AND %s <> %.1f AND date(\'%s\') BETWEEN %s AND %s "
                        + "ORDER BY %s",
                        WGNDatasetStructure.colTablename,
                        WGNDatasetStructure.colLat,
                        lat,
                        WGNDatasetStructure.colLat,
                        lat,
                        WGNDatasetStructure.colLong,
                        lon,
                        WGNDatasetStructure.colDistance,
                        WGNDatasetStructure.colElevation,
                        WGNDatasetStructure.tblStations,
                        WGNDatasetStructure.colType, type.toStationType(),
                        WGNDatasetStructure.colID, (double) stationID,
                        df.format(date), WGNDatasetStructure.colStartdate, WGNDatasetStructure.colEnddate,
                        WGNDatasetStructure.colDistance);
                rs = Query.getDataTable(sql, hydroClimateDir);
                while (rs.next()) {
                    String tlbName = rs.getString(1);
                    double dist = rs.getDouble(2);
                    double elevDiff = Math.abs(rs.getDouble(3) - elev);
                    if (dist <= this.maxDistance && elevDiff <= this.maxElevation) {
                        otherStations.add(tlbName);
                    }
                }
                if (otherStationsList.get(stationID) == null) {
                    otherStationsList.put(stationID, new HashMap());
                }
                otherStationsList.get(stationID).put(type, otherStations);
            }

            if (!otherStations.isEmpty()) {

                List<Double> values = this.getOtherStationValues(otherStations, df.format(date));

                for (int j = 0; j < values.size(); j++) {
                    double value = values.get(j);
                    if (value > noDataValue) {
                        out = value;
                        break;
                    }
                }

//                sql = String.format("SELECT %s FROM %s WHERE %s",
//                        this.getValueString(otherStations),
//                        this.getTableNameString(otherStations),
//                        this.getDateString(otherStations, df.format(date)));
//
//                rs = Query.getDataTable(sql, hydroClimateDir);
//                while (rs.next()) {
//                    for (int j = 1; j <= otherStations.size(); j++) {
//                        double value = rs.getDouble(j);
//                        if (value > noDataValue) {
//                            out = value;
//                            break;
//                        }
//                    }
//
//                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return out;
    }

    private Map<String, Double> searchedValue;

    private List<Double> getOtherStationValues(List<String> otherStations, String dateS) throws SQLException {
        if (searchedValue == null) {
            searchedValue = new HashMap();
        }
        List<Double> out = new ArrayList();
        String key;

        for (int i = 0; i < otherStations.size(); i++) {
            key = otherStations.get(i) + dateS;
            if (searchedValue.containsKey(key)) {
                out.add(searchedValue.get(key));
            } else {
                String sql = String.format("SELECT %s FROM %s WHERE %s BETWEEN date(\'%s\') AND date(\'%s\')",
                        WGNDatasetStructure.colValue,
                        otherStations.get(i),
                        WGNDatasetStructure.colDate, dateS, dateS);

                ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
                while (rs.next()) {
                    searchedValue.put(key, rs.getDouble(1));
                    break;
                }
                out.add(searchedValue.get(key));
            }
        }
        return out;
    }

    private String getValueString(List<String> otherStations) {
        String out = "";
        for (int i = 0; i < otherStations.size(); i++) {
            if (i < otherStations.size() - 1) {
                out += String.format(" %s.%s,", otherStations.get(i), WGNDatasetStructure.colValue);
            } else {
                out += String.format(" %s.%s ", otherStations.get(i), WGNDatasetStructure.colValue);
            }
        }

        return out;
    }

    private String getTableNameString(List<String> otherStations) {
        String out = "";
        for (int i = 0; i < otherStations.size(); i++) {
            if (i < otherStations.size() - 1) {
                out += String.format(" %s INNER JOIN ", otherStations.get(i));
            } else {
                out += String.format(" %s ", otherStations.get(i));
            }
        }

        return out;
    }

    private String getDateString(List<String> otherStations, String dateS) {
        String out = "";
        for (int i = 0; i < otherStations.size(); i++) {
            if (i < otherStations.size() - 1) {
                out += String.format(" %s.%s BETWEEN date(\'%s\') AND date(\'%s\') AND ", otherStations.get(i), WGNDatasetStructure.colDate, dateS, dateS);
            } else {
                out += String.format(" %s.%s BETWEEN date(\'%s\') AND date(\'%s\') ", otherStations.get(i), WGNDatasetStructure.colDate, dateS, dateS);
            }
        }

        return out;
    }

    private class DateWGN {

        private final Date[] startEndDates;
        private final WeatherGenerator wgn;

        public DateWGN(Date[] startEndDates, WeatherGenerator wgn) {
            this.wgn = wgn;
            this.startEndDates = startEndDates;
        }

        public boolean isWithinDate(Date date) {
            int test = (int) (date.getTime() / 1000 / 86400);
            int start = (int) (startEndDates[0].getTime() / 1000 / 86400);
            int end = (int) (startEndDates[1].getTime() / 1000 / 86400);

            if (test <= end && test >= start) {
                return true;
            } else {
                return false;
            }
        }

        public WeatherGenerator getWGN() {
            return wgn;
        }
    }

    private Map<Integer, List<DateWGN>> wgnList;

    public WeatherGenerator getWGN(int stationID, Date date) {
        if (wgnList == null) {
            wgnList = new HashMap();
        }

        boolean flag = false;

        if (wgnList.containsKey(stationID)) {
            for (int i = 0; i < wgnList.get(stationID).size(); i++) {
                if (wgnList.get(stationID).get(i).isWithinDate(date)) {
                    return wgnList.get(stationID).get(i).getWGN();
                }
            }
        }

        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String sql = String.format("SELECT %s, %s, %s, %s, %s, %s FROM %s WHERE %s = %.1f AND %s = \"%s\" AND date(\'%s\') BETWEEN %s AND %s",
                    WGNDatasetStructure.colLat,
                    WGNDatasetStructure.colLong,
                    WGNDatasetStructure.colElevation,
                    WGNDatasetStructure.colStartdate,
                    WGNDatasetStructure.colEnddate,
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.WGN.toString(),
                    df.format(date), WGNDatasetStructure.colStartdate, WGNDatasetStructure.colEnddate);
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            double lat = 0, lon = 0, elev = 0;
            int startYr = 0, endYr = 0;
            Date[] startEndDate = new Date[2];
            String tableName = "";
            while (rs.next()) {
                lat = rs.getDouble(1);
                lon = rs.getDouble(2);
                elev = rs.getDouble(3);
                startEndDate[0] = df.parse(rs.getString(4));
                startEndDate[1] = df.parse(rs.getString(5));
                startYr = Double.valueOf(rs.getString(4).split("-")[0]).intValue();
                endYr = Double.valueOf(rs.getString(5).split("-")[0]).intValue();
                tableName = rs.getString(6);
            }
            WeatherGenerator wgn = new WeatherGenerator();
            wgn.readWGN(elev, lat, lon, endYr - startYr + 1, hydroClimateDir, tableName);

            if (!wgnList.containsKey(stationID)) {
                wgnList.put(stationID, new ArrayList());
            }

            wgnList.get(stationID).add(new DateWGN(startEndDate, wgn));
        } catch (SQLException ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (int i = 0; i < wgnList.get(stationID).size(); i++) {
            if (wgnList.get(stationID).get(i).isWithinDate(date)) {
                return wgnList.get(stationID).get(i).getWGN();
            }
        }

        return new WeatherGenerator();
    }

    public void fixPCP(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.PCP.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String tableName = "";
            while (rs.next()) {
                tableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    tableName,
                    WGNDatasetStructure.colUID);
            DataTable pcpTable = DataTable.importSQLiteTable(hydroClimateDir, sql, tableName);

            double value;
            Date date;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < pcpTable.getRecordCount(); i++) {
                value = Double.valueOf(pcpTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    boolean isPrePCP = false;
                    if (i > 0) {
                        isPrePCP = Double.valueOf(pcpTable.getField(WGNDatasetStructure.colValue).get(i - 1).toString()) > 0;
                    }
                    date = df.parse(pcpTable.getField(WGNDatasetStructure.colDate).get(i).toString());

                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.PCP, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        fixValue = getWGN(stationID, date).generatePCP(date.getMonth() + 1, isPrePCP);
                    }

                    pcpTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                pcpTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixTMN(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.TMN.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String tmpTableName = "";
            while (rs.next()) {
                tmpTableName = rs.getString(1);
            }

            sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.PCP.toStationType());
            rs = Query.getDataTable(sql, hydroClimateDir);
            String pcpTableName = "";
            while (rs.next()) {
                pcpTableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    tmpTableName,
                    WGNDatasetStructure.colUID);
            DataTable tmpTable = DataTable.importSQLiteTable(hydroClimateDir, sql, tmpTableName);

            double value;
            Date date;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < tmpTable.getRecordCount(); i++) {
                value = Double.valueOf(tmpTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    boolean isPCP = false;
                    sql = String.format("SELECT %s FROM %s WHERE %s = \"%s\"",
                            WGNDatasetStructure.colValue,
                            pcpTableName,
                            WGNDatasetStructure.colDate, tmpTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    rs = Query.getDataTable(sql, hydroClimateDir);
                    while (rs.next()) {
                        isPCP = rs.getDouble(1) > 0;
                    }

                    date = df.parse(tmpTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.TMN, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        fixValue = getWGN(stationID, date).generateTMP(date.getMonth() + 1, isPCP)[1];
                    }

                    tmpTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                tmpTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixTMX(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, "TMax");
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String tmpTableName = "";
            while (rs.next()) {
                tmpTableName = rs.getString(1);
            }

            sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.PCP.toStationType());
            rs = Query.getDataTable(sql, hydroClimateDir);
            String pcpTableName = "";
            while (rs.next()) {
                pcpTableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    tmpTableName,
                    WGNDatasetStructure.colUID);
            DataTable tmpTable = DataTable.importSQLiteTable(hydroClimateDir, sql, tmpTableName);

            double value;
            Date date;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < tmpTable.getRecordCount(); i++) {
                value = Double.valueOf(tmpTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    boolean isPCP = false;
                    sql = String.format("SELECT %s FROM %s WHERE %s = \"%s\"",
                            WGNDatasetStructure.colValue,
                            pcpTableName,
                            WGNDatasetStructure.colDate, tmpTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    rs = Query.getDataTable(sql, hydroClimateDir);
                    while (rs.next()) {
                        isPCP = rs.getDouble(1) > 0;
                    }

                    date = df.parse(tmpTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.TMX, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        fixValue = getWGN(stationID, date).generateTMP(date.getMonth() + 1, isPCP)[0];
                    }

                    tmpTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                tmpTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixSR(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.SLR.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String slrTableName = "";
            while (rs.next()) {
                slrTableName = rs.getString(1);
            }

            sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.PCP.toStationType());
            rs = Query.getDataTable(sql, hydroClimateDir);
            String pcpTableName = "";
            while (rs.next()) {
                pcpTableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    slrTableName,
                    WGNDatasetStructure.colUID);
            DataTable slrTable = DataTable.importSQLiteTable(hydroClimateDir, sql, slrTableName);

            double value;
            Date date;
            Calendar cal;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < slrTable.getRecordCount(); i++) {
                value = Double.valueOf(slrTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    boolean isPCP = false;
                    sql = String.format("SELECT %s FROM %s WHERE %s = \"%s\"",
                            WGNDatasetStructure.colValue,
                            pcpTableName,
                            WGNDatasetStructure.colDate, slrTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    rs = Query.getDataTable(sql, hydroClimateDir);
                    while (rs.next()) {
                        isPCP = rs.getDouble(1) > 0;
                    }

                    date = df.parse(slrTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    cal = Calendar.getInstance();
                    cal.setTime(date);
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.SLR, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        getWGN(stationID, date).weatgn();
                        getWGN(stationID, date).clgen(isPCP, cal.get(Calendar.DAY_OF_YEAR));
                        fixValue = getWGN(stationID, date).generateSLR(date.getMonth() + 1, isPCP);
                    }

                    slrTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                slrTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixRM(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.HMD.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String rmTableName = "";
            while (rs.next()) {
                rmTableName = rs.getString(1);
            }

            sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.PCP.toStationType());
            rs = Query.getDataTable(sql, hydroClimateDir);
            String pcpTableName = "";
            while (rs.next()) {
                pcpTableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    rmTableName,
                    WGNDatasetStructure.colUID);
            DataTable rmTable = DataTable.importSQLiteTable(hydroClimateDir, sql, rmTableName);

            double value;
            Date date;
            Calendar cal;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < rmTable.getRecordCount(); i++) {
                value = Double.valueOf(rmTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    boolean isPCP = false;
                    sql = String.format("SELECT %s FROM %s WHERE %s = \"%s\"",
                            WGNDatasetStructure.colValue,
                            pcpTableName,
                            WGNDatasetStructure.colDate, rmTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    rs = Query.getDataTable(sql, hydroClimateDir);
                    while (rs.next()) {
                        isPCP = rs.getDouble(1) > 0;
                    }

                    date = df.parse(rmTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    cal = Calendar.getInstance();
                    cal.setTime(date);
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.HMD, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        getWGN(stationID, date).weatgn();
                        getWGN(stationID, date).clgen(isPCP, cal.get(Calendar.DAY_OF_YEAR));
                        fixValue = getWGN(stationID, date).generateRH(date.getMonth() + 1, isPCP);
                    }

                    rmTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                rmTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixWD(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.WDIR.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String tableName = "";
            while (rs.next()) {
                tableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    tableName,
                    WGNDatasetStructure.colUID);
            DataTable wdTable = DataTable.importSQLiteTable(hydroClimateDir, sql, tableName);

            double value;
            Date date;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < wdTable.getRecordCount(); i++) {
                value = Double.valueOf(wdTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    date = df.parse(wdTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.WDIR, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        fixValue = getWGN(stationID, date).generateWD(date.getMonth() + 1);
                    }

                    wdTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                wdTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void fixWS(int stationID, double noDataValue) {
        try {
            String sql = String.format("SELECT %s FROM %s WHERE %s = %.1f AND %s = \"%s\"",
                    WGNDatasetStructure.colTablename,
                    WGNDatasetStructure.tblStations,
                    WGNDatasetStructure.colID, (double) stationID,
                    WGNDatasetStructure.colType, InputType.WSPD.toStationType());
            ResultSet rs = Query.getDataTable(sql, hydroClimateDir);
            String tableName = "";
            while (rs.next()) {
                tableName = rs.getString(1);
            }

            sql = String.format("SELECT * FROM %s ORDER BY %s",
                    tableName,
                    WGNDatasetStructure.colUID);
            DataTable wsTable = DataTable.importSQLiteTable(hydroClimateDir, sql, tableName);

            double value;
            Date date;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            boolean hasMissing = false;
            for (int i = 0; i < wsTable.getRecordCount(); i++) {
                value = Double.valueOf(wsTable.getField(WGNDatasetStructure.colValue).get(i).toString());
                if (value <= noDataValue) {
                    date = df.parse(wsTable.getField(WGNDatasetStructure.colDate).get(i).toString());
                    // Find fix value from nearby stations first, if not found, use weather generator
                    double fixValue = !enableStationSearch ? noDataValue : this.getClimateFromOtherStation(stationID, InputType.WSPD, date, noDataValue);
                    if (fixValue <= noDataValue) {
                        fixValue = getWGN(stationID, date).generateWS10(date.getMonth() + 1);
                    }

                    wsTable.getField(WGNDatasetStructure.colValue).set(i, fixValue);
                    hasMissing = true;
                }
            }

            if (hasMissing) {
                Query.CloseConnection(hydroClimateDir);
                wsTable.exportSQLite(hydroClimateDir);
            }
        } catch (Exception ex) {
            Logger.getLogger(IMWEBsHydroClimate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
