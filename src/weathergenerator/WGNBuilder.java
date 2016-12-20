/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package weathergenerator;

import datasetjava.DataTable;
import datasetjava.Query;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import static weathergenerator.WeatherGenerator.getDstn1;
import static weathergenerator.WeatherGenerator.initRainHHMXSmooth;

/**
 *
 * @author Shawn
 */
public class WGNBuilder {

    private static final int[] numDays_leap = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
    private static final int[] numDays_noleap = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
    private static final double defWndDirStd = 6;

    public static class Statistics {

        Double[] data;
        Double[] sortedData;
        int size;
        double noDataValue;

        public Statistics(Double[] data, double noData) {
//            this.data = data;
            int count = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] != noData || (noData < 0 && data[i] > noData)) {
                    count++;
                }
            }

            this.data = new Double[count];
            int index = 0;

            for (int i = 0; i < data.length; i++) {
                if (data[i] != noData || (noData < 0 && data[i] > noData)) {
                    this.data[index] = data[i];
                    index++;
                }
            }
            size = data.length;
        }

        double getMean() {
            double sum = 0.0;
            for (double a : data) {
                sum += a;
            }
            return sum / size;
        }

        double getVariance() {
            double mean = getMean();
            double temp = 0;
            for (double a : data) {
                temp += (a - mean) * (a - mean);
            }
            return temp / size;
        }

        double getStdDev() {
            return Math.sqrt(getVariance());
        }

        public double getMedian() {
            if (sortedData == null) {
                sortedData = new Double[data.length];
                for (int i = 0; i < data.length; i++) {
                    sortedData[i] = data[i];
                }

                Arrays.sort(sortedData);
            }

            if (sortedData.length % 2 == 0) {
                return (sortedData[(sortedData.length / 2) - 1] + sortedData[sortedData.length / 2]) / 2.0;
            }
            return sortedData[sortedData.length / 2];
        }

        public double getSkew() {
            double mean = getMean();
            double median = getMedian();
            double std = this.getStdDev();
            if (std != 0) {
                return 3 * (mean - median) / std;
            } else {
                return 0.0;
            }
        }

        public int getCount(double lower, double upper) {
            int out = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] > lower && data[i] < upper) {
                    out++;
                }
            }
            return out;
        }

        public int getSize() {
            return size;
        }

        public Double[] getData() {
            return data;
        }
    }

    public static Map<GeneratorType, Map<Integer, Double>> generateWGN(String climatePath,
            String tbName,
            double noDataValue,
            double lat,
            double lon,
            String refPath,
            Date startDate,
            Date endDate) {
        Map<GeneratorType, Map<Integer, Double>> out = new HashMap();
        String sql;
        ResultSet rs;

        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            sql = String.format("SELECT DISTINCT Year FROM %s WHERE %s BETWEEN date(\'%s\') AND date(\'%s\')",
                    tbName,
                    WGNDatasetStructure.colDate, df.format(startDate), df.format(endDate));
            rs = Query.getDataTable(sql, climatePath);
            List<Integer> yearList = new ArrayList();

            while (rs.next()) {
                yearList.add(rs.getInt(1));
            }

            sql = String.format("SELECT DISTINCT Month FROM %s WHERE %s BETWEEN date(\'%s\') AND date(\'%s\')",
                    tbName,
                    WGNDatasetStructure.colDate, df.format(startDate), df.format(endDate));
            rs = Query.getDataTable(sql, climatePath);
            List<Integer> monList = new ArrayList();
            while (rs.next()) {
                monList.add(rs.getInt(1));
            }

            Map<InputType, Map<Integer, List<Double>>> inputs = new HashMap();
            for (InputType type : InputType.values()) {
                if (type == InputType.WGN) {
                    continue;
                }
                
                inputs.put(type, new HashMap());

                List<Double> valueList;

                for (int i = 0; i < monList.size(); i++) {
                    sql = String.format("SELECT %s FROM %s WHERE Month = %.1f AND %s BETWEEN date(\'%s\') AND date(\'%s\') ORDER BY %s",
                            type.toString(),
                            tbName,
                            (double) monList.get(i),
                            WGNDatasetStructure.colDate, df.format(startDate), df.format(endDate),
                            WGNDatasetStructure.colRecID);
                    rs = Query.getDataTable(sql, climatePath);
                    valueList = new ArrayList();
                    while (rs.next()) {
                        valueList.add(rs.getDouble(type.toString()));
                    }
                    inputs.get(type).put(monList.get(i), valueList);
                }
            }

            Map<Integer, List<Double>> dewPnt = new HashMap();
            for (int i = 0; i < monList.size(); i++) {
                sql = String.format("SELECT %s, %s FROM %s WHERE Month = %.1f AND TMX > %.2f AND TMN > %.2f AND HMD > %.2f  AND %s BETWEEN date(\'%s\') AND date(\'%s\') ORDER BY %s",
                        String.format("(%s + %s) / 2", InputType.TMX.toString(), InputType.TMN.toString()),
                        InputType.HMD.toString(),
                        tbName,
                        (double) monList.get(i),
                        noDataValue,
                        noDataValue,
                        noDataValue,
                        WGNDatasetStructure.colDate, df.format(startDate), df.format(endDate),
                        WGNDatasetStructure.colRecID);
                rs = Query.getDataTable(sql, climatePath);
                List<Double> valueList = new ArrayList();
                while (rs.next()) {
                    valueList.add(getDewPoint(rs.getDouble(1), rs.getDouble(2) / 100.0));
                }
                dewPnt.put(monList.get(i), valueList);
            }

            Statistics stat;
            double pcpskw, pcpstd, pcpdaymm, pcpmonmm, pcpdays, prw3, prw1, prw2;
            out.put(GeneratorType.PCPSKW, new HashMap());
            out.put(GeneratorType.PCPSTD, new HashMap());
            out.put(GeneratorType.PCPDAYMM, new HashMap());
            out.put(GeneratorType.PCPMONMM, new HashMap());
            out.put(GeneratorType.PCPDAYS, new HashMap());
            out.put(GeneratorType.PR_W1, new HashMap());
            out.put(GeneratorType.PR_W2, new HashMap());
            out.put(GeneratorType.PR_W3, new HashMap());
            for (int mon : inputs.get(InputType.PCP).keySet()) {
                stat = new Statistics(inputs.get(InputType.PCP).get(mon).toArray(new Double[inputs.get(InputType.PCP).get(mon).size()]), noDataValue);

                pcpskw = stat.getSkew();
                pcpstd = stat.getStdDev();
                pcpdaymm = stat.getMean();
                pcpmonmm = pcpdaymm * (numDays_noleap[mon] - numDays_noleap[mon - 1]);
                pcpdays = ((double) stat.getCount(0, Double.MAX_VALUE)) / stat.getSize() * (numDays_noleap[mon] - numDays_noleap[mon - 1]);

                double dryWet = 0, wetWet = 0, totalWet = (double) stat.getCount(0, Double.MAX_VALUE);
                double prePCP = 0;
                for (int i = 0; i < stat.getSize(); i++) {
                    if (stat.getData()[i] > 0) {
                        if (prePCP > 0) {
                            wetWet++;
                        } else if (prePCP == 0) {
                            dryWet++;
                        }
                        prePCP = stat.getData()[i];
                    } else {
                        prePCP = 0;
                    }
                }

                prw1 = dryWet / stat.getSize();
                prw2 = wetWet / stat.getSize();
                prw3 = totalWet / stat.getSize();

                out.get(GeneratorType.PCPSKW).put(mon, pcpskw);
                out.get(GeneratorType.PCPSTD).put(mon, pcpstd);
                out.get(GeneratorType.PCPDAYMM).put(mon, pcpdaymm);
                out.get(GeneratorType.PCPMONMM).put(mon, pcpmonmm);
                out.get(GeneratorType.PCPDAYS).put(mon, pcpdays);
                out.get(GeneratorType.PR_W1).put(mon, prw1);
                out.get(GeneratorType.PR_W2).put(mon, prw2);
                out.get(GeneratorType.PR_W3).put(mon, prw3);
            }

            out.put(GeneratorType.TMPMN, new HashMap());
            out.put(GeneratorType.TMPMX, new HashMap());
            out.put(GeneratorType.TMPSTDMN, new HashMap());
            out.put(GeneratorType.TMPSTDMX, new HashMap());
            for (int mon : inputs.get(InputType.TMN).keySet()) {
                stat = new Statistics(inputs.get(InputType.TMN).get(mon).toArray(new Double[inputs.get(InputType.TMN).get(mon).size()]), noDataValue);
                out.get(GeneratorType.TMPMN).put(mon, stat.getMean());
                out.get(GeneratorType.TMPSTDMN).put(mon, stat.getStdDev());
            }

            for (int mon : inputs.get(InputType.TMX).keySet()) {
                stat = new Statistics(inputs.get(InputType.TMX).get(mon).toArray(new Double[inputs.get(InputType.TMX).get(mon).size()]), noDataValue);
                out.get(GeneratorType.TMPMX).put(mon, stat.getMean());
                out.get(GeneratorType.TMPSTDMX).put(mon, stat.getStdDev());
            }

            out.put(GeneratorType.DEWPT, new HashMap());
            for (int mon : dewPnt.keySet()) {
                stat = new Statistics(dewPnt.get(mon).toArray(new Double[dewPnt.get(mon).size()]), noDataValue);
                out.get(GeneratorType.DEWPT).put(mon, stat.getMean());
            }

            out.put(GeneratorType.SOLARAV, new HashMap());
            for (int mon : inputs.get(InputType.SLR).keySet()) {
                stat = new Statistics(inputs.get(InputType.SLR).get(mon).toArray(new Double[inputs.get(InputType.SLR).get(mon).size()]), noDataValue);
                out.get(GeneratorType.SOLARAV).put(mon, stat.getMean());
            }

            out.put(GeneratorType.WNDAV, new HashMap());
            for (int mon : inputs.get(InputType.WSPD).keySet()) {
                stat = new Statistics(inputs.get(InputType.WSPD).get(mon).toArray(new Double[inputs.get(InputType.WSPD).get(mon).size()]), noDataValue);
                out.get(GeneratorType.WNDAV).put(mon, stat.getMean());
            }

            out.put(GeneratorType.WNDDIR, new HashMap());
            out.put(GeneratorType.WNDDIRSTD, new HashMap());
            for (int mon : inputs.get(InputType.WDIR).keySet()) {
                stat = new Statistics(inputs.get(InputType.WDIR).get(mon).toArray(new Double[inputs.get(InputType.WDIR).get(mon).size()]), noDataValue);
                out.get(GeneratorType.WNDDIR).put(mon, stat.getMean());
                double wndDirStd = stat.getStdDev();
                if (wndDirStd == 0) {
                    wndDirStd = defWndDirStd;
                }
                out.get(GeneratorType.WNDDIRSTD).put(mon, wndDirStd);
            }

            out.put(GeneratorType.RAINHHMX, new HashMap());
            Map<Integer, Double> refRAINHHMX = getRefRAINHHMX(lat, lon, refPath);
            for (int mon : out.get(GeneratorType.PCPDAYMM).keySet()) {
                out.get(GeneratorType.RAINHHMX).put(mon, refRAINHHMX.get(mon) * out.get(GeneratorType.PCPDAYMM).get(mon) / out.get(GeneratorType.PR_W3).get(mon));
            }

            initRainHHMXSmooth(out);

            initPCF_AMPR(out, yearList.size());

        } catch (SQLException ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return out;
    }

    private static void initPCF_AMPR(Map<GeneratorType, Map<Integer, Double>> wgn, int sumYears) {
        Random rand = new Random();
        wgn.put(GeneratorType.PCF, new HashMap());
        wgn.put(GeneratorType.AMP_R, new HashMap());

        for (int mon = 1; mon < 13; mon++) {
            double r6, xlv, pcp, sum;
            double rn1, rn2;
            rn1 = rand.nextDouble();
            sum = 0;
            r6 = wgn.get(GeneratorType.PCPSKW).get(mon) / 6.0;
            int count = 1000;
            for (int j = 0; j < count; j++) {
                rn2 = rand.nextDouble();
                xlv = (getDstn1(rn1, rn2) - r6) * r6 + 1;
                rn1 = rn2;
                xlv = (Math.pow(xlv, 3) - 1) * 2.0 / wgn.get(GeneratorType.PCPSKW).get(mon);
                pcp = xlv * wgn.get(GeneratorType.PCPSTD).get(mon) + wgn.get(GeneratorType.PCPDAYMM).get(mon);
                if (pcp < 0.01) {
                    pcp = 0.01;
                }
                sum += pcp;
            }

            if (sum > 0) {
                wgn.get(GeneratorType.PCF).put(mon, count * wgn.get(GeneratorType.PCPDAYMM).get(mon) / sum);
            } else {
                wgn.get(GeneratorType.PCF).put(mon, 1.0);
            }

            double x1, x2, x3;
            x1 = 0.5 / sumYears;
            x2 = x1 / wgn.get(GeneratorType.PCPDAYS).get(mon);
            x3 = wgn.get(GeneratorType.RAINHHMXSmooth).get(mon) / Math.log(x2);

            if (wgn.get(GeneratorType.PCPDAYMM).get(mon) > 1.e-4) {
                wgn.get(GeneratorType.AMP_R).put(mon, 1.0 * (1 - Math.exp(x3 / wgn.get(GeneratorType.PCPDAYMM).get(mon))));
            } else {
                wgn.get(GeneratorType.AMP_R).put(mon, 0.95);
            }

            if (wgn.get(GeneratorType.AMP_R).get(mon) > 0.95) {
                wgn.get(GeneratorType.AMP_R).put(mon, 0.95);
            }
            if (wgn.get(GeneratorType.AMP_R).get(mon) < 0.1) {
                wgn.get(GeneratorType.AMP_R).put(mon, 0.1);
            }
        }
    }

    private static double getDewPoint(double tmpAvg, double hmd) {
        return tmpAvg - (100 - hmd * 100) / 5;
        // reference from: https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
    }

    public static void exportWGN2SQLite(Map<GeneratorType, Map<Integer, Double>> wgn, String tableName, String outPath) {
        DataTable table = new DataTable(tableName);
        table.addField(WGNDatasetStructure.colMonth, DataTable.fieldType.Integer);
        for (GeneratorType type : wgn.keySet()) {
            table.addField(type.toString(), DataTable.fieldType.Double);
        }

        for (int i = 1; i <= 12; i++) {
            table.addRecord();
            table.getField(WGNDatasetStructure.colMonth).set(table.getRecordCount() - 1, i);
            for (GeneratorType type : wgn.keySet()) {
                table.getField(type.toString()).set(table.getRecordCount() - 1, wgn.get(type).get(i));
            }
        }

        table.exportSQLite(outPath);
    }

    public static double getDistance(double origLat, double origLon, double destLat, double destLon) {
        double value = Math.pow(Math.sin((origLat - destLat) * Math.PI / 180 / 2), 2) + Math.cos(origLat * Math.PI / 180) * Math.cos(destLat * Math.PI / 180) * Math.pow(Math.sin((origLon - destLon) * Math.PI / 180 / 2), 2);
        value = Math.sqrt(value);
        return 3956 * 2 * Math.asin(value);
    }

    public static Map<Integer, Double> getRefRAINHHMX(double lat, double lon, String refPath) {
        Map<Integer, Double> out = new HashMap();
        try {
            String sql = String.format("SELECT "
                    + "%s, "
                    + "3956 * 2 * ASIN(SQRT(POWER(SIN((%s - (%.2f)) * pi()/180 / 2), 2) + COS(%s * pi()/180) * COS((%.2f) * pi()/180) * POWER(SIN((%s - (%.2f)) * pi()/180 / 2), 2))) AS %s "
                    + "FROM %s ORDER BY %s LIMIT 1",
                    WGNDatasetStructure.colID,
                    WGNDatasetStructure.colLatitude,
                    lat,
                    WGNDatasetStructure.colLatitude,
                    lat,
                    WGNDatasetStructure.colLongitude,
                    lon,
                    WGNDatasetStructure.colDistance,
                    WGNDatasetStructure.tblRefStations,
                    WGNDatasetStructure.colDistance);
            ResultSet rs = Query.getDataTable(sql, refPath);

            int stationID = 0;

            while (rs.next()) {
                stationID = rs.getInt(1);
            }

            if (stationID > 0) {
                sql = String.format("SELECT "
                        + "%s, %s FROM %s WHERE %s = %d ORDER BY %s",
                        WGNDatasetStructure.colMonth,
                        WGNDatasetStructure.colA05,
                        WGNDatasetStructure.tblRefA05,
                        WGNDatasetStructure.colStationID, stationID,
                        WGNDatasetStructure.colMonth);

                rs = Query.getDataTable(sql, refPath);

                while (rs.next()) {
                    out.put(rs.getInt(1), rs.getDouble(2));
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return out;
    }
}
