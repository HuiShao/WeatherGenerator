/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package weathergenerator;

import datasetjava.DataTable;
import datasetjava.Query;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shawn
 */
public class WeatherGenerator {

    public WeatherGenerator() {
        initDirectory();
    }

    public void readWGN(double elev, double lat, double longi, int sumYr, String path, String tbName) {
        elevation = elev;
        latitude = lat;
        longitude = longi;
        sumYears = sumYr;
        dsPath = path;
        tableName = tbName;
        rand = new Random();
        for (int i = 0; i < 10; i++) {
            rndNum[i] = getAUNIF(i);
        }
        for (int i = 0; i < 3; i++) {
            wgnold[i] = 0;
        }
        initWGN(path, tbName);
    }

    private String resourcesDirectory;
    private String applicationDirectory;
    private String dsDirectory;

    private void initDirectory() {
        try {
            boolean isDebuging = false;
            // initialize the pathSep and GraphicsDirectory variables
            String pathSep = File.separator;

            applicationDirectory = java.net.URLDecoder.decode(getClass().getProtectionDomain().getCodeSource().getLocation().getPath(), "UTF-8");
            if (applicationDirectory.endsWith(".exe") || applicationDirectory.endsWith(".jar")) {
                applicationDirectory = new File(applicationDirectory).getParent();
            } else {
                // Add the path to the class files
                applicationDirectory += getClass().getName().replace('.', File.separatorChar);

                // Step one level up as we are only interested in the
                // directory containing the class files
                applicationDirectory = new File(applicationDirectory).getParent();
                applicationDirectory = new File(applicationDirectory).getParent();
                applicationDirectory = new File(applicationDirectory).getParent();
                applicationDirectory = new File(applicationDirectory).getParent();

                isDebuging = true;
            }
            resourcesDirectory = applicationDirectory + pathSep + "resources" + pathSep;
            dsDirectory = resourcesDirectory + pathSep + "databases" + pathSep;
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private final String refA05DSName = "Ref_a05.db3";

    public String getRefA05Path() {
        return dsDirectory + refA05DSName;
    }

    private Random rand;
    private final double[] rndNum = new double[10];

    private Map<GeneratorType, Map<Integer, Double>> wgn;

    private static final int[] numDays_leap = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
    private static final int[] numDays_noleap = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};

    public static int getNumDays(int year, int mon) {
        if (isLeapYear(year)) {
            return numDays_leap[mon] - numDays_leap[mon - 1];
        } else {
            return numDays_noleap[mon] - numDays_noleap[mon - 1];
        }
    }

    private static boolean isLeapYear(int year) {
        if (year % 4 == 0 && year % 100 != 0) {
            return true;
        } else if (year % 100 == 0 && year % 400 == 0) {
            return true;
        } else {
            return false;
        }
    }

    private final int[] rndseed = {748932582, 1985072130, 1631331038, 67377721, 366304404, 1094585182, 1767585417, 1980520317, 392682216, 64298628};

    private double elevation;

    private double latitude;

    private double longitude;

    private String dsPath;
    private String tableName;

    private int sumYears;

    private int idist = 0; // initial as 0 for skewed distribution

    private double rexp = 1.3; // Exponent for IDIST=1

    public void setREXP(double value) {
        if (value > 0) {
            rexp = value;
        } else {
            rexp = 1.3;
        }
    }

    /**
     * rainfall distribution code 0 for skewed distribution 1 for mixed
     * exponential distribution
     */
    public int getIDIST() {
        return idist;
    }

    /**
     * rainfall distribution code 0 for skewed distribution 1 for mixed
     * exponential distribution
     *
     * @param value
     */
    public void setIDIST(int value) {
        if (value == 0) {
            idist = 0; // 0 for skewed distribution
        } else {
            idist = 1; // 1 for mixed exponential distribution
        }
    }

    public Map<GeneratorType, Map<Integer, Double>> getWGN() {
        if (wgn == null) {
            initWGN(dsPath, tableName);
        }
        return wgn;
    }

    private void initWGN(String dsPath, String tbName) {
        wgn = new HashMap();
        String sql = String.format("SELECT * FROM %s", tbName);
        try {
            ResultSet rs = Query.getDataTable(sql, dsPath);
            int mon;
            double value;
            List<String> columnNames = null;
            while (rs.next()) {
                if (columnNames == null) {
                    columnNames = Query.getTableColumnNames(rs);
                }
                mon = rs.getInt("Month");
                for (GeneratorType type : GeneratorType.values()) {
                    if (columnNames.contains(type.toString())) {
                        value = rs.getDouble(type.toString());
                        if (!wgn.containsKey(type)) {
                            wgn.put(type, new HashMap());
                        }
                        wgn.get(type).put(mon, value);
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }

        this.initMissingPar();
    }

    public double getLatSin() {
        return Math.sin(latitude / 57.296);
    }

    public double getLatCos() {
        return Math.cos(latitude / 57.296);
    }

    public double getLatTan() {
        return Math.tan(latitude / 57.296);
    }

    public double getDayLengthMin() {
        double x1 = 0.4348 * Math.abs(this.getLatTan());
        double x2 = 0;
        if (x1 < 1) {
            x2 = Math.acos(x1);
        }
        return 7.6394 * x2;
    }

    public double getDormHr() {
        double dl;
        double lat = Math.abs(this.latitude);
        if (lat > 40) {
            dl = 1;
        } else if (lat < 20) {
            dl = -1;
        } else {
            dl = lat / 20.0 - 1.0;
        }

        return dl;
    }

    public static void initRainHHMXSmooth(Map<GeneratorType, Map<Integer, Double>> wgn) {
        if (!wgn.containsKey(GeneratorType.RAINHHMXSmooth)
                && wgn.containsKey(GeneratorType.RAINHHMX)) {
            Map<Integer, Double> values = new HashMap();
            double value = (wgn.get(GeneratorType.RAINHHMX).get(12)
                    + wgn.get(GeneratorType.RAINHHMX).get(1)
                    + wgn.get(GeneratorType.RAINHHMX).get(2)) / 3.0;
            values.put(1, value);

            value = (wgn.get(GeneratorType.RAINHHMX).get(11)
                    + wgn.get(GeneratorType.RAINHHMX).get(12)
                    + wgn.get(GeneratorType.RAINHHMX).get(1)) / 3.0;
            values.put(12, value);

            for (int mon = 2; mon < 12; mon++) {
                value = (wgn.get(GeneratorType.RAINHHMX).get(mon - 1)
                        + wgn.get(GeneratorType.RAINHHMX).get(mon)
                        + wgn.get(GeneratorType.RAINHHMX).get(mon + 1)) / 3.0;
                values.put(mon, value);
            }

            wgn.put(GeneratorType.RAINHHMXSmooth, values);
        }
    }

    private Map<Integer, Integer> irelh;

    private void initMissingPar() {
        this.initRainHHMXSmooth(wgn);

        irelh = new HashMap();

        for (int i = 1; i < 13; i++) {
            if (wgn.get(GeneratorType.DEWPT).get(i) > 1 || wgn.get(GeneratorType.DEWPT).get(i) < 0) {
                irelh.put(i, 0);
            } else {
                irelh.put(i, 1);
            }
        }

        wgn.put(GeneratorType.PCF, new HashMap());
        wgn.put(GeneratorType.PR_W3, new HashMap());
        wgn.put(GeneratorType.PCPDAYMM, new HashMap());
        wgn.put(GeneratorType.AMP_R, new HashMap());

        for (int mon = 1; mon < 13; mon++) {
            int mdays = getNumDays(2001, mon);

            if (this.wgn.get(GeneratorType.PR_W2).get(mon) <= this.wgn.get(GeneratorType.PR_W1).get(mon)
                    || this.wgn.get(GeneratorType.PR_W1).get(mon) <= 0) {
                if (this.wgn.get(GeneratorType.PCPDAYS).get(mon) < 0.1) {
                    wgn.get(GeneratorType.PCPDAYS).put(mon, 0.1);
                }
                wgn.get(GeneratorType.PR_W1).put(mon, 0.75 * wgn.get(GeneratorType.PCPDAYS).get(mon) / mdays);
                wgn.get(GeneratorType.PR_W2).put(mon, 0.25 + wgn.get(GeneratorType.PR_W1).get(mon));
            } else {
                double value = mdays * wgn.get(GeneratorType.PR_W1).get(mon)
                        / (1 - wgn.get(GeneratorType.PR_W2).get(mon) + wgn.get(GeneratorType.PR_W1).get(mon));
                wgn.get(GeneratorType.PCPDAYS).put(mon, value);
            }

            if (wgn.get(GeneratorType.PCPDAYS).get(mon) <= 0) {
                wgn.get(GeneratorType.PCPDAYS).put(mon, 0.001);
            }

            wgn.get(GeneratorType.PR_W3).put(mon, wgn.get(GeneratorType.PCPDAYS).get(mon) / mdays);

            wgn.get(GeneratorType.PCPDAYMM).put(mon, wgn.get(GeneratorType.PCPMONMM).get(mon)
                    / wgn.get(GeneratorType.PCPDAYS).get(mon));

            if (wgn.get(GeneratorType.PCPSKW).get(mon) < 0.2) {
                wgn.get(GeneratorType.PCPSKW).put(mon, 0.2);
            }
        }

        for (int mon = 1; mon < 13; mon++) {
            double r6, xlv, pcp, sum;
            double rn1, rn2;
            rn1 = this.getAUNIF(3);
            sum = 0;
            r6 = wgn.get(GeneratorType.PCPSKW).get(mon) / 6.0;
            int count = 1000;
            for (int j = 0; j < count; j++) {
                rn2 = this.getAUNIF(3);
                xlv = (this.getDstn1(rn1, rn2) - r6) * r6 + 1;
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

    public double getTMPAV(int mon) {
        return (this.getWGN().get(GeneratorType.TMPMX).get(mon)
                + this.getWGN().get(GeneratorType.TMPMN).get(mon)) / 2.0;
    }

    public double getTMPAVAN() {
        double out = 0;
        for (int mon = 1; mon < 13; mon++) {
            out += this.getTMPAV(mon);
        }
        return out / 12.0;
    }

    public double getPCPDaysAN() {
        double out = 0;
        for (int mon = 1; mon < 13; mon++) {
            out += (getWGN().get(GeneratorType.PCPDAYS).get(mon));
        }
        return out;
    }

    private double rcor = -9999;

    public double getRCOR() {
        if (rcor == -9999) {
            rcor = initRCOR();
        }
        return rcor;
    }

    private double initRCOR() {
        double out;
        double sumv = 0;
        for (int i = 0; i < 10000; i++) {
            sumv += Math.pow(-1 * Math.log(this.getAUNIF(3)), rexp);
        }

        if (sumv > 0) {
            out = 10100.0 / sumv;
        } else {
            out = 1.0;
        }
        return out;
    }

    /**
     * this subroutine generates precipitation data when the user chooses to
     * simulate or when data is missing for particular days in the weather file
     *
     * @param mon month number
     * @param isPrevWet is previous day having precipitation
     * @return
     */
    public double generatePCP(int mon, boolean isPrevWet) {
        double vv, pcpgen, rn2, r6, xlv;
        vv = this.getAUNIF(1);
        if (vv > wgn.get(isPrevWet ? GeneratorType.PR_W2 : GeneratorType.PR_W1).get(mon)) {
            pcpgen = 0;
        } else {
            rn2 = this.getAUNIF(3);
            if (idist == 0) {
                r6 = wgn.get(GeneratorType.PCPSKW).get(mon) / 6.0;
                xlv = (this.getDstn1(rndNum[3], rn2) - r6) * r6 + 1;
                rndNum[3] = rn2;
                xlv = (Math.pow(xlv, 3) - 1) * 2.0 / wgn.get(GeneratorType.PCPSKW).get(mon);
                pcpgen = xlv * wgn.get(GeneratorType.PCPSTD).get(mon) + wgn.get(GeneratorType.PCPDAYMM).get(mon);
                pcpgen *= wgn.get(GeneratorType.PCF).get(mon);
            } else {
                pcpgen = (Math.pow(-1 * Math.log(rn2), rexp)
                        * wgn.get(GeneratorType.PCPDAYMM).get(mon))
                        * this.getRCOR();
            }

            if (pcpgen < 0.1) {
                pcpgen = 0.1;
            }
        }
        return pcpgen;
    }

    private double[] wgncur = new double[3];
    private double[] wgnold = new double[3];

    public void weatgn() {
        double[][] a = {{.567, .253, -.006}, {.086, .504, -.039}, {-.002, -.050, .244}};
        double[][] b = {{.781, .328, .238}, {0., .637, -.341}, {0., 0., .873}};
        double[] xx = new double[3];
        double[] e = new double[3];
        double v2;
        v2 = this.getAUNIF(8);
        e[0] = this.getDstn1(rndNum[8], v2);
        rndNum[8] = v2;

        v2 = this.getAUNIF(9);
        e[1] = this.getDstn1(rndNum[9], v2);
        rndNum[9] = v2;

        v2 = this.getAUNIF(2);
        e[2] = this.getDstn1(rndNum[2], v2);
        rndNum[2] = v2;

        for (int n = 0; n < 3; n++) {
            wgncur[n] = 0;
            for (int l = 0; l < 3; l++) {
                wgncur[n] = wgncur[n] + b[n][l] * e[l];
                xx[n] = xx[n] + a[n][l] * wgnold[l];
            }
        }

        for (int n = 0; n < 3; n++) {
            wgncur[n] += xx[n];
            wgnold[n] = wgncur[n];
        }
    }

    public double[] generateTMP(int mon, boolean isPCP) {
        this.weatgn();

        double tamp = (wgn.get(GeneratorType.TMPMX).get(mon)
                - wgn.get(GeneratorType.TMPMN).get(mon)) / 2.0;
        double txxm = wgn.get(GeneratorType.TMPMX).get(mon) + tamp * wgn.get(GeneratorType.PR_W3).get(mon);

        if (isPCP) {
            txxm = txxm - tamp;
        }

        double tmxg = txxm + wgn.get(GeneratorType.TMPSTDMX).get(mon) * wgncur[1];
        double tmng = wgn.get(GeneratorType.TMPMN).get(mon) + wgn.get(GeneratorType.TMPSTDMN).get(mon) * wgncur[2];

        if (tmng > tmxg) {
            tmng = tmxg - 0.2 * Math.abs(tmxg);
        }
        return new double[]{tmxg, tmng};
    }

    private double daylength;
    private double radMx;

    public void clgen(boolean isPCP, int dayOfYear) {
        // Calculate daylength
        // calculate solar declination: equation 2.1.2 in SWAT manual
        double sd = Math.asin(0.4 * Math.sin((dayOfYear - 82) / 58.09));

        // calculate the relative distance of the earth from the sun
        // the eccentricity of the orbit
        // equation 2.1.1 in SWAT manual
        double dd = 1.0 + 0.033 * Math.cos(dayOfYear / 58.09);

        //daylength = 2 * Acos(-Tan(sd) * Tan(lat)) / omega
        //where the angular velocity of the earth's rotation, omega, is equal
        // to 15 deg/hr or 0.2618 rad/hr and 2/0.2618 = 7.6374
        // equation 2.1.6 in SWAT manual
        double ch;
        double h;
        ch = -1 * getLatSin() * Math.tan(sd) / this.getLatCos();
        if (ch > 1.) {// ch will be >= 1. if latitude exceeds +  / -66.5 deg in winter h = 0.
            h = 0;
        } else if (ch >= -1.) {
            h = Math.acos(ch);
        } else {
            h = 3.1416;// latitude exceeds +/- 66.5 deg in summer endif
        }
        daylength = 7.6394 * h;

        // Calculate Potential (maximum) Radiation //
        // equation 2.2.7 in SWAT manual
        double ys;
        double yc;
        ys = this.getLatSin() * Math.sin(sd);
        yc = this.getLatCos() * Math.cos(sd);
        radMx = 30. * dd * (h * ys + yc * Math.sin(h));

        // Calculate fraction of radiation recieved during each hour in day
        // this calculation assumes solar noon (when the angle between the
        // observer on the earth to the sun and a line normal to the earth's
        // at that position is at a minimum) falls at 12 noon in day.
        // equation 2.2.10 in SWAT manual
    }

    /**
     * this subroutine generates solar radiation
     *
     * @param mon
     * @return
     */
    public double generateSLR(int mon, boolean isPCP) {
        double rav = wgn.get(GeneratorType.SOLARAV).get(mon) / (1 - 0.5 * wgn.get(GeneratorType.PR_W3).get(mon));
        if (isPCP) {
            rav *= 0.5;
        }
        double rx = this.radMx - rav;
        double genSLR = rav + wgncur[2] * rx / 4.0;
        if (genSLR <= 0) {
            genSLR = 0.05 * radMx;
        }
        return genSLR;
    }

    /**
     * this subroutine generates weather relative humidity, solar radiation, nd
     * wind speed.
     *
     * @param mon
     * @param isPCP
     * @return
     */
    public double generateRH(int mon, boolean isPCP) {
        double tmpmean = this.getTMPAV(mon);
        double rhmo;

        if (irelh.get(mon) == 1) {
            rhmo = wgn.get(GeneratorType.DEWPT).get(mon);
        } else {
            rhmo = getSAP(wgn.get(GeneratorType.DEWPT).get(mon)) / getSAP(tmpmean);
        }

        double yy = 0.9 * wgn.get(GeneratorType.PR_W3).get(mon);
        double rhm = (rhmo - yy) / (1.0 - yy);
        if (rhm < 0.05) {
            rhm = 0.5 * rhmo;
        }
        if (isPCP) {
            rhm = rhm * 0.1 + 0.9;
        }
        double vv = rhm - 1.;
        double uplm = rhm - vv * Math.exp(vv);
        double blm = rhm * (1.0 - Math.exp(-rhm));

        return this.getATRI(blm, rhm, uplm, 7);
    }

    /**
     * this subroutine generates wind speed
     *
     * @param mon month number
     * @return
     */
    public double generateWS10(int mon) {
        double v6 = this.getAUNIF(5);
        return wgn.get(GeneratorType.WNDAV).get(mon) * Math.pow(-Math.log(v6), 0.3);
    }

    public int generateWD(int mon) {
        double out = wgn.get(GeneratorType.WNDDIR).get(mon) + rand.nextGaussian() * wgn.get(GeneratorType.WNDDIRSTD).get(mon);
        if (out < 0) {
            out += 36;
        }
        out = Math.abs(out) % 36;
        return Double.valueOf(out).intValue();
    }

    /**
     * This function calculates saturation vapor pressure at a given air
     * temperature.
     *
     * @param tmpAvg mean air temperature
     * @return
     */
    private double getSAP(double tmpAvg) {
        double out = 0;
        if (tmpAvg + 23.7 != 0) {
            out = (16.78 * tmpAvg - 116.9) / (tmpAvg + 237.3);
            out = Math.exp(out);
        }

        return out;
    }

    /**
     * this function computes the distance from the mean of a normal
     * distribution with mean = 0 and standard deviation = 1, given two random
     * numbers
     *
     * @param rn1 first random number
     * @param rn2 second random number
     * @return
     */
    public static double getDstn1(double rn1, double rn2) {
        double dstn1;
        dstn1 = Math.sqrt(-2. * Math.log(rn1)) * Math.cos(6.283185 * rn2);
        return dstn1;
    }

    private double getATRI(double at1, double at2, double at3, int at4i) {
        double u3, rn, y, b1, b2, x1, xx, yy, amn;
        double atri;

        u3 = at2 - at1;
        rn = getAUNIF(at4i);
        y = 2.0 / (at3 - at1);
        b2 = at3 - at2;
        b1 = rn / y;
        x1 = y * u3 / 2.0;

        if (rn <= x1) {
            xx = 2.0 * b1 * u3;
            if (xx <= 0.) {
                yy = 0.;
            } else {
                yy = Math.sqrt(xx);
            }
            atri = yy + at1;
        } else {
            xx = b2 * b2 - 2.0 * b2 * (b1 - 0.5 * u3);
            if (xx <= 0.) {
                yy = 0.;
            } else {
                yy = Math.sqrt(xx);
            }
            atri = at3 - yy;
        }

        amn = (at3 + at2 + at1) / 3.0;
        atri = atri * at2 / amn;

        if (atri >= 1.0) {
            atri = 0.99;
        }
        if (atri <= 0.0) {
            atri = 0.001;

        }
        return atri;
    }

    public final double getAUNIF(int seedID) {
        int x2;
        double unif;

        x2 = rndseed[seedID] / 127773;
        rndseed[seedID] = 16807 * (rndseed[seedID] - x2 * 127773) - x2 * 2836;
        if (rndseed[seedID] < 0) {
            rndseed[seedID] = rndseed[seedID] + 2147483647;
        }
        unif = rndseed[seedID] * 4.656612875d - 10;
        return rand.nextDouble();
    }

//    public static Map<Integer, Double> generateRAINHHMX(double lat, double lon, String refPath) {
//        
//    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            //        double elev = 179.80;
//        double lat = 33.25;
//        double longi = -95.78;
//        int sumYr = 10;
//        String path = "C:\\Users\\Shawn\\Desktop\\wgn.db3";
//        String tbName = "wgn";
//        WeatherGenerator wgn = new WeatherGenerator(elev, lat, longi, sumYr, path, tbName);
//        int year = 2000;
//        Date start = new Date(year - 1900, 0, 1);
//        Date end = new Date(year - 1900, 11, 31);
//        long runningDate = start.getTime();
//        Date runDate;
//        int oneDayTime = 86400 * 1000;
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//        DecimalFormat formatter = new DecimalFormat("#0.000");
//        double pcp = 0, slr, rhm, wspd, wdir;
//        boolean isPCP;
//        double[] tmp = new double[2];
//        int mon;
//        while (runningDate <= end.getTime()) {
//            runDate = new Date(runningDate);
//            mon = runDate.getMonth() + 1;
//            isPCP = pcp > 0;
//            pcp = wgn.generatePCP(mon, isPCP);
//
//            isPCP = pcp > 0;
//
//            wgn.weatgn();
//            tmp = wgn.generateTMP(mon, isPCP);
//
//            wgn.clgen(isPCP, (int) (runningDate - start.getTime()) / oneDayTime);
//            slr = wgn.generateSLR(mon, isPCP);
//
//            rhm = wgn.generateRH(mon, isPCP);
//            wspd = wgn.generateWS10(mon);
//            wdir = wgn.generateWD(mon);
//            System.out.println(String.format("Date %s; PCP: %s; TMX: %s; TMN: %s; SLR: %s; RHM: %s; WSPD: %s; WDIR: %s",
//                    df.format(runDate),
//                    formatter.format(pcp),
//                    formatter.format(tmp[0]),
//                    formatter.format(tmp[1]),
//                    formatter.format(slr),
//                    formatter.format(rhm),
//                    formatter.format(wspd),
//                    formatter.format(wdir)));
//            runningDate += oneDayTime;
//        }
//        System.out.println("FINISH GENERATING!");
//            String cliPath = "C:\\Users\\Shawn\\Desktop\\IF_WeatherData.db3";
//            String refPath = new WeatherGenerator().getRefA05Path();
//            String outPath = "C:\\Users\\Shawn\\Desktop\\IF_WGN.db3";
//            double noData = -99;
//            double lat = 49.48;
//            double lon = -113;
//            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//            int startYr = 1971;
//            int endYr = 1980;
//            int decade = 1970;
//            for (int i = 1; i <= 4; i++) {
//                Date startDate = df.parse(String.format("%d-01-01", startYr));
//                Date endDate = df.parse(String.format("%d-12-31", endYr));
//                for (int j = 1; j <= 4; j++) {
//                    String outTableName = String.format("IF_WGN_%02d_%ds", j, decade);
//                    String sourceTable = String.format("Weather_station_%02d", j);
//                    WGNBuilder.exportWGN2SQLite(WGNBuilder.generateWGN(cliPath, sourceTable, noData, lat, lon, refPath, startDate, endDate), outTableName, outPath);
//                }
//
//                decade += 10;
//                startYr += 10;
//                endYr += 10;
//            }

            String hydroPath = "C:\\Users\\Shawn\\Desktop\\IF_HydroClimate_NoMissing.db3";
//            IMWEBsHydroClimate ihc = new IMWEBsHydroClimate(hydroPath, 100, 100, true);
//            long startTime = System.currentTimeMillis();
//
//            for (int i = 1; i <= 4; i++) {
//                ihc.fixPCP(i, -99);
//                System.out.println(String.format("Fix PCP time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixTMX(i, -99);
//                System.out.println(String.format("Fix TMX time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixTMN(i, -99);
//                System.out.println(String.format("Fix TMN time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixSR(i, -99);
//                System.out.println(String.format("Fix SR time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixRM(i, -99);
//                System.out.println(String.format("Fix RM time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixWS(i, -99);
//                System.out.println(String.format("Fix WS time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//                ihc.fixWD(i, -99);
//                System.out.println(String.format("Fix WD time: %d", System.currentTimeMillis() - startTime));
//                startTime = System.currentTimeMillis();
//            }
            String sql = "SELECT TABLENAME FROM stations where TYPE = \"WGN\"";
            ResultSet rs = Query.getDataTable(sql, hydroPath);
            DataTable table = new DataTable("wgt");
            table.addField("Month", DataTable.fieldType.Double);
            table.addField("RAINHHMX", DataTable.fieldType.Double);
            table.addField("PCPMM", DataTable.fieldType.Double);
            table.addField("PCPD", DataTable.fieldType.Double);
            Map<Integer, Double> rainhhmx = new HashMap();
            Map<Integer, Double> pcpmm = new HashMap();
            Map<Integer, Double> pcpd = new HashMap();
            int count = 0;
            ResultSet rs1;
            while(rs.next()) {
                count++;
                String tableName = rs.getString(1);
                sql = "SELECT Month, RAINHHMX, PCPDAYS, PCPMONMM FROM " + tableName + " ORDER BY Month";
                
                rs1 = Query.getDataTable(sql, hydroPath);
                
                int month;
                
                while (rs1.next()) {
                    month = (int) rs1.getDouble(1);
                    if (rainhhmx.containsKey(month)) {
                        rainhhmx.put(month, rainhhmx.get(month) + rs1.getDouble(2));
                    } else {
                        rainhhmx.put(month, rs1.getDouble(2));
                    }
                    
                    if (pcpmm.containsKey(month)) {
                        pcpmm.put(month, pcpmm.get(month) + rs1.getDouble(3));
                    } else {
                        pcpmm.put(month, rs1.getDouble(3));
                    }
                    
                    if (pcpd.containsKey(month)) {
                        pcpd.put(month, pcpd.get(month) + rs1.getDouble(4));
                    } else {
                        pcpd.put(month, rs1.getDouble(4));
                    }
                }
            }
            
            for (int mon : rainhhmx.keySet()) {
                table.addRecord();
                int recNum = table.getRecordCount() - 1;
                table.getField("Month").set(recNum, mon);
                table.getField("RAINHHMX").set(recNum, rainhhmx.get(mon) / count);
                table.getField("PCPMM").set(recNum, pcpmm.get(mon) / count);
                table.getField("PCPD").set(recNum, pcpd.get(mon) / count);
            }
            
            String out = "C:\\Users\\Shawn\\Desktop\\wgt.csv";
            table.exportCSV(out);
            
        } catch (Exception ex) {
            Logger.getLogger(WeatherGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
