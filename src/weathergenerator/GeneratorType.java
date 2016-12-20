/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package weathergenerator;

/**
 *
 * @author Shawn
 */
public enum GeneratorType {
    TMPMX, // |deg C         |avg monthly maximum air temperature
    TMPMN, // |deg C         |avg monthly minimum air temperature
    TMPSTDMX, // |deg C         |standard deviation for avg monthly maximum air temperature
    TMPSTDMN, // |deg C         |standard deviation for avg monthly minimum air temperature
    PCPMONMM,// |mm/mon         |average amount of precipitation falling in that month
    PCPDAYMM, // |mm/day         |average amount of precipitation falling in one day for the month
    PCPSTD, // |mm/day         |standard deviation for the average daily precipitation
    PCPSKW, // |none         |skew coefficient for the average daily precipitation
    PR_W1, // |none         |probability of wet day after dry day in month
    PR_W2, // |none         |probability of wet day after wet day in month
    PR_W3, // |none         |proportion of wet days in the month
    PCPDAYS,// |days          |average number of days of precipitation in the month
    RAINHHMX, // |mm            |maximum 0.5 hour rainfall in month for entire period of record
    RAINHHMXSmooth, // |mm            |smoothed maximum 0.5 hour rainfall in month for entire period of record
    SOLARAV,// |MJ/m^2/day    |average daily solar radiation for the month
    DEWPT, // |deg C         |average dew point temperature for the month
    WNDAV, // |m/s            |average wind speed for the month
    WNDGUST, // |m/s            |average gust speed for the month
    WNDDIR,  // |10's deg            |most possible wind direction
    WNDDIRSTD,  // |10's deg            |most possible wind direction standard deviation
    PCF, // |none          |normalization coefficient for precipitation generator
    AMP_R// |none          |average fraction of total daily rainfall occuring in maximum half-hour period for month
}
