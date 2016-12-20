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
public enum InputType {
    PCP, TMX, TMN, HMD, SLR, WSPD, WDIR, WGN;

    public String toStationType() {
        switch (this) {
            case PCP:
                return "P";
            case TMX:
                return "TMax";
            case TMN:
                return "TMin";
            case HMD:
                return "RM";
            case SLR:
                return "SR";
            case WSPD:
                return "WS";
            case WDIR:
                return "WD";
            case WGN:
                return "WGN";
        }
        return "";
    }
}
