package com.starrocks.connector.parser.pinot;

public class DurationInfo {
    public int days = 0;
    public int hours = 0;
    public int minutes = 0;
    public double seconds = 0.0;

    @Override
    public String toString() {
        return "Days: " + days + ", Hours: " + hours +
                ", Minutes: " + minutes + ", Seconds: " + seconds;
    }
}
