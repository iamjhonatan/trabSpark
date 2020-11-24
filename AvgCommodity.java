package pairrdd.tdeSpark;

import java.io.Serializable;

public class AvgCommodity implements Serializable {

    private int n; // qtdade de comodities
    private double v; // valor dos comodities

    public AvgCommodity (){

    }

    public AvgCommodity(double v, int n) {
        this.n = n;
        this.v = v;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getV() {
        return v;
    }

    public void setV(double v) {
        this.v = v;
    }
}
