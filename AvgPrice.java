package pairrdd.tdeSpark;

public class AvgPrice {

    private int n;
    private double preco;

    public AvgPrice() {
    }

    public AvgPrice(int n, double preco) {
        this.n = n;
        this.preco = preco;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getPreco() {
        return preco;
    }

    public void setPreco(double preco) {
        this.preco = preco;
    }

    @Override
    public String toString() {
        return "AvgPrice{" +
                "n=" + n +
                ", preco=" + preco +
                '}';
    }
}
