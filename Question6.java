package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Question6 {

    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        int type = 0;
        int year = 0;
        int category = 0;



    }
}
