package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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

        // removendo o cabecalho
        linhas = linhas.filter(l -> !l.startsWith("country_or_area"));

        // fazendo o split por ';', gerando strings de cada coluna que será utilizada, retornando uma nova tupla <commodity, unit e year como chave e price como valor>
        JavaPairRDD<String, Double> prdd = linhas.mapToPair(l -> {
            String[] colunas = l.split(";");

            String unit = colunas[7];
            String year = colunas[1];
            String commodity = colunas[3];
            double price = Double.parseDouble(colunas[5]);

            return new Tuple2<>(("Commodity = " + commodity + " Unit = " + unit + " Year = " + year), price);
        });

        // usando o reduceByKey para somar os valores com a mesma chave/ Classe Math para calcular o valor máximo
        JavaPairRDD<String, Double> maxPrice = prdd.reduceByKey((v1, v2) -> Math.max(v1, v2));

        // ordenando o resultado de fora decrescente
        JavaPairRDD<String, Double> ordenado = maxPrice.sortByKey(false);

        // salvando o resultado em um arquivo txt em apenas uma única partição
        ordenado.coalesce(1).saveAsTextFile("output/question6.txt");
    }
}
