package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Question4 {

    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // definindo a posicao das variaveis dentro de um vetor, formado pelas colunas do arquivo
        int trade_usd = 5;
        int year = 1;

        // removendo o cabecalho
        linhas = linhas.filter(l -> !l.startsWith("country_or_area"));

        // transformando o RDD para um PAIRRDD com saída de TUPLAS (chave, valor)
        JavaPairRDD<String, AvgCommodity> prdd = linhas.mapToPair(x -> {
            String[] colunas = x.split(";");
            String valor = colunas[trade_usd];
            String chave = colunas[year];

            return new Tuple2<>(chave, new AvgCommodity(Double.parseDouble(valor), 1));
        });

        // fazendo a soma de todos os resultados que possuem a mesma chave (reduceByKey é uma transformação)
        JavaPairRDD<String, AvgCommodity> soma = prdd.reduceByKey((x, y) -> new AvgCommodity(x.getV() + y.getV(), x.getN() + y.getN()));

        // selecionando apenas os valores (sem mudar a chave)
        JavaPairRDD<String, Double> prddMedia = soma.mapValues(x -> x.getV()/ x.getN());

        // print dos resultados em uma funcao de repeticao (collect retorna uma lista de OBJETOS tipo TUPLA)
        for(Tuple2<String, Double> r: prddMedia.collect()){
            System.out.println(r._1() + "\t" + r._2() + " %"); }
    }
}
