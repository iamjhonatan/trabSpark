package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Question1 {

    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // o pais dentro do arquivo é a posição 0 no vetor
        int country = 0;

        // filtrando o arquivo, separando as palavras com ";" e comparando a posição 0 do vetor
        JavaRDD<String> brazil = linhas.filter(a -> a.split(";")[country].equals("Brazil"));

        // contando cada ocorrencia
        long countTransactions = brazil.count();

        System.out.println("Transactions involving Brazil = " + countTransactions);



        // ** TENTATIVA FRUSTRADA DE TRABALHAR COM TUPLAS E PAIRRDDs
        // quebrar linhas em palavras
        //JavaRDD<String> palavras = linhas.flatMap(l -> Arrays.asList(l.split (";")).iterator());

        // para cada palavra, gerar <p, 1>
        //JavaPairRDD<String, Integer> palavraBrazil = palavras.mapToPair(p  -> new Tuple2<>("brazil", 1));

        // somar as ocorrencias de cada palavra
        // os dois elementos são sempre valores
        // reduceByKey não permite trabalhar com chaves, pois o agrupamento eh automatico
        //<String, Integer> ocorrencias = palavraUnidade.reduceByKey((x ,y) -> x + y);

        // resultado: <p, ocorrencia somada>
        //List<Tuple2<String, Integer>>resultado = ocorrencias.collect();
        //}
    }
}
