package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Question2 {

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

        // pensando como um vetor, a coluna 1 corresponde a posição do ANO no dataBase
        int posicaoAno = 1;

        // dividindo as linhas em colunas, tendo o ";" como parâmetro e selecionando a posicaoAno
        JavaRDD<String> year = linhas.flatMap(l -> Arrays.asList(l.split(";")[posicaoAno]).iterator());

        // usando a funcao mapToPair paran transformar um RDD em um PairRDD com uma tupla como saída
        JavaPairRDD<String, Integer> chaveValor = year.mapToPair(p -> new Tuple2<>(p, 1));

        // sempre pega os valores e vai somando de 2 em 2
        JavaPairRDD<String, Integer> reduce = chaveValor.reduceByKey((x, y) -> x + y);

        // colocando as tuplas dentro de uma um formato lista (collect), agrupando as que tem a mesma chave (reduce) e usando o collect
        List<Tuple2<String, Integer>> result = reduce.collect();

        // printando os resultados com um laço de repetição
        for (Tuple2<String, Integer> r : result){
            System.out.println("O número de transações no ano de " + r._1() + " é de: " + r._2());
        }
    }
}
