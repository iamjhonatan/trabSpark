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

public class Question7 {
    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // selecionando as colunas flow e year, pensando em um vetor
        int flow = 4;
        int year = 1;

        // mapeando o arquivo, separando as linhas em palavras com "split ';'", selecionando os dados da coluna FLOW e da coluna YEAR
        JavaRDD<String> mapeamento = linhas.flatMap( l-> Arrays.asList(l.split(";")[flow]+"\t"+l.split(";")[year]).iterator());

        // usando a função mapToPair para criar um PairRDD a partir de um RDD comum, com saída no formato de TUPLA (chave, valor)
        JavaPairRDD<String,Integer> novaTupla = mapeamento.mapToPair(p ->  new Tuple2<>(p,1));  //chave valor

        // usando o reduce para somar os valores que possuem a mesma chave
        JavaPairRDD<String,Integer> reduce = novaTupla.reduceByKey((x,y) -> x+y);

        // usando o collect para colocar as tuplas em uma lista, usando o reduce para agrupar as tuplas com a mesma chave
        List<Tuple2<String,Integer>> result = reduce.collect();

        // laço de repetição para printar o resultado
        for(Tuple2<String,Integer> r: result){
            System.out.println(r._1()+" = "+r._2());
        }

    }
}
