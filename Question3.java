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

public class Question3 {

    public static void main(String args[]) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // selecionando a posicao da coluna year, commCommodity e flow dentro do vetor
        int year = 1;
        int commCommodity = 2;
        int flow = 4;

        // filtrando as linhas, fazendo o 'split ";"' e buscando os valores da coluna year que são iguais a 2016
        linhas = linhas.filter(l-> l.split(";")[year].equals("2016")); //filtra as linhas

        // mapeando as linhas, separando as palavras por ";" com o split, e selecionando as colunas FLOW e commCommodity
        JavaRDD<String> mapeamento = linhas.flatMap( l-> Arrays.asList(l.split(";")[flow]+"\t"+l.split(";")[commCommodity]).iterator()); //mapear - minha chave é um vetor

        // usando a função mapToPair para criar um PairRDD a partir de um RDD comum, com saída no formato de TUPLA (chave, valor)
        JavaPairRDD<String,Integer> novaTupla = mapeamento.mapToPair( p ->  new Tuple2<>(p,1));  //chave valor

        // usando a funcao reducebyKey para somar os valores com a mesma chave
        JavaPairRDD<String,Integer> reduce = novaTupla.reduceByKey((x,y) -> x+y);

        // utilizando o collect para colocar as tuplas dentro de uma lista
        List<Tuple2<String,Integer>> result = reduce.collect();

        // laço de repetição para printar os resultados
        for(Tuple2<String,Integer> r: result){
            System.out.println(r._1()+" = "+r._2());
        }
    }
}
