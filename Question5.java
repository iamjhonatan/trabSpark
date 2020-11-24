package pairrdd.tdeSpark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Question5 {

    public static void main(String args[]) {

        // NÃO ESTÁ FUNCIONANDO!!

        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("transactionsInvolvingBrazil").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo de entrada
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // tratando as colunas como um vetor, respectivas posições
        int country = 0;
        int unit = 7;
        int year = 1;
        int category = 9;
        int price = 5;

        // removendo o cabeçalho e fazendo o split no arquivo por ";", além de filtrar, na coluna COUNTRY as informações = "BRAZIL"
        linhas = linhas.filter(l -> !l.startsWith("country_or_area") && l.split(";")[country].equals("Brazil"));

        // transformando em strings, retornando uma nova tupla
        JavaPairRDD<String, AvgPrice> prdd = linhas.mapToPair(l -> {
            String[] colunas = l.split(";");

            String u = colunas[unit];
            String y = colunas[year];
            String c = colunas[category];
            double p = Double.parseDouble(colunas[price]);

            return new Tuple2<>(("Categoria = " + c + " unidade = " + u + "Ano = " + y), new AvgPrice(1, p));
        });

        // somando os valores e instanciando a classe de media
        JavaPairRDD<String, AvgPrice> soma = prdd.reduceByKey((x, y) ->
                new AvgPrice(x.getN() + y.getN(), x.getPreco() + y.getN()));

        // print dos resultados parciais
        soma.foreach(v -> System.out.println(v._1() + "\t" + v._2()));

        // calculando a media, divindo a soma pelo total de commodities
        JavaPairRDD<String, Double> prddMedia = soma.mapValues(x -> x.getPreco() / x.getN());

        // ordenando por ordem crescente (true)
        JavaPairRDD<String, Double> ordenar = prddMedia.sortByKey(true);

        // salvando o resultado em um único arquivo de saída (coalesce 1)
        ordenar.coalesce(1).saveAsTextFile("output/question5.txt");
    }
}
