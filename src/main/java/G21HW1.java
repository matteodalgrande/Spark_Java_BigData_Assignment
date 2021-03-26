import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.*;


import java.lang.Float;
import java.lang.Long;
import java.util.*;

public class G21HW1 {
    public static void main(String[] args) {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        SparkConf conf = new SparkConf(true).setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 1 INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Read number of partitions
        int K = Integer.parseInt(args[0]);
        int T = Integer.parseInt(args[1]);
        // Read input file and subdivide it into K random partitions
        JavaRDD<String> RawData = sc.textFile(args[2]).repartition(K).cache();//partition and save in cache
        System.out.println("num of chunks/partitions: " + RawData.getNumPartitions());//print number of chunks

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Assignment
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //JavaPairRDD<String, Float> normalizedRatings;   // normalizedRatings --> for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp),
                                                        // NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating
                                                        // and AvgRating is the average rating of all reviews by the user "UserID".




        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // definition of function for create pairs like (key=UserID, value=(ProductID,Rating,Timestamp))
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //  ProductID (string), UserID (string), Rating (integer in [1,5] represented as a real), Timestamp (integer)
        //JavaPairRDD inputPairs = sc.parallelize(RawData);
        PairFunction<String,Tuple2<String,String>, Tuple3<String,Float,Long>> keyDataFunction = new PairFunction<String, Tuple2<String,String>,Tuple3<String,Float,Long>>() {
            @Override
            public Tuple2<Tuple2<String,String>, Tuple3<String, Float, Long>> call(String s) throws Exception {
                //setto il ProductID come key e il resto come valore eliminando pero' il productID tra i value
                return new Tuple2<Tuple2<String,String>, Tuple3<String, Float, Long>> (new Tuple2<String,String>(s.split(",")[1], s.split(",")[0]), new Tuple3<String,Float,Long>(s.split(",")[0], Float.parseFloat(s.split(",")[2]), Long.parseLong(s.split(",")[3])));
            }
        };

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // definition of function for create pairs like (key= utente, value=(rating, ratings utente))
        // ProductID (string), UserID (string), Rating (integer in [1,5] represented as a real), Timestamp (integer)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        PairFunction<String, String, Tuple2<Float,String>> userAndRatingFunction = new PairFunction<String, String, Tuple2<Float,String>>() {
            @Override
            public Tuple2<String, Tuple2<Float,String>> call(String s) throws Exception {
                //setto il UserID come key e rating like value
                return new Tuple2<String, Tuple2<Float,String>>(s.split(",")[1], new Tuple2<Float,String>(Float.parseFloat(s.split(",")[2]), s.split(",")[0]));
            }
        };
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALCOLO L'oggetto per la media DI OGNI utente (key=userID, value=Object(averageValue))
        // creo la classe, creo le tre funzioni per combineByKey(createCombiner(), mergeValue(), mergeCombiners())
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        class AvgCount implements Serializable {
            public AvgCount(String s,Float total, Float num) { aL_.add(s); total_ = total; num_ = (Float) num; }
            public Float total_;
            public Float num_;
            public ArrayList<String> aL_ = new ArrayList<String>();
            public Float avg() { return (Float) total_ / (Float) num_; }
        }
        Function<Tuple2<Float,String>, AvgCount> createAcc =//String, Tuple2<Float,String>>
                new Function<Tuple2<Float,String>, AvgCount>() {    //If it’s a new element, combineByKey() uses a function we provide, called createCombiner(),
            @Override
            public AvgCount call(Tuple2<Float,String> x) {  // to create the initial value for the accumulator on that key
                return new AvgCount(x._2(), x._1(), 1f);
            }
        };
        Function2<AvgCount, Tuple2<Float,String>, AvgCount> addAndCount = //If it is a value we have seen before while processing that partition, it will instead use
            new Function2<AvgCount, Tuple2<Float,String>, AvgCount>() {//the provided function, mergeValue(), with the current value for the accumulator for that key and the new value.
            @Override
            public AvgCount call(AvgCount a, Tuple2<Float,String> x) {
                a.aL_.add(x._2());
                a.total_ += x._1();
                a.num_ += 1;
                return a;
            }
        };
        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        Iterator<String> b_ = b.aL_.iterator();
                        while(b_.hasNext()){
                            a.aL_.add(b_.next());
                        }
                        a.total_ += b.total_;
                        a.num_ += b.num_;
                        return a;
                    }
                };

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 2
        // CALCOLO NormalizedRatings = (ProductID,NormRating)
        // NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating
        // and AvgRating is the average rating of all reviews by the user "UserID".
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //normalizzo il valore
        JavaPairRDD <String, Float> normalizedRatings = RawData.mapToPair(keyDataFunction).rightOuterJoin(RawData.mapToPair(userAndRatingFunction).combineByKey(createAcc, addAndCount, combine) //combineByKey(createCombiner(), mergeValue(), mergeCombiners())
                .mapToPair( s -> {
                    return new Tuple2<Tuple2<String,ArrayList<String>>,Float> (new Tuple2<String,ArrayList<String>>(s._1, s._2.aL_), s._2.avg());
                }).flatMapToPair(s->{
                    ArrayList<Tuple2<Tuple2<String,String>,Float>> a = new ArrayList<>();
                    for(String st: s._1._2){
                        a.add(new Tuple2<Tuple2<String,String>,Float>(new Tuple2<>(s._1._1(),st),s._2));
                    }
                    return a.iterator();
                })).mapToPair(s ->{
            return new Tuple2<String,Float>(s._2()._1.get()._1(), s._2._1.get()._2() - s._2._2() );//(ProductID,NormRating), where NormRating=Rating-AvgRating
        });

        //normalizedRatings.collect().forEach(s-> System.out.println(s));



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 3 - maxNormRatings
        // Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called
        // maxNormRatings which, for each ProductID contains exactly one pair (ProductID, MNR)
        // where MNR is the maximum normalized rating of product "ProductID".
        // The maximum should be computed either using the reduceByKey method or
        // the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program).
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        JavaPairRDD<String, Float> maxNormRatings =normalizedRatings.reduceByKey(
            (v1, v2) -> Math.max(v1, v2)
        );

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 4
        // Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

       // invert key,value -> value,key
        PairFunction<Tuple2<String, Float>, Float, String> swapFunction =
            new PairFunction<Tuple2<String, Float>, Float, String>() {
                @Override
                public Tuple2<Float, String> call(Tuple2<String, Float> item) throws Exception {
                    return item.swap();
                }
            };

       //reorder from  In base float
        class FloatComparator implements Comparator<Float>, Serializable{
            @Override
            public int compare(Float f1, Float f2){
                return Float.valueOf(f1).compareTo(Float.valueOf(f2));//trasformo in stringa e uso il comparatore in stringa
            }
        }

        //reinverto invert value,key -> key,value
        PairFunction<Tuple2<Float, String>, String, Float> swapFunctionSecond =
            new PairFunction<Tuple2<Float, String>, String, Float>() {
                @Override
                public Tuple2<String,Float> call(Tuple2<Float,String> item) throws Exception {
                    return item.swap();
                }
            };


        // T is the value in input
        System.out.println();
        System.out.println("Results:");
        maxNormRatings.mapToPair(swapFunction).sortByKey(new FloatComparator(),false).mapToPair(swapFunctionSecond).take(T).forEach(s-> System.out.println(s));


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Useful to access the spark's web interface
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //serve per guardare la dashboard di spark
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Vedi dashboard di spark at: http://192.168.1.13:4040/jobs/");
        String input = myObj.nextLine();  // Read user input
    }
}
