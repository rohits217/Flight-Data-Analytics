import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDataAnalysis {

    public static class FlightScheduleMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final int delayThreshold = 10;
            String dataLine = value.toString();
            String[] data = dataLine.split(","); 
            String dateYear = data[0];
            String airline = data[8];
            String arrivalDelay = data[14];
            String departureDelay = data[15];
            
      	    if(!dateYear.equals("NA") && !dateYear.equals("Year") &&
                      !airline.equals("NA") && !airline.equals("UniqueCarrier") &&
                      !arrivalDelay.equals("NA") && !arrivalDelay.equals("ArrDelay") &&
                      !departureDelay.equals("NA") && !departureDelay.equals("DepDelay") )
            {
                if ( (Integer.parseInt(arrivalDelay) + Integer.parseInt(departureDelay) ) <= delayThreshold)
                    context.write(new Text(airline), new IntWritable(1));
                else
                    context.write(new Text(airline), new IntWritable(0));
            }
      }
    }

    public static class FlightScheduleReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        List<OnTimeProb> flightScheduleList = new ArrayList<>();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int tCount = 0;
          int onTime = 0;
          String arlineDepartureDelay;
          for (IntWritable e : values) {
              tCount += 1;
              if(e.get() == 1)
              {
                onTime += 1;
              }
          }
          double prob = (double)onTime/(double)tCount;
          flightScheduleList.add(new OnTimeProb(prob,key.toString()));
       }

      
       class OnTimeProb {
           double probability;
           String airline;
           OnTimeProb(double prob, String carr) 
           {
                this.probability = prob;
                this.airline = carr;
           }
       }

       
       class ReverseSort implements Comparator<OnTimeProb> {
            @Override
            public int compare(OnTimeProb o1, OnTimeProb o2) {
              if( o1.probability > o2.probability )
                return -1;
              else if( o1.probability < o2.probability )
                return 1;
              else 
                return 0;
            }
       }

       
       protected void cleanup(Context context) throws IOException, InterruptedException {
          Collections.sort(flightScheduleList, new ReverseSort());
          for (OnTimeProb elem : flightScheduleList) {
              context.write(new Text(elem.airline), new DoubleWritable(elem.probability ));
          }
      }
    }

    public static class AirportTaxiTimeMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dataLine = value.toString();
            String[] data = dataLine.split(",");
            String origin = data[16];
            String destination = data[17];
            String taxiIn = data[19];
            String taxiOut = data[20];
          
            if(!origin.equals("NA") && !origin.equals("Origin") &&
                      !destination.equals("NA") && !destination.equals("Dest") &&
                      !taxiIn.equals("NA") && !taxiIn.equals("TaxiIn") &&
                      !taxiOut.equals("NA") && !taxiOut.equals("TaxiOut") )
            {
                int in = Integer.parseInt(taxiIn);
                int out = Integer.parseInt(taxiOut);
                context.write(new Text(origin), new IntWritable(out));
                context.write(new Text(destination), new IntWritable(in));
            }
      }
    }

    public static class AirportTaxiTimeReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        List<AvgTaxiTime> taxiTimeAvg = new ArrayList<>();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int tCount = 0;
          int taxiTime = 0;
          for (IntWritable val : values) {
              tCount += 1;
              taxiTime += val.get();
          }
          double avg = (double)taxiTime/(double)tCount;
          taxiTimeAvg.add(new AvgTaxiTime(avg,key.toString()));
       }

       class AvgTaxiTime {
           double avg;
           String airportName;
           AvgTaxiTime(double avg, String airport)
           {
                this.avg = avg;
                this.airportName = airport;
           }
       }

       class ReverseSort implements Comparator<AvgTaxiTime> {
            @Override
            public int compare(AvgTaxiTime o1, AvgTaxiTime o2) {
              if( o1.avg > o2.avg )
                return -1;
              else if( o1.avg < o2.avg )
                return 1;
              else
                return 0;
            }
       }

       protected void cleanup(Context context) throws IOException, InterruptedException {
          Collections.sort(taxiTimeAvg, new ReverseSort());
          for (AvgTaxiTime elem : taxiTimeAvg) {
              context.write(new Text(elem.airportName), new DoubleWritable(elem.avg ));
          }
      }
    }

    public static class FlightCancellationMapper extends Mapper<Object, Text, Text, IntWritable>{
        IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dataLine = value.toString();
            String[] data = dataLine.split(",");
            String cancelled = data[21];
            String code = data[22];
            
            if(!cancelled.equals("NA") && !cancelled.equals("Cancelled") && cancelled.equals("1") &&
                      !code.equals("NA") && !code.equals("CancellationCode") && !code.isEmpty())
            {
                context.write(new Text(code), one);
            }
      }
    }

    public static class FlightCancellationReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int tCount = 0;
          for (IntWritable val : values) {
              tCount += val.get();
          }
          context.write(new Text(key), new IntWritable(tCount));
       }
    }

}
