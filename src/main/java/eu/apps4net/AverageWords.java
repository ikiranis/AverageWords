package eu.apps4net;

/**
 * Created by Yiannis Kiranis <yiannis.kiranis@gmail.com>
 * https://apps4net.eu
 * Date: 26/2/23
 * Time: 10:49 μ.μ.
 *
 * Υπολογισμός μέσου όρου μήκους λέξεων που αρχίζουν με έναν συγκεκριμένο χαρακτήρα
 *
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageWords {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final static FloatWritable wordLength = new FloatWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Αφαίρεση σημείων στίξης και μετατροπή σε lower case
            line = line.replaceAll("\\p{Punct}", " ").toLowerCase();

            // Δημιουργία tokens από την τρέχουσα γραμμή
            StringTokenizer itr = new StringTokenizer(line);

            // Επεξεργασία κάθε token
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

                // Αν δεν αρχίζει από αριθμό
                if(!token.matches("^\\d.*")) {
                    // Παίρνει τον πρώτο χαρακτήρα της λέξης
                    word.set(String.valueOf(token.charAt(0)));

                    // Παίρνει το μήκος της λέξης
                    wordLength.set(token.length());

                    // Προσθέτει στο context με το αρχικό της λέξης και το μήκος της
                    context.write(word, wordLength);
                }

            }
        }
    }

    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            // Υπολογίζει το άθροισμα των μεγεθών των λέξεων και τον αριθμό των λέξεων
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            // Υπολογίζει το μέσο όρο
            float average = (float) sum/count;

            // Μορφοποίηση του μέσου όρου σε 1 δεκαδικό ψηφίο
            String formattedAverage = String.format("%.1f", average);

            // Προσθέτει το μέσο όρο στο context, για το συγκεκριμένο αρχικό της λέξης
            result.set(Float.parseFloat(formattedAverage));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average words length");
        job.setJarByClass(AverageWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
