import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        // Path tmpPath = new Path("/mp2/tmp");
        // fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "League Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LeagueLinkCountMap.class);
        jobA.setReducerClass(LeagueLinkCountReduce.class);

        jobA.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(PopularityLeague.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    // TODO
    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static ArrayList<Integer> getLeagueList(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));
        ArrayList<Integer> retArray = new ArrayList<Integer>(); 
        String line;
        while( (line = buffIn.readLine()) != null) {
            Integer leagueMember = Integer.parseInt(line.trim());
            retArray.add(leagueMember);
        }
        return retArray;
    }


    public static class LeagueLinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        // TODO
        ArrayList<Integer> leagueList;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            String leagueFileName = conf.get("league");
            this.leagueList = getLeagueList(leagueFileName, conf);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        		// TODO
		  		String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line, ": "); 
				Boolean first = true;
				while (tokenizer.hasMoreTokens()) {
					String nextToken = tokenizer.nextToken().trim();
					Integer page = Integer.parseInt(nextToken);
					if (first == true) {
						first = false;
						continue;
					}
                    if (this.leagueList.contains(page)) {
				        context.write(new IntWritable(page), new IntWritable(1));	
                    }
				}
    	  }
    }

    public static class LeagueLinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // TODO
        ArrayList<Pair<Integer, Integer>> pageReferenceList;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.pageReferenceList = new ArrayList<Pair<Integer, Integer>>();
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		  	 int sum = 0;
			 for (IntWritable val : values) {
				sum += val.get();
			 }
             // Write page, count
          //   context.write(key, new IntWritable(sum));
             Pair<Integer, Integer> np = new Pair<Integer, Integer>(new Integer(sum), new Integer(key.get()));
             this.pageReferenceList.add(np);
		  }

        // Use collections.sort here
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int i;
            int j;
            int lessthan = 0;

            for (i = 0; i < this.pageReferenceList.size(); i++) {
                lessthan = 0; 
                Integer thispage = this.pageReferenceList.get(i).second;
                Integer thiscount = this.pageReferenceList.get(i).first;

                for (j = 0; j < this.pageReferenceList.size(); j++) {
                   if (i == j) { continue; }
                   Integer otherpage = this.pageReferenceList.get(j).second;
                   Integer othercount = this.pageReferenceList.get(j).first;
                   if (thiscount > othercount) {
                        lessthan += 1;
                   }
                }
                IntWritable wPage = new IntWritable(thispage);
                IntWritable wLessthan = new IntWritable(lessthan);
                context.write(wPage, wLessthan);
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}

