package hk.ust.csit5970;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CORStripes.class);

	/*
	 * write your first-pass Mapper here.
	 */
	private static class CORMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text WORD = new Text();
		private static final IntWritable ONE = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> word_set = new HashMap<String, Integer>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
			/*
			 * TODO: Your implementation goes here.
			 */
			while (doc_tokenizer.hasMoreTokens()) {
				String w = doc_tokenizer.nextToken();
				if (w.length() == 0) {
					continue;
				}
				WORD.set(w);
				context.write(WORD, ONE);
	        }
		}
	}

	/*
	 * Write your first-pass reducer here.
	 */
	private static class CORReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private final static IntWritable SUM = new IntWritable();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * Your implementation goes here.
			 */
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	/*
	 * Write your second-pass Mapper here.
	 */
	public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
		private static final Text KEY = new Text();
		private static final MapWritable STRIPE = new MapWritable();
		private static final IntWritable ONE = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> sorted_word_set = new TreeSet<String>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
			while (doc_tokenizers.hasMoreTokens()) {
				sorted_word_set.add(doc_tokenizers.nextToken());
			}
			/*
			 * Your implementation goes here.
			 */
			Object[] sorted_word_set_array = sorted_word_set.toArray();
			
			System.out.println("Mapper2 start*******************");
			
			ArrayList<PairOfStrings> counted = new ArrayList<PairOfStrings>();
			for (int id1 = 0; id1 < sorted_word_set.size(); id1++)
			{
				KEY.set(sorted_word_set_array[id1].toString());
				for (int id2 = id1 + 1; id2 < sorted_word_set.size(); id2++)
				{
					PairOfStrings word_pair = new PairOfStrings();
					Boolean contextWritten = false;
					for (PairOfStrings c : counted)
					{
						if (c.getLeftElement().equals(word_pair.getLeftElement()) 
								&& c.getRightElement().equals(word_pair.getRightElement()))
						{
							contextWritten = true;
							break;
						}
					}
					if (!contextWritten)
					{
						STRIPE.put(new Text(sorted_word_set_array[id2].toString()), ONE);
						context.write(KEY, STRIPE);
						//KEY.set(sorted_word_set_array[id2].toString());
						System.out.println("KEY: " + sorted_word_set_array[id1]);
						System.out.println("KEY2: " + sorted_word_set_array[id2]);
						STRIPE.clear();
						
						counted.add(word_pair);
					}
				}
			}
		}
	}

	/*
	 * TODO: Write your second-pass Combiner here.
	 */
	public static class CORStripesCombiner2 extends Reducer<Text, MapWritable, Text, MapWritable> {
		static IntWritable ZERO = new IntWritable(0);
		private static final IntWritable ONE = new IntWritable(1);
		private final static MapWritable SUM_STRIPES = new MapWritable();

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * Your implementation goes here.
			 */
			Iterator<MapWritable> iter = values.iterator();
			
			System.out.println("Combine2 start*******************");

			while (iter.hasNext()) {
				for ( Writable second_w : iter.next().keySet() ) {
					IntWritable value_of_key_second_w = new IntWritable();
					if (SUM_STRIPES.containsKey((Text)second_w))
					{
						System.out.println("SUM_STRIPES.containsKey(second_w)");
						System.out.print("first_w: " + key.toString() + " ");
						System.out.println("second_w: " + ((Text)second_w).toString());
						value_of_key_second_w.set(((IntWritable) (SUM_STRIPES.get(second_w))).get());
						value_of_key_second_w.set(value_of_key_second_w.get() + 1);
						SUM_STRIPES.put(second_w, value_of_key_second_w);
						System.out.println("value: " + value_of_key_second_w.get());
						context.write(key, SUM_STRIPES);
						
					}
					else
					{
						System.out.println("!SUM_STRIPES.containsKey(second_w)");
						System.out.print("first_w: " + key.toString() + " ");
						System.out.println("second_w: " + ((Text)second_w).toString());
						SUM_STRIPES.put(second_w, ONE);
						System.out.println("value: ONE");
						context.write(key, SUM_STRIPES);
					}
				}
			}
			System.out.println("Combine2 iter done*******");
			
			SUM_STRIPES.clear();
		}
	}

	/*
	 * TODO: Write your second-pass Reducer here.
	 */
	public static class CORStripesReducer2 extends Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {
		private static Map<String, Integer> word_total_map = new HashMap<String, Integer>();
		private static IntWritable ZERO = new IntWritable(0);
		private final static MapWritable SUM_STRIPES = new MapWritable();

		/*
		 * Preload the middle result file.
		 * In the middle result file, each line contains a word and its frequency Freq(A), seperated by "\t"
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path middle_result_path = new Path("mid/part-r-00000");
			Configuration middle_conf = new Configuration();
			try {
				FileSystem fs = FileSystem.get(URI.create(middle_result_path.toString()), middle_conf);

				if (!fs.exists(middle_result_path)) {
					throw new IOException(middle_result_path.toString() + "not exist!");
				}

				FSDataInputStream in = fs.open(middle_result_path);
				InputStreamReader inStream = new InputStreamReader(in);
				BufferedReader reader = new BufferedReader(inStream);

				LOG.info("reading...");
				String line = reader.readLine();
				String[] line_terms;
				while (line != null) {
					line_terms = line.split("\t");
					word_total_map.put(line_terms[0], Integer.valueOf(line_terms[1]));
					LOG.info("read one line!");
					line = reader.readLine();
				}
				reader.close();
				LOG.info("finished！");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

		/*
		 * Write your second-pass Reducer here.
		 */
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			/*
			 * Your implementation goes here.
			 */
			Iterator<MapWritable> iter = values.iterator();
			String first_w = key.toString();
			int sum = 0;
			System.out.println("Reduce2 start**************************");
			while (iter.hasNext()) {
				MapWritable map_second_w = iter.next();
				for (Writable second_w : map_second_w.keySet())
				{
					IntWritable value_of_second_w = new IntWritable();
					IntWritable value_of_second_w_in_SUM_STRIPES = new IntWritable();
					IntWritable value_of_second_w_in_values = new IntWritable();
					System.out.println("for loop second_w*******");
					if (SUM_STRIPES.containsKey(second_w))
					{
						value_of_second_w_in_SUM_STRIPES = (IntWritable) (SUM_STRIPES.get(second_w));
						value_of_second_w_in_values = (IntWritable) map_second_w.get(second_w);
						value_of_second_w.set(value_of_second_w_in_SUM_STRIPES.get() + value_of_second_w_in_values.get());
						System.out.println("SUM_STRIPES.containsKey(second_w)");
						System.out.println("first_w: " + first_w + " " + "second_w " + ((Text)second_w).toString());
						System.out.print("SUM_STRIPES: ");
						System.out.print(value_of_second_w_in_SUM_STRIPES.get());
						System.out.print(" values: ");
						System.out.println(value_of_second_w_in_values.get());
					}
					else
					{
						value_of_second_w_in_values = (IntWritable) map_second_w.get(second_w);
						value_of_second_w.set(value_of_second_w_in_values.get());
						System.out.println("!SUM_STRIPES.containsKey(second_w)");
						System.out.println("first_w: " + first_w + " " + "second_w " + ((Text)second_w).toString());
						System.out.print("values: ");
						System.out.println(value_of_second_w_in_values.get());
					}
					sum += value_of_second_w_in_values.get();
					SUM_STRIPES.put(second_w, value_of_second_w);
				}
			}
			
			System.out.println("iterator done*******");
		    
		    DoubleWritable first_w_freq = new DoubleWritable();
		    DoubleWritable second_w_freq = new DoubleWritable();
		    DoubleWritable COR = new DoubleWritable();
		    
		    for (Entry<Writable, Writable> mapElement : SUM_STRIPES.entrySet()) { 
	            String second_w = ((Text) (mapElement.getKey())).toString();
	            first_w_freq.set(word_total_map.get(first_w));
	            second_w_freq.set(word_total_map.get(second_w));
	            COR.set((double)sum / (first_w_freq.get() * second_w_freq.get()));
	            context.write(new PairOfStrings(first_w, second_w), COR);
	        }
		    
		    SUM_STRIPES.clear();
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public CORStripes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String middlePath = "mid";
		String outputPath = cmdline.getOptionValue(OUTPUT);

		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + CORStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - middle path: " + middlePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Setup for the first-pass MapReduce
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "Firstpass");

		job1.setJarByClass(CORStripes.class);
		job1.setMapperClass(CORMapper1.class);
		job1.setReducerClass(CORReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(middlePath));

		// Delete the output directory if it exists already.
		Path middleDir = new Path(middlePath);
		FileSystem.get(conf1).delete(middleDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		// Setup for the second-pass MapReduce

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf1).delete(outputDir, true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Secondpass");

		job2.setJarByClass(CORStripes.class);
		job2.setMapperClass(CORStripesMapper2.class);
		job2.setCombinerClass(CORStripesCombiner2.class);
		job2.setReducerClass(CORStripesReducer2.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		// Time the program
		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CORStripes(), args);
	}
}