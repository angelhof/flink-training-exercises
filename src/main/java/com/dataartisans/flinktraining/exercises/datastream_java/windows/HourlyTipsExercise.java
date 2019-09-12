/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
// import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// import org.apache.flink.streaming.api.datastream.KeyedStream;
// import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.common.functions.ReduceFunction;
// import org.apache.flink.api.java.typeutils.TupleTypeInfo;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// We keep the driverId, the windowEnd, and the sum of tips for this driver
		DataStream<Tuple3<Long, Long, Float>> totals = fares
			// .map(new CleanUp())        // I cannot think of an easy way to put this after keyBy as then it becomes a singleoutput stream operator
			// .keyBy(0)
			.keyBy(fare -> fare.driverId)
			// .map(fare -> fare.tip) // I cannot make this solution work. For some reason java doesn't like window after map
			.window(TumblingEventTimeWindows.of(Time.hours(1)))
			// .reduce(new SumTip(), new HourWindow());
			.process(new HourWindow());

		// This seems to return a wrong result. While one would expect that it would return the correct result
		// DataStream<Tuple3<Long, Long, Float>> hourlyMax = totals
		// 	.keyBy(1)   // By the timestamp at the end of the window. Assume that all the windows have the exact same end timestamp
		// 	.maxBy(2);  // Return the one with the highest tip

		// Is this actually parallel (it says on the tutorial that window followed by windowall is not really parallel
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = totals
			.timeWindowAll(Time.hours(1))   // By the timestamp at the end of the window. Assume that all the windows have the exact same end timestamp
			.maxBy(2);                      // Return the one with the highest tip
		
		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}


	private static class HourWindow
		extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		
		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) {

			Float tipSum = new Float(0);
			for (TaxiFare fare : fares) {
				tipSum += fare.tip;
			}
			
			// We assume that key == fare.driverId for all fares
			// TODO: Can we assert that somehow and fail if it isn't?
			out.collect(new Tuple3<Long, Long, Float>(context.window().getEnd(), key, tipSum));
		}
	}

	
	// I cannot make this solution work
	//
	// private static class CleanUp implements MapFunction<TaxiFare, Tuple2<Long, Float>> {

	// 	@Override
	// 	public Tuple2<Long, Float> map(TaxiFare fare) throws Exception {
	// 		return new Tuple2(fare.driverId, fare.tip);
	// 	}
	// }
	
	// private static class SumTip implements ReduceFunction<Tuple2<Long, Float>> {
	// 	public Tuple2<Long, Float> reduce(Tuple2<Long, Float> tip1, Tuple2<Long, Float> tip2) {
	// 		// Since this is run in the keyed window, both driverIds should be the same.
	// 		// TODO: Can we assert that somehow and fail if it isn't?
	// 		return new Tuple2(tip1.f0, tip1.f1 + tip2.f1);
	// 	}
	// }
	
	// private static class HourWindow
	// 	extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		
	// 	@Override
	// 	public void process(Long key, Context context, Iterable<Tuple2<Long, Float>> sumTips, Collector<Tuple3<Long, Long, Float>> out) {
			
	// 		// This should only have one element (the aggregated sum)
	// 		Tuple2<Long, Float> tipSum = sumTips.iterator().next();

	// 		// We assume that key == tipSum.f0
	// 		// TODO: Can we assert that somehow and fail if it isn't?
	// 		out.collect(new Tuple3<Long, Long, Float>(key, context.window().getEnd(), tipSum.f1));
	// 	}
	// }

	
}
