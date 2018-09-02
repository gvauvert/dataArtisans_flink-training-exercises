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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.data-artisans.com).
 * <p>
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 * <p>
 * Parameters:
 * -input path-to-input-file
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

        // https://ci.apache.org/projects/flink/flink-docs-release-1.5/dev/stream/operators/windows.html
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares.keyBy((TaxiFare fare) -> fare.driverId).
                window(TumblingEventTimeWindows.of(Time.hours(1))).
                apply(new SumTips());

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.timeWindowAll(Time.hours(1)).maxBy(2);

        //throw new MissingSolutionException();

        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

    public static class SumTips implements WindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow window, Iterable<TaxiFare> input, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            Float[] tipSum = new Float[]{0.f};
            input.forEach(taxiFare -> tipSum[0] += taxiFare.tip);
            out.collect(Tuple3.of(window.getEnd(), key, tipSum[0]));
        }
    }
}
