/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_java.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.data-artisans.com).
 * <p>
 * The goal for this exercise is to enrich TaxiRides with fare information.
 * <p>
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 */
public class RidesAndFaresExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", pathToRideData);
        final String faresFile = params.get("fares", pathToFareData);

        final int delay = 60;                    // at most 60 seconds of delay
        final int servingSpeedFactor = 1800;    // 30 minutes worth of events are served every second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> ride.isStart).keyBy("rideId");

        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor))).keyBy("rideId");

        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides.connect(fares)
                //.keyBy("rideId", "rideId") // implicit ?
                .flatMap(new EnrichmentFunction());

        printOrTest(enrichedRides);

        env.execute("Join Rides with Fares (java RichCoFlatMap)");
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        private transient ValueState<TaxiRide> m_taxiRideValueState;
        private transient ValueState<TaxiFare> m_taxiFareValueState;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<TaxiRide> taxiRideValueStateDescriptor =//
                    new ValueStateDescriptor<TaxiRide>("taxiRide", TypeInformation.of(new TypeHint<TaxiRide>() {
                    }), null);
            m_taxiRideValueState = getRuntimeContext().getState(taxiRideValueStateDescriptor);
            ValueStateDescriptor<TaxiFare> taxiFareValueStateDescriptor =//
                    new ValueStateDescriptor<TaxiFare>("taxiFare", TypeInformation.of(new TypeHint<TaxiFare>() {
                    }), null);
            m_taxiFareValueState = getRuntimeContext().getState(taxiFareValueStateDescriptor);
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare taxiFareValue = m_taxiFareValueState.value();
            if (taxiFareValue != null) {
                m_taxiFareValueState.clear();
                out.collect(Tuple2.of(ride, taxiFareValue));
            } else {
                m_taxiRideValueState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide taxiRideValue = m_taxiRideValueState.value();
            if (taxiRideValue != null) {
                m_taxiRideValueState.clear();
                out.collect(Tuple2.of(taxiRideValue, fare));
            } else {
                m_taxiFareValueState.update(fare);
            }
        }
    }
}
