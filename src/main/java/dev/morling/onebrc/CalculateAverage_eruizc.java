/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CalculateAverage_eruizc {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var fr = new FileReader(FILE);
        var br = new BufferedReader(fr);
        var lines = br.lines();
        var map = new TreeMap<String, Something>();

        lines.forEach(line -> {
            var split = line.split(";");
            var city = split[0];
            var temp = Double.parseDouble(split[1]);

            if (map.containsKey(city)) {
                var something = map.get(city);
                something.add(temp);
            }
            else {
                map.put(city, new Something(temp));
            }
        });

        br.close();
        System.out.println(map);
    }

    public static class Something {
        private double min;
        private double max;
        private final List<Double> temps = new ArrayList<>(); // I don't like the double -> Double conversion

        public Something(double temp) {
            min = temp;
            max = temp;
            temps.add(temp);
        }

        @Override
        public String toString() {
            return min() + "/" + mean() + "/" + max(); // What's faster, concatenation or string.format?
        }

        public double min() { // Does this method add any overhead?
            return min;
        }

        public double max() { // Does this method add any overhead?
            return max;
        }

        private boolean calculatedMean = false;
        private double mean = 0;

        public double mean() {
            if (!calculatedMean) {
                var count = temps.size();
                mean = count % 2 == 0
                        ? (temps.get(count / 2 - 1) + temps.get(count / 2)) / 2
                        : temps.get(count / 2);
            }
            return mean;
        }

        public void add(double temp) {
            if (temp > max) {
                max = temp;
            }
            else if (temp < min) {
                min = temp;
            }
            temps.add(temp);
        }
    }
}
