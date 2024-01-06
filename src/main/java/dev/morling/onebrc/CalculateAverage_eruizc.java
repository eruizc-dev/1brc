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
import java.util.concurrent.*;

public class CalculateAverage_eruizc {
    private static final String MEASUREMENTS = "./measurements.txt";

    // Optimizations
    private static final int WORKERS = Runtime.getRuntime().availableProcessors();
    private static final int BUFFER_SIZE = 8 * 1024;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var measurements = new ConcurrentSkipListMap<String, Station>();
        var jobs = jobs(measurements, MEASUREMENTS, WORKERS);
        var threads = new Thread[WORKERS];

        for (var i = 0; i < WORKERS; i++) {
            threads[i] = Thread.ofPlatform().start(jobs[i]);
        }

        for (var thread : threads) {
            thread.join();
        }

        System.out.println(measurements);
    }

    private static Job[] jobs(ConcurrentMap<String, Station> map, String filename, int count) throws FileNotFoundException, IOException {
        var jobs = new Job[count];
        var size = filename.length() / count;
        var start = 0l;
        while (--count >= 0) {
            jobs[count] = new Job(map, filename, start, size);
            start += size;
        }
        return jobs;
    }

    public static class Job implements Runnable {
        private final ConcurrentMap<String, Station> map;
        private final BufferedReader reader;
        private long bytesToRead;

        public Job(ConcurrentMap<String, Station> map, String filename, long start, long size) throws FileNotFoundException, IOException {
            this.map = map;
            this.reader = new BufferedReader(new FileReader(filename), BUFFER_SIZE);
            this.bytesToRead = size;

            // Move pointer to start and discard first line as it would be processed by another worker
            if (start != 0) {
                reader.skip(start);
                var line = reader.readLine();
                if (line != null) {
                    this.bytesToRead -= line.getBytes().length;
                }
            }
        }

        public void run() {
            try {
                String line;
                while (bytesToRead > 0 && (line = reader.readLine()) != null) {
                    var split = line.split(";"); // Improve with binary search
                    var station = map.putIfAbsent(split[0], new Station(split[1]));
                    if (station != null) {
                        station.add(split[1]);
                    }
                    bytesToRead -= line.getBytes().length;
                }
                reader.close();
            }
            catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }

    public static class Station {
        private double max;
        private double min;
        private double sum;
        private long measurements;

        public Station(String temperature) {
            var temp = Double.parseDouble(temperature);
            this.max = temp;
            this.min = temp;
            this.sum = temp;
            this.measurements = 1;
        }

        public void add(String temperature) {
            var temp = Double.parseDouble(temperature);
            this.sum += temp;
            this.measurements++;

            if (temp > max) {
                max = temp;
            }
            else if (temp < min) {
                min = temp;
            }
        }

        public double mean() {
            return Math.round((sum / measurements) * 10d) / 10d;
        }

        @Override
        public String toString() {
            return min + "/" + mean() + "/" + max;
        }
    }
}
