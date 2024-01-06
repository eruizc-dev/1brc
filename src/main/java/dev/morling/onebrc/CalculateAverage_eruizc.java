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
    private static final int BUFFER_SIZE = 8 * 1024; // Could play with this a little

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        final var workers = Runtime.getRuntime().availableProcessors();
        final var map = new ConcurrentSkipListMap<String, Measurement>();
        final var jobs = jobs(map, new File(MEASUREMENTS), workers);
        final var threads = new Thread[workers];

        for (var i = 0; i < workers; i++) {
            threads[i] = Thread.ofPlatform().start(jobs[i]);
        }

        for (var thread : threads) {
            thread.join();
        }

        System.out.println(map);
    }

    private static Job[] jobs(ConcurrentMap<String, Measurement> map, File file, int count) throws FileNotFoundException, IOException {
        final var jobs = new Job[count];
        final var size = file.length() / count;
        var start = 0l;
        while (--count >= 0) {
            jobs[count] = new Job(map, file, start, start + size);
            start += size;
        }
        return jobs;
    }

    public static class Job implements Runnable {
        private final ConcurrentMap<String, Measurement> map;
        private final BufferedReader reader;
        private long bytesToRead;

        public Job(ConcurrentMap<String, Measurement> map, File f, long start, long end) throws FileNotFoundException, IOException {
            this.map = map;
            this.reader = new BufferedReader(new FileReader(f), BUFFER_SIZE);
            this.bytesToRead = end - start;

            // Move pointer to start and discard first line as it would be processed by another worker
            if (start != 0) {
                reader.skip(start);
                this.bytesToRead -= reader.readLine().getBytes().length; // Readline could be null in small files
            }
        }

        public void run() {
            try {
                String line;
                while (bytesToRead > 0 && (line = reader.readLine()) != null) {
                    var split = line.split(";"); // Improve with binary search
                    var station = map.putIfAbsent(split[0], new Measurement(split[1]));
                    if (station != null) {
                        station.add(split[1]);
                    }
                    bytesToRead -= line.getBytes().length;
                }
                reader.close();
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }

    public static class Measurement {
        private double max;
        private double min;
        private double sum = 0;
        private long count = 1;

        public Measurement(String temperature) {
            var temp = Double.parseDouble(temperature);
            this.max = temp;
            this.min = temp;
            this.sum += temp;
        }

        public void add(String temperature) {
            var temp = Double.parseDouble(temperature);
            sum += temp;
            count++;

            if (temp > max) {
                max = temp;
            }
            else if (temp < min) {
                min = temp;
            }
        }

        public double mean() {
            return Math.round((sum / count) * 10d) / 10d;
        }

        @Override
        public String toString() {
            return min + "/" + mean() + "/" + max;
        }
    }
}
