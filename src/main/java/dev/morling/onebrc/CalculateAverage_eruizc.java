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
import java.util.concurrent.*;

public class CalculateAverage_eruizc {
    private static final String MEASUREMENTS = "./measurements.txt";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        final var workers = Runtime.getRuntime().availableProcessors();
        final var map = new ConcurrentHashMap<String, Measurement>(10_000, 1f, workers);
        final var jobs = jobs(map, new File(MEASUREMENTS), workers);
        for (var job : jobs) {
            System.out.println(job);
            job.close();
        }
    }

    private static Job[] jobs(Map<String, Measurement> map, File file, int count) throws FileNotFoundException, IOException {
        final var jobs = new Job[count];
        final var size = file.length() / count;
        var start = 0l;
        while (--count >= 0) {
            jobs[count] = new Job(map, file, start, start + size);
            start += size;
        }
        return jobs;
    }

    public static class Job {
        private final Map<String, Measurement> map;
        private final RandomAccessFile file;
        private final long end;

        public Job(Map<String, Measurement> map, File f, long start, long end) throws FileNotFoundException, IOException {
            this.map = map;
            this.file = new RandomAccessFile(f, "r");
            this.end = end;

            // Move pointer to start and discard first line as it would be processed by another worker
            if (start != 0) {
                file.seek(start);
                file.readLine();
            }
        }

        public void close() throws IOException {
            file.close();
        }

        public void start() throws IOException {
            while (file.getFilePointer() < end) {
                var line = file.readLine();
                var split = line.split(";"); // Improve with binary search
                var station = map.get(split[0]);
                if (station == null) {
                    map.put(split[0], new Measurement(split[1]));
                }
                else {
                    station.add(split[1]);
                }
            }
            file.close();
        }

        @Override
        public String toString() {
            try {
                return "[ start: " + file.getFilePointer() + ", end: " + end + ", will_run: " + (file.getFilePointer() < end) + " ]";
            }
            catch (IOException e) {
                throw new RuntimeException(e);
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
