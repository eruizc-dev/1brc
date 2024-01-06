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
import java.util.concurrent.ExecutionException;

public class CalculateAverage_eruizc {
    private static final String MEASUREMENTS = "./measurements.txt";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var workers = Runtime.getRuntime().availableProcessors();
        var file = new File(MEASUREMENTS);
        var segments = getSegments(file, workers);
        var pool = new Thread[workers];
        for (var w = 0; w < workers; w++) {
            if (segments[w] == null) {
                continue;
            }
            pool[w] = Thread.ofPlatform().start(new Runnable() {
                private Segment segment;

                public Runnable init(Segment segment) {
                    this.segment = segment;
                    return this;
                }

                @Override
                public void run() {
                    System.out.println(segment);
                }
            }.init(segments[w]));
        }
    }

    private static Segment[] getSegments(File f, int count) throws FileNotFoundException, IOException {
        try (var file = new RandomAccessFile(f, "r")) {
            var size = file.length() / count;
            var segments = new Segment[count--];
            var start = 0l;
            var end = size;

            while (start < file.length()) {
                file.seek(end);
                while (file.read() != '\n' && end < file.length()) {
                    end++;
                }
                segments[count--] = new Segment(start, end);
                start = end + 1;
                end = Math.min(file.length(), end + size);
            }
            return segments;
        }
    }

    private record Segment(long start, long end) {
    }
}
