/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.addthis.ccompressor.bench;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.List;

import com.addthis.ccompressor.Column;
import com.addthis.ccompressor.ColumnRowCompressor;
import com.addthis.ccompressor.DeltaLongColumn;
import com.addthis.ccompressor.RunLengthColumn;
import com.addthis.ccompressor.Text255Column;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueFactory;

import org.xerial.snappy.SnappyOutputStream;

public class BenchSizeTest {

    public static void main(String[] args) throws IOException {
        String inputFilename = "search-us.20130809-0700.0100.log";
        FileReader fis = new FileReader(inputFilename);
        BufferedReader br = new BufferedReader(fis);

//		TIMESTAMP UID GEO URL REFERRER TERMS CATEGORIES USERAGENT
        ListBundleFormat format = new ListBundleFormat();

        BundleField[] bfArray = {
                format.getField("TIMESTAMP"),
                format.getField("UID"),
                format.getField("GEO"),
                format.getField("URL"),
                format.getField("REFERRER"),
                format.getField("TERMS"),
                format.getField("CATAGORY"),
                format.getField("USERAGENT"),
                format.getField("DUMMY")};

        List<Bundle> bundles = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            // process the line.
            String[] tokens = line.split("\t");
            Bundle b = new ListBundle(format);
            int index = 0;
            for (String token : tokens) {
                if (index == 0) {
                    b.setValue(bfArray[index], ValueFactory.create(Long.valueOf(token)));
                } else {
                    b.setValue(bfArray[index], ValueFactory.create(token));
                }
                index++;
            }
            b.setValue(bfArray[index], ValueFactory.create("DUMMYVALUE"));
            bundles.add(b);
        }
        br.close();
        System.out.println("read " + bundles.size() + " rows");

        List<Column> columns = new ArrayList<>();
        int index = 0;
        for (BundleField bundleField : bfArray) {
            if (index++ == 0 && index < bfArray.length) {
                columns.add(new DeltaLongColumn(bundleField.getName()));
            } else if (index < (bfArray.length)) {
                columns.add(new Text255Column(bundleField.getName()));
            } else {
                columns.add(new RunLengthColumn(bundleField.getName()));
            }
        }

        File colFile = new File("out.cols");
        DataOutputStream dos = new DataOutputStream(new SnappyOutputStream(new FileOutputStream(colFile)));
        ColumnRowCompressor compressor = new ColumnRowCompressor(columns, 10000);
        long startTime = System.currentTimeMillis();
        for (Bundle bundle : bundles) {
            compressor.write(bundle, dos);
        }
        compressor.flush(dos);
        long elapsedTime = System.currentTimeMillis() - startTime;
        dos.close();
        System.out.println("bundle column size: " + humanReadableByteCount(colFile.length(), false) + " bytes");
        System.out.println("bundle writing time: " + elapsedTime + " ms");

        File bundleFile = new File("out.bundles");
        FileOutputStream bfos = new FileOutputStream(bundleFile);
        OutputStream bundleos = new SnappyOutputStream(bfos);
        long bundleStartTime = System.currentTimeMillis();
        for (Bundle bundle : bundles) {
            bundleos.write(DataChannelCodec.encodeBundle(bundle));
        }
        long bundleElapsedTime = System.currentTimeMillis() - bundleStartTime;
        bundleos.close();
        System.out.println("bundle file size: " + humanReadableByteCount(bundleFile.length(), false) + " bytes");
        System.out.println("bundle writing time: " + bundleElapsedTime + " ms");

        double improvementPercentage = (colFile.length() - bundleFile.length()) / (double) bundleFile.length();
        System.out.println("improvement % = " + improvementPercentage);


    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
