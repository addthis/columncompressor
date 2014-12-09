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
package com.addthis.ccompressor.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.addthis.ccompressor.Column;
import com.addthis.ccompressor.ColumnRowCompressor;
import com.addthis.ccompressor.ColumnRowReader;
import com.addthis.ccompressor.DefaultColumn;
import com.addthis.ccompressor.DeltaIntColumn;
import com.addthis.ccompressor.DeltaLongColumn;
import com.addthis.ccompressor.RunLengthColumn;
import com.addthis.ccompressor.Text255Column;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnRowCompressorTest {

    @Test
    public void testWrite() throws Exception {
        ListBundleFormat format = new ListBundleFormat();
        List<Column> columns = new ArrayList<>();
        columns.add(new DefaultColumn("fooColumn"));
        columns.add(new DefaultColumn("dontread1"));
        columns.add(new DefaultColumn("barColumn"));
        columns.add(new Text255Column("dontread2"));
        columns.add(new Text255Column("textColumn"));
        columns.add(new RunLengthColumn("runlength"));
        columns.add(new RunLengthColumn("dontread3"));
        columns.add(new DeltaIntColumn("deltaint"));
        columns.add(new DeltaIntColumn("dontread4"));
        columns.add(new DeltaLongColumn("deltalong"));
        columns.add(new DeltaLongColumn("dontread5"));
        columns.add(new Text255Column("somenulls"));
        columns.add(new DefaultColumn("somenulls2"));
        columns.add(new DeltaIntColumn("somenulls3"));
        columns.add(new DeltaLongColumn("somenulls4"));
        columns.add(new RunLengthColumn("somenulls5"));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int blockSize = 1000;
        ColumnRowCompressor columnRowCompressor = new ColumnRowCompressor(columns, blockSize);
        int bundles = 100000;
        int startVal = 234242421;
        for (int i = 0; i < bundles; i++) {
            Bundle b = new ListBundle(format);
            b.setValue(format.getField("fooColumn"), ValueFactory.create("foobar"));
            b.setValue(format.getField("dontread1"), ValueFactory.create(22));
            b.setValue(format.getField("dontread2"), ValueFactory.create("dontread1me"));
            b.setValue(format.getField("dontread3"), ValueFactory.create("dontread1me"));
            b.setValue(format.getField("dontread4"), ValueFactory.create(22 + startVal));
            b.setValue(format.getField("dontread5"), ValueFactory.create(33 + startVal));
            b.setValue(format.getField("barColumn"), ValueFactory.create("barfoo"));
            b.setValue(format.getField("textColumn"), ValueFactory.create("textfoo"));
            b.setValue(format.getField("runlength"), ValueFactory.create("runlength"));
            b.setValue(format.getField("deltaint"), ValueFactory.create(startVal + i));
            b.setValue(format.getField("deltalong"), ValueFactory.create((long) startVal + i));
            if (i % 3 == 0) {
                b.setValue(format.getField("somenulls"), ValueFactory.create("somenulls" + i));
                b.setValue(format.getField("somenulls2"), ValueFactory.create("somenulls2"));
                b.setValue(format.getField("somenulls3"), ValueFactory.create(3));
                b.setValue(format.getField("somenulls4"), ValueFactory.create(4));
                b.setValue(format.getField("somenulls5"), ValueFactory.create("somenulls5"));
            }
            columnRowCompressor.write(b, bos);
        }
        columnRowCompressor.flush(bos);
        byte[] bytes = bos.toByteArray();
        ByteArrayOutputStream compressedBos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(compressedBos);
        gos.write(bytes);
        gos.close();

        ColumnRowReader reader = new ColumnRowReader(new GZIPInputStream(new ByteArrayInputStream(compressedBos.toByteArray()), 512), new String[]{"fooColumn", "barColumn", "textColumn",
                                                                                                                                                   "runlength", "deltaint", "deltalong", "somenulls", "somenulls2", "somenulls3", "somenulls4", "somenulls5"});
        Iterator<Bundle> results = reader.iterator();
        int resultCount = 0;
        while (results.hasNext()) {
            Bundle b = results.next();
            assertEquals(b.getValue(b.getFormat().getField("fooColumn")).asString().toString(), "foobar");
            assertEquals(b.getValue(b.getFormat().getField("barColumn")).asString().toString(), "barfoo");
            assertEquals(b.getValue(b.getFormat().getField("textColumn")).asString().toString(), "textfoo");
            assertEquals(b.getValue(b.getFormat().getField("runlength")).asString().toString(), "runlength");
            assertEquals("miss on " + resultCount, b.getValue(b.getFormat().getField("deltaint")).asLong().asNative().intValue(), startVal + resultCount);
            assertEquals("miss on " + resultCount, b.getValue(b.getFormat().getField("deltalong")).asLong().asNative().longValue(), (long) (startVal + resultCount));
            if (resultCount % 3 == 0) {
                assertEquals(b.getValue(b.getFormat().getField("somenulls")).asString().toString(), "somenulls" + resultCount);
                assertEquals(b.getValue(b.getFormat().getField("somenulls2")).asString().toString(), "somenulls2");
                assertEquals(b.getValue(b.getFormat().getField("somenulls3")).asLong().asNative().intValue(), 3);
                assertEquals(b.getValue(b.getFormat().getField("somenulls4")).asLong().asNative(), new Long(4));
                assertEquals(b.getValue(b.getFormat().getField("somenulls5")).asString().toString(), "somenulls5");
            }
            resultCount++;
        }
        assertEquals(bundles, resultCount);
    }
}
