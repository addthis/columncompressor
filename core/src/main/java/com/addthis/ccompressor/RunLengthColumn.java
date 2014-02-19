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

package com.addthis.ccompressor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Bytes;


public class RunLengthColumn extends AbstractColumn<byte[]> {

    private int runCounter;
    private ByteArrayWrapper prevValue = null;
    private final List<RunCount> runCountSet = new ArrayList<>();

    public RunLengthColumn(String name) {
        super(name);
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.RUNLENGTH;
    }

    @Override
    public void push(byte[] columnValue) throws IOException {
        ByteArrayWrapper wrapper = new ByteArrayWrapper(columnValue);
        if (prevValue == null || prevValue.equals(wrapper)) {
            runCounter++;
            prevValue = wrapper;
        } else {
            flushCounter(wrapper);
        }
    }

    private void flushCounter(ByteArrayWrapper bytes) {
        if (runCounter > 0) {
            runCountSet.add(new RunCount(runCounter, prevValue));
        }
        prevValue = bytes;
        if (bytes != null) {
            runCounter = 1;
        } else {
            runCounter = 0;
        }
    }

    @Override
    public byte[] flush() throws IOException {
        flushCounter(null);
        if (runCountSet.size() > 0) {
            for (RunCount runCount : runCountSet) {
                byteArrayOutputStream.write(VarInt.writeUnsignedVarInt(runCount.runLength));
                writeColumnChunk(runCount.key.getData());
            }
        }
        runCountSet.clear();
        int totalLength = byteArrayOutputStream.size();
        byte[] columnBlockBytes = new byte[totalLength + HEADER_SIZE];
        writeColumnHeader(columnBlockBytes, VERSION, ColumnType.RUNLENGTH, totalLength);
        System.arraycopy(byteArrayOutputStream.toByteArray(), 0, columnBlockBytes, HEADER_SIZE, totalLength);
        byteArrayOutputStream.reset();
        return columnBlockBytes;

    }

    public static List<String> readColumnValues(InputStream inputStream, int payloadLength) throws IOException {

        byte[] bytes = Bytes.readBytes(inputStream, payloadLength);

        List<String> byteValues = new ArrayList<>();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        while (bis.available() > 0) {
            int runLength = VarInt.readUnsignedVarInt(bis);
            byte[] value = decodeColumnValue(bis);
            for (int i = 0; i < runLength; i++) {
                byteValues.add(new String(value));
            }
        }
        return byteValues;
    }

    private class RunCount {

        private final int runLength;
        private final ByteArrayWrapper key;

        private RunCount(int runLength, ByteArrayWrapper key) {
            this.runLength = runLength;
            this.key = key;
        }
    }
}
