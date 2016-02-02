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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.LessBytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueObject;

public abstract class AbstractColumn<T> implements Column<T> {

    protected static final byte VERSION = 1;
    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private final String name;
    private BundleFormat lastFormat = null;
    private BundleField lastField = null;

    protected AbstractColumn(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ValueObject getValue(Bundle bundle) {
        if (bundle.getFormat() != lastFormat) {
            lastFormat = bundle.getFormat();
            lastField = lastFormat.getField(name);
        }
        return bundle.getValue(lastField);
    }

    @Override
    public byte[] flush() throws IOException {
        int totalLength = byteArrayOutputStream.size();
        byte[] columnBlockBytes = new byte[totalLength + HEADER_SIZE];
        writeColumnHeader(columnBlockBytes, VERSION, getColumnType(), totalLength);
        System.arraycopy(byteArrayOutputStream.toByteArray(), 0, columnBlockBytes, HEADER_SIZE, totalLength);
        byteArrayOutputStream.reset();
        return columnBlockBytes;
    }

    public static void writeColumnHeader(byte[] columnBlockBytes, byte version, ColumnType columnType, int totalLength) {
        columnBlockBytes[0] = version;
        System.arraycopy(LessBytes.toBytes(columnType.getId()), 0, columnBlockBytes, 1, 4);
        System.arraycopy(LessBytes.toBytes(totalLength), 0, columnBlockBytes, 5, 4);
    }

    public static ColumnHeader readHeader(InputStream inputStream) throws IOException {
        return new ColumnHeader((byte) inputStream.read(), ColumnType.get(LessBytes.readInt(inputStream)), LessBytes.readInt(inputStream));
    }

    protected void writeColumnChunk(byte[] bytes) throws IOException {
        // figure how big array is and split into n components of size 2^7 or smaller
        if (bytes.length == 0) {
            byteArrayOutputStream.write(VarInt.writeUnsignedVarInt(0));
        } else {
            int length = bytes.length;
            byteArrayOutputStream.write(VarInt.writeUnsignedVarInt(length));
            byteArrayOutputStream.write(bytes);
        }
    }

    public static List<byte[]> decodeBytes(InputStream inputStream) throws IOException {
        List<byte[]> bytes = new ArrayList<>();
        while (inputStream.available() > 0) {
            byte[] value = decodeColumnValue(inputStream);
            if (value.length != 0) {
                bytes.add(value);
            } else {
                bytes.add(new byte[0]);
            }
        }
        return bytes;
    }

    protected static byte[] decodeColumnValue(InputStream inputStream) throws IOException {
        int length = VarInt.readUnsignedVarInt(inputStream);
        return LessBytes.readBytes(inputStream, length);
    }

    public static Column createColumn(String name, ColumnType columnType) {
        Column c;
        switch (columnType) {
            case RAW:
                c = new DefaultColumn(name);
                break;
            case TEXT255:
                c = new Text255Column(name);
                break;
            case DELTAINT:
                c = new DeltaIntColumn(name);
                break;
            case DELTALONG:
                c = new DeltaLongColumn(name);
                break;
            case RUNLENGTH:
                c = new RunLengthColumn(name);
                break;
            default:
                throw new RuntimeException("Unexpected column type: " + columnType);
        }
        return c;
    }
}
