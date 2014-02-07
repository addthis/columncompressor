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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Bytes;

public class DeltaIntColumn extends AbstractColumn<Integer> {

    private int prevValue = 0;

    public DeltaIntColumn(String name) {
        super(name);
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.DELTAINT;
    }

    @Override
    public void push(Integer columnValue) throws IOException {
        byteArrayOutputStream.write(VarInt.writeSignedVarInt(columnValue - prevValue));
        prevValue = columnValue;
    }

    @Override
    public byte[] flush() throws IOException {
        prevValue = 0;
        return super.flush();
    }

    public static List<Integer> readColumnValues(InputStream inputStream, int payloadLength) throws IOException {

        byte[] bytes = Bytes.readBytes(inputStream, payloadLength);
        List<Integer> byteValues = new ArrayList<>();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        int previousValue = 0;
        while (dis.available() > 0) {
            int value = VarInt.readSignedVarInt(dis) + previousValue;
            previousValue = value;
            byteValues.add(value);
            bos.reset();
        }
        return byteValues;
    }

}
