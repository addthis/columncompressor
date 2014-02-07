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

import java.io.IOException;

public class DefaultColumn extends AbstractColumn<byte[]> {

    private final ColumnType columnType = ColumnType.RAW;

    public DefaultColumn(String name) {
        super(name);
    }

    @Override
    public ColumnType getColumnType() {
        return columnType;
    }

    @Override
    public void push(byte[] columnValue) throws IOException {
        writeColumnChunk(columnValue);
    }

}
