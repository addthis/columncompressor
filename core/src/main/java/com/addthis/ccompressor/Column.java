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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;

public interface Column<T> {

    int HEADER_SIZE = 9;

    String getName();

    ColumnType getColumnType();

    ValueObject getValue(Bundle bundle);

    /**
     * Suggest replacing with a pushBundle(Bundle bunle) or pushValue(ValueObject val) method.
     * This would elimintate the case analysis in row reader and compressor, abstract the column
     * type specific logic to its class where it belongs, and let us remove the generic typing
     * of this class which is really rather not appropriate.
     */
    void push(T bytes) throws IOException;

    byte[] flush() throws IOException;
}
