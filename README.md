# Column Compressor

Column Compressor is a library that supports column based compression with row based semantics. It provides support for reading and writing data in blocks of rows.  Each of those blocks stores the data for each column sequentially.  Each column may use a specific encoding option for that particular column.  For example a column with large integer or long values that have a small variance from row to row may use variable length integer or long encoding.

## Supported Column Types

1. bytes: Accepts byte arrays as input and output.  Encoding/Decoding is left to calling class
2. Text255: Creates a dictionary of the top 255 words.  Useful for columns with commonly repeated string values.  All values outside of the top 255 are encoded as direct byte arrays.
3. RunLength: Encodes column values by counting the number of consecutive occurrences of each value.  For example the number 10 repeated 100 times would be encoded as 10,100
4. DeltaInt:  uses variable length integer encoding to record the delta between the current value and the previous value.  Useful for columns with large integer values that do not have large deltas from one value to the next.
5. DeltaLong:  uses variable length long encoding to record the delta between the current value and the previous value.  Useful for columns with large long values that do not have large deltas from one value to the next.

## Performance

No formal benchmarks are available at this time.  However on data sets that resemble web logs we are seeing 10-15% size reduction in the output data sets as compared to a traditional row-based compression scheme.

## Production Use?

AddThis is currently using this utility in a small number of production processes.
