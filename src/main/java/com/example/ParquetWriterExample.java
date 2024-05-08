package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetWriterExample {
    public static void main(String[] args) throws IOException {
        // Define the Avro schema
        String schemaString = "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"city\",\"type\":\"string\"}]}";
        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a AvroParquetWriter
        Configuration conf = new Configuration();
        Path file = new Path("data.parquet");
        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(file)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        // Create a GenericRecord object and write it to the Parquet file
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "John");
        record.put("age", 25);
        record.put("city", "New York");
        writer.write(record);

        // Close the AvroParquetWriter
        writer.close();
    }
}