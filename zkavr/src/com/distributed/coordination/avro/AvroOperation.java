package com.distributed.coordination.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class AvroOperation {
	
	Schema schema = null;
	GenericRecord recordWrite = null;
	
	public AvroOperation(String schemaPath) throws IOException {
		schema = new Schema.Parser().parse(new File(schemaPath));
		recordWrite = new GenericData.Record(schema);
	}
	
	public byte[] serialize(Map<String,Object> m) throws IOException {
		byte[] serializedBytes = null;
		
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			recordWrite.put(entry.getKey(), entry.getValue());
		}
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(recordWrite, encoder);
		encoder.flush();
		out.close();
		
		serializedBytes = out.toByteArray();
		return serializedBytes;
	}
	
	public String deserialize(byte[] serializedBytes) {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		
		Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
		try {
			GenericRecord recordRead = datumReader.read(null, decoder);
			return recordRead.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
}

