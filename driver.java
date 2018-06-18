package khafka;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

//import khafka.KafkaProducerSrvc;

public class driver{
public static void main(String[] args) throws IOException{
if(args.length!=2)
{
	System.out.println("Driver: <File path> <seperator>");
}
System.out.println("Input1" + args[0] + "input2" + args[1] );
Properties prop = new Properties();
prop.put("bootstrap.servers","localhost:9092");
prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
ProducerRecord<String, String> producerRecord= null;

File file = new File(args[0]);
FileReader fileReader = new FileReader(file);
BufferedReader bufferedReader = new BufferedReader(fileReader);
String line;
String separator = args[1];
String topic = "";
String key = "";
String value = "";
//KafkaProducerSrvc kafkaProducer=null;
while ((line = bufferedReader.readLine()) != null) {
	System.out.println("Entere while loop");
	String[] items = line.split(separator); 
	topic = items[0];
	key = items[1];
	value = items[2];
	producerRecord =new ProducerRecord<String,String>(topic,key,value);
producer.send(producerRecord);	
System.out.printf("Record sent to topic: %s. Key:%s, Vale:%s\n", topic, key, value);
}
	producer.close();
	System.out.println("Contents of file:");
}
}