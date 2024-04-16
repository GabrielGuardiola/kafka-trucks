package com.helloworld.kafka.producers;
import java.io.IOException;
import org.apache.kafka.common.serialization.StringSerializer;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// Camiones
 
// 5 camiones
 
// Cantidad de kilómetros que ha realizado cada camión.
 
// Simular los camiones en un único productor.
 
// Los camiones van a mandar como mensaje el kilométro de la carretera por la que van
 
// Los camiones van a mandar 10 mediciones. en cada medición avanzan 1,3 km.
 
// Siempre van por la misma carretera
 
// Pero no sabemos en qué kilómetro de la carreta empiezan
 
// En la simulación cada camión debe empezar en un kilómetro aleatorio entre 10 y 20.
 
// Objetivos:
// - Plantear la estructura de topics ---> Un topic. km_posicion (gps), en la clave el id de camión.
// - Implementar el productor  --> productor asíncrono, opcionalmente tratar errores con callback
// - Utilizar un consumidor kafka-console-consumer
// - Objetivo parcial: 
//     En el consumidor debe aparecer todos los datos de todos los camiones.

public class AsyncProducer {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
    public static void main(final String[] args){
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        
        final String topic = "truck-topic-monitoring";

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 5; i++) {
            Random random = new Random();
            int initialKm = random.nextInt(10) + 10;
            log.info("Camión {} empieza en el kilómetro {}", i, initialKm);
            for (int j = 0; j < 10; j++) {
                initialKm += 1.3;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(initialKm));
                producer.send(record, new ProducerCallback());
            }
        }
        producer.close();

    }
    
}

class ProducerCallback implements Callback {
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			System.out.printf("Produced event to topic %s offset= %d partition=%d%n", 
					metadata.topic(), metadata.offset(), metadata.partition());
		} else {
			exception.printStackTrace();
		}
		
	}
}

