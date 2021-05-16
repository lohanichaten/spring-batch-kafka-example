package learning.batch.consumer;

import java.util.List;
import java.util.Properties;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import learning.batch.model.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
@Log
public class ConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	private final StepBuilderFactory stepBuilderFactory;
	private final JobBuilderFactory jobBuilderFactory;
	private  final KafkaTemplate<Long,Customer> template;
	private final KafkaProperties kafkaProperties;
	
	
	@Bean
	Step step() {
		return this.stepBuilderFactory
					.get("kafka-reader-step")
					.<Customer,Customer>chunk(10)
					.reader(kafkaReader())
					.writer(writer())
					.build();
	}
	
	@Bean
	ItemWriter<Customer> writer(){
		return new ItemWriter<Customer>() {

			@Override
			public void write(List<? extends Customer> items) throws Exception {
					items.forEach(item->System.out.println(item));		
			}
		};
	}
	
	
	private KafkaItemReader<Long,Customer> kafkaReader() {
		var props=new Properties();
		props.putAll(this.kafkaProperties.buildConsumerProperties());
		return new KafkaItemReaderBuilder<Long,Customer>()
					.consumerProperties(props)
					.saveState(true)
					.name("batch-reader")
					.topic("customers")
					.partitions(0)
					.build();
					
	}


	@Bean
	Job job() {
		
		return this.jobBuilderFactory
					.get("kafka-consumer")
					.start(step())
					.incrementer(new RunIdIncrementer())
					.build();
					
	}
	
	
}
