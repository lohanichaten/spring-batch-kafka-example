package learning.batch.producer;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.KafkaTemplate;

import com.github.javafaker.Faker;

import learning.batch.model.Customer;
import lombok.RequiredArgsConstructor;


@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

	private final StepBuilderFactory stepBuilderFactory;
	private final JobBuilderFactory jobBuilderFactory;
	private  final KafkaTemplate<Long,Customer> template;
	
	@Bean
	Job job() {
		return this.jobBuilderFactory
					.get("kafka-job")
					.start(start())
					.incrementer(new RunIdIncrementer())
					
					.build();
	}
	
	@Bean
	KafkaItemWriter<Long,Customer> kafkaItemWriter(){
		return new KafkaItemWriterBuilder<Long,Customer>()
						.kafkaTemplate(template)
						.itemKeyMapper(new Converter<Customer, Long>() {
							
							@Override
							public Long convert(Customer source) {
								
								return source.getId();
							}
						})
						.build();
	}
	
	
	@Bean
	ItemProcessor<Customer,Customer> processor(){
		
		return new ItemProcessor<Customer, Customer>() {

			@Override
			public Customer process(Customer item) throws Exception {
				System.out.println(item);
				return item;
			}
		};
	}

	@Bean
	ItemReader<Customer> itemReader(){
		var id=new AtomicLong();
		var faker=new Faker();
		return new ItemReader<Customer>() {

			@Override
			public Customer read()
					throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				var name=faker.name().fullName();
				if(id.incrementAndGet()<10_1000) {
					return new Customer(id.get(),name);
				}
				return null;
			}
		};
	}
	
	private Step start() {
	
		return this.stepBuilderFactory
					.get("read-kafka")
					.<Customer,Customer>chunk(10)
					.reader(itemReader())
					.processor(processor())
					.writer(kafkaItemWriter())
					.build();
					
		
	}
	
}
