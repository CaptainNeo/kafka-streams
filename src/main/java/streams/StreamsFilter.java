package streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/*
 * 토픽으로 들어온 문자열 데이터 중 문자열의 길이가 5보다 큰 경우만 필터링하는 스트림즈 애플리케이션을 스트림 프로세서를 사용하여 만들 수 있다.
 * 메시지 키 또는 메시지 값을 필터링하여 특정 조건에 맞는 데이터를 골라낼 때는 fiter() 메서드를 사용.
 * filter() 메서드는 스트림즈DSL에서 사용 가능한 필터링 스트림 프로세서이다.
 * 
 * ./bin/windows/kafka-topics.bat --bootstrap-server my-kafka:9092 --topic stream_log --create
 * ./bin/windows/kafka-topics.bat --bootstrap-server my-kafka:9092 --topic stream_log_filter --create
 * 
 * 이후 카프카 콘솔 프로듀서 와 카프카 콘솔 컨슈머를 작동
 * ./bin/windows/kafka-console-producer.bat --bootstrap-server my-kafka:9092 --topic stream_log
 * 상위 콘솔에서 값을 012345 입력시 StreamsFilter 되어 stream_log_filter에 적재
 * 
 * ./bin/windows/kafka-console-consumer.bat --bootstrap-server my-kafka:9092 --topic stream_log_filter
 */
public class StreamsFilter {
	
	private static String APPLICATION_NAME = "streams-filter-application";  // 카프카 컨슈머 그룹 아이디같이 활용된다.
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";  // 스트림 로그에 데이터가 적재되면 
    private static String STREAM_LOG_FILTER = "stream_log_filter";  // 스트림 로그 필터에 처리 로직 이후 필터되어 적재된다.

	public static void main(String[] args) {
		
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);  // <--- 소스프로세서
//	        KStream<String, String> filteredStream = streamLog.filter(
//	                (key, value) -> value.length() > 5);
//	        filteredStream.to(STREAM_LOG_FILTER);

        // filter 함수가 스트림 프로세서 , to.() 싱크 프로세서 이다.
        streamLog.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER);


        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();

	}

}
