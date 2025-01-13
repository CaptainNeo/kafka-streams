package streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

/*
 * KTable과 KStream 은 케시지 키를 기준으로 조인할 수 있다. 대부분의 데이터베이스는 정적으로 저장된 데이터를 조인하여 사용했지만 
 * 카프카에서는 실시간으로 들어오는 데이터들을 조인할 수 있다. 사용자의 이벤트 데이터를 데이터베이스에 저장하지 않고도 조인하여 스트리밍 처리할 수 있다는 장점이 있다.
 * 이를 통해 이벤트 기반 트스리밍 데이터 파이프라인을 구성할 수 있는 것이다. 
 * 최근메시지키에대해서 메시지 값을 사용 ktable task내부에서 mv 머티리얼라이브쥬를 사용 
 * kstream 은 컨슈머에서 poll() 과 비슷하다.
 * 
 * 이름을 메시지 키, 주소를 메시지 값으로 가자고 있는 KTable이 있고 이름을 메시지 키, 주문한 물품을 메시지 값으로 가지고 있는 KStream이 존재한다고 
 * 가정하자. 사용자가 물품을 주문하면 이미 토픽에 저장된 이름:주소로 구성된 KTable과 조인하여 물품과 주소가 조합된 데이터를 새로 생성할 수 있다.
 * 
 * KStream                           KTable
 * doodoo 삼겹살 3kg					doodoo 광명
 * eun    토마토 5kg					eun    안산
 * 
 *                   JOIN
 *                 doodoo 삼겹살 3kg send to 광명
 *                 eun    토마도 5kg send to 안산
 * 
 * 만약 사용자의 주소가 변경되는 경우엔 어떻게 될까? KTable은 동일한 메시지 키가 들어올 경우 가장마지막의 레코드를 유효한 데이터로 보기 때문에 가장 최근에
 * 바뀐 주소로 조인을 수행할 것이다.
 * 
 * 스트림 데이터 join을 위한 토픽 생성 
 * ./bin/windows/kafka-topics.bat --create --bootstrap-server my-kafka:9092 --partitions 3 --topic address
 * ./bin/windows/kafka-topics.bat --create --bootstrap-server my-kafka:9092 --partitions 3 --topic order
 * ./bin/windows/kafka-topics.bat --create --bootstrap-server my-kafka:9092 --partitions 3 --topic order_join
 */
public class KStreamJoinKTable {
	
	private static String APPLICATION_NAME = "order-join-application";
	private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
	private static String ADDRESS_TABLE = "address";
	private static String ORDER_STREAM = "order";
	private static String ORDER_JOIN_STREAM = "order_join";

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		
		// 소스 프로세스 생성 2개 
		KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);  // 가장 최신의 키의 값만 사용 
		KStream<String, String> orderStream = builder.stream(ORDER_STREAM);
		
		orderStream.join(addressTable, (order, address) -> order + " send to " + address).to(ORDER_JOIN_STREAM);
		
		KafkaStreams streams;
		
		streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
		
		
		
		
		
		

	}

}


//
//
// ./bin/windows/kafka-console-producer.bat --bootstrap-server my-kafka:9092 --topic address --property "parse.key=true" --property "key.separator=:"
// >doodoo:Seoul
// >somin:Newyork
// >doodoo:Siheung
// >somin:Newyork
//
// ./bin/windows/kafka-console-producer.bat --bootstrap-server my-kafka:9092 --topic order --property "parse.key=true" --property "key.separator=:"
// >somin:cup
// >somin:cup
// >doodoo:iPhone 17
//
// ./bin/windows/kafka-console-consumer.bat --bootstrap-server my-kafka:9092 --topic order_join --from-beginning
// cup send to Newyork
// cup send to Newyork
// cup send to Newyork
// iPhone send to Busan
//
