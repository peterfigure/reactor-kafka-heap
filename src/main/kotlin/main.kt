import com.google.protobuf.Message
import com.google.protobuf.Parser
import io.ys.PetDomain
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serializer
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

val logger = KotlinLogging.logger { }

fun main(args: Array<String>) {
    publishElementsToKafkaTopic()
    receiveElementsFromKafkaTopic()
}

fun receiveElementsFromKafkaTopic() {
    val kafkaReceiver = receiver(
        setOf(kafkaTopic),
        commitBatchSize = kafkaConfig.consumer.commitBatchSize,
        commitIntervalDuration = Duration.of(kafkaConfig.consumer.commitIntervalSeconds, ChronoUnit.SECONDS)
    ) {
        receiverOptions<Int, PetDomain.Pets>(kafkaConfig, "theTestGroupID")
            .maxCommitAttempts(100)
            .withKeyDeserializer(IntegerDeserializer())
            .withValueDeserializer(KafkaProtoDeserializer<PetDomain.Pets>(PetDomain.Pets.parser()))
    }

    val count = AtomicInteger(0)
    kafkaReceiver
        .doOnNext {
            logger.info("received $it")
        }
        .delayElements(Duration.ofMillis(50))
        .doOnNext {
            logger.info("processed key [${it.key()}] count [${count.incrementAndGet()}]")
            it.receiverOffset().commit()
        }
        .subscribe()
}

fun publishElementsToKafkaTopic() {
    val kafkaSender = sender {
        senderOptions<Int, PetDomain.Pets>(kafkaConfig)
            .withKeySerializer(IntegerSerializer())
            .withValueSerializer(KafkaProtoSerializer<PetDomain.Pets>())
            .stopOnError(true)
    }
    val elements = (1..10000).map {
        val pets = (1..100).map {
            PetDomain.Pet.newBuilder().setAge(Random().nextInt(100)).setName(UUID.randomUUID().toString()).build()
        }
        SenderRecord.create(ProducerRecord(kafkaTopic, it, PetDomain.Pets.newBuilder().addAllPets(pets).build()), it)
    }

    kafkaSender.send(Flux.fromIterable(elements))
        .subscribe { println("materialized $it") }
}

val kafkaTopic = "test-topic"
val kafkaConfig = KafkaConfig(
    bootstrapServer = "localhost:9092",
    consumer = KafkaConfig.KafkaConsumerConfig(
        clientID = "the-consumer",
        commitBatchSize = 100,
        commitIntervalSeconds = 2,
    ),
    producer = KafkaConfig.KafkaProducerConfig(
        clientID = "the-producer"
    ),
    securityProtocol = "plaintext",
    trustStorePath = "faketruststorepassword",
    trustStorePassword = "faketrustorepath",
    keystorePath = "fakekeystorepath",
    keystorePassword = "fakekeystorepassword",
    keyPassword = "fakekeypassword",
)

fun <K, V>senderOptions(kafkaConfig: KafkaConfig, retries: Int = 5): SenderOptions<K, V> {
    val props: MutableMap<String, Any> = HashMap()

    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaConfig.bootstrapServer
    props[ProducerConfig.CLIENT_ID_CONFIG] = kafkaConfig.producer.clientID
    props[ProducerConfig.ACKS_CONFIG] = "all"
    props[ProducerConfig.RETRIES_CONFIG] = retries
    props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = kafkaConfig.securityProtocol
    props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = kafkaConfig.trustStorePath
    props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = kafkaConfig.trustStorePassword
    props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = kafkaConfig.keystorePath
    props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = kafkaConfig.keystorePassword
    props[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = kafkaConfig.keyPassword
    return SenderOptions.create(props)
}

fun <K, V>sender(senderOptions: () -> SenderOptions<K, V>): KafkaSender<K, V> = KafkaSender.create(senderOptions())

fun <K, V>receiverOptions(kafkaConfig: KafkaConfig, groupId: String, offsetReset: String = "earliest"): ReceiverOptions<K, V> {
    val props: MutableMap<String, Any> = HashMap()

    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaConfig.bootstrapServer
    props[ConsumerConfig.CLIENT_ID_CONFIG] = kafkaConfig.consumer.clientID
    props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = offsetReset
    props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = kafkaConfig.securityProtocol
    props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = kafkaConfig.trustStorePath
    props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = kafkaConfig.trustStorePassword
    props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = kafkaConfig.keystorePath
    props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = kafkaConfig.keystorePassword
    props[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = kafkaConfig.keyPassword

    return ReceiverOptions.create(props)
}

fun <K, V>receiver(
    topics: Set<String>,
    commitBatchSize: Int = 0,
    commitIntervalDuration: Duration = Duration.ZERO,
    receiverOptions: () -> ReceiverOptions<K, V>
): Flux<ReceiverRecord<K, V>> {

    val options = receiverOptions()
        .subscription(topics)
        .commitBatchSize(commitBatchSize)
        .commitInterval(commitIntervalDuration)
        .addAssignListener { partitions -> logger.info("onPartitionsAssigned {}", partitions) }
        .addRevokeListener { partitions -> logger.info("onPartitionsRevoked {}", partitions) }

    return KafkaReceiver.create(options).receive()
}

data class KafkaConfig(
    val bootstrapServer: String,
    val consumer: KafkaConsumerConfig,
    val producer: KafkaProducerConfig,
    val securityProtocol: String,
    val trustStorePath: String,
    val trustStorePassword: String,
    val keystorePath: String,
    val keystorePassword: String,
    val keyPassword: String
) {
    data class KafkaConsumerConfig(
        val clientID: String,
        val commitBatchSize: Int,
        val commitIntervalSeconds: Long
    )

    data class KafkaProducerConfig(
        val clientID: String
    )

    fun withConsumerClientIDSuffix(suffix: String): KafkaConfig {
        return this.copy(consumer = this.consumer.copy(clientID = "${this.consumer.clientID}-$suffix"))
    }

    fun withProducerClientIDSuffix(suffix: String): KafkaConfig {
        return this.copy(producer = this.producer.copy(clientID = "${this.producer.clientID}-$suffix"))
    }
}

class KafkaProtoDeserializer<V : Message>(private val parser: Parser<V>) : Deserializer<V> {
    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun deserialize(topic: String?, data: ByteArray?): V? {
        return try {
            if (data != null) parser.parseFrom(data) else null
        } catch (e: Throwable) {
            // We have to log here since kafka eats the actual reason the message failed.
            logger.error("Failed to deserialize proto data [$data]", e)
            null
        }
    }
}

class KafkaProtoSerializer<V : Message> : Serializer<V> {
    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun serialize(topic: String?, data: V): ByteArray = data.toByteArray()
}