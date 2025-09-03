import { Kafka } from 'kafkajs';
import * as Interface from '../interfaces/kafka.interface';
import { config } from '.';

class KafkaHelper {
    public kafka: any;

    private producer: any;

    // constructor() {
    //     this.connect();
    // }

    public async connect() {
        try {
            const KAFKA_BROKER = config.config.KAFKA_BROKER.split(',');
            console.log('KAFKA_BROKER:', KAFKA_BROKER);

            const kafkaCredentials: Interface.KafkaCrednetials = {
                clientId: String(config.config.KAFKA_CLIENT_ID),
                brokers: KAFKA_BROKER,
            };

            this.kafka = new Kafka(kafkaCredentials);
            this.producer = this.kafka.producer();
            await this.producer.connect();
            console.log('✅ Kafka producer connected');
        } catch (error) {
            console.error('❌ Error connecting Kafka producer:', error);
        }
    }

    public async produceMessage(topic: string, messageData: any) {
        try {
            await this.producer.send({
                topic: topic,
                acks: 1,
                messages: [{ value: JSON.stringify(messageData) }],
            });
        } catch (error) {
            console.log('🔥 ~ ~ produceMessage error', error);
        }
    }

}

export default new KafkaHelper();
