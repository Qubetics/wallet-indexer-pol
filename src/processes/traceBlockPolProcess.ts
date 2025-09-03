
import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { readBlockProcess } from "./readBlocksProcess";

class TraceBlockPolProcess {

  public startTraceBlockPolProcess = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.KAFKA_TRANSACTIONS.TRACE_BLOCK_POL_DEPOSIT_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: config.KAFKA_TRANSACTIONS.TRACE_BLOCK_POL_DEPOSIT_TOPIC,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              // console.log("🚀 ~ startTraceBlockEthProcess topic >>>>", topic);
              // console.log("🚀 ~ startTraceBlockEthProcess message.value >>>>", message.value.toString());
              await readBlockProcess.checkPolDeposit(JSON.parse(message.value.toString()))

            } catch (error) {
              console.error(`❌ Error processing message:`, error);
            }
          },
        });
        isConnected = true;
      } catch (error) {
        console.error(`❌ Failed to connect consumer:`, error);
        isConnected = false;
        throw error;
      }
    };

    // ✅ Add reconnection logic
    const startWithReconnection = async () => {
      while (true) {
        try {
          if (!isConnected) {
            console.log(`🔄 Attempting to connect consumer startDepositQueue...`);
            await connectConsumer();
          }

          // Keep the consumer alive
          await new Promise(resolve => setTimeout(resolve, 30000));

        } catch (error) {
          console.error(`❌ Consumer connection error:`, error);
          isConnected = false;

          // Wait before retrying
          console.log(`⏳ Waiting 10 seconds before reconnecting...`);
          await new Promise(resolve => setTimeout(resolve, 10000));
        }
      }
    };

    // Start the consumer with reconnection
    await startWithReconnection();
  };

}

export const traceBlockPolProcess = new TraceBlockPolProcess();
