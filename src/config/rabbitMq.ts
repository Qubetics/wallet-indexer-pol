// import * as amqp from "amqplib/callback_api";
// import { LogsConstants } from "../constants/logs.constants";
// // import { RABBIT_MQ } from "./stage";
// /*"RABBIT_MQ": "amqp://guest:guest@localhost:5672",**/
// import { config } from "./config";

// class RabbitMq {
//   public channel: amqp.Channel;

//   constructor() {
//     this.startServer();
//   }

//   public startServer = async () => {
//     try {
//       await this.connect();
//     } catch (error) {
//       console.error((error as Error).message);
//     }
//   };

//   public connect = async () => {
//     amqp.connect(config.RABBIT_MQ || "", (err, conn) => {
//       if (err) console.error(LogsConstants.RABBIT_MQ_CONNECTION_FAILED, err);
//       console.log(LogsConstants.RABBIT_MQ_CONNECTED);

//       conn.createChannel((err, ch) => {
//         if (err) console.error(LogsConstants.RABBIT_MQ_CHANNEL_FAILED, err);
//         this.channel = ch;
//         console.log(LogsConstants.RABBIT_MQ_CHANNEL_CREATED);
//       });
//     });
//   };

//   public assertQueue = async (queueName: string) => {
//     this.channel.assertQueue(queueName, { durable: true }, (err, res) => {
//       if (err) console.error(`${LogsConstants.ASSERT_QUEUE_FAILED} ${queueName}`);
//     });
//   };

//   public sendToQueue = async (queueName: string, msg: Buffer) => {
//     this.channel.sendToQueue(queueName, msg);
//   };

//   public consumeQueue = async (
//     queueName: string,
//     cb: (data: any) => Promise<void>
//   ) => {
//     await this.assertQueue(queueName);
//     this.channel.prefetch(1);
//     this.channel.consume(queueName, async (msg) => {
//       if (!msg) return;
//       const data: any = JSON.parse(msg.content.toString());
//       await cb(data);
//       this.channel.ack(msg);
//     });
//   };
//   public async send_tx_to_queue(queueName: string, data: Buffer) {
//     this.assertQueue(queueName)
//     this.channel.sendToQueue(queueName, data)
//   };
//   public async only_assert_queue(queueName: any) {
//     this.channel.assertQueue(queueName, { durable: true }, (err: any, res: any) => {
//       if (err) console.error(`assertQueue err`, err);
//     });
//   }

//   public async only_consume_queue(queue: string, fun: any) {
//     this.channel.consume(queue, async (msg: any) => {
//       let data = JSON.parse(msg.content.toString());
//       fun(data, msg);
//       this.channel.ack(msg);
//       return true;
//     },
//       { noAck: false });
//   }
//   public async queue_length(queueName: string) {
//     try {
//       let queue_length: any = await this.assertQueueToGetLength(queueName)
//       console.log("queue_length>>>", queue_length)
//       return queue_length;

//     } catch (err: any) {
//       console.error("Error in queue_length>>", err);
//     }
//   }
//   public assertQueueToGetLength = async (queueName: string): Promise<number> => {
//     return new Promise((resolve, reject) => {
//       this.channel.assertQueue(queueName, { durable: true }, (err, res) => {
//         if (err) {
//           console.error(`Error in ${LogsConstants.ASSERT_QUEUE_FAILED} ${queueName}`);
//           reject(err);
//         } else {
//           resolve(res.messageCount);
//         }
//       });
//     });
//   };
// }

// export default new RabbitMq();
