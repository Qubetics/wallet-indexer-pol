import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { walletHelper } from "../helpers";
import { CoinsModel, WalletModel } from "../models";
import polProcessHelper from "./process.helper";

class UpdateBalanceProcess {
  public pol_pending_balance_update_topic =
    config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_TOPIC;


  public updateBalanceConsumer = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: this.pol_pending_balance_update_topic,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              await this.updateBalance(JSON.parse(message.value.toString()));

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
            console.log(`🔄 Attempting to connect consumer updateBalanceConsumer...`);
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

  public updateBalance = async (data: any) => {
    try {
      if (data.queue_count > 5) {
        console.log(`Balance update attempts exceeded (5) for address: ${data.wallet_address}`);
        return;
      }
      const balance: any = await walletHelper.updateBalanceQueue(
        data.wallet_address,
        data.coin_data
      );
      if (balance !== null) {
        const currentBalance = data.balance !== undefined ? Number(data.balance) : undefined;
        if (currentBalance === undefined || currentBalance !== balance) {
          await WalletModel.update(
            { balance: Number(balance) },
            {
              where: {
                wallet_address: data.wallet_address,
                coin_id: data.coin_data.coin_id,
              },
            }
          );
        } else {
        }
      } else {
        if (data.queue_count <= 5) {

          const queueData: any = {
            coin_data: data.coin_data,
            wallet_address: data.wallet_address,
            queue_count: data.queue_count + 1,
          }
          if (data.balance !== undefined) {
            queueData.balance = data.balance;
          }
          await kafkaHelper.produceMessage(
            this.pol_pending_balance_update_topic,
            queueData
          );
        }
      }

    } catch (err: any) {
      console.error("Error in updateBalance", err);
    }
  };

  public updateWalletBalance = async () => {
    try {
      let coin_limit_count = 0;

      const GLOBALBALANCEUPDATECOUNT = `WalletCount_${CoinFamilyEnum.POL}`;
      const limit = 50;

      const findCoinCounter = await polProcessHelper.getKeyValuePair(
        config.config.REDISKEYS.ALL_PROCESSES,
        GLOBALBALANCEUPDATECOUNT
      );

      if (findCoinCounter) {
        coin_limit_count = Number(findCoinCounter);
      }
      const wallets: any = await WalletModel.findAll({
        attributes: ["wallet_address", "balance"],
        where: {
          coin_family: CoinFamilyEnum.POL,
        },
        include: [
          {
            attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
            model: CoinsModel,
            as: "coin_data",
          },
        ],
        limit,
        offset: coin_limit_count,
        order: [['wallet_id', 'ASC']],
        nest: true,
      });
      if (!wallets) {
        await polProcessHelper.setKeyValuePair(
          config.config.REDISKEYS.ALL_PROCESSES,
          GLOBALBALANCEUPDATECOUNT,
          "0"
        );
      }
      if (wallets.length > 0) {
        for (const wallet of wallets) {
          const coin_data = wallet.coin_data;
          if (!coin_data) return false;
          const payload = {
            coin_data: coin_data,
            wallet_address: wallet.wallet_address,
            balance: wallet.balance,
            queue_count: 0,
          };

          await kafkaHelper.produceMessage(
            this.pol_pending_balance_update_topic,
            payload
          );
        }
      }
      if (wallets.length < limit) {
        await polProcessHelper.setKeyValuePair(
          config.config.REDISKEYS.ALL_PROCESSES,
          GLOBALBALANCEUPDATECOUNT,
          "0"
        );
      } else {
        coin_limit_count += limit;
        await polProcessHelper.setKeyValuePair(
          config.config.REDISKEYS.ALL_PROCESSES,
          GLOBALBALANCEUPDATECOUNT,
          coin_limit_count.toString()
        );
      }
      return true;
    } catch (err: any) {
      console.error("Error in updateWalletBalanceByWallet", err);
    }
  };
}
export const updateBalanceProcess = new UpdateBalanceProcess();

