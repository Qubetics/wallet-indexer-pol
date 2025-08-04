import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { Wallet_Helper } from "../helpers";
import { CoinsModel, WalletModel } from "../models";
import polProcessHelper from "./process.helper";

class UpdateBalanceController {
  public pol_pending_balance_update_topic =
    config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_TOPIC;

  public update_wallet_balance = async () => {
    let consumer = await kafkaHelper.kafka.consumer({
      groupId: config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_GROUP_ID,
    });
    await consumer.subscribe({
      topic: this.pol_pending_balance_update_topic,
      fromBeginning: true,
    });
    await consumer
      .run({
        eachMessage: async ({
          topic,
          partition,
          message,
        }: {
          topic: any;
          partition: any;
          message: any;
        }) => {
          console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
          console.log(
            "🚀 ~ startTxStatusUpdateQueue message.value >>>>",
            message.value.toString()
          );
          await this.updateBalance(JSON.parse(message.value.toString()));
        },
      })
      .catch((err: Error) => {
        console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
      });
  };

  public updateBalance = async (data: any) => {
    try {
      if (data.queue_count < 10) {
        let balance = await Wallet_Helper.Update_Balance_Queue(
          data.wallet_address,
          data.coin_data
        );
        if (balance !== null) {
          if (data?.balance && data.balance == balance) {
            return;
          }
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
          if (data.queue_count <= 5) {
            await kafkaHelper.produceMessage(
              this.pol_pending_balance_update_topic,
              {
                coin_data: data.coin_data,
                wallet_address: data.wallet_address,
                queue_count: data.queue_count + 1,
              }
            );
          }
        }

        console.log("balance:::", balance);
      } else {
        console.log(`updating balance attempts are more than 10 `);
      }
    } catch (err: any) {
      console.error("Error in updateBalance", err);
    }
  };

  public updateWalletBalanceByWallet = async () => {
    try {
      let coin_limit_count = 0;

      const GLOBALBALANCEUPDATECOUNT = `WalletCount_${CoinFamilyEnum.POL}`;
      const limit = 50;

      const findCoinCounter = await polProcessHelper.getKeyValuePair(
        config.config.REDISKEYS.ALL_PROCESSES,
        GLOBALBALANCEUPDATECOUNT
      );
      console.log(
        "🚀 ~ polUpdateBalanceController ~ publicupdateWalletBalanceByWal ~ findCoinCounter:",
        findCoinCounter
      );

      if (findCoinCounter) {
        coin_limit_count = Number(findCoinCounter);
      }
      console.log(
        "🚀 ~ polUpdateBalanceController ~ publicupdateWalletBalanceByWal ~ coin_limit_count:",
        coin_limit_count
      );
      const wallets: any = await WalletModel.findAll({
        attributes: ["wallet_address", "coin_id", "coin_family", "balance"],
        where: {
          coin_family: CoinFamilyEnum.POL,
        },
        include: [
          {
            model: CoinsModel,
            as: "coin_data",
          },
        ],
        limit,
        offset: coin_limit_count,
        nest: true,
        logging: true,
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
            queue_count: 0,
          };

          await kafkaHelper.produceMessage(
            this.pol_pending_balance_update_topic,
            payload
          );
        }
      }
      if (wallets.length < limit) {
        console.log("inside wallets limit");
        await polProcessHelper.setKeyValuePair(
          config.config.REDISKEYS.ALL_PROCESSES,
          GLOBALBALANCEUPDATECOUNT,
          "0"
        );
      } else {
        coin_limit_count += limit;
        console.log("incrementing cnt");
        await polProcessHelper.setKeyValuePair(
          config.config.REDISKEYS.ALL_PROCESSES,
          GLOBALBALANCEUPDATECOUNT,
          coin_limit_count.toString()
        );
      }
      console.log("\nEND");
      return true;
    } catch (err) {
      console.error(
        "Error in updateWalletBalanceByWallet",
        (err as Error).message
      );
    }
  };
}

export default new UpdateBalanceController();
