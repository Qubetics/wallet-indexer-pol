import { TxType, WebHookTxType } from "../types/ERC20Tokens.types";
import { NotificationInterface, TrnxHistoryModel } from "../models";
import { config } from "../config/config";
import polProcessHelper from "./process.helper";
import { BlockChainStatusEnum, BooleanEnum, NotificationTypeEnum, TrnxTypeEnum, TxReqTypesEnum } from "../enum";
import { dbHelper, utilityHelper, walletHelper } from "../helpers"
import { Op } from "sequelize";
import WalletModel from "../models/tables/model.wallets";
import kafkaHelper from "../config/kafka.helper";
import { coinData, pushNotification } from "../interfaces/global.interface";


class DepositWithdrawProcess {
  public pol_deposit_withdraw_group_id = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID;
  public pol_deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

  public startDepositWithdrawConsumer = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: this.pol_deposit_withdraw_group_id,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: this.pol_deposit_withdraw_topic,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              console.log('🚀 ~ startDepositWithdrawConsumer topic', topic);
              console.log('🚀 ~ startDepositWithdrawConsumer message.value', message.value.toString());
              await this.checkIfOurTx(JSON.parse(message.value.toString()));

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

  private checkIfOurTx = async (data: TxType) => {
    if (data.amount == 0) {
      console.log("amount === 0")
    } else {
      console.log("amount is more than 0", data)
      let coinData: any = null;
      if (data.token) {
        console.log("token trnx")
        coinData = data.token;
      } else if (data.isNFTToken) {
        coinData = data.isNFTToken;
      } else {
        console.log("coin trnx")
        coinData = await polProcessHelper.checkIfContract('NativeCoin');
      }
      const webhookTx: WebHookTxType = {
        coin: {
          coinId: coinData.coin_id,
          isToken: coinData.is_token,
          tokenType: coinData.token_type,
          coinSymbol: coinData.coin_symbol,
          coinFamily: coinData.coin_family,
          cmcId: coinData.cmc_id,
          tokenAddress: Number(coinData.is_token) === Number(BooleanEnum.true) ? coinData.token_address : coinData.coin_symbol,
          addedBy: coinData.added_by
        },
        txId: data.txId,
        toAddress: data.toAddress,
        fromAddress: data.fromAddress,
        blockId: data.blockId,
        amount: data.amount,
        isNFTToken: data.isNFTToken || false,
        status: 'completed',
        blockchainStatus: BlockChainStatusEnum.CONFIRMED,
        txType: data.txType,
        trnxFee: data.trnxFee
      };
      console.log(webhookTx, "DEPOSIT");
      await this.saveNewTrnx(webhookTx);
    }

  };

  private saveNewTrnx = async (
    trnxData: WebHookTxType
  ) => {
    try {
      const txHash: string = trnxData.txId;
      const toAddress: string = trnxData.toAddress;
      const fromAddress: string = trnxData.fromAddress;
      const amount: number = trnxData.amount;
      const status: string = trnxData.status;

      const coinData: coinData = {
        coin_id: trnxData.coin.coinId,
        is_token: trnxData.coin.isToken,
        token_type: trnxData.coin.tokenType,
        coin_symbol: trnxData.coin.coinSymbol,
        coin_family: trnxData.coin.coinFamily,
        cmc_id: trnxData.coin.cmcId,
        token_address: Number(trnxData.coin.isToken) === Number(BooleanEnum.true) ? trnxData.coin.tokenAddress : trnxData.coin.coinSymbol,
        added_by: trnxData.coin.addedBy
      }

      let blockNumber: number;
      if (typeof trnxData?.blockId === "string" && trnxData?.blockId?.startsWith("0x")) {
        // convert hex string → decimal
        blockNumber = parseInt(trnxData.blockId, 16);
      } else {
        // already a number or decimal string
        blockNumber = Number(trnxData?.blockId);
      }

      const blockchain_status: BlockChainStatusEnum = trnxData.blockchainStatus;
      const trnx_fee: any = trnxData.trnxFee ? trnxData.trnxFee : 0;
      let tx_type: TrnxTypeEnum = trnxData.txType ? trnxData.txType : TrnxTypeEnum.DEPOSIT;
      const fiat_currency = "USD";
      let checkOldTrnx: any = null;
      if (tx_type == TrnxTypeEnum.WITHDRAW) {
        checkOldTrnx = await TrnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: txHash,
            },
            from_adrs: {
              [Op.like]: fromAddress,
            },
            coin_id: coinData.coin_id,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnx == null) {
          const checkCoinInWallet: any = await WalletModel.findOne({
            attributes: ["coin_id", "coin_family"],
            where: {
              wallet_address: fromAddress,
              coin_id: coinData.coin_id
            },
            raw: true
          })
          if (!checkCoinInWallet) {
            console.error(`Coin not exist under this wallet.`);
            return false;
          }
        }
      } else if (tx_type == TrnxTypeEnum.DEPOSIT) {
        checkOldTrnx = await TrnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: txHash,
            },
            to_adrs: {
              [Op.like]: toAddress,
            },
            coin_id: coinData.coin_id,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
          logging: true
        });
      }

      if (checkOldTrnx) {
        console.error(`Transaction ${txHash} is already exists.`);
        return false;
      }

      if (tx_type !== TrnxTypeEnum.WITHDRAW && checkOldTrnx && checkOldTrnx.tx_id == txHash) {
        tx_type = checkOldTrnx.type;
      }

      const addresses = [fromAddress, toAddress].filter(Boolean);
      const results = await polProcessHelper.getMultipleKeyValuePairs(`${config.POL_WALLET_ADDRESS}`, addresses);
      if (results) {
        const [addressWithdraw, addressDeposit]: any = results as string[];


        const userId: number = addressWithdraw ? JSON.parse(addressWithdraw)?.user_id : 0;
        const toUserId: number = addressDeposit ? JSON.parse(addressDeposit)?.user_id : 0;

        const trx_data: any = {
          user_id: userId,
          to_user_id: toUserId,
          req_type: TxReqTypesEnum.EXNG,
          from_adrs: fromAddress,
          to_adrs: toAddress,
          coin_id: coinData.coin_id,
          coin_family: coinData.coin_family,
          amount: amount,
          status: status,
          block_id: blockNumber,
          tx_id: txHash,
          blockchain_status: blockchain_status,
          type: tx_type,
          tx_fee: trnx_fee,
          fiat_type: fiat_currency
        }
        const newTrnx: any = await TrnxHistoryModel.create(trx_data);

        if (addressDeposit) {
          const trnxTypeD: string = "Deposit";
          /** update to adrs wallet balance */
          const balance: any = await walletHelper.updateBalance(
            toAddress, /// wallet_address,
            coinData /// coin
          );
          await dbHelper.checkCoinInWallet(toAddress, coinData, balance)
          const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
            amount
          )} ${coinData?.coin_symbol?.toUpperCase()} has been confirmed.`;

          const notifData: pushNotification = {
            title: NotificationTypeEnum.DEPOSIT?.toUpperCase(),
            message: notiMsg,
            amount: amount,
            from_user_id: userId,
            to_user_id: toUserId,
            wallet_address: toAddress,
            tx_id: newTrnx.id,
            coin_symbol: coinData.coin_symbol,
            coin_id: coinData.coin_id,
            coin_family: trx_data?.coin_family,
            tx_type: tx_type,
            notification_type: trnxTypeD,
          };
          utilityHelper.SendNotification(notifData);
        }

        if (addressWithdraw) {
          let trnxTypeW: string = "Withdraw";

          switch (tx_type) {
            case TrnxTypeEnum.SMARTCONTRACTINTERACTION:
              trnxTypeW = "Smart Contract Interaction";
              break;
            case TrnxTypeEnum.APPROVE:
              trnxTypeW = "Approval";
              break;
            case TrnxTypeEnum.SWAP:
              trnxTypeW = "Swap";
              break;
            case TrnxTypeEnum.CROSS_CHAIN:
              trnxTypeW = "Cross Chain Swap";
              break;
            default:
              break;
          }

          await walletHelper.updateBalance(
            fromAddress, /// wallet_address,
            coinData /// coin
          );
          if (coinData.is_token) {
            const nativeCoin: any =
              await polProcessHelper.checkIfContract('NativeCoin');
            await walletHelper.updateBalance(
              fromAddress, /// wallet_address,
              nativeCoin /// coin
            );
          }


          const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(
            amount
          )} ${coinData.coin_symbol?.toUpperCase()} has been confirmed.`;

          const notifData: pushNotification = {
            title: trnxTypeW?.toUpperCase(),
            amount: amount,
            message: notiMsg,
            from_user_id: 0,
            to_user_id: userId,
            wallet_address: fromAddress,
            tx_id: newTrnx.id,
            tx_type: tx_type,
            coin_symbol: coinData.coin_symbol,
            coin_id: coinData.coin_id,
            coin_family: trx_data?.coin_family,
            notification_type: trnxTypeW,
          };

          utilityHelper.SendNotification(notifData);
        }

        return true;
      } else {
        console.log('Not own address');
      }
    } catch (error: any) {
      console.error(`SaveTrnx_Process-saveNewTrnx error >>>`, error);
      return false;
    }
  };

}
export const depositWithdrawProcess = new DepositWithdrawProcess();
