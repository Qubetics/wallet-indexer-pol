import { config } from "../config/config";
import { BlockChainStatusEnum, BooleanEnum, NotificationTypeEnum, TokenStandard, TrnxTypeEnum } from "../enum";
import { NotificationInterface, TrnxHistoryModel } from "../models";
import { utilityHelper, walletHelper, blockchainHelper, dbHelper } from "../helpers";
import polProcessHelper from "./process.helper";
import { Op } from "sequelize";
// import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt, TransactionResponse } from "ethers";
import { coinData, pendingTrnx, trnxStatusUpdate, updateBroadCastTrnx } from "../interfaces/global.interface";
class UpdateInternalTrnxStatusProcess {

  public pol_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.POL_PENDING_WITHDRAWAL_TX_TOPIC;
  public pol_pending_withdrawal_tx_group_id = config.KAFKA_TRANSACTIONS.POL_PENDING_WITHDRAWAL_TX_GROUP_ID;

  public startTxStatusUpdateConsumer = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: this.pol_pending_withdrawal_tx_group_id,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: this.pol_pending_withdrawal_tx_topic,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
              console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
              await this.getTx(JSON.parse(message.value.toString()))

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
            console.log(`🔄 Attempting to connect consumer startTxStatusUpdateConsumer...`);
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
  // public startTxStatusUpdateConsumer = async () => {
  //   const consumer = await kafkaHelper.kafka.consumer({ groupId: this.bsc_pending_withdrawal_tx_group_id });
  //   await consumer.subscribe({ topic: this.bsc_pending_withdrawal_tx_topic, fromBeginning: true });
  //   await consumer.run({
  //     eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
  //       console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
  //       console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
  //       await this.getTx(JSON.parse(message.value.toString()))
  //     }
  //   }).catch((err: Error) => {
  //     console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
  //   });
  // };

  private getTx = async (data: pendingTrnx) => {
    try {
      // Initialize provider
      const provider = blockchainHelper.provider;

      const transaction: TransactionResponse | null = await provider.getTransaction(data.txId);
      if (transaction && transaction?.blockNumber) {
        const transactionReceipt: TransactionReceipt | null = await provider.getTransactionReceipt(data.txId);

        let gasReverted: number | undefined;
        const gasUsed = transactionReceipt?.gasUsed || BigInt(0);
        const gasPrice = transaction.gasPrice || BigInt(0);
        const gasTotal = transaction.gasLimit || BigInt(0);
        const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', gasPrice);
        const gasInPol: number = Number(ethers.formatEther(txFee));

        if (gasTotal > BigInt(0) && gasUsed > BigInt(0)) {
          const gasDiff = await utilityHelper.bigNumberSafeMath(gasTotal, '-', gasUsed);
          const reverted = await utilityHelper.bigNumberSafeMath(gasPrice, '*', gasDiff);
          gasReverted = Number(ethers.formatEther(reverted));
        }

        if (transactionReceipt?.status && transaction.blockHash != null) {
          const txUpdate = {
            coin: {
              coinId: data.coinId,
              isToken: data.isToken,
              tokenType: data.tokenType,
              tokenAddress: data.tokenAddress,
            },
            status: "success",
            txId: transaction.hash,
            gasReverted: gasReverted !== undefined ? gasReverted : undefined,
            fromAddress: transactionReceipt.from.toLowerCase(),
            txFee: gasInPol
          };
          await this.sendTxUpdate(txUpdate);
        } else if (transactionReceipt && transactionReceipt?.status === 0) {
          const txUpdate: trnxStatusUpdate = {
            coin: {
              coinId: data.coinId,
              isToken: data.isToken,
              tokenType: data.tokenType,
              tokenAddress: data.tokenAddress,
            },
            status: "failed",
            txId: transaction.hash,
            gasReverted: gasReverted !== undefined ? gasReverted : undefined,
            fromAddress: transactionReceipt.from.toLowerCase(),
            txFee: gasInPol
          };
          await this.sendTxUpdate(txUpdate);
        }
      }
    } catch (err: any) {
      console.error('getTx >>>>>', err.message);
    }
  };

  private sendTxUpdate = async (txData: trnxStatusUpdate) => {
    try {
      console.log('txData.coin >>', txData.coin);
      let coinData: any = {};
      if (!txData.coin?.isToken) {
        console.log('in case of coin');
        coinData = await polProcessHelper.checkIfContract('NativeCoin');
      } else if (txData.coin?.tokenType?.toLowerCase() == TokenStandard.ERC20?.toLowerCase()) {
        coinData = await polProcessHelper.checkIfContract(txData.coin.tokenAddress);
      }
      console.log('coin_data >>', coinData);
      if (coinData) {
        // let coin_data: any = await bscProcessHelper.CoinByCoinId(txData.coin_id);
        // const coin_data: any = await CoinsModel.findOne({ attributes: ['is_token', 'token_type', 'coin_symbol', 'coin_family', 'cmc_id', 'coin_symbol', 'added_by'], where: { coin_id: txData.coin_id } });
        const trxData: updateBroadCastTrnx = {
          status: txData?.status,
          txId: txData?.txId,
          gasReverted: txData?.gasReverted,
          txFee: txData?.txFee,
          fromAddress: txData?.fromAddress,
          coin: {
            coinId: txData?.coin.coinId,
            isToken: coinData.is_token,
            tokenType: coinData.token_type,
            coinSymbol: coinData.coin_symbol,
            coinFamily: coinData.coin_family,
            cmcId: coinData.cmc_id,
            tokenAddress: Number(coinData.is_token) === Number(BooleanEnum.true) ? coinData.token_address : coinData.coin_symbol,
            addedBy: coinData.added_by
          }
        };
        console.log(trxData, "withdrwal");
        await this.updateBroadcastTx(trxData);
      } else {
        console.log('coin does not exists', coinData);
      }



    } catch (err: any) {
      console.error('sendTxUpdate error >>>>>>>', err);
      return false;
    }
  };

  private updateBroadcastTx = async (trxData: updateBroadCastTrnx) => {
    const txnData: any = await TrnxHistoryModel.findOne({ attributes: ['from_adrs', 'amount', 'user_id', 'id', 'to_adrs', 'type', 'coin_family', 'fiat_type'], where: { tx_id: trxData.txId, status: 'completed', [Op.or]: [{ blockchain_status: { [Op.is]: null as any } }, { blockchain_status: 'pending' }] }, raw: true });
    if (txnData) {
      const fromAddress = txnData.from_adrs;
      const amount = txnData.amount;
      const userId = txnData.user_id;
      const txRowId = txnData.id;
      const statusTx = trxData.status == "success" ? BlockChainStatusEnum.CONFIRMED : BlockChainStatusEnum.FAILED;
      const toAddress = txnData.to_adrs;
      const txType = txnData?.type
      const coinSymbol = trxData?.coin?.coinSymbol
      const coinFamily = txnData.coin_family;
      //Update transaction status
      const txUpdateRes = await TrnxHistoryModel.update({ blockchain_status: statusTx, tx_fee: trxData.txFee, gas_reverted: trxData.gasReverted }, { where: { id: txRowId } });

      const coinData: coinData = {
        coin_id: trxData.coin.coinId,
        is_token: trxData.coin.isToken,
        token_type: trxData.coin.tokenType,
        coin_symbol: trxData.coin.coinSymbol,
        coin_family: trxData.coin.coinFamily,
        cmc_id: trxData.coin.cmcId,
        token_address: trxData.coin.isToken === true ? trxData.coin.tokenAddress : trxData.coin.coinSymbol,
        added_by: trxData.coin.addedBy
      }

      if (txUpdateRes) {
        if (statusTx == BlockChainStatusEnum.CONFIRMED) {
          console.log('tx_type >>', txType)
          const addresses = [fromAddress, toAddress].filter(Boolean);
          const results = await polProcessHelper.getMultipleKeyValuePairs(`${config.POL_WALLET_ADDRESS}`, addresses);
          if (results) {
            const [addressWithdraw, addressDeposit]: any = results as string[];

            // const AddressWithdraw: any = await bscProcessHelper.check_our_wallet_address(from_address);
            if (addressWithdraw) {
              /** update withdraw user Pol balance */
              if (txType !== TrnxTypeEnum.DAPP) {
                const native_coin: any = await polProcessHelper.checkIfContract('NativeCoin');
                await walletHelper.updateBalance(fromAddress, native_coin);
                console.log("trx_data.coin?.token_type>>>>", trxData)
                if (trxData?.coin?.isToken === true && trxData.coin?.tokenType?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
                  /** Update User ERC20 Token Balance */
                  await walletHelper.updateBalance(fromAddress, coinData);
                }
              } else { // Entered in DAPP transactioin
                await walletHelper.updateAllActiveCoinBalance(fromAddress, coinFamily);
              }

              let trnxTypeW: string = "Withdraw";
              switch (txType) {
                case TrnxTypeEnum.DAPP:
                  trnxTypeW = "Smart Contract Execution";
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
              const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(
                amount
              )} ${coinSymbol.toUpperCase()} has been confirmed.`;

              const notifData: NotificationInterface = {
                title: trnxTypeW.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: 0,
                to_user_id: userId,
                coin_symbol: coinSymbol,
                wallet_address: fromAddress,
                tx_id: txRowId,
                coin_id: trxData.coin?.coinId,
                coin_family: trxData?.coin?.coinFamily,
                tx_type: txType,
                notification_type: trnxTypeW,
                state: "confirmed"
              };
              utilityHelper.SendNotification(notifData);
            }
            //const Addressdeposit: any = await bscProcessHelper.check_our_wallet_address(to_address);
            if (addressDeposit) {
              const toUserId: number = addressDeposit ? JSON.parse(addressDeposit)?.user_id : 0;
              // const to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;
              const trnxTypeD: string = "Deposit";
              /** update to adrs wallet balance */
              if (toAddress !== fromAddress) {
                if (txType !== TrnxTypeEnum.DAPP) {
                  const balance: any = await walletHelper.updateBalance(
                    toAddress, /// wallet_address,
                    coinData /// coin
                  );
                  await dbHelper.checkCoinInWallet(toAddress, coinData, balance)
                } else {
                  await walletHelper.updateAllActiveCoinBalance(toAddress, coinFamily);
                }
              }
              const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
                amount
              )} ${trxData?.coin?.coinSymbol.toUpperCase()} has been confirmed.`;

              const notifData: NotificationInterface = {
                title: NotificationTypeEnum.DEPOSIT.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: userId,
                to_user_id: toUserId,
                wallet_address: toAddress,
                tx_id: txRowId,
                coin_symbol: coinSymbol,
                coin_id: trxData.coin?.coinId,
                coin_family: trxData?.coin?.coinFamily,
                tx_type: txType,
                notification_type: trnxTypeD,
                state: "confirmed"
              };
              utilityHelper.SendNotification(notifData);
            }
            return true;
          }

        } else if (statusTx == BlockChainStatusEnum.FAILED) {
          let trnxTypeW: string = "Withdraw";
          switch (txType) {
            case TrnxTypeEnum.DAPP:
              trnxTypeW = "Smart Contract Execution";
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
          const notiMsg = trxData?.coin.tokenType === "ERC721" ? `Withdraw of NFT(${trxData?.coin.coinSymbol.toUpperCase()}) with token id ${amount} has been confirmed.` : `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(amount)} ${trxData?.coin.coinSymbol.toUpperCase()} has been failed.`;
          const notifData: NotificationInterface = {
            title: trnxTypeW.toUpperCase(),
            message: notiMsg,
            amount: amount,
            from_user_id: 0,
            to_user_id: userId,
            wallet_address: fromAddress,
            tx_id: txRowId,
            coin_symbol: coinSymbol,
            coin_id: trxData.coin?.coinId,
            coin_family: trxData?.coin?.coinFamily,
            tx_type: txType,
            notification_type: trnxTypeW,
            state: "failed"
          };
          utilityHelper.SendNotification(notifData);

          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}
export const updateInternalTrnxStatusProcess = new UpdateInternalTrnxStatusProcess();

