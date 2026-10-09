import { config } from "../config/config";
import { Op } from "sequelize";
import { BlockChainStatusEnum } from "../enum";
import { CoinsModel, TrnxHistoryModel } from "../models";
import kafkaHelper from "../config/kafka.helper";
import { pendingTrnx } from "../interfaces/global.interface";

class ReadInternalTxProcess {

  public pol_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.POL_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    const tx: any = await TrnxHistoryModel.findAll({
      attributes: ["tx_id", "coin_id"],
      include: [{
        model: CoinsModel,
        attributes: ['is_token', 'token_type', 'token_address'],
        as: "coin_data"
      }],
      where: {
        status: 'completed',
        [Op.or]: [{ blockchain_status: null }, { blockchain_status: BlockChainStatusEnum.PENDING }],
        req_type: { [Op.in]: ['APP', 'EXNG'] },
        coin_family: config.STATIC_COIN_FAMILY.POL
      }
    })
    if (tx.length > 0) {
      await Promise.all(tx.map(async (el: any) => {
        const trnx: pendingTrnx = {
          txId: el.tx_id,
          coinId: el.coin_id,
          isToken: el.coin_data.is_token,
          tokenType: el.coin_data.token_type,
          tokenAddress: el.coin_data.token_address,
        };
        return this.addTxToQueue(trnx); // Directly return the promise
      }));
    }
  };

  private addTxToQueue = async (data: pendingTrnx) => {
    await kafkaHelper.produceMessage(this.pol_pending_withdrawal_tx_topic, data);
  };
}

export const readInternalTxProcess = new ReadInternalTxProcess();
