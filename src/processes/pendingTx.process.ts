// import rabbitMq from "../config/rabbitMq";
import { config } from "../config/config";
import { Op } from "sequelize";
import { BlockChainStatusEnum } from "../enum";
import { TrnxHistoryModel } from "../models";
import kafkaHelper from "../config/kafka.helper";

class POLPendingWithdrawalProcess {
  public pol_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.POL_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    let tx: any = await TrnxHistoryModel.findAll({
      attributes: ["tx_id", "coin_id"],
      where: {
        status: 'completed',
        [Op.or]: [{ blockchain_status: null }, { blockchain_status: BlockChainStatusEnum.PENDING }],
        req_type: { [Op.notIn]: ['TRANSAK', 'ALCHEMY'] },
        coin_family: config.STATIC_COIN_FAMILY.POL
      },
      raw: true
    })

    if (tx.length > 0) {
      for await (const el of tx) {
        await this.addTxToQueue(el);
      }
    }
  };

  public addTxToQueue = async (data: { tx_id: string; coin_id: number }) => {
    await kafkaHelper.produceMessage(this.pol_pending_withdrawal_tx_topic, data);
    // await rabbitMq.assertQueue(config.PENDING_WITHDRAWAL_TX_PROCESS_POL || "");
    // await rabbitMq.sendToQueue(
    //   config.PENDING_WITHDRAWAL_TX_PROCESS_POL || "",
    //   Buffer.from(JSON.stringify(data))
    // );
  };
}

export const polPendingWithdrawalProcess = new POLPendingWithdrawalProcess();
