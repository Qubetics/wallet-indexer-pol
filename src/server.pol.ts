import kafkaHelper from "./config/kafka.helper";
import { polWithdrawDepositProcess, polPendingWithdrawalProcess, polTxStatusUpdateProcess, polBlocksProcess, processedBlockTransactions, updateBalanceProcess } from "./processes/index.process"
import schedule from "node-schedule";
class POLStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readSpecificBlock();
    this.consumeBlockTxQueue();
    this.consumeBehindBlockTxQueue();
    this.consumeWithdrawDepositTxsQueue();
    this.internalTxsInQueue();
    this.consumeInternalTxQueue();
    this.consumeUpdateBalanceQueue();
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlockProcess() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await polBlocksProcess.readCurrentBlocks();
      await polBlocksProcess.readBehindBlock()
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await polBlocksProcess.readSpecificBlock();
    })
  }
  public consumeBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBlockTxQueue();

    }, 10000);
  }
  public consumeBehindBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBehindBlockTxQueue();
    }, 10000);
  }
  public async consumeWithdrawDepositTxsQueue() {
    setTimeout(async () => {
      await polWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public internalTxsInQueue() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await polPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async consumeInternalTxQueue() {
    setTimeout(async () => {
      await polTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async consumeUpdateBalanceQueue() {
    setTimeout(async () => {
      await updateBalanceProcess.updateWalletBalance();
    }, 10000);
  }
}

new POLStartProcess();
