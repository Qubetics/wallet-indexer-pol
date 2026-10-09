import kafkaHelper from "./config/kafka.helper";
import {
  readBlockProcess,
  depositWithdrawProcess,
  readInternalTxProcess,
  updateInternalTrnxStatusProcess,
  updateBalanceProcess,
  traceBlockPolProcess
} from "./processes/index.process";
import schedule from "node-schedule";
class PolStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readBehinedBlockProcess();
    this.readSpecificBlock();
    this.startDepositWithdrawConsumer();
    this.internalWithdrawalTxProcess();
    this.startInternalTxStatusUpdateConsumer();
    this.startUpdateBalanceConsumer();
    this.updateBalanceByWallet();
    this.consumeTraceBlockPolTxsQueue();
  }
  private async connectKafka() {
    await kafkaHelper.connect();
  }


  private readBlockProcess() {
    schedule.scheduleJob("*/2 * * * * *", async function () {
      await readBlockProcess.readCurrentBlocks();
    });
  }

  private readBehinedBlockProcess() {
    schedule.scheduleJob("*/2 * * * * *", async function () {
      await readBlockProcess.readBehindBlocks()
    });
  }


  private readSpecificBlock() {
    console.log("readSpecificBlock started after 1 min 🔆🔆🔆");
    schedule.scheduleJob('*/1 * * * *', async function () {
      await readBlockProcess.readSpecificBlock();
    })
  }

  private async consumeTraceBlockPolTxsQueue() {
    setTimeout(async () => {
      console.log("consumer TraceBlockPolTxsQueue started 🔆🔆🔆");
      await traceBlockPolProcess.startTraceBlockPolProcess();
    }, 1000);
  }

  private async startDepositWithdrawConsumer() {
    setTimeout(async () => {
      await depositWithdrawProcess.startDepositWithdrawConsumer();
    }, 1000);
  }
  private internalWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await readInternalTxProcess.getTransactionFromDB();
    });
  }
  private async startInternalTxStatusUpdateConsumer() {
    setTimeout(async () => {
      await updateInternalTrnxStatusProcess.startTxStatusUpdateConsumer();
    }, 1000);
  }

  private async startUpdateBalanceConsumer() {
    setTimeout(async () => {
      await updateBalanceProcess.updateBalanceConsumer();
    }, 1000);
  }

  private async updateBalanceByWallet() {
    schedule.scheduleJob("*/2 * * * *", async function () {
      await updateBalanceProcess.updateWalletBalance();
    });
  }


}

new PolStartProcess();
