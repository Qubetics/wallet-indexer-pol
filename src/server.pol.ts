import { polWithdrawDepositProcess, polPendingWithdrawalProcess, polTxStatusUpdateProcess, pol_blocks_process, pol_tx_status_deposit_withdraw, POL_balance_update } from "./processes/index.process"
import schedule from "node-schedule";
class POLStartProcess {
  constructor() {
    this.read_block_process()
    this.readSpecificBlock()
    this.consume_read_block_queue()
    this.polWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.POLUpdateBalance();
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("*/12 * * * * *", async function () {
      await pol_blocks_process.getBlocks();
      await pol_blocks_process.readBehindBlock()
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await pol_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await pol_blocks_process.readSpecificBlock();
    })
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await pol_tx_status_deposit_withdraw.startBlockTxQueue();
      await pol_tx_status_deposit_withdraw.startBehindBlockTxQueue();
    }, 10000);
  }
  public async polWithdrawDepositProcess() {
    setTimeout(async () => {
      await polWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await polPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await polTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async POLUpdateBalance() {
    setTimeout(async () => {
      await POL_balance_update.default.update_wallet_balance();
    }, 10000);
  }
}

new POLStartProcess();
