import { ethWithdrawDepositProcess, ethPendingWithdrawalProcess, ethTxStatusUpdateProcess, eth_blocks_process, eth_tx_status_deposit_withdraw, ETH_balance_update } from "./processes/index.process"
import schedule from "node-schedule";
class ETHStartProcess {
  constructor() {
    this.read_block_process()
    this.readSpecificBlock()
    this.consume_read_block_queue()
    this.ethWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.ETHUpdateBalance();
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("*/12 * * * * *", async function () {
      await eth_blocks_process.getBlocks();
      await eth_blocks_process.readBehindBlock()
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await eth_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await eth_blocks_process.readSpecificBlock();
    })
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await eth_tx_status_deposit_withdraw.startBlockTxQueue();
      await eth_tx_status_deposit_withdraw.startBehindBlockTxQueue();
    }, 10000);
  }
  public async ethWithdrawDepositProcess() {
    setTimeout(async () => {
      await ethWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await ethPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await ethTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async ETHUpdateBalance() {
    setTimeout(async () => {
      await ETH_balance_update.default.update_wallet_balance();
    }, 10000);
  }
}

new ETHStartProcess();
