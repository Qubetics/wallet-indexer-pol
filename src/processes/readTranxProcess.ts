// import { config } from "../config/config";
// import { Blockchain_Helper, Utility_Helper } from "../helpers";
// import { blocks_trnx } from "../interfaces/global_helper";
// import bscProcessHelper from "./process.helper";
// import { TxType } from "../types/ERC20Tokens.types";
// import { TrnxTypeEnum } from "../enum";
// import { global_helper } from "../helpers/global_helper";
// import kafkaHelper from "../config/kafka.helper";
// import { ethers, TransactionReceipt } from "ethers";

// class ReadTransactionsProcess {
//     public approval: string | null;
//     public swap: string | null;
//     public transfer: string | null;

//     public bsc_block_tx_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TX_TOPIC;
//     public bsc_block_tx_grpup_id = config.KAFKA_BLOCKS.BSC_BLOCK_TX_GROUP_ID;
//     public bsc_behind_block_tx_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_TOPIC;
//     public bsc_behind_block_tx_group_id = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_GROUP_ID;
//     public bsc_deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

//     constructor() {
//         this.approval = ethers.id('Approval(address,address,uint256)');
//         this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
//         this.transfer = ethers.id('Transfer(address,address,uint256)');

//     }

//     public startBlockTxConsumer = async () => {
//         const consumer = await kafkaHelper.kafka.consumer({ groupId: this.bsc_block_tx_grpup_id });
//         await consumer.subscribe({ topic: this.bsc_block_tx_topic, fromBeginning: true });

//         await consumer.run({
//             eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
//                 this.readTrnx(JSON.parse(message.value.toString()))
//             }
//         }).catch((err: Error) => {
//             console.log("🔥 ~ startBlockTxConsumer consumer error", err);
//         });
//     };

//     public startBehindBlockTxConsumer = async () => {
//         const consumer = await kafkaHelper.kafka.consumer({ groupId: this.bsc_behind_block_tx_group_id });
//         await consumer.subscribe({ topic: this.bsc_behind_block_tx_topic, fromBeginning: true });

//         await consumer.run({
//             eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
//                 this.readTrnx(JSON.parse(message.value.toString()))
//             }
//         }).catch((err: Error) => {
//             console.log("🔥 ~ startBehindBlockTxConsumer consumer error", err);
//         });
//     };

//     private readTrnx = async (data: blocks_trnx) => {
//         try {
//             const txReceipt: TransactionReceipt | null = await Blockchain_Helper.provider.getTransactionReceipt(data.tx_id);
//             if (txReceipt == null && Number(data.retry_count) < 6) {
//                 data.retry_count = Number(data.retry_count) + 1;
//                 await kafkaHelper.produceMessage(this.bsc_behind_block_tx_topic, data)
//                 return false;
//             }
//             if (txReceipt !== null && txReceipt.status == 0) {
//                 // console.log("(txReceipt.status === false && txReceipt !== null")
//             } else if (txReceipt) {
//                 await this.buildTx(txReceipt, data)
//             }
//         } catch (error: any) {
//             console.error("Error in read_trnx>>>", error)
//             if (Number(data.retry_count) < 6) {
//                 data.retry_count = Number(data.retry_count) + 1;
//                 console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data)
//                 await kafkaHelper.produceMessage(this.bsc_behind_block_tx_topic, data)
//             }
//         }
//     }
//     private buildTx = async (txReceipt: TransactionReceipt, data: blocks_trnx) => {
//         try {
//             if (txReceipt.status !== 0 && txReceipt.logs.length > 0) {
//                 for await (const log of txReceipt.logs) {
//                     if (log?.topics && log.topics[2] !== undefined) {
//                         if (log?.topics[0] == this.approval) {
//                             continue;
//                         } else if (log?.topics[0] == this.transfer) {
//                             const isToken: any = await bscProcessHelper.checkIfContract(log.address);
//                             if (isToken) {
//                                 const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
//                                 const hexToNumberGasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
//                                 const txFee = await Utility_Helper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);
//                                 const gasInBsc = await Blockchain_Helper.convertWeiToBsc(txFee);

//                                 const decimalPrecision = isToken.decimals.toString().length - 1;
//                                 let finalAmount = await Utility_Helper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
//                                 finalAmount = await Utility_Helper.exponentialToDecimal(finalAmount)
//                                 let toAddress: string = '0x' + log.topics[2].substring(26, log.topics[2].length);
//                                 /* in case of swap trnx */
//                                 let fromAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
//                                 const txData: TxType = {
//                                     txId: log.transactionHash,
//                                     fromAddress: fromAddress,
//                                     toAddress: toAddress,
//                                     token: isToken,
//                                     amount: finalAmount,
//                                     blockId: log.blockNumber,
//                                     isNFTToken: false,
//                                     txType: null,
//                                     trnx_fee: Number(gasInBsc)
//                                 };
//                                 await this.checkAddressInDB(txData);
//                             } else if (isToken == null) {
//                                 await this.check_bsc_deposit(data, txReceipt)
//                             }
//                         }
//                     }
//                     /** swap transaction then also check bsc */
//                     if (log?.topics && log?.topics[0] == this.swap) {
//                         await this.check_bsc_deposit(data, txReceipt);
//                     }
//                 }
//             } else {
//                 await this.check_bsc_deposit(data, txReceipt);
//             }
//         } catch (error: any) {
//             console.error("buildTx error >>>", (error as Error).message);
//             await global_helper.saveErrorLogs('Bsc_buildTx', error.message)
//         }
//     };
//     private check_bsc_deposit = async (data: blocks_trnx, txReceipt: TransactionReceipt) => {
//         try {
//             /** for check the bsc deposit */
//             if (
//                 data.value !== '0' &&
//                 data.toAddress &&
//                 data.block_number
//             ) {
//                 const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
//                 const effectiveGasPrice: number = Number(txReceipt.gasPrice) || 0;
//                 const txFee = await Utility_Helper.bigNumberSafeMath(
//                     gasUsed, '*', effectiveGasPrice
//                 )
//                 const gasInbsc = await Blockchain_Helper.convertWeiToBsc(txFee);
//                 const bscValue = await Blockchain_Helper.convertWeiToBsc((data.value ? data.value : 0));

//                 const amount = parseFloat(bscValue.toString());
//                 const txData: TxType = {
//                     txId: data.tx_id,
//                     fromAddress: data.fromAddress.toLowerCase(),
//                     toAddress: data.toAddress.toLowerCase(),
//                     token: null,
//                     amount,
//                     blockId: data?.block_number,
//                     isNFTToken: false,
//                     txType: TrnxTypeEnum.NULL,
//                     trnx_fee: Number(gasInbsc)
//                 };
//                 await this.checkAddressInDB(txData);
//             }
//         } catch (error: any) {
//             console.error("💥 ~ ~ check_bsc_deposit error", (error as Error).message);
//             await global_helper.saveErrorLogs('BSC_check_deposit', error.message)

//         }
//     }

//     private checkAddressInDB = async (txData: TxType) => {
//         try {
//             let ourTrnx: any = null;
//             let withdraw = await bscProcessHelper.check_our_wallet_address(txData.fromAddress);
//             if (withdraw) {
//                 console.log("Found the user in FROM (WITHDRAW) tx 😸");
//                 txData.txType = TrnxTypeEnum.WITHDRAW;
//                 ourTrnx = txData;
//             }
//             let deposit = await bscProcessHelper.check_our_wallet_address(txData.toAddress);
//             if (deposit) {
//                 console.log("Found the user in to (DEPOSIT) tx 😸");
//                 txData.txType = TrnxTypeEnum.DEPOSIT;
//                 ourTrnx = txData;
//             }
//             if (ourTrnx) {
//                 await this.addTxToQueue(
//                     ourTrnx,
//                     this.bsc_deposit_withdraw_topic || ""
//                 );
//             }
//         } catch (err: any) {
//             console.error("Error in checkAddressInDB>>", err)
//         }
//     }

//     private addTxToQueue = async (data: TxType, queueName: string) => {
//         try {
//             await kafkaHelper.produceMessage(queueName, data);
//         } catch (err: any) {
//             console.error("addTxToQueue", (err as Error).message);
//             await global_helper.saveErrorLogs('ETH_checkAddressInDB', err.message)

//         }
//     };

// }
// export const readTransactionsProcess = new ReadTransactionsProcess()
