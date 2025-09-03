// import { blockchainHelper, dbHelper } from "../helpers/index";
// import { config } from "../config/config";
// import bscProcessHelper from "./process.helper";
// import { BscOldBLockModel } from "../models";
// // import { globalHelper } from "../helpers/global.helper";
// import kafkaHelper from "../config/kafka.helper";
// import { ethers, TransactionReceipt } from "ethers";
// import { blocksTrnx } from "../interfaces/global.interface";
// import { utilityHelper } from "../helpers/utility.helper";
// import { TokenData } from "../interfaces/ether.interface";
// import { TrnxTypeEnum } from "../enum";
// import { TxType } from "../types/ERC20Tokens.types";



// class ReadBlockTrnxProcess {
//     public minBlockConfirmation: number;
//     public swap: string | null;
//     public approval: string | null;
//     public transfer_by_partition: string | null;
//     public approval_by_partition: string | null;
//     public processed_dividend_tracker: string | null;
//     public dividend_withdrawn: string | null;
//     public deposit: string | null;
//     public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;
//     public trace_block_bsc_deposit_topic = config.KAFKA_TRANSACTIONS.TRACE_BLOCK_BSC_DEPOSIT_TOPIC;

//     public bsc_block_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TOPIC;
//     public bsc_block_group = config.KAFKA_BLOCKS.BSC_BLOCK_GROUP_ID;
//     public bsc_behind_block_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TOPIC;
//     public bsc_behind_block_group = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_GROUP_ID;

//     public bsc_block_tx_topic = config.KAFKA_BLOCKS.BSC_BLOCK_TX_TOPIC;
//     public bsc_behind_block_tx_topic = config.KAFKA_BLOCKS.BSC_BEHIND_BLOCK_TX_TOPIC;

//     constructor() {
//         this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
//         this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
//         this.approval = ethers.id('Approval(address,address,uint256)');
//         this.transfer_by_partition = ethers.id('TransferByPartition(bytes32,address,address,address,uint256,bytes,bytes)');
//         this.approval_by_partition = ethers.id('ApprovalByPartition(bytes32,address,address,uint256)');
//         this.processed_dividend_tracker = ethers.id('ProcessedDividendTracker(uint256,uint256,uint256,bool,uint256,address)');
//         this.dividend_withdrawn = ethers.id('DividendWithdrawn(address,uint256)');
//         this.deposit = ethers.id('Deposit(address,uint256)');
//     }

//     public startLatestBlockEvenConsumer = async () => {
//         let consumer: any = null;
//         let isConnected = false;

//         const connectConsumer = async () => {
//             try {

//                 if (consumer) {
//                     await consumer.disconnect();
//                 }

//                 consumer = await kafkaHelper.kafka.consumer({
//                     groupId: `${this.bsc_block_group}_even`,
//                     // Add session timeout for better error handling
//                     sessionTimeout: 30000,
//                     // Add heartbeat interval
//                     heartbeatInterval: 3000,
//                 });

//                 await consumer.subscribe({
//                     topic: `${this.bsc_block_topic}_even`,
//                     fromBeginning: true, // ✅ Changed to false to prevent reprocessing
//                 });

//                 await consumer.run({
//                     // ✅ Add proper autoCommit configuration
//                     autoCommit: true,
//                     autoCommitInterval: 5000,
//                     autoCommitThreshold: 100,

//                     eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
//                         try {
//                             console.log('🚀 ~ startLatestBlockEvenConsumer topic', topic);
//                             console.log('🚀 ~ startLatestBlockEvenConsumer message.value', message.value.toString());
//                             await new Promise(resolve => setTimeout(resolve, 300));
//                             const blockId = JSON.parse(message.value.toString());
//                             this.getBlockReceipts(blockId);
//                             this.getTraceBlock(blockId);

//                         } catch (error) {
//                             console.error(`❌ Error processing message:`, error);
//                         }
//                     },
//                 });
//                 isConnected = true;
//             } catch (error) {
//                 console.error(`❌ Failed to connect consumer:`, error);
//                 isConnected = false;
//                 throw error;
//             }
//         };

//         // ✅ Add reconnection logic
//         const startWithReconnection = async () => {
//             while (true) {
//                 try {
//                     if (!isConnected) {
//                         console.log(`🔄 Attempting to connect consumer startLatestBlockEvenConsumer...`);
//                         await connectConsumer();
//                     }

//                     // Keep the consumer alive
//                     await new Promise(resolve => setTimeout(resolve, 30000));

//                 } catch (error) {
//                     console.error(`❌ Consumer connection error:`, error);
//                     isConnected = false;

//                     // Wait before retrying
//                     console.log(`⏳ Waiting 10 seconds before reconnecting...`);
//                     await new Promise(resolve => setTimeout(resolve, 10000));
//                 }
//             }
//         };

//         // Start the consumer with reconnection
//         await startWithReconnection();
//     };

//     public startLatestBlockOddConsumer = async () => {
//         let consumer: any = null;
//         let isConnected = false;

//         const connectConsumer = async () => {
//             try {

//                 if (consumer) {
//                     await consumer.disconnect();
//                 }

//                 consumer = await kafkaHelper.kafka.consumer({
//                     groupId: `${this.bsc_block_group}_odd`,
//                     // Add session timeout for better error handling
//                     sessionTimeout: 30000,
//                     // Add heartbeat interval
//                     heartbeatInterval: 3000,
//                 });

//                 await consumer.subscribe({
//                     topic: `${this.bsc_block_topic}_odd`,
//                     fromBeginning: true, // ✅ Changed to false to prevent reprocessing
//                 });

//                 await consumer.run({
//                     // ✅ Add proper autoCommit configuration
//                     autoCommit: true,
//                     autoCommitInterval: 5000,
//                     autoCommitThreshold: 100,

//                     eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
//                         try {
//                             console.log('🚀 ~ startLatestBlockOddConsumer topic', topic);
//                             console.log('🚀 ~ startLatestBlockOddConsumer message.value', message.value.toString());
//                             await new Promise(resolve => setTimeout(resolve, 350));
//                             const blockId = JSON.parse(message.value.toString());
//                             this.getBlockReceipts(blockId);
//                             this.getTraceBlock(blockId);

//                         } catch (error) {
//                             console.error(`❌ Error processing message:`, error);
//                         }
//                     },
//                 });
//                 isConnected = true;
//             } catch (error) {
//                 console.error(`❌ Failed to connect consumer:`, error);
//                 isConnected = false;
//                 throw error;
//             }
//         };

//         // ✅ Add reconnection logic
//         const startWithReconnection = async () => {
//             while (true) {
//                 try {
//                     if (!isConnected) {
//                         console.log(`🔄 Attempting to connect consumer startLatestBlockOddConsumer...`);
//                         await connectConsumer();
//                     }

//                     // Keep the consumer alive
//                     await new Promise(resolve => setTimeout(resolve, 30000));

//                 } catch (error) {
//                     console.error(`❌ Consumer connection error:`, error);
//                     isConnected = false;

//                     // Wait before retrying
//                     console.log(`⏳ Waiting 10 seconds before reconnecting...`);
//                     await new Promise(resolve => setTimeout(resolve, 10000));
//                 }
//             }
//         };

//         // Start the consumer with reconnection
//         await startWithReconnection();
//     };

//     public startBehindBlockConsumer = async () => {
//         let consumer: any = null;
//         let isConnected = false;

//         const connectConsumer = async () => {
//             try {

//                 if (consumer) {
//                     await consumer.disconnect();
//                 }

//                 consumer = await kafkaHelper.kafka.consumer({
//                     groupId: this.bsc_behind_block_group,
//                     // Add session timeout for better error handling
//                     sessionTimeout: 30000,
//                     // Add heartbeat interval
//                     heartbeatInterval: 3000,
//                 });

//                 await consumer.subscribe({
//                     topic: this.bsc_behind_block_topic,
//                     fromBeginning: true, // ✅ Changed to false to prevent reprocessing
//                 });

//                 await consumer.run({
//                     // ✅ Add proper autoCommit configuration
//                     autoCommit: true,
//                     autoCommitInterval: 5000,
//                     autoCommitThreshold: 100,

//                     eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
//                         try {
//                             console.log('🚀 ~ startBehindBlockConsumer topic', topic);
//                             console.log('🚀 ~ startBehindBlockConsumer message.value', message.value.toString());
//                             await new Promise(resolve => setTimeout(resolve, 350));
//                             const blockId = JSON.parse(message.value.toString());
//                             this.getBlockReceipts(blockId);
//                             this.getTraceBlock(blockId);

//                         } catch (error) {
//                             console.error(`❌ Error processing message:`, error);
//                         }
//                     },
//                 });
//                 isConnected = true;
//             } catch (error) {
//                 console.error(`❌ Failed to connect consumer:`, error);
//                 isConnected = false;
//                 throw error;
//             }
//         };

//         // ✅ Add reconnection logic
//         const startWithReconnection = async () => {
//             while (true) {
//                 try {
//                     if (!isConnected) {
//                         console.log(`🔄 Attempting to connect consumer startBehindBlockConsumer...`);
//                         await connectConsumer();
//                     }

//                     // Keep the consumer alive
//                     await new Promise(resolve => setTimeout(resolve, 30000));

//                 } catch (error) {
//                     console.error(`❌ Consumer connection error:`, error);
//                     isConnected = false;

//                     // Wait before retrying
//                     console.log(`⏳ Waiting 10 seconds before reconnecting...`);
//                     await new Promise(resolve => setTimeout(resolve, 10000));
//                 }
//             }
//         };

//         // Start the consumer with reconnection
//         await startWithReconnection();
//     };

//     // public async getBlocks() {
//     //     try {
//     //         let old_block: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC)
//     //         let new_block: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC)
//     //         if (!old_block) {
//     //             console.log("No block present in old block")
//     //             await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, this.minBlockConfirmation, null)
//     //         }
//     //         if (!new_block) {
//     //             console.log("No block present in new block")
//     //             await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, null)
//     //         }
//     //         if (old_block && new_block) {
//     //             let oldBlock: number = Number(old_block);
//     //             let newBlock = Number(new_block);
//     //             if (newBlock <= oldBlock) {
//     //                 await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
//     //             } else {
//     //                 let block_diff: number = newBlock - oldBlock;
//     //                 if (block_diff > 50) {
//     //                     await bscProcessHelper.diffBlocksAddInDb(newBlock, oldBlock)
//     //                     await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, newBlock.toString())
//     //                     await bscProcessHelper.setBlocks(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.NEW_BLOCK_BSC, this.minBlockConfirmation, newBlock)
//     //                 } else {
//     //                     await this.readBlocks(oldBlock, newBlock)
//     //                 }
//     //             }
//     //         } else {
//     //             console.log(" no old block and no new block")
//     //             await global_helper.saveErrorLogs('Bsc_setBlocks', 'No old block no new block')
//     //         }
//     //     } catch (err: any) {
//     //         console.error("Error in getBlocks", err)
//     //         await global_helper.saveErrorLogs('bsc_getBlocks', err.message)
//     //     }
//     // }
//     // public async readBlocks(blockNumber: number, toBlockNumber: number) {
//     //     try {
//     //         console.log("blockNumber", blockNumber)
//     //         let block_updated: number = blockNumber + 1;
//     //         await bscProcessHelper.setKeyValuePair(config.REDISKEYS.BSC_BLOCKS, config.REDISKEYS.OLD_BLOCK_BSC, block_updated.toString());
//     //         console.table({
//     //             LATEST_BLOCK: blockNumber,
//     //             BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
//     //             TO_BLOCK_NUMBER: toBlockNumber
//     //         });
//     //         this.getTxFromBlock(blockNumber, KEYS.latestBlock);
//     //     } catch (err: any) {
//     //         console.error("Error in readBlocks>>.", (err as Error).message)
//     //         await global_helper.saveErrorLogs('BSC_readBlocks', err.message)
//     //     }
//     // }

//     //old function
//     // private async getTxFromBlock(blockId: number, key: string) {
//     //     try {
//     //         console.log('blockId >>>>', blockId, 'key >>>>', key);
//     //         const blockInfo = await blockchainHelper.provider.getBlock(blockId, true);
//     //         if (!blockInfo) {
//     //             console.log("No block info");
//     //             await BscOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.POL, start_block: blockId, end_block: blockId, status: 0 })
//     //             return;
//     //         }
//     //         const transactions = blockInfo.prefetchedTransactions || [];
//     //         if (transactions.length === 0) {
//     //             console.log("No block info transactions");
//     //             return;
//     //         }

//     //         const transactionPromises = transactions.map(async (tx) => {
//     //             const data: blocks_trnx = {
//     //                 block_number: blockId,
//     //                 tx_id: tx.hash || '',
//     //                 value: tx.value.toString(),
//     //                 fromAddress: tx.from || '',
//     //                 toAddress: tx.to || '',
//     //                 retry_count: 0,
//     //                 key: null,
//     //                 pair: null
//     //             };

//     //             data.key = (key === KEYS.latestBlock) ? 'latest' : 'behined & specific';
//     //             const targetTopic = (key === KEYS.latestBlock) ? this.bsc_block_tx_topic : this.bsc_behind_block_tx_topic;

//     //             try {
//     //                 await kafkaHelper.produceMessage(targetTopic, data);
//     //             } catch (err) {
//     //                 console.error("Error while producing message for transaction", tx.hash, err);
//     //             }
//     //         });
//     //         await Promise.allSettled(transactionPromises);
//     //     } catch (err: any) {
//     //         console.error("Error in getTxFromBlock:", (err as Error).message);
//     //         await global_helper.saveErrorLogs('bsc_getTxFromBlock', err.message)
//     //     }
//     // }

//     // private async readBehindBlock() {
//     //     try {
//     //         // REDIS BASED
//     //         const data_exist: any = await bscProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
//     //         if (data_exist) {
//     //             const data_exists = JSON.parse(data_exist);
//     //             if (data_exists) {
//     //                 const { id, start_block, end_block } = data_exists;
//     //                 if (end_block > start_block) {
//     //                     const newStartBlock = start_block + 1;
//     //                     const updateKeyData = {
//     //                         id: id,
//     //                         start_block: newStartBlock,
//     //                         end_block: end_block,
//     //                         diff: end_block - newStartBlock
//     //                     }
//     //                     await bscProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC, JSON.stringify(updateKeyData))

//     //                     console.log("This block go for getTxFromBlock")
//     //                     bsc_blocks_process.getTxFromBlock(start_block, KEYS.behinedBlock);

//     //                 } else {
//     //                     console.log("id>>>>>>>>>>>", id)
//     //                     await BscOldBLockModel.update({ status: 1 }, { where: { id: id } })
//     //                     await bscProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_BSC)
//     //                     await bscProcessHelper.behindBlocks()

//     //                 }
//     //             }
//     //         } else {
//     //             // DB BASED
//     //             await bscProcessHelper.behindBlocks()
//     //         }
//     //     } catch (err: any) {
//     //         console.error("Error in readBehindBlock>>", err)
//     //         await global_helper.saveErrorLogs('BSC_readBehindBlock', err.message)
//     //     }
//     // }

//     public async readSpecificBlock() {
//         try {
//             console.log("Enter into readSpecificBlock>>>")
//             const specificBlocks: any = await bscProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC)
//             const specific_block: any = JSON.parse(specificBlocks);
//             if (specific_block && Number(specific_block) > 0) {
//                 console.table({ SPECIFIC_BLOCK: specific_block })
//                 // await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
//                 this.getBlockReceipts(specific_block);
//                 this.getTraceBlock(specific_block);
//                 await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC, '0')
//             } else {
//                 console.log("Specific block is not present>>>>")
//                 await bscProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BSC, '0')
//             }
//         } catch (error: any) {
//             console.error("Error in readSpecificBlock>>>", error)
//         }
//     }

//     // public async testingNode() {
//     //     try {
//     //         const provider = blockchainHelper.provider;

//     //         // Get Block Number
//     //         const latestBlock = await provider.getBlockNumber();
//     //         console.log("Block number>>", latestBlock);

//     //         const blockInfo = await provider.getBlock(latestBlock, true);
//     //         console.log("blockInfo>>", blockInfo);

//     //         if (blockInfo && blockInfo.transactions.length > 0) {
//     //             let firstTxHash: string;
//     //             const firstTx = blockInfo.transactions[0];

//     //             if (typeof firstTx === 'string') {
//     //                 firstTxHash = firstTx;
//     //             } else {
//     //                 // Assert the type to access the hash property safely
//     //                 firstTxHash = (firstTx as ethers.TransactionResponse).hash;
//     //             }
//     //             // Get Block Receipt
//     //             const txReceipt = await provider.getTransactionReceipt(firstTxHash);
//     //             console.log("txreceipt>>", txReceipt);

//     //             // Get more details (not reduced in ethers.js)
//     //             const transaction = await provider.getTransaction(firstTxHash);
//     //             console.log("transaction>>", transaction);
//     //         } else {
//     //             console.log("No transactions found in the block");
//     //         }
//     //     } catch (err: any) {
//     //         console.error("Error in testingNode>>", err)
//     //     }
//     // }

//     public collectCalls(calls: any[], depth: number = 0, results: any[] = [], txHash?: string): any[] {
//         for (const call of calls) {
//             if (call?.error) {
//                 console.log("(call?.error", call?.error, 'txHash >>>', txHash);
//             } else {
//                 if (call?.value && Number(call.value) > 0) {
//                     results.push({
//                         depth,
//                         type: call.type,
//                         txHash: txHash,
//                         value: call.value,
//                         callData: call,
//                     });
//                 }
//                 // If nested calls exist, process them recursively
//                 if (call?.calls?.length > 0) {
//                     this.collectCalls(call.calls, depth + 1, results, txHash);
//                 }
//             }
//         }
//         return results;

//     }

//     public async processTraces(response: any[]) {
//         const allCalls: any[] = [];

//         if (response?.length > 0) {
//             for (const trace of response) {
//                 const result = trace?.result;

//                 if (!result?.error) {
//                     // Top-level CALL
//                     if (result?.type === 'CALL' && Number(result?.value) > 0) {

//                         allCalls.push({
//                             depth: 0,
//                             type: result.type,
//                             value: result.value,
//                             txHash: trace.txHash,
//                             callData: result,
//                         });

//                     }
//                     // Collect child calls if they exist
//                     if (result?.calls?.length > 0) {
//                         const collected = this.collectCalls(result.calls, 1, [], trace.txHash);
//                         allCalls.push(...collected);
//                     }
//                 } else {
//                     console.log("(result?.error", result?.error, 'trace.txHash <<', trace.txHash);
//                 }
//             }
//         }

//         return allCalls;
//     }


//     public getTraceBlock = async (blockId: number) => {
//         try {
//             console.log("INSIDE getTraceBlock", blockId);

//             const hexBlockValue = ethers.toBeHex(blockId.toString());
//             const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');

//             const response = await blockchainHelper.provider.send("debug_traceBlockByNumber", [
//                 formattedBlockValue,
//                 { "tracer": "callTracer" }
//             ]);

//             if (response?.length > 0) {
//                 const allValueCalls = await this.processTraces(response);

//                 // console.log("All calls with value > 0:", allValueCalls);

//                 // Save to Kafka
//                 for (const call of allValueCalls) {
//                     const txReceipt: blocksTrnx = {
//                         txId: call.txHash,
//                         fromAddress: call.callData.from,
//                         toAddress: call.callData.to,
//                         value: call.value.toString(),
//                         gasUsed: null,
//                         gasPrice: null,
//                         blockNumber: blockId,
//                     };

//                     //   console.log(`Processed Tx at depth ${call.depth}`, txReceipt);
//                     this.addTraceTxToQueue(txReceipt, config.KAFKA_TRANSACTIONS.TRACE_BLOCK_BSC_DEPOSIT_TOPIC);
//                 }

//             } else {
//                 console.error("Error fetching data from QuickNode API.");
//                 this.getTxFromBlock(blockId);
//             }

//         } catch (error) {
//             console.log("🔥 ~ getTraceBlock error", (error as Error).message);
//             this.getTxFromBlock(blockId);
//         }
//     };


//     public getBlockReceipts = async (blockId: number) => {
//         try {
//             console.log("INSIDE getBlockReceipts")
//             // Get block receipts
//             const blockReceipts = await this.fetchBlockReceipts(blockId);
//             if (!blockReceipts) {
//                 console.error(`💥 ~ No transactions found in block ${blockId}`);
//                 return false;
//             }

//             // Process each transaction receipt
//             for (const txReceipt of blockReceipts) {
//                 await this.processTxReceipt(txReceipt);
//             }

//             return true;
//         } catch (error) {
//             console.log("🔥 ~ getBlockReceipts error", (error as Error).message);
//             return false;
//         }
//     };

//     public async getTxFromBlock(blockId: number) {
//         try {
//             console.error("In getTxFromBlock>>.", blockId)
//             const blockInfo = await blockchainHelper.provider.getBlock(blockId, true);
//             if (!blockInfo) {
//                 console.log("No block info");
//                 await BscOldBLockModel.create({
//                     block_number: blockId,
//                     coin_family: config.STATIC_COIN_FAMILY.BSC,
//                     start_block: blockId,
//                     end_block: blockId,
//                     status: 0
//                 });
//                 return;
//             }
//             const transactions = blockInfo.prefetchedTransactions || [];
//             if (transactions.length === 0) {
//                 console.log("No block info transactions");
//                 return;
//             }


//             const transactionPromises = transactions.map(async (tx) => {
//                 const amount: BigNumber = new BigNumber(tx?.value.toString());
//                 if (amount.gt(0)) {
//                     const txReceipt: TransactionReceipt | null = await blockchainHelper.provider.getTransactionReceipt(tx?.hash);

//                     if (txReceipt?.status == 0) {
//                         console.log("(txReceipt.status === false && txReceipt !== null");
//                         return false;
//                     }
//                     const gasUsed = txReceipt?.gasUsed ? txReceipt.gasUsed : 0;
//                     const gasPrice: number = Number(txReceipt?.gasPrice) || 0;
//                     const blockTransaction: blocksTrnx = {
//                         txId: tx.hash || '',
//                         value: amount?.toString(),
//                         fromAddress: tx.from || '',
//                         toAddress: tx.to || '',
//                         blockNumber: blockId,
//                         gasUsed: Number(gasUsed.toString()),
//                         gasPrice: gasPrice,
//                     }
//                     await this.checkPokDeposit(blockTransaction);
//                 }
//             });

//             // Wait for all the transaction processing to complete
//             await Promise.allSettled(transactionPromises);
//         } catch (err: any) {
//             console.error("Error in getTxFromBlock:", (err as Error).message);
//             await dbHelper.saveErrorLogs('Eth_readBehindBlock', err.message)
//         }
//     }

//     private fetchBlockReceipts = async (blockId: number) => {
//         const hexBlockValue = ethers.toBeHex(blockId.toString());
//         const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');
//         return await blockchainHelper.provider.send("eth_getBlockReceipts", [formattedBlockValue]);
//     };

//     private processTxReceipt = async (txReceipt: TransactionReceipt) => {
//         // Only process successful transactions with logs
//         if (!txReceipt?.status || !txReceipt?.logs?.length) {
//             return;
//         }

//         const gasInfo = await this.extractGasInfo(txReceipt);
//         const processedTokens = new Set<string>(); // Track processed NFT tokens per transaction

//         for (const log of txReceipt.logs) {
//             await this.processLog(log, gasInfo, processedTokens);
//         }
//     };

//     private extractGasInfo = async (txReceipt: any) => {
//         const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
//         const hexToNumberGasPrice = txReceipt?.effectiveGasPrice ? Number(txReceipt.effectiveGasPrice) : 0;
//         const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);
//         const gasInEth = await blockchainHelper.convertWeiToBsc(txFee);

//         return {
//             gasUsed: gasUsed,
//             gasPrice: hexToNumberGasPrice,
//             trnx_fee: Number(gasInEth)
//         };
//     };

//     private processLog = async (log: any, gasInfo: any, processedTokens: Set<string>) => {


//         const contractInfo = await bscProcessHelper.checkIfContract(log.address);
//         if (this.isTokenTransfer(log, contractInfo)) {
//             //here  we have contractInfo.token_address
//             await this.processTokenTransfer(log, gasInfo, contractInfo);
//         }
//         else {

//             //NFT  not required  here 
//             // const nftInfo = await bscProcessHelper.checkIfOurNFTContract(log.address)
//             // if (this.isNFTTransfer(log, nftInfo)) {
//             //     await this.processNFTTransfer(log, gasInfo, nftInfo, processedTokens);
//             //     return;
//             if (this.isTokenTransferNotExist(log)) {
//                 //here  we have the from and to address 
//                 const toAddress = '0x' + log.topics[2]?.substring(26);
//                 const addresses = [toAddress].filter(Boolean);

//                 if (addresses.length > 0) {
//                     const results = await bscProcessHelper.getMultipleKeyValuePairs(`${config.BSC_WALLET_ADDRESS}`, addresses);
//                     if (results) {
//                         const [depositTx] = results as string[];
//                         if (depositTx) {
//                             const nativeWalletData: number = depositTx ? JSON.parse(depositTx) : null;
//                             if (nativeWalletData) {
//                                 console.log("contractInfo not exist in db 💥💥 inserting into db", log.address)
//                                 const tokenDetail: TokenData | null = await blockchainHelper.tokenDetails(log.address);
//                                 if (tokenDetail) {
//                                     const contractInfo = await bscProcessHelper.insertTokenAddressInDB(log.address, tokenDetail, nativeWalletData);
//                                     if (contractInfo) {
//                                         await this.processNotExistTokenTransfer(log, gasInfo, contractInfo, TrnxTypeEnum.DEPOSIT)
//                                     } else {
//                                         return;
//                                     }

//                                 }
//                             } else {
//                                 return;
//                             }

//                         } else {
//                             return;
//                         }
//                     }
//                 }
//             }
//         }
//     };


//     private isTokenTransfer = (log: any, contractInfo: any): boolean => {
//         return contractInfo && Object.keys(contractInfo)?.length > 0 &&
//             log.topics[2] !== undefined &&
//             !this.isExcludedTopic(log.topics[0]);
//     };

//     private isTokenTransferNotExist = (log: any): boolean => {
//         return log &&
//             log.topics[1] !== undefined &&
//             log.topics[2] !== undefined &&
//             !this.isExcludedTopic(log.topics[0]);
//     };

//     private isExcludedTopic = (topic: string): boolean => {
//         const excludedTopics = [
//             this.approval,
//             this.approval_by_partition,
//             this.transfer_by_partition,
//             this.processed_dividend_tracker,
//             this.dividend_withdrawn
//         ];
//         return excludedTopics.includes(topic);
//     };


//     private processTokenTransfer = async (log: any, gasInfo: any, contractInfo: any) => {
//         const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);

//         if (amount <= 0) {
//             return;
//         }

//         const txData: TxType = {
//             txId: log.transactionHash,
//             fromAddress: '0x' + log.topics[1]?.substring(26),
//             toAddress: '0x' + log.topics[2]?.substring(26),
//             token: contractInfo,
//             amount: amount,
//             blockId: log.blockNumber,
//             isNFTToken: false,
//             txType: null,
//             trnx_fee: gasInfo.trnx_fee
//         };

//         await this.checkAddressInDB(txData);
//     };

//     private processNotExistTokenTransfer = async (log: any, gasInfo: any, contractInfo: any, txType: TrnxTypeEnum.DEPOSIT) => {
//         const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);
//         if (amount <= 0) {
//             return;
//         }

//         const txData: TxType = {
//             txId: log.transactionHash,
//             fromAddress: '0x' + log.topics[1]?.substring(26),
//             toAddress: '0x' + log.topics[2]?.substring(26),
//             token: contractInfo,
//             amount: amount,
//             blockId: log.blockNumber,
//             isNFTToken: false,
//             txType: txType,
//             trnx_fee: gasInfo.trnx_fee
//         };

//         if (txData.txType !== null) {
//             await this.addTxToQueue(
//                 txData,
//                 this.deposit_withdraw_topic || ""
//             );
//         }
//     };

//     private calculateTokenAmount = async (data: string, decimals: number): Promise<number> => {
//         const decimalPrecision = decimals.toString().length - 1;
//         return await utilityHelper.bigNumberSafeMath(data, '/', Math.pow(10, decimalPrecision));
//     };

//     public async checkPolDeposit(data: blocksTrnx) {
//         try {
//             /** for check the eth deposit */
//             if (
//                 data.toAddress
//             ) {

//                 // console.log("inside checkEthDeposit this is data.toAddress ", data.toAddress)

//                 const addresses = [data.fromAddress, data.toAddress].filter(Boolean);

//                 let txType = null;
//                 if (addresses.length > 0) {
//                     const results = await bscProcessHelper.getMultipleKeyValuePairs(`${config.BSC_WALLET_ADDRESS}`, addresses);


//                     if (results) {
//                         const [withdrawTx, depositTx] = results as string[];
//                         if (depositTx) {
//                             txType = TrnxTypeEnum.DEPOSIT;
//                         } else if (withdrawTx) {
//                             txType = TrnxTypeEnum.WITHDRAW;
//                         }
//                     }
//                 }
//                 if (txType !== null) {

//                     if (data.gasUsed == null && data.gasPrice == null) {
//                         const txReceipt: TransactionReceipt | null = await blockchainHelper.provider.getTransactionReceipt(data?.txId);
//                         const gasUsed: number = Number(txReceipt?.gasUsed) || 0;
//                         data.gasUsed = gasUsed;
//                         data.gasPrice = Number(txReceipt?.gasPrice) || 0;
//                     }

//                     const gasUsed = data.gasUsed ? data.gasUsed : 0;
//                     const effectiveGasPrice: number = Number(data.gasPrice) || 0;
//                     const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
//                     const gasInEth = await blockchainHelper.convertWeiToBsc(txFee);

//                     const ethValue = await blockchainHelper.convertWeiToBsc((data.value ? data.value : 0));
//                     const amount = parseFloat(ethValue.toString());

//                     const txData: TxType = {
//                         txId: data.txId,
//                         fromAddress: data.fromAddress.toLowerCase(),
//                         toAddress: data.toAddress.toLowerCase(),
//                         token: null,
//                         amount,
//                         blockId: data?.blockNumber,
//                         isNFTToken: false,
//                         txType: txType,
//                         trnx_fee: Number(gasInEth)
//                     };

//                     await this.addTxToQueue(
//                         txData,
//                         this.deposit_withdraw_topic || ""
//                     );
//                 }
//             } else {
//                 console.log("else block checkEthDeposit")
//             }
//         } catch (error: any) {
//             console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
//             await dbHelper.saveErrorLogs('ETH_check_deposit', error.message)

//         }
//     }

//     private async checkAddressInDB(txData: TxType) {
//         try {
//             // let ourTrnx: any = null;
//             const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

//             if (addresses.length > 0) {
//                 const results = await bscProcessHelper.getMultipleKeyValuePairs(`${config.BSC_WALLET_ADDRESS}`, addresses);
//                 if (results) {
//                     const [withdrawTx, depositTx] = results as string[];
//                     if (depositTx) {
//                         txData.txType = TrnxTypeEnum.DEPOSIT;
//                     } else if (withdrawTx) {
//                         txData.txType = TrnxTypeEnum.WITHDRAW;
//                     }
//                 }
//             }

//             if (txData.txType !== null) {
//                 await this.addTxToQueue(
//                     txData,
//                     this.deposit_withdraw_topic || ""
//                 );
//             }
//         } catch (err: any) {
//             console.error("Error in checkAddressInDB>>", err)
//         }
//     }

//     private async addTxToQueue(data: TxType, queueName: string) {
//         try {
//             await kafkaHelper.produceMessage(queueName, data)
//         } catch (err: any) {
//             console.error("addTxToQueue", (err as Error).message);
//             await dbHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

//         }
//     };

//     private async addTraceTxToQueue(data: blocksTrnx, queueName: string) {
//         try {
//             await kafkaHelper.produceMessage(queueName, data)
//         } catch (err: any) {
//             console.error("addTxToQueue", (err as Error).message);
//             await dbHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

//         }
//     };






// }

// export const readBlockTrnxProcess = new ReadBlockTrnxProcess();