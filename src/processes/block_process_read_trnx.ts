import { Blockchain_Helper } from "../helpers/index";
import { config } from "../config/config";
import polProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
// import rabbitMq from "../config/rabbitMq";
import { PolOldBLockModel } from "../models";
import { Op } from "sequelize";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class Pol_block_process {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public pol_block_tx_topic = config.KAFKA_BLOCKS.POL_BLOCK_TX_TOPIC;
    public pol_behind_block_tx_topic = config.KAFKA_BLOCKS.POL_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
    }

    public async getBlocks() {
        try {
            /** Testing */
            // let { getBlockNumber } = Blockchain_Helper.Web3.eth;
            // let latestBlock = await getBlockNumber();
            // console.log("latestBlock>>", latestBlock)
            // return;
            let old_block: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL)
            let new_block: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL)
            if (!old_block) {
                console.log("No block present in old block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                let oldBlock: number = Number(old_block);
                let newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, newBlock)
                } else {
                    let block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await polProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, newBlock.toString())
                        await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await global_helper.save_error_logs('Pol_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await global_helper.save_error_logs('Pol_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            // console.log("blockNumber", blockNumber)
            let block_updated: number = blockNumber + 1;
            await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, block_updated.toString());
            // console.log("blockNumber updated", block_updated)
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await global_helper.save_error_logs('POL_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            const blockInfo = await Blockchain_Helper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await PolOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.POL, start_block: blockId, end_block: blockId, status: 0 })
                return;
            }
            const transactions = blockInfo.prefetchedTransactions || [];
                if (transactions.length === 0) {
                    console.log("No block info transactions");
                    return;
                }
            const transactionPromises = transactions.map(async (tx) => {
                // console.log('tx.hash >>', tx.hash, 'blockId >>>', blockId);
                const data: blocks_trnx = {
                    block_number: blockId,
                    tx_id: tx.hash || '',
                    value: tx.value ? tx.value.toString() : '',
                    fromAddress: tx.from || '',
                    toAddress: tx.to || '',
                    retry_count: 0,
                    key: null,
                    pair: null
                };

                data.key = (key === KEYS.latestBlock) ? 'latest' : 'behined & specific';
                const targetTopic = (key === KEYS.latestBlock) ? this.pol_block_tx_topic : this.pol_behind_block_tx_topic;

                try {
                    await kafkaHelper.produceMessage(targetTopic, data);
                    // If you were using RabbitMQ, you'd uncomment the appropriate line:
                    // const queue = (key === KEYS.latestBlock) ? config.TRANSACTIONS.eth_all_blocks : config.KAFKA_BLOCKS.eth_behined_block_topic;
                    // rabbitMq.send_tx_to_queue(queue, Buffer.from(JSON.stringify(data)));
                } catch (err) {
                    console.error("Error while producing message for transaction", tx.hash, err);
                }
            });
            await Promise.allSettled(transactionPromises);
            // if (blockInfo) {
            //     if (blockInfo.transactions) {
            //         for await (let tx of blockInfo.transactions) {
            //             console.log('tx.hash >>', tx.hash, 'blockId >>>', blockId);
            //             let data: blocks_trnx = {
            //                 block_number: blockId,
            //                 tx_id: tx.hash ? tx.hash : '',
            //                 value: tx.value ? tx.value : '',
            //                 fromAddress: tx.from ? tx.from : '',
            //                 toAddress: tx.to ? tx.to : '',
            //                 retry_count: 0,
            //                 key: null,
            //                 pair: null
            //             }
            //             if (key == KEYS.latestBlock) {
            //                 data.key = 'latest';
            //                 // let queue_length: any = await rabbitMq.queue_length(config.TRANSACTIONS.pol_all_blocks)
            //                 // console.log("queue_length>>>", queue_length)
            //                 await kafkaHelper.produceMessage(this.pol_block_tx_topic, data);
            //                 //rabbitMq.send_tx_to_queue(config.TRANSACTIONS.pol_all_blocks, Buffer.from(JSON.stringify(data)))
            //             } else {
            //                 data.key = 'behined & specific'
            //                 await kafkaHelper.produceMessage(this.pol_behind_block_tx_topic, data);
            //                 //rabbitMq.send_tx_to_queue(config.KAFKA_BLOCKS.pol_behined_block_topic, Buffer.from(JSON.stringify(data)))
            //             }
            //         }
            //     } else {
            //         console.log("no block info trnasactions")
            //     }
            // } else {
            //     console.log("No blockinfo");
            //     await PolOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: blockId, end_block: blockId, status: 0 })
            // }
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await global_helper.save_error_logs('Pol_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            let data_exist: any = await polProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL)

            if (data_exist) {
                let data_exists = JSON.parse(data_exist)
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;


                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        let add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await polProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        // await PolOldBLockModel.destroy({ where: { id: id } })
                        await PolOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await polProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL)
                        await polProcessHelper.behindBlocks();
                    }
                }
            } else {
                // DB BASED
                await polProcessHelper.behindBlocks()
            }

            // REDIS BASED
            // let data: any = await polProcessHelper.getHashTable(config.READ_BEHINED_BLOCK_POL)
            // console.log("data?>>??", data.length)
            // if (data.length > 0) {
            //     for (let i: number = 0; i < data.length; i++) {
            //         await pol_blocks_process.getTxFromBlock(Number(data[i]), KEYS.behinedBlock);
            //         await polProcessHelper.deleteKeyValuePair(config.READ_BEHINED_BLOCK_POL, data[i].toString())
            //     }
            // }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await global_helper.save_error_logs('POL_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL)
            let specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }
    public async testingNode() {
        try {
            const provider = Blockchain_Helper.provider;
            
            // Get Block Number
            const latestBlock = await provider.getBlockNumber();
            console.log("Block number>>", latestBlock);

            const blockInfo = await provider.getBlock(latestBlock, true);
            console.log("blockInfo>>", blockInfo);

            if (blockInfo && blockInfo.transactions.length > 0) {
                let firstTxHash: string;
                const firstTx = blockInfo.transactions[0];
                
                if (typeof firstTx === 'string') {
                    firstTxHash = firstTx;
                } else {
                    // Assert the type to access the hash property safely
                    firstTxHash = (firstTx as ethers.TransactionResponse).hash;
                }
                // Get Block Receipt
                const txReceipt = await provider.getTransactionReceipt(firstTxHash);
                console.log("txreceipt>>", txReceipt);
                
                // Get more details (not reduced in ethers.js)
                const transaction = await provider.getTransaction(firstTxHash);
                console.log("transaction>>", transaction);
            } else {
                console.log("No transactions found in the block");
            }
        } catch (err: any) {
            console.error("Error in testingNode>>", err)
        }
    }
}

export const pol_blocks_process = new Pol_block_process();