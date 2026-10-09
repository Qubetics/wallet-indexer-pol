import { blockchainHelper } from "../helpers/index";
import { config } from "../config/config";
import polProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocksTrnx } from "../interfaces/globalHelper";
import { polOldBLockModel } from "../models";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers } from "ethers";


class PolBlockProcess {
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

    public async readCurrentBlocks() {
        try {
            const results = await polProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.POL_BLOCKS, [config.REDISKEYS.OLD_BLOCK_POL, config.REDISKEYS.NEW_BLOCK_POL]);

            // Validate results array
            if (!Array.isArray(results) || results.length < 2) {
                console.error("Invalid results from Redis operation:", results);
                return;
            }
            const [old_block, new_block] = results;
            if (!old_block) {
                console.log("No block present in old block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                const oldBlock: number = Number(old_block);
                const newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, newBlock)
                    return;
                } else {
                    const block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await polProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, newBlock.toString())
                        await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.NEW_BLOCK_POL, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log("No old block and no new block")
                await globalHelper.saveErrorLogs('Pol_getBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await globalHelper.saveErrorLogs('Pol_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            const block_updated: number = blockNumber + 1;
            await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await globalHelper.saveErrorLogs('POL_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            console.error("In getTxFromBlock>>.", key, blockId)
            const blockInfo = await blockchainHelper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await polOldBLockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.POL,
                    start_block: blockId,
                    end_block: blockId,
                    status: 0
                });
                return;
            }
            const transactions = blockInfo.prefetchedTransactions || [];
            if (transactions.length === 0) {
                console.log("No block info transactions");
                return;
            }

            const transactionPromises = transactions.map(async (tx) => {
                const data: blocksTrnx = {
                    block_number: blockId,
                    tx_id: tx.hash || '',
                    value: tx.value.toString(),
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
                } catch (err) {
                    console.error("Error while producing message for transaction", tx.hash, err);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await globalHelper.saveErrorLogs('Pol_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            const data_exist: any = await polProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL)
            if (data_exist) {
                const data_exists = JSON.parse(data_exist);
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;
                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        const add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await polProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        await polOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await polProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL)

                        await polProcessHelper.behindBlocks()
                    }
                }
            } else {
                // DB BASED
                await polProcessHelper.behindBlocks()
            }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await globalHelper.saveErrorLogs('POL_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            const specificBlocks: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL)
            const specific_block: any = JSON.parse(specificBlocks);
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


}

export const polBlocksProcess = new PolBlockProcess();