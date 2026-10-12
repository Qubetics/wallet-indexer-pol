import { Blockchain_Helper } from "../helpers/index";
import { config } from "../config/config";
import polProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
import { PolOldBLockModel } from "../models";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";


class Pol_block_process {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public pol_block_tx_topic = config.KAFKA_BLOCKS.POL_BLOCK_TX_TOPIC;
    public pol_behind_block_tx_topic = config.KAFKA_BLOCKS.POL_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = Blockchain_Helper.Web3.utils.sha3('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = Blockchain_Helper.Web3.utils.sha3('Approval(address,address,uint256)');
    }

    public async getBlocks() {
        try {
            /** Testing */
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
            let block_updated: number = blockNumber + 1;
            await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.OLD_BLOCK_POL, block_updated.toString());
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
        const { getBlock } = Blockchain_Helper.Web3.eth;
        try {
            const blockInfo = await getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await PolOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.POL, start_block: blockId, end_block: blockId, status: 0 })
                return;
            }
            if (!blockInfo.transactions || blockInfo.transactions.length === 0) {
                console.log("No block info transactions");
                return; // Exit early if there are no transactions
            }
            const transactionPromises = blockInfo.transactions.map(async (tx) => {
                const data: blocks_trnx = {
                    block_number: blockId,
                    tx_id: tx.hash || '',
                    value: tx.value || '',
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
            await Promise.allSettled(transactionPromises);
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
                        await PolOldBLockModel.update({ status: 1 }, { where: { id: id } })
                        await polProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL)
                        await polProcessHelper.behindBlocks();
                    }
                }
            } else {
                // DB BASED
                await polProcessHelper.behindBlocks()
            }

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
            // Get Block Number
            let { getBlockNumber } = Blockchain_Helper.Web3.eth;
            let latestBlock = await getBlockNumber();
            console.log("Block  number>>", latestBlock)

            // Get Block Info
            const { getBlock } = Blockchain_Helper.Web3.eth;
            const blockInfo = await getBlock(latestBlock, true);
            console.log("blockInfo>>", blockInfo)

            // Get Block Receipt
            const { getTransactionReceipt } = Blockchain_Helper.Web3.eth;
            const txReceipt = await getTransactionReceipt(blockInfo.transactions[0].hash);
            console.log("txreceipt>>", txReceipt)

            // Get more details which was reduced
            const { getTransaction } = Blockchain_Helper.Web3.eth;
            const transaction = await getTransaction(blockInfo.transactions[0].hash);
            console.log("transaction>>", transaction)

        } catch (err: any) {
            console.error("Error in testingNode>>", err)
        }
    }
}

export const pol_blocks_process = new Pol_block_process();