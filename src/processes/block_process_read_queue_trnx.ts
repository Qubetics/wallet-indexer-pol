import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Blockchain_Helper, Utility_Helper } from "../helpers";
import { blocks_trnx } from "../interfaces/global_helper";
import polProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { ethers } from "ethers";

class queue_transactions {
    public approval: string | null;
    public swap: string | null;
    public transfer: string | null;
    public log_fee_transfer: string | null;

    public pol_block_tx_topic = config.KAFKA_BLOCKS.POL_BLOCK_TX_TOPIC;
    public pol_block_tx_group_id = config.KAFKA_BLOCKS.POL_BLOCK_TX_GROUP_ID;
    public pol_behind_block_tx_topic = config.KAFKA_BLOCKS.POL_BEHIND_BLOCK_TX_TOPIC;
    public pol_behind_block_tx_group_id = config.KAFKA_BLOCKS.POL_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.transfer = ethers.id('Transfer(address,address,uint256)');
        this.log_fee_transfer = ethers.id('LogFeeTransfer(address,address,address,uint256,uint256,uint256,uint256,uint256)');

    }

    public startBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.pol_block_tx_group_id });
        await consumer.subscribe({ topic: this.pol_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBlockTxQueue consumer error", err);
        });
    };

    public startBehindBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.pol_behind_block_tx_group_id });
        await consumer.subscribe({ topic: this.pol_behind_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
        });
    };

    public async read_trnx(data: blocks_trnx) {
        try {
            const txReceipt = await Blockchain_Helper.provider.getTransactionReceipt(data.tx_id);
            if (txReceipt == null) {
                if (Number(data.retry_count) < 6) {
                    data.retry_count = Number(data.retry_count) + 1;
                    await kafkaHelper.produceMessage(this.pol_behind_block_tx_topic, data)
                }
            }

            if (txReceipt?.status === false && txReceipt !== null) {
                console.log("(txReceipt.status === false && txReceipt !== null")
            } else {
                await pol_tx_status_deposit_withdraw.buildTx(txReceipt, data)
            }
        } catch (error: any) {
            console.error("Error in read_trnx>>>", error)
            if (Number(data.retry_count) < 3) {
                data.retry_count = Number(data.retry_count) + 1;
                console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data)
                await kafkaHelper.produceMessage(this.pol_behind_block_tx_topic, data)
            }
        }
    }
    public async buildTx(txReceipt: any, data: blocks_trnx) {
        try {
            if (txReceipt.status == true && txReceipt.logs.length > 0) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const effectiveGasPrice: number = txReceipt?.effectiveGasPrice ? txReceipt?.effectiveGasPrice : 0;
                const gasInPol = await Blockchain_Helper.convertWeiToPOL(
                    await Utility_Helper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice)
                );
                for await (const log of txReceipt.logs) {
                    const isToken: any = await polProcessHelper.checkIfContract(log.address);
                    if (isToken !== null && log.topics[2] !== undefined) {
                        if (log?.topics && log?.topics[0] == this.approval) {
                            continue;
                        } else if (log?.topics && log?.topics[0] == this.transfer) {
                            const decimalPrecision = isToken.decimals.toString().length - 1;
                            let finalAmount = await Utility_Helper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                            finalAmount = await Utility_Helper.exponentialToDecimal(finalAmount)
                            let toAddress: string = '0x' + log.topics[2].substring(26, log.topics[2].length);
                            /* in case of swap trnx */
                            let fromAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
                            const txData: TxType = {
                                txId: log.transactionHash,
                                fromAddress: fromAddress,
                                toAddress: toAddress,
                                token: isToken,
                                amount: finalAmount,
                                blockId: log.blockNumber,
                                isNFTToken: false,
                                txType: null,
                                trnx_fee: Number(gasInPol)
                            };
                            await this.checkAddressInDB(txData);
                        }
                    }
                    /** swap transaction then also check pol */
                    if (log?.topics && log?.topics[0] == this.swap || log?.topics && log?.topics[0] == this.log_fee_transfer) {
                        await pol_tx_status_deposit_withdraw.check_pol_deposit(data, txReceipt);
                    } else if (isToken == null && log.topics[2] !== undefined && log?.topics[0] == this.transfer) {
                        await pol_tx_status_deposit_withdraw.check_pol_deposit(data, txReceipt)
                    }
                }
            } else {
                await pol_tx_status_deposit_withdraw.check_pol_deposit(data, txReceipt);
            }
        } catch (error: any) {
            console.error("buildTx error >>>", (error as Error).message);
            await global_helper.save_error_logs('Pol_buildTx', error.message)
        }
    };
    public async check_pol_deposit(data: blocks_trnx, txReceipt: any) {
        try {
            const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
            const effectiveGasPrice: number = txReceipt?.effectiveGasPrice ? txReceipt?.effectiveGasPrice : 0;
            const gasInPol = await Blockchain_Helper.convertWeiToPOL(
                await Utility_Helper.bigNumberSafeMath(
                    gasUsed, '*', effectiveGasPrice
                )
            );

            /** for check the pol deposit */
            if (
                txReceipt.status == true,
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                const polValue = await Blockchain_Helper.convertWeiToPOL((data.value ? data.value : 0));
                const amount = parseFloat(polValue.toString());
                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: TrnxTypeEnum.NULL,
                    trnx_fee: Number(gasInPol)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_pol_deposit error", (error as Error).message);
            await global_helper.save_error_logs('POL_check_deposit', error.message)

        }
    }
    public async checkAddressInDB(txData: TxType) {
        try {
            let ourTrnx: any = null;
            let withdraw = await polProcessHelper.check_our_wallet_address(txData.fromAddress);
            if (withdraw) {
                console.log("Found the user in FROM (WITHDRAW) tx 😸");
                txData.txType = TrnxTypeEnum.WITHDRAW;
                ourTrnx = txData;
            }
            let deposit = await polProcessHelper.check_our_wallet_address(txData.toAddress);
            if (deposit) {
                console.log("Found the user in to (DEPOSIT) tx 😸");
                txData.txType = TrnxTypeEnum.DEPOSIT;
                ourTrnx = txData;
            }
            if (ourTrnx) {
                await pol_tx_status_deposit_withdraw.addTxToQueue(
                    ourTrnx,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }
    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await global_helper.save_error_logs('POL_checkAddressInDB', err.message)

        }
    };

}
export const pol_tx_status_deposit_withdraw = new queue_transactions()
