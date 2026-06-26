import { blockchainHelper, dbHelper, utilityHelper } from "../helpers/index";
import { config } from "../config/config";
import polProcessHelper from "./process.helper";
import { TrnxTypeEnum } from "../enum";
import { blocksTrnx } from "../interfaces/global.interface";
import { PolOldBLockModel } from "../models";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt } from "ethers";
import { TxType } from "../types/ERC20Tokens.types";
import { BigNumber } from "bignumber.js";
import { TokenData } from "../interfaces/ether.interface";
import { isValidToken } from "../helpers/validToken.helper";

class ReadBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public transfer_by_partition: string | null;
    public approval_by_partition: string | null;
    public processed_dividend_tracker: string | null;
    public dividend_withdrawn: string | null;
    public deposit: string | null;
    public LogFeeTransfer: string | null;
    public LogTransfer : string | null;
    public DelegateChanged : string | null;
    public pol_block_tx_topic = config.KAFKA_BLOCKS.POL_BLOCK_TX_TOPIC;
    public pol_behind_block_tx_topic = config.KAFKA_BLOCKS.POL_BEHIND_BLOCK_TX_TOPIC;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;
    public trace_block_eth_deposit_topic = config.KAFKA_TRANSACTIONS.TRACE_BLOCK_POL_DEPOSIT_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.transfer_by_partition = ethers.id('TransferByPartition(bytes32,address,address,address,uint256,bytes,bytes)');
        this.approval_by_partition = ethers.id('ApprovalByPartition(bytes32,address,address,uint256)');
        this.processed_dividend_tracker = ethers.id('ProcessedDividendTracker(uint256,uint256,uint256,bool,uint256,address)');
        this.dividend_withdrawn = ethers.id('DividendWithdrawn(address,uint256)');
        this.deposit = ethers.id('Deposit(address,uint256)');
        this.LogFeeTransfer = ethers.id('LogFeeTransfer(address,address,address,uint256,uint256,uint256,uint256,uint256)');
        this.LogTransfer = ethers.id('LogTransfer(address,address,address,uint256,uint256,uint256,uint256,uint256)');
        this.DelegateChanged = ethers.id('DelegateChanged(address,address,address)');
    }

    public async readCurrentBlocks() {
        try {
            const results = await polProcessHelper.getMultipleKeyValuePairs(config.REDISKEYS.POL_BLOCKS, [config.REDISKEYS.START_BLOCK, config.REDISKEYS.END_BLOCK]);
            console.log("🚀 ~ EthBlockProcess ~ readCurrentBlocks ~ results:", results)

            // Validate results array
            if (!Array.isArray(results) || results.length < 2) {
                console.error("Invalid results from Redis operation:", results);
                return;
            }
            const [old_block, new_block] = results;
            if (!old_block) {
                console.log("No block present in old block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.START_BLOCK, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                const oldBlock: number = Number(old_block);
                const newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, newBlock)
                    return;
                } else {
                    const block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await polProcessHelper.diffBlocksAddInDb(newBlock, oldBlock)
                        await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.START_BLOCK, newBlock.toString())
                        await polProcessHelper.setBlocks(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.END_BLOCK, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log("No old block and no new block")
                await dbHelper.saveErrorLogs('Eth_getBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await dbHelper.saveErrorLogs('Eth_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            const block_updated: number = blockNumber + 1;
            await polProcessHelper.setKeyValuePair(config.REDISKEYS.POL_BLOCKS, config.REDISKEYS.START_BLOCK, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            // await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
            this.getBlockReceipts(blockNumber);
            this.getTraceBlock(blockNumber);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await dbHelper.saveErrorLogs('ETH_readBlocks', err.message)
        }
    }
    public async readBehindBlocks() {
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
                        // await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        this.getBlockReceipts(start_block);
                        this.getTraceBlock(start_block);
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
                        await PolOldBLockModel.update({ status: 1 }, { where: { id: id } })
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
            await dbHelper.saveErrorLogs('ETH_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Enter into readSpecificBlock>>>")
            const specificBlocks: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL)
            const specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                // await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                this.getBlockReceipts(specific_block);
                this.getTraceBlock(specific_block);
                await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_POL, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }

    // Recursive function to collect calls dynamically
    public collectCalls(calls: any[], depth: number = 0, results: any[] = [], txHash?: string): any[] {
        for (const call of calls) {
            if (call?.error) {
                console.log("(call?.error", call?.error, 'txHash >>>', txHash);
            } else {
                if (call?.value && Number(call.value) > 0) {
                    results.push({
                        depth,
                        type: call.type,
                        txHash: txHash,
                        value: call.value,
                        callData: call,
                    });
                }
                // If nested calls exist, process them recursively
                if (call?.calls?.length > 0) {
                    this.collectCalls(call.calls, depth + 1, results, txHash);
                }
            }
        }
        return results;

    }

    public async processTraces(response: any[]) {
        const allCalls: any[] = [];

        if (response?.length > 0) {
            for (const trace of response) {
                const result = trace?.result;

                if (!result?.error) {
                    // Top-level CALL
                    if (result?.type === 'CALL' && Number(result?.value) > 0) {

                        allCalls.push({
                            depth: 0,
                            type: result.type,
                            value: result.value,
                            txHash: trace.txHash,
                            callData: result,
                        });

                    }
                    // Collect child calls if they exist
                    if (result?.calls?.length > 0) {
                        const collected = this.collectCalls(result.calls, 1, [], trace.txHash);
                        allCalls.push(...collected);
                    }
                } else {
                    console.log("(result?.error", result?.error, 'trace.txHash <<', trace.txHash);
                }
            }
        }

        return allCalls;
    }


    public getTraceBlock = async (blockId: number) => {
        try {
            console.log("INSIDE getTraceBlock", blockId);

            const hexBlockValue = ethers.toBeHex(blockId.toString());
            const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');

            const response = await blockchainHelper.provider.send("debug_traceBlockByNumber", [
                formattedBlockValue,
                { "tracer": "callTracer" }
            ]);

            if (response?.length > 0) {
                const allValueCalls = await this.processTraces(response);

                // console.log("All calls with value > 0:", allValueCalls);

                // Save to Kafka
                for (const call of allValueCalls) {
                    const txReceipt: blocksTrnx = {
                        txId: call.txHash,
                        fromAddress: call.callData.from,
                        toAddress: call.callData.to,
                        value: call.value.toString(),
                        gasUsed: null,
                        gasPrice: null,
                        blockNumber: blockId,
                    };

                    //   console.log(`Processed Tx at depth ${call.depth}`, txReceipt);
                    this.addTraceTxToQueue(txReceipt, this.trace_block_eth_deposit_topic);
                }

            } else {
                console.error("Error fetching data from QuickNode API.");
                this.getTxFromBlock(blockId);
            }

        } catch (error) {
            console.log("🔥 ~ getTraceBlock error", (error as Error).message);
            this.getTxFromBlock(blockId);
        }
    };


    public getBlockReceipts = async (blockId: number) => {
        try {
            console.log("INSIDE getBlockReceipts")
            // Get block receipts
            const blockReceipts = await this.fetchBlockReceipts(blockId);
            if (!blockReceipts) {
                console.error(`💥 ~ No transactions found in block ${blockId}`);
                return false;
            }

            // Process each transaction receipt
            for (const txReceipt of blockReceipts) {
                await this.processTxReceipt(txReceipt);
            }

            return true;
        } catch (error) {
            console.log("🔥 ~ getBlockReceipts error", (error as Error).message);
            return false;
        }
    };

    public async getTxFromBlock(blockId: number) {
        try {
            console.error("In getTxFromBlock>>.", blockId)
            const blockInfo = await blockchainHelper.provider.getBlock(blockId, true);
            if (!blockInfo) {
                console.log("No block info");
                await PolOldBLockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.ETH,
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
                const amount: BigNumber = new BigNumber(tx?.value.toString());
                if (amount.gt(0)) {
                    const txReceipt: TransactionReceipt | null = await blockchainHelper.provider.getTransactionReceipt(tx?.hash);

                    if (txReceipt?.status == 0) {
                        console.log("(txReceipt.status === false && txReceipt !== null");
                        return false;
                    }
                    const gasUsed = txReceipt?.gasUsed ? txReceipt.gasUsed : 0;
                    const gasPrice: number = Number(txReceipt?.gasPrice) || 0;
                    const blockTransaction: blocksTrnx = {
                        txId: tx.hash || '',
                        value: amount?.toString(),
                        fromAddress: tx.from || '',
                        toAddress: tx.to || '',
                        blockNumber: blockId,
                        gasUsed: Number(gasUsed.toString()),
                        gasPrice: gasPrice,
                    }
                    await this.checkPolDeposit(blockTransaction);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await dbHelper.saveErrorLogs('Eth_readBehindBlock', err.message)
        }
    }

    private fetchBlockReceipts = async (blockId: number) => {
        const hexBlockValue = ethers.toBeHex(blockId.toString());
        const formattedBlockValue = hexBlockValue.replace(/^0x0+/g, '0x');
        return await blockchainHelper.provider.send("eth_getBlockReceipts", [formattedBlockValue]);
    };

    private processTxReceipt = async (txReceipt: TransactionReceipt) => {
        // Only process successful transactions with logs
        if (!txReceipt?.status || !txReceipt?.logs?.length) {
            return;
        }

        const gasInfo = await this.extractGasInfo(txReceipt);

        for (const log of txReceipt.logs) {
            await this.processLog(log, gasInfo);
        }
    };

    private extractGasInfo = async (txReceipt: any) => {
        const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
        const hexToNumberGasPrice = txReceipt?.effectiveGasPrice ? Number(txReceipt.effectiveGasPrice) : 0;
        const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);
        const gasInEth = await blockchainHelper.convertWeiToPol(txFee);

        return {
            gasUsed: gasUsed,
            gasPrice: hexToNumberGasPrice,
            trnx_fee: Number(gasInEth)
        };
    };

    private processLog = async (log: any, gasInfo: any) => {

        const contractInfo = await polProcessHelper.checkIfContract(log.address);
        if (this.isTokenTransfer(log, contractInfo)) {
            //here  we have contractInfo.token_address
            await this.processTokenTransfer(log, gasInfo, contractInfo);
        }
        else {
            if (this.isTokenTransferNotExist(log)) {
                //here  we have the from and to address 
                const toAddress = '0x' + log.topics[2]?.substring(26);
                const addresses = [toAddress].filter(Boolean);

                if (addresses.length > 0) {
                    const results = await polProcessHelper.getMultipleKeyValuePairs(`${config.POL_WALLET_ADDRESS}`, addresses);
                    if (results) {
                        const [depositTx] = results as string[];
                        if (depositTx) {
                            const nativeWalletData: number = depositTx ? JSON.parse(depositTx) : null;
                            if (nativeWalletData) {
                                console.log("contractInfo not exist in db 💥💥 inserting into db", log.address)
                                const tokenDetail: TokenData | null = await blockchainHelper.tokenDetails(log.address);
                                if (tokenDetail) {
                                    //here we need to check valid token address
                                    const isValidTokenAddress : boolean = await isValidToken(log.address);

                                    //if not valid token address, return
                                    if (!isValidTokenAddress) {
                                        return;
                                    }

                                    const contractInfo = await polProcessHelper.insertTokenAddressInDB(log.address, tokenDetail, nativeWalletData);
                                    if (contractInfo) {
                                        await this.processNotExistTokenTransfer(log, gasInfo, contractInfo, TrnxTypeEnum.DEPOSIT)
                                    } else {
                                        return;
                                    }

                                }
                            } else {
                                return;
                            }

                        } else {
                            return;
                        }
                    }
                }
            }
        }
    };

    private isNFTTransfer = (log: any, nftInfo: any): boolean => {
        return nftInfo && Object.keys(nftInfo)?.length > 0 &&
            log.topics[1] !== undefined &&
            log.topics[2] !== undefined;
    };

    private isTokenTransfer = (log: any, contractInfo: any): boolean => {
        return contractInfo && Object.keys(contractInfo)?.length > 0 &&
            log.topics[2] !== undefined &&
            !this.isExcludedTopic(log.topics[0]);
    };

    private isTokenTransferNotExist = (log: any): boolean => {
        return log &&
            log.topics[1] !== undefined &&
            log.topics[2] !== undefined &&
            !this.isExcludedTopic(log.topics[0]);
    };

    private isExcludedTopic = (topic: string): boolean => {
        const excludedTopics = [
            this.approval,
            this.approval_by_partition,
            this.transfer_by_partition,
            this.processed_dividend_tracker,
            this.dividend_withdrawn,
            this.LogFeeTransfer,
            this.LogTransfer,
            this.DelegateChanged
        ];
        return excludedTopics.includes(topic);
    };

    private processTokenTransfer = async (log: any, gasInfo: any, contractInfo: any) => {
        const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);

        if (amount <= 0) {
            return;
        }

        const txData: TxType = {
            txId: log.transactionHash,
            fromAddress: '0x' + log.topics[1]?.substring(26),
            toAddress: '0x' + log.topics[2]?.substring(26),
            token: contractInfo,
            amount: amount,
            blockId: log.blockNumber,
            isNFTToken: false,
            txType: null,
            trnxFee: gasInfo.trnx_fee
        };

        await this.checkAddressInDB(txData);
    };

    private processNotExistTokenTransfer = async (log: any, gasInfo: any, contractInfo: any, txType: TrnxTypeEnum.DEPOSIT) => {
        const amount = await this.calculateTokenAmount(log.data, contractInfo.decimals);
        if (amount <= 0) {
            return;
        }

        const txData: TxType = {
            txId: log.transactionHash,
            fromAddress: '0x' + log.topics[1]?.substring(26),
            toAddress: '0x' + log.topics[2]?.substring(26),
            token: contractInfo,
            amount: amount,
            blockId: log.blockNumber,
            isNFTToken: false,
            txType: txType,
            trnxFee: gasInfo.trnx_fee
        };

        if (txData.txType !== null) {
            await this.addTxToQueue(
                txData,
                this.deposit_withdraw_topic || ""
            );
        }
    };

    private calculateTokenAmount = async (data: string, decimals: number): Promise<number> => {
        const decimalPrecision = decimals.toString().length - 1;
        return await utilityHelper.bigNumberSafeMath(data, '/', Math.pow(10, decimalPrecision));
    };

    public async checkPolDeposit(data: blocksTrnx) {
        try {
            /** for check the eth deposit */
            if (
                data.toAddress
            ) {

                // console.log("inside checkEthDeposit this is data.toAddress ", data.toAddress)

                const addresses = [data.fromAddress, data.toAddress].filter(Boolean);

                let txType = null;
                if (addresses.length > 0) {
                    const results = await polProcessHelper.getMultipleKeyValuePairs(`${config.POL_WALLET_ADDRESS}`, addresses);


                    if (results) {
                        const [withdrawTx, depositTx] = results as string[];
                        if (depositTx) {
                            txType = TrnxTypeEnum.DEPOSIT;
                        } else if (withdrawTx) {
                            txType = TrnxTypeEnum.WITHDRAW;
                        }
                    }
                }
                if (txType !== null) {

                    if (data.gasUsed == null && data.gasPrice == null) {
                        const txReceipt: TransactionReceipt | null = await blockchainHelper.provider.getTransactionReceipt(data?.txId);
                        const gasUsed: number = Number(txReceipt?.gasUsed) || 0;
                        data.gasUsed = gasUsed;
                        data.gasPrice = Number(txReceipt?.gasPrice) || 0;
                    }

                    const gasUsed = data.gasUsed ? data.gasUsed : 0;
                    const effectiveGasPrice: number = Number(data.gasPrice) || 0;
                    const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
                    const gasInEth = await blockchainHelper.convertWeiToPol(txFee);

                    const ethValue = await blockchainHelper.convertWeiToPol((data.value ? data.value : 0));
                    const amount = parseFloat(ethValue.toString());

                    const txData: TxType = {
                        txId: data.txId,
                        fromAddress: data.fromAddress.toLowerCase(),
                        toAddress: data.toAddress.toLowerCase(),
                        token: null,
                        amount,
                        blockId: data?.blockNumber,
                        isNFTToken: false,
                        txType: txType,
                        trnxFee: Number(gasInEth)
                    };

                    await this.addTxToQueue(
                        txData,
                        this.deposit_withdraw_topic || ""
                    );
                }
            } else {
                console.log("else block checkEthDeposit")
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
            await dbHelper.saveErrorLogs('ETH_check_deposit', error.message)

        }
    }

    private async checkAddressInDB(txData: TxType) {
        try {
            // let ourTrnx: any = null;
            const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

            if (addresses.length > 0) {
                const results = await polProcessHelper.getMultipleKeyValuePairs(`${config.POL_WALLET_ADDRESS}`, addresses);
                if (results) {
                    const [withdrawTx, depositTx] = results as string[];
                    if (depositTx) {
                        txData.txType = TrnxTypeEnum.DEPOSIT;
                    } else if (withdrawTx) {
                        txData.txType = TrnxTypeEnum.WITHDRAW;
                    }
                }
            }

            if (txData.txType !== null) {
                await this.addTxToQueue(
                    txData,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    private async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await dbHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

        }
    };

    private async addTraceTxToQueue(data: blocksTrnx, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await dbHelper.saveErrorLogs('ETH_checkAddressInDB', err.message)

        }
    };
}

export const readBlockProcess = new ReadBlockProcess();