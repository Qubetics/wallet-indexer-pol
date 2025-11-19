import { TokenStandard } from "../enum";
import { utilityHelper } from "./utility.helper";
import { config } from '../config';
import { ethers } from 'ethers';
import { TokenData } from "../interfaces/ether.interface";
import { coinData } from "../interfaces/global.interface";

class BlockchainHelper {
    public provider: ethers.JsonRpcProvider;

    constructor() {
        this.provider = new ethers.JsonRpcProvider(config.config.NODE.POL_RPC_URL || "");
        this.initialize();
    }

    public initialize() { }

    //POL
    public async fetchPolBalance(address: string, coin: coinData) {
        try {
            let balance: number | null = null;
            if (coin.is_token == true) {
                switch (coin?.token_type?.toLowerCase()) {
                    case TokenStandard.ERC20.toLowerCase():
                        balance = await this.erc20TokenBalance(
                            address,
                            coin.token_address ? coin.token_address : null
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.polCoinBalance(address);
            }
            return balance;
        } catch (err: any) {
            console.error(`fetcPolBalance error >>`, err);
            return null;
        }
    }

    public async erc20TokenBalance(wallet_address: string, contract_address: string | null) {
        try {
            if (contract_address) {
                const contract = new ethers.Contract(
                    contract_address,
                    config.config.CONTRACT_ABI,
                    this.provider
                );
                const decimals = Number(await contract.decimals())
                const balance = await contract.balanceOf(wallet_address);
                const tokenBalance = await utilityHelper.bigNumberSafeMath(
                    balance, '/', Math.pow(10, decimals)
                );
                const tokBalance: string = await utilityHelper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
            }
            return null;
        } catch (err: any) {
            console.error(`erc20TokenBalance error >>`, err);
            return null;
            // throw err;
        }
    }

    public async polCoinBalance(address: string) {
        try {
            const balance = await this.provider.getBalance(address);
            const tokenBalance = await utilityHelper.bigNumberSafeMath(
                balance, '/', Math.pow(10, 18)
            );
            const coinBalance: string = await utilityHelper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance);
        } catch (err: any) {
            console.error(`polCoinBalance error >>`, err);
            return null;
            // throw err;
        }
    }

    public async convertWeiToPol(value: number | string) {
        try {
            const valueAsString = value.toString();
            return ethers.formatEther(valueAsString);;
        } catch (err: any) {
            console.error(`ConvertWeiToPol error >>`, err);
            return 0;
        }
    }

    public async tokenDetails(tokenAddress: string) {
        try {
            const contract = new ethers.Contract(tokenAddress, config.config.CONTRACT_ABI, this.provider);
            const [decimals, name, symbol] = await Promise.all([
                contract.decimals(),
                contract.name(),
                contract.symbol()
            ]);

            const data: TokenData = {
                decimals: Number(decimals),
                name,
                symbol
            };

            return data;
        } catch (err: any) {
            console.error("tokenDetails", err)
            return null;
        }
    }

}
export let blockchainHelper = new BlockchainHelper();