import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { coinData } from "../interfaces/global.interface";
import { CoinInterface, CoinsModel } from "../models";
import WalletModel from "../models/tables/model.wallets";
import { blockchainHelper } from "./blockchain.helper";
import { utilityHelper } from "./utility.helper";
class WalletHelper {
    public pol_pending_balance_update_topic = config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_TOPIC;

    public async fetchBalance(address: string, coin: coinData,) {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.POL:
                        balance = await blockchainHelper.fetchPolBalance(address, coin);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }
            return balance !== null ? Number(await utilityHelper.exponentialToDecimal(balance)) : null;
        } catch (err: any) {
            console.error(`error WalletHelper fetchBalance error >>>`, err);
            return null;
        }
    }


    public async updateBalance(walletAddress: string, coin: coinData) {
        try {
            if (!walletAddress) {
                console.error(`walletAddress missing >>> `);
                return false;
            }
            if (!coin) {
                console.error(`coin missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            const balance: number | null = await this.fetchBalance(
                walletAddress,
                coin
            );
            if (balance !== null) {
                const walletData: any = await WalletModel.findOne({
                    attributes: ["balance"],
                    where: {
                        wallet_address: walletAddress,
                        coin_id: coin.coin_id,
                    },
                    raw: true
                })
                if (walletData) {
                    await WalletModel.update({
                        balance: balance,
                        status: 1
                    }, {
                        where: {
                            wallet_address: walletAddress,
                            coin_id: coin.coin_id,
                        }
                    });
                }
            }
            else {
                const data = {
                    coin_data: coin,
                    wallet_address: walletAddress,
                    queue_count: 0,
                }
                await kafkaHelper.produceMessage(this.pol_pending_balance_update_topic, data)
            }
            return balance;
        } catch (err: any) {
            console.error(`WalletHelper updateBalance error >>>`, err);
            return null;
        }
    }

    public async updateAllActiveCoinBalance(wallet_address: string, coin_family: CoinFamilyEnum) {
        try {
            const addressCoins: any = await WalletModel.findAll({
                attributes: ["coin_id", "wallet_address"],
                where: {
                    wallet_address: wallet_address,
                    status: 1,
                    coin_family: coin_family
                },
                include: [{
                    model: CoinsModel,
                    attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                    as: "coin_data",
                    required: true,
                    where: {
                        coin_status: 1,
                    },
                }],
            })
            for (let i = 0; i < addressCoins.length; i++) {
                await this.updateBalance(addressCoins[i].wallet_address, addressCoins[i].coin_data);
            }
            return 1;
        } catch (err: any) {
            console.error(`WalletHelper updateAllActiveCoinBalance error >>>`, err);
            return 0;

        }
    }

    // update balance queue
    public async updateBalanceQueue(walletAddress: string, coin: coinData) {
        try {
            if (!walletAddress) {
                console.error(`walletAddress missing >>> `);
                return false;
            }
            if (!coin) {
                console.error(`coin missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            const balance: number | null = await this.fetchBalance(
                walletAddress,
                coin
            );
            return balance;
        } catch (err: any) {
            console.error(`WalletHelper updateBalanceQueue error >>>`, err);
            return null;
        }
    }
}
export let walletHelper = new WalletHelper();
