import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface, coinsModel } from "../models";
import walletModel from "../models/tables/model.wallets";
import { blockchainHelper } from "./blockchain.helper";
import { utilityHelper } from "./utility.helper";

class WalletHelper {
    public pol_pending_balance_update_topic = config.config.KAFKA_BALANCE.POL_PENDING_BALANCE_UPDATE_TOPIC;

    fetchBalance = async (
        address: string,
        coin: CoinInterface,
    ): Promise<number | null> => {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.POL:
                        balance = await blockchainHelper.fetchBalance(address, coin);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }

            // Convert exponential notation to decimal
            return Number(await utilityHelper.exponentialToDecimal(balance || 0));
        } catch (error: any) {
            console.error(`WalletHelper fetchBalance error for address ${address}, coin ${coin?.coin_id}:`, error);
            return null;
        }
    }

    updateBalance = async (
        wallet_address: string,
        coin: CoinInterface
    ): Promise<number | null> => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return null;
            }

            if (!coin) {
                console.error(`coin parameter missing >>> `);
                return null;
            }

            /* fetch wallet balance */
            const balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );

            if (balance !== null) {
                // Optimize: Use update with where clause directly instead of findOne + update
                await walletModel.update({
                    balance: balance,
                    status: 1
                }, {
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id,
                    }
                });
            } else {
                // Queue balance update when fetch fails
                await this.queueBalanceUpdate(coin, wallet_address);
            }

            return balance;
        } catch (error: any) {
            console.error(`WalletHelper updateBalance error >>>`, error);
            return null;
        }
    }

    private async queueBalanceUpdate(coin: CoinInterface, wallet_address: string): Promise<void> {
        try {
            const data = {
                coin_data: coin,
                wallet_address: wallet_address,
                queue_count: 0,
            };
            await kafkaHelper.produceMessage(this.pol_pending_balance_update_topic, data);
        } catch (error: any) {
            console.error(`Failed to queue balance update for ${wallet_address}:`, error);
        }
    }

    updateAllActiveCoinBalance = async (wallet_address: string, coin_family: CoinFamilyEnum): Promise<number> => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return 0;
            }

            if (!coin_family) {
                console.error(`coin_family missing >>> `);
                return 0;
            }

            const address_coins: any = await walletModel.findAll({
                attributes: ["coin_id", "wallet_address"],
                where: {
                    wallet_address: wallet_address,
                    status: 1,
                    coin_family: coin_family
                },
                include: [{
                    model: coinsModel,
                    attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                    as: "coin_data",
                    required: true,
                    where: {
                        coin_status: 1,
                    },
                }],
            });

            if (!address_coins || address_coins.length === 0) {
                console.log(`No active coins found for wallet: ${wallet_address}, coin_family: ${coin_family}`);
                return 0;
            }

            let successCount = 0;
            for (let i = 0; i < address_coins.length; i++) {
                try {
                    const result = await this.updateBalance(address_coins[i].wallet_address, address_coins[i].coin_data);
                    if (result !== null) {
                        successCount++;
                    }
                } catch (coinError: any) {
                    console.error(`Failed to update balance for coin ${address_coins[i].coin_id}:`, coinError);
                }
            }

            console.log(`Updated ${successCount} out of ${address_coins.length} coins for wallet: ${wallet_address}`);
            return successCount > 0 ? 1 : 0;
        } catch (err: any) {
            console.error(`WalletHelper updateAllActiveCoinBalance error >>>`, err);
            return 0;
        }
    }

    public async getBalance(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return null;
            }

            if (!coin) {
                console.error(`coin parameter missing >>> `);
                return null;
            }

            /* fetch wallet balance */
            const balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );

            return balance;
        } catch (err: any) {
            console.error(`WalletHelper getBalance error >>>`, err);
            return null;
        }
    }
}
export let walletHelper = new WalletHelper();
