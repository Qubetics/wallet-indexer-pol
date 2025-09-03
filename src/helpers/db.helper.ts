import { Op } from "sequelize";
import {
    BooleanEnum,
    // CoinFamilyEnum,
    // TokenStandard,
} from "../enum";

import {
    CatchErrorMsgsModel,
    CoinInterface,
    CoinsModel,
    CustomTokennModel,
    // CatchErrorMsgsModel,
    // CoinInterface,
    // CoinPriceInFiatModel,
    // CoinsModel,
    DeviceTokenModel,
    WalletModel,
    // ReferralModel,
    // RewardHistoryModel,
    // SettingsModel,
    // WalletModel
} from "../models/index";
import polProcessHelper from "../processes/process.helper";
import { config } from "../config/config";
import { coinData } from "../interfaces/global.interface";

// import { utilityHelper } from "./utility.helper";

class DbHelper {

    // public async getCoinData(
    //     coinType: string,
    //     coinFamily: number
    // ): Promise<CoinInterface | null> {
    //     let result: CoinInterface | null = await CoinsModel.findOne({
    //         where: { coin_symbol: coinType, coin_family: coinFamily },
    //     });
    //     if (result) return result;
    //     else return null;
    // }

    // public async getWalletInfoByAddressAndCoinId(
    //     wallet_address: string,
    //     coin_id: number | undefined
    // ) {
    //     try {
    //         let queryRes: any = await WalletModel.findOne({
    //             where: { wallet_address, coin_id },
    //         });
    //         if (queryRes) return queryRes;
    //         else return null;
    //     } catch (error) {
    //         return null;
    //     }
    // }

    /** list of all active crypto coins */
    // ActiveCoinsListQry = async () => {
    //     try {
    //         return await CoinsModel.findAll({
    //             where: {
    //                 coin_status: BooleanEnum.true,
    //             }
    //         });
    //     } catch (error: any) {
    //         console.error(`dbHelper ActiveCoinsList error >>> \n `, error);
    //         return {};
    //     }
    // }

    /** list of all crypto coins with token  */
    // TokenListQry = async (
    //     coin_family: CoinFamilyEnum,
    //     token_standard: TokenStandard
    // ) => {
    //     try {
    //         return await CoinsModel.findAll({
    //             attributes: [
    //                 'id',
    //                 'coin_name',
    //                 'coin_symbol',
    //                 'price_source_slug',
    //                 'coin_image',
    //                 'coin_family',
    //                 'is_token',
    //                 'token_address',
    //                 'token_type',
    //                 'decimals',
    //                 'coin_status',
    //                 // 'default_wallet',
    //                 'price_source'
    //             ],
    //             where: {
    //                 coin_status: BooleanEnum.true,
    //                 is_token: BooleanEnum.true,
    //                 coin_family: coin_family,
    //                 token_type: token_standard
    //             },
    //             raw: true
    //         });
    //     } catch (error: any) {
    //         console.error(`DbHelper TokenListQry error >>> \n `, error);
    //         return {};
    //     }
    // }

    public async checkCoinInWallet(
        walletAddress: string,
        coin: coinData,
        balance: any
    ): Promise<void | Error> {
        try {
            // Fetch wallet if already exists
            let walletData = await WalletModel.findOne({
                attributes: ["balance", "user_id", "wallet_name"],
                where: {
                    wallet_address:walletAddress,
                    coin_id: coin.coin_id,
                },
                raw: true,
            });

            // If wallet not found, check if it belongs to us and create it
            if (!walletData) {
                const walletExist = await polProcessHelper.checkOurWalletAddress(walletAddress);

                if (walletExist) {
                    const dataObj = {
                        user_id: walletExist.user_id,
                        wallet_name: walletExist.wallet_name,
                        wallet_address:walletAddress,
                        coin_id: coin.coin_id,
                        coin_family: coin.coin_family,
                        balance: balance ?? 0,
                        status: 1,
                    };

                    walletData = await WalletModel.create(dataObj, { raw: true }) as any;
                    console.log("Wallet created >>", walletData);
                }
            }

            // If wallet exists and coin is user-added, check custom tokens
            if (walletData && coin.added_by === "user") {
                const userExist = await CustomTokennModel.findOne({
                    attributes: ["id"],
                    where: { user_id: walletData.user_id, coin_id: coin.coin_id },
                    raw: true,
                });

                if (!userExist) {
                    console.log("Custom token does not exist, creating...");

                    const currentUTCDate: string = new Date()
                        .toISOString()
                        .replace(/T/, " ")
                        .replace(/\..+/, "");

                    await CustomTokennModel.create({
                        coin_id: coin.coin_id,
                        user_id: walletData.user_id,
                        created_at: currentUTCDate,
                        updated_at: currentUTCDate,
                    });
                } else {
                    console.log("Custom token already exists");
                }
            } else if (!coin.added_by) {
                console.log("coin.added_by does not exist");
            }

        } catch (err: any) {
            console.error("Error in checkCoinInWallet >>>", err);
            return err;
        }
    }

    public async saveErrorLogs(fn_name: string, error_message: any) {
        try {
            const valueExist: any = await polProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.POL_ERROR_LOGS)
            if (valueExist) {
                if (Number(valueExist) == 0) {
                    await CatchErrorMsgsModel.create({
                        fx_name: fn_name,
                        error_msg: error_message || {}
                    })
                    await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.POL_ERROR_LOGS, '1')
                } else {
                    console.log("value other than 0 exist")
                }
            } else {
                await polProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.POL_ERROR_LOGS, '0')
                console.log("value does not exist")
            }
        } catch (err: any) {
            console.error("Error in saveErrorLogs", err)
        }
    }

    public async coinCreate(obj: any) {
        try {
            const data: any = await CoinsModel.create(obj);
            return data;
        } catch (err: any) {
            console.error("Error in coin_create>>", err);

            const uniqueConstraint =
                err.name === 'SequelizeUniqueConstraintError' &&
                err.errors?.some(
                    (e: any) => e.path === 'unique_coinfamily_tokenaddress' || e.message?.includes('unique_coinfamily_tokenaddress')
                );

            if (uniqueConstraint) {
                const tokenAddress = obj.token_address;
                const coinFamily = obj.coin_family;

                try {
                    const existing = await CoinsModel.findOne({
                        where: {
                            token_address: tokenAddress,
                            coin_family: coinFamily,
                        },
                        raw: true
                    });
                    console.log("🚀 ~ CoinQueries ~ coin_create ~ existing:", existing)

                    if (existing) {
                        return existing;
                    }
                } catch (findErr) {
                    console.error("Error finding existing coin>>", findErr);
                    await this.saveErrorLogs("coin_create_find_existing", (findErr as any).message);
                }
            }

            await this.saveErrorLogs("coin_create", err.message);
            return null;
        }
    }

    public async customTokenCreate(obj: any) {
        try {
            const data: any = await CustomTokennModel.create(obj)
            return data;
        } catch (err: any) {
            console.error("Error in custom_token_create>>", err);
            await this.saveErrorLogs("custom_token_create", err.message);
            return null;
        }
    }
    /** get user device token using user id */
    getDeviceTokens = async (user_id: number, address: string, coin_family: number) => {
        try {
            return await DeviceTokenModel.findAll({
                attributes: ["device_token"],
                where: {
                    address: {
                        [Op.like]: address,
                    },
                    coin_family: coin_family,
                    push: BooleanEnum.true
                },
                order: [
                    ["updated_at", "DESC"],
                    ['id', 'DESC']
                ],
                raw: true
            });
        } catch (error: any) {
            console.error(`getDeviceToken error >>>`, error);
            return false;
        }
    };

    /** save reward data */
    // save_reward = async (data: any) => {
    //     try {
    //         console.log('save_reward data >>>>>>>>>>>>>>...', data);
    //         let user_id = data.user_id;
    //         let fiat_type = data.fiat_type;
    //         let referral_data: any = await ReferralModel.findOne({ attributes: ['referrer_from'], where: { referrer_to: user_id }, raw: true });
    //         console.log('referral_data >>>', referral_data);
    //         if (referral_data) {
    //             let referrer_from = referral_data.referrer_from;
    //             let wallet_data = await WalletModel.findOne({ attributes: ['wallet_address'], where: { user_id: referrer_from }, raw: true });
    //             console.log('referral_data wallet_data >>>', wallet_data);
    //             if (wallet_data) {
    //                 let address = wallet_data.wallet_address;
    //                 let reward_setting_data = await SettingsModel.findOne({ attributes: ['value'], where: { title: 'REWARD_PERCENTAGE' }, raw: true });
    //                 console.log('reward_setting_data >>>', reward_setting_data);
    //                 if (reward_setting_data) {
    //                     let reward_already_exist = await RewardHistoryModel.findOne({ attributes: ['id', 'tx_id'], where: { tx_id: data.tx_id } });
    //                     console.log('reward_already_exist >>>', reward_already_exist);
    //                     if (!reward_already_exist) {
    //                         console.log('save history in >>>');
    //                         let percentage = reward_setting_data?.value;
    //                         console.log('percentage >>>', percentage);
    //                         let amount_in_usd = data.amount_in_fiat;

    //                         if (fiat_type.toUpperCase() !== 'USD' && data.cmc_id !== null) {//10 usd 0.1
    //                             let cmc_id = data.cmc_id;

    //                             /** get current fiat price of swap crypto currency */
    //                             let currency_fiat_data = await CoinPriceInFiatModel.findOne({ attributes: ['value'], where: { cmc_id: cmc_id, fiat_type: fiat_type }, raw: true });
    //                             let amount_in_fiat = data.amount_in_fiat;
    //                             if (currency_fiat_data) {
    //                                 let current_fiat_value = currency_fiat_data.value;
    //                                 amount_in_fiat = utilityHelper.bigNumberSafeMath(amount_in_fiat, '/', current_fiat_value);
    //                             }

    //                             /*** get usd price of current swap currency */
    //                             let fiat_coin_data = await CoinPriceInFiatModel.findOne({ attributes: ['value'], where: { cmc_id: cmc_id, fiat_type: 'USD' }, raw: true });
    //                             if (fiat_coin_data) {
    //                                 let fiat_value = fiat_coin_data.value;
    //                                 amount_in_usd = utilityHelper.bigNumberSafeMath(amount_in_fiat, '*', fiat_value);
    //                             }

    //                             console.log('amount_in_usd >>>', amount_in_usd)
    //                             let amount_percent = await utilityHelper.bigNumberSafeMath(amount_in_usd, '*', percentage);
    //                             console.log('amount_percent >>>', amount_percent);
    //                             let exact_percent_amount = await utilityHelper.bigNumberSafeMath(amount_percent, '/', 100);
    //                             console.log('exact_percent_amount >>>', exact_percent_amount)

    //                             let reward_data: any = {
    //                                 tx_id: data.tx_id,
    //                                 address: address,
    //                                 amount: exact_percent_amount,
    //                                 percentage: percentage,
    //                                 currency: 'usd',
    //                                 type: data.type,
    //                                 user_id: referrer_from
    //                             }
    //                             console.log('reward_data >>>', reward_data)
    //                             await RewardHistoryModel.create(reward_data);
    //                             return true;
    //                         } else if (fiat_type.toUpperCase() == 'USD') {
    //                             console.log('amount_in_usd >>>', amount_in_usd)
    //                             let amount_percent = await utilityHelper.bigNumberSafeMath(amount_in_usd, '*', percentage);
    //                             console.log('amount_percent >>>', amount_percent)

    //                             let exact_percent_amount = await utilityHelper.bigNumberSafeMath(amount_percent, '/', 100);
    //                             console.log('exact_percent_amount >>>', exact_percent_amount)
    //                             let reward_data: any = {
    //                                 tx_id: data.tx_id,
    //                                 address: address,
    //                                 amount: exact_percent_amount,
    //                                 percentage: percentage,
    //                                 currency: 'usd',
    //                                 type: data.type,
    //                                 user_id: referrer_from
    //                             }
    //                             console.log('reward_data >>>', reward_data)
    //                             await RewardHistoryModel.create(reward_data);
    //                             return true;
    //                         } else {
    //                             let error_msg = {
    //                                 fiat_type: fiat_type || '',
    //                                 cmc_id: data.cmc_id || '',
    //                                 tx_id: data.tx_id || ''
    //                             }
    //                             await CatchErrorMsgsModel.create({
    //                                 fx_name: 'save_reward',
    //                                 error_msg: error_msg
    //                             })
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     } catch (error: any) {
    //         console.error(`save_rewards error >>>`, error);
    //         let error_msg = {
    //             fiat_type: error.message || ''
    //         }
    //         await CatchErrorMsgsModel.create({
    //             fx_name: 'save_reward',
    //             error_msg: error_msg
    //         })
    //     }
    // }

}
export let dbHelper = new DbHelper();
