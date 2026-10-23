import { Op } from "sequelize";
import {
    BooleanEnum,
} from "../enum";

import { CatchErrorMsgsModel, CoinsModel, CustomTokennModel, DeviceTokenModel, WalletModel } from "../models/index";
import polProcessHelper from "../processes/process.helper";
import { config } from "../config/config";
import { coinData } from "../interfaces/global.interface";


class DbHelper {


    /** list of all active crypto coins */

    /** list of all crypto coins with token  */

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


}
export let dbHelper = new DbHelper();
