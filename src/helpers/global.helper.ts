// import axios from "axios";
import { config } from "../config/config";
// import { CatchErrorMsgsModel, CoinInterface } from "../models";
// import { WalletModel, CustomTokennModel, CoinsModel } from "../models/tables/index"
// import bscProcessHelper from "../processes/process.helper";
import redisClient from "../config/redis";
import { dbHelper } from "./db.helper";

class GlobalHelper {
    // public async checkCoinInWallet(wallet_address: string, coin: CoinInterface, balance: any) {
    //     try {
    //         let walletData: any = await WalletModel.findOne({
    //             attributes: ["balance", "user_id", "wallet_name"],
    //             where: {
    //                 wallet_address: wallet_address,
    //                 coin_id: coin.coin_id,
    //             },
    //             raw: true
    //         })
    //         if (!walletData) {
    //             walletData = await bscProcessHelper.checkOurWalletAddress(wallet_address);
    //             if (walletData) {
    //                 let data_obj: any = {
    //                     user_id: walletData.user_id,
    //                     wallet_name: walletData.wallet_name,
    //                     wallet_address: wallet_address,
    //                     coin_id: coin.coin_id,
    //                     coin_family: coin.coin_family,
    //                     balance: balance !== null ? balance : 0,
    //                     // default_wallet: 1,
    //                     status: 1
    //                 }
    //                 await WalletModel.create(data_obj);
    //             }
    //         }

    //         console.log("wallet created>>", walletData)
    //         if (walletData && coin.added_by) {
    //             console.log("coin.added_by exist", coin.added_by)
    //             if (coin.added_by == 'user') {
    //                 console.log("added by user")
    //                 const userExist: any = await CustomTokennModel.findOne({
    //                     attributes: ["id"],
    //                     where: { user_id: walletData.user_id, coin_id: coin.coin_id },
    //                     raw: true
    //                 })
    //                 if (userExist) {
    //                     console.log("custom token already exist")
    //                 } else {
    //                     console.log("custom token not exist")
    //                     const currentUTCDate: string = new Date()
    //                         .toISOString()
    //                         .replace(/T/, " ")
    //                         .replace(/\..+/, "");
    //                     await CustomTokennModel.create({
    //                         coin_id: coin.coin_id,
    //                         user_id: walletData.user_id,
    //                         created_at: currentUTCDate,
    //                         updated_at: currentUTCDate
    //                     })
    //                 }

    //             }
    //         } else {
    //             console.log("coin.added_by does not exist")
    //         }
    //     } catch (err: any) {
    //         console.error("Error in checkCoinInWallet>>>", err)
    //         return err;
    //     }

    // }


    // public async sendMail(email: string, text: string, subject: string) {
    //     try {
    //         const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
    //         const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
    //         const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
    //         const response: any = await axios({
    //             method: 'post',
    //             url: `${MAILGUN_BASE_URL}/messages`,
    //             auth: {
    //                 username: 'api',
    //                 password: MAILGUN_API_KEY
    //             },
    //             params: {
    //                 from: config.MAILGUN.FROM_GMAIL,
    //                 to: email,
    //                 subject: subject,
    //                 html: text
    //             }
    //         });
    //     } catch (err: any) {
    //         console.error("sendMail", err)
    //         await this.saveErrorLogs("MAILGUN_ERROR", err.message)
    //     }
    // }

    public async setHashTable(key: string, field: any, value: any, expiryInSeconds?: number) {
        try {
            redisClient.HSET(key, field.toUpperCase(), value, (err: any, reply: any) => {
                if (err) {
                    console.error('redis set string data error...', err)
                }
            });
            if (expiryInSeconds) {
                redisClient.EXPIRE(key, expiryInSeconds);
            }
        } catch (err: any) {
            console.error('💥 ~ ~ set_hash_table error');
            throw err;
        }
    }

    public async updateTokenInRedisHash(coin_data: any) {
        try {
            await this.setHashTable(
                config.TOKEN_TYPE_POL,
                coin_data.token_address,
                JSON.stringify(coin_data)
            );

        } catch (err: any) {
            console.error("Error in update_token_in_redis_hash.", err);
            await dbHelper.saveErrorLogs(
                "update_token_in_redis_hash",
                err.message
            );
            return null;
        }

    }


}
export let globalHelper = new GlobalHelper();
