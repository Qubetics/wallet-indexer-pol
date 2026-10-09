import {
    BooleanEnum,
    CoinFamilyEnum,
    TokenStandard,
} from "../enum";

import {
    CoinInterface,
    coinsModel,
    deviceTokenModel,
    walletModel,

} from "../models/index";
import { Op } from "sequelize";

class DbHelper {

    public async getCoinData(
        coinType: string,
        coinFamily: number
    ): Promise<CoinInterface | null> {
        let result: CoinInterface | null = await coinsModel.findOne({
            where: { coin_symbol: coinType, coin_family: coinFamily },
        });
        if (result) return result;
        else return null;
    }

    public async getWalletInfoByAddressAndCoinId(
        wallet_address: string,
        coin_id: number | undefined
    ) {
        try {
            let queryRes: any = await walletModel.findOne({
                where: { wallet_address, coin_id },
            });
            if (queryRes) return queryRes;
            else return null;
        } catch (error) {
            return null;
        }
    }

    /** list of all active crypto coins */
    activeCoinsListQry = async () => {
        try {
            return await coinsModel.findAll({
                where: {
                    coin_status: BooleanEnum.true,
                }
            });
        } catch (error: any) {
            console.error(`Db_Helper ActiveCoinsList error >>> \n `, error);
            return {};
        }
    }

    /** list of all crypto coins with token  */
    tokenListQry = async (
        coin_family: CoinFamilyEnum,
        token_standard: TokenStandard
    ) => {
        try {
            return await coinsModel.findAll({
                attributes: [
                    'id',
                    'coin_name',
                    'coin_symbol',
                    'price_source_slug',
                    'coin_image',
                    'coin_family',
                    'is_token',
                    'token_address',
                    'token_type',
                    'decimals',
                    'coin_status',
                    'default_wallet',
                    'price_source'
                ],
                where: {
                    coin_status: BooleanEnum.true,
                    is_token: BooleanEnum.true,
                    coin_family: coin_family,
                    token_type: token_standard
                },
                raw: true
            });
        } catch (error: any) {
            console.error(`DbHelper TokenListQry error >>> \n `, error);
            return {};
        }
    }

    /** get user device token using user id */
    getDeviceTokens = async (address: string, coin_family: number) => {
        try {
            return await deviceTokenModel.findAll({
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

