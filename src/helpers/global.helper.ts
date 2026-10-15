import { config } from "../config/config";
import redisClient from "../config/redis";
import { dbHelper } from "./db.helper";

class GlobalHelper {


    public async setHashTable(key: string, field: any, value: any, expiryInSeconds?: number) {
        try {
            await redisClient.hSet(key, field.toUpperCase(), value).catch((err: any) => {
                console.error('redis set string data error...', err)
            });
            if (expiryInSeconds) {
                await redisClient.expire(key, expiryInSeconds);
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
