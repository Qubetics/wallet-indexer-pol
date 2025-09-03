import database from "../config/db";
import { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, PolOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { TokenStandard } from "../enum";
import { globalHelper } from "../helpers/global.helper";
import { blockchainHelper, dbHelper } from "../helpers";
import { Op } from "sequelize";
import kafkaHelper from "../config/kafka.helper";

class PolProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchERC20Tokens();
    this.fetchWalletAddress();
  }

  public getKeyValuePair = async (keyName: string, field: string) => {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }

  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return new Promise((resolve, reject) => {
        redisClient.hmget(keyName, ...upperFields, (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
      return null;
    }
  }

  public setBlocks = async (field: string, key: string, minBlockConfirmation: number, newBlock: number | null) => {
    try {
      let latestBlock = await blockchainHelper.provider.getBlockNumber();
      console.log('latestBlock <<<', latestBlock);
      console.log('minBlockConfirmation <<<', minBlockConfirmation);
      console.log('key <<<', key);
      latestBlock = latestBlock - minBlockConfirmation;
      if (newBlock) {
        if (newBlock < latestBlock) {
          await this.setKeyValuePair(field, key, latestBlock.toString())
        } else {
          console.log("No updation of blocks")
        }
      } else {
        await this.setKeyValuePair(field, key, latestBlock.toString())
      }
    } catch (err: any) {
      console.error("Error in  getBlocks", err)
      await dbHelper.saveErrorLogs('Pol_setBlocks', err.message)
    }
  }

  public setKeyValuePair = async (keyName: string, field: string, value: string) => {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public async diffBlocksAddInDb(current_block: number, old_block: number) {
    try {

      const dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.POL, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await PolOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await dbHelper.saveErrorLogs('diffBlocksAddInDb', err.message)
      return false;
    }
  }

  public getHashTable = async (keyName: string) => {
    try {
      const getAsync: any = promisify(redisClient.hvals).bind(redisClient)
      const value: any = await getAsync(keyName)
      return value;
    } catch (err: any) {
      console.error("Error in getHashTable>>>", err)
      return null;
    }
  }

  public deleteKeyValuePair = async (keyName: string, field: string) => {
    try {
      redisClient.hdel(keyName, field.toUpperCase(), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }

  public fetchERC20Tokens = async () => {
    try {
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.POL,
          [Op.or]: [{ token_type: { [Op.ne]: "ERC721" } }, { token_type: null }]
        },
        raw: true
      });
      for await (let token of tokens) {
        let findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_POL}`,
          token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_POL}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }
      // redisClient.set(config.TOKEN_TYPE_BSC || "", JSON.stringify(tokens));
    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await dbHelper.saveErrorLogs('Pol_fetchERC20Tokens', err.message)
    }
  }

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.POL} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        const findAddress = await this.getKeyValuePair(
          `${config.POL_WALLET_ADDRESS}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.POL_WALLET_ADDRESS}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchAllWalletAddress error', error);
    }
  };


  public checkIfContract = async (address: string) => {
    try {
      const token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_POL, address);
      // if (token_data) {
      //   console.log("token_data>>>>>>>>", token_data)
      // }
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('checkIfContract >>>>>', err);
      return null;
    }
  };

  // public checkIfContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc20Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
  //     );
  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfContract >>>', error);
  //     return null;
  //   }
  // };

  // public fetchERC721Tokens = async () => {
  //   try {
  //     const getTokenQuery = `SELECT * FROM coins WHERE coin_status=1 AND coin_family=${config.STATIC_COIN_FAMILY.POL} AND (token_type!='${config.TOKEN_TYPE_POL}' OR token_type is null)`;

  //     const tokens: CoinInterface[] = await database.db_read.query(getTokenQuery, {
  //       type: Sequelize.QueryTypes.SELECT,
  //     });

  //     redisClient.set(config.TOKEN_TYPE721_POL, JSON.stringify(tokens));
  //   } catch (error: any) {
  //     console.error('fetchERC721Tokens >>>>', error);
  //   }
  // };

  // public checkIfOurNFTContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE721_BSC);
  //     const erc721Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc721Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address.toUpperCase() === address?.toUpperCase()
  //     );

  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfOurNFTContract >>>>>', error);
  //     return null;
  //   }
  // };

  public checkOurWalletAddress = async (address: string) => {
    try {
      const walletData: any = await this.getKeyValuePair(config.POL_WALLET_ADDRESS, address);
      // if (walletData) {
      //   console.log("walletData>>>>>>>>", walletData)
      // }
      return walletData ? JSON.parse(walletData) : null;
    } catch (err: any) {
      console.error('checkOurWalletAddress >>>>>', err);
      return null;
    }
  };

  // public NativeCoinByCoinFamily = async (coin_family: CoinFamilyEnum) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");
  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
  //     );

  //   } catch (err: any) {
  //     console.error(`NativeCoinByCoinFamily >>>`, err);
  //     return 0;
  //   }
  // }

  // public CoinByCoinId = async (coin_id: number) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_BSC || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => el.coin_id === coin_id
  //     );

  //   } catch (err: any) {
  //     console.error(`CoinByCoinId >>>`, err);
  //     return null;
  //   }
  // }

  public behindBlocks = async () => {
    try {
      const behindBlocks: any = await PolOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        const addData = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_POL, JSON.stringify(addData))
      } else {
        console.log("you have not any behind block yet")
      }
    } catch (err: any) {
      console.error(`behindBlocks >>>`, err);
      return null;
    }
  }

  private async addCoinToTable(
    tokenAddress: string,
    symbol: string,
    name: string,
    coinFamily: number,
    tokenType: string,
    decimals: any,
    currentUTCDate: any,
    userId: number
  ) {
    try {

      const coinData: any = {
        coin_name: name,
        coin_symbol: symbol.toUpperCase(),
        coin_image: null,
        cmc_id: 0,
        coin_family: coinFamily,
        coin_status: 1,
        is_token: 1,
        is_on_cmc: 1,
        for_swap: 0,
        added_by: "user",
        token_type: tokenType.toUpperCase(),
        decimals: Math.pow(10, parseInt(decimals)),
        token_address: tokenAddress,
        created_at: currentUTCDate,
        updated_at: currentUTCDate,
        graph_pair: null,
        graph_source: null,
        is_on_trading_view: 1,
      };
      const response: any = await dbHelper.coinCreate(coinData);
      console.log("🚀 ~ processHelper ~ response:", response)

      coinData.coin_id = response.coin_id;
      const redisCoinData = {
        coin_id: coinData.coin_id,
        is_token: coinData.is_token,
        token_address: coinData.token_address,
        coin_family: coinData.coin_family,
        decimals: coinData.decimals,
        token_type: coinData.token_type,
        coin_symbol: coinData.coin_symbol,
        added_by: coinData.added_by,
        for_swap: coinData.for_swap
      }

      //update  token in redis hash 
      await globalHelper.updateTokenInRedisHash(redisCoinData);
      //insert token in custom token table 
      await dbHelper.customTokenCreate({
        coin_id: coinData.coin_id,
        user_id: userId,
      });
      return redisCoinData;
    } catch (err: any) {
      console.error("Error in add_coin_to_table>>", err);
      await dbHelper.saveErrorLogs("add_coin_to_table", err.message);
      return null;
    }
  }

  public async insertTokenAddressInDB(tokenAddress: string, tokenDetail: any, nativeWalletData: any) {
    try {
      console.log("data in insertTokenAddressInDB", nativeWalletData)
      const userId = nativeWalletData.user_id;
      const coinFamily = config.STATIC_COIN_FAMILY.POL;
      const tokenType = TokenStandard.ERC20;
      const decimals = tokenDetail.decimals;
      const symbol = tokenDetail.symbol;
      const name = tokenDetail.name;
      const currentUTCDate: string = new Date()
        .toISOString()
        .replace(/T/, " ")
        .replace(/\..+/, "");

      //check if token already exist in db
      const coinDataExist: any = await CoinsModel.findOne({
        attributes: [
          "coin_id",
          "coin_status",
          "added_by",
          "coin_family",
          "token_address",
          "is_token",
          "token_type",
          "decimals",
          "coin_symbol",
        ],
        where: {
          token_address: tokenAddress,
          coin_family: coinFamily
        },
        raw: true
      });

      if (coinDataExist) {
        return coinDataExist;
      }


      //if token not exist in db then add it to db
      const coinData: any = await this.addCoinToTable(
        tokenAddress,
        symbol,
        name,
        coinFamily,
        tokenType,
        decimals,
        currentUTCDate,
        userId
      );


      if (
        coinFamily == config.STATIC_COIN_FAMILY.POL
      ) {
        const redisTokenData: any = {
          coin_family: coinFamily,
          token_address: tokenAddress,
        };

        //Todo : Need  to confirm from barjinder sir  if we need to produce message to kafka or not 
        await kafkaHelper.produceMessage(
          config.MORALIS_TRADINGVIEW_TOPIC,
          redisTokenData
        );
      }

      return coinData;

    } catch (error) {
      console.error(`insertTokenAddressInDB >>>`, error);
      return null;
    }

  }

}
const polProcessHelper = new PolProcessHelper();
export default polProcessHelper;