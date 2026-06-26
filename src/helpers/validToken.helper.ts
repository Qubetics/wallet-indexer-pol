import { ethers } from "ethers";
import axios from "axios";
import { config } from "../config/config";
import { fakeTokensModel } from "../models/tables";
import processHelper from "../processes/process.helper";

const  RPC_URL  = config.NODE.POL_RPC_URL;


const provider = new ethers.JsonRpcProvider(RPC_URL);

const BASE_URL = `https://api.polygonscan.com/api`;
const ETHERSCAN_API_KEY = config.GAS_FEE_API_KEY;
const CHAIN_ID = 137; // POLYGON

interface TokenInfo {
  address: string;
  name: string;
  symbol: string;
  decimals: number;
  verified: boolean;
  category: string; // OK | SUSPICIOUS | FAKE
}

async function isVerifiedOnEtherscan(tokenAddress: string): Promise<boolean> {
  try {
    
    const response = await axios.get(
      `${BASE_URL}?module=contract&action=getsourcecode&address=${tokenAddress}&apikey=${ETHERSCAN_API_KEY}&chainid=${CHAIN_ID}`
    );

    const result = response.data.result[0];
    return result.SourceCode && result.SourceCode.length > 0;
  } catch {
    return false;
  }
}

async function getTokenInfo(tokenAddress: string): Promise<TokenInfo> {
  const abi = [
    "function name() view returns (string)",
    "function symbol() view returns (string)",
    "function decimals() view returns (uint8)",
  ];

  try {
    const contract = new ethers.Contract(tokenAddress, abi, provider);

    let [name, symbol, decimals] = await Promise.all([
      contract.name(),
      contract.symbol(),
      contract.decimals(),
    ]);

    if (typeof decimals === "bigint") {
        decimals = Number(decimals);
      }

    console.log(`Token info while checking valid token: ${name} ${symbol} ${decimals}`);

    const verified = await isVerifiedOnEtherscan(tokenAddress);

    console.log(`Verified status: ${verified}`);

    // heuristic detection
     // heuristic detection
     let category = "OK";
     if (!verified) category = "SUSPICIOUS";
     if (symbol.length > 10 || name.length > 32) category = "FAKE";
     if (decimals > 18) category = "FAKE";
 
     const returnData = {
       address: tokenAddress,
       name,
       symbol,
       decimals,
       verified,
       category,
     };
 
     // Safely stringify with BigInt handling
     console.log(
       `Token info: `,
       JSON.parse(JSON.stringify(returnData, bigIntToStringReplacer))
     );
 
     return returnData;
  } catch (err) {
    return {
      address: tokenAddress,
      name: "",
      symbol: "",
      decimals: 0,
      verified: false,
      category: "FAKE",
    };
  }
}

// Helper function: convert object BigInts to string recursively
function bigIntToStringReplacer(key: any, value: any): any {
    if (typeof value === 'bigint') {
      return value.toString();
    } else if (Array.isArray(value)) {
      return value.map((item) => bigIntToStringReplacer(key, item));
    }
    return value;
  }

export async function isValidToken(tokenAddress: string): Promise<boolean> {
    try {

    //check in redis first if token is fake
    const fakeTokenExists = await processHelper.getKeyValuePair(config.REDISKEYS.FAKE_TOKENS, tokenAddress);
    console.log(`Fake token exists in redis: ${fakeTokenExists}`);

    if (fakeTokenExists) return false;

      const tokenInfo = await getTokenInfo(tokenAddress);

      console.log(
        `Token info while checking valid token: ${JSON.stringify(
          tokenInfo,
          bigIntToStringReplacer
        )}`
      );

      const  boolean  = tokenInfo.category === "OK";
      console.log(`Is valid token: ${boolean}`);

    if (!boolean) {
        // Create entry in fake_coins table
        const fakeCoinData = {
            token_address: tokenAddress,
            name: tokenInfo?.name,
            symbol: tokenInfo?.symbol,
            decimals: tokenInfo?.decimals,
            verified: tokenInfo?.verified?.toString() ?? null, // store as string, not boolean
            category: tokenInfo?.category,
        }
        await fakeTokensModel.create(fakeCoinData);

        //enter in redis also 
        await processHelper.setKeyValuePair(config.REDISKEYS.FAKE_TOKENS, tokenAddress, JSON.stringify(fakeCoinData));
    }

      return boolean;

    } catch (err) {
      return false;
    }
}