import {
  BlockChainStatusEnum,
  TrnxTypeEnum
} from "../enum";


type ERC20Tokens = {
  coin_symbol: string;
  token_address: string;
  decimals: number;
};


export type TxType = {
  txId: string;
  fromAddress: string;
  toAddress: string;
  token: ERC20Tokens | null;
  amount: number;
  blockId: number;
  isNFTToken?: boolean;
  txType: TrnxTypeEnum | null;
  trnxFee: number
};

export type WebHookTxType = {
  txId: string;
  fromAddress: string;
  toAddress: string;
  amount: number;
  blockId: number | string;
  coin: {
    coinId: number,
    isToken: boolean,
    tokenType: string | null,
    coinSymbol: string,
    coinFamily: number,
    cmcId: number,
    tokenAddress: string,
    addedBy: string
  };
  isNFTToken: boolean;
  status: string;
  blockchainStatus: BlockChainStatusEnum,
  txType: TrnxTypeEnum | null;
  trnxFee: number
};
