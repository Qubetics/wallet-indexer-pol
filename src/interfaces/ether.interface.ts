export type AbiInput = {
  name: string;
  type: string;
  indexed?: boolean;
};

export type AbiOutput = {
  name: string;
  type: string;
};

export type AbiItem = {
  name?: string;
  type: "function" | "constructor" | "event" | "fallback" | "receive";
  stateMutability?: "pure" | "view" | "nonpayable" | "payable";
  constant?: boolean;
  payable?: boolean;
  anonymous?: boolean;
  inputs?: AbiInput[];
  outputs?: AbiOutput[];
};

export interface TokenData {
  decimals: number;
  name: string;
  symbol: string;
}
