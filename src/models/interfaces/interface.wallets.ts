export interface WalletInterface {
  wallet_id: number;
  user_id: number;
  wallet_name: string;
  wallet_address: string;
  coin_id: number;
  balance: number | string;
  status?: number | 0;
  sort_order?: number | null;
  coin_family?: number;
}
