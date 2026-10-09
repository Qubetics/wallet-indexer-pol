export interface pushNotification {
    title: string,
    notification_id?: number;
    message: string;
    amount: any;
    coin_symbol: string | null;
    coin_id: number;
    coin_family: number;
    from_user_id: number;
    to_user_id?: number | null;
    wallet_address: string;
    notification_type: string;
    tx_id?: number | null;
    tx_type?: string;
    view_status?: string | null,
    announcement_title?: string,
    announcement_message?: string
}
export interface blocksTrnx {
    blockNumber: number,
    txId: string | '',
    value: string | '',
    fromAddress: string | '',
    toAddress: string | '',
    gasUsed: number | null,
    gasPrice: number | null
}

export interface pendingTrnx {
    txId: string;
    coinId: number,
    isToken: boolean,
    tokenType: string | null,
    tokenAddress: string
}

export interface trnxStatusUpdate {
    coin: {
        coinId: number,
        isToken: boolean,
        tokenType: string | null,
        tokenAddress: string
    };
    status: string;
    txId: string;
    gasReverted: number | undefined;
    fromAddress: string;
    txFee: number
}

export interface updateBroadCastTrnx {
    status: string,
    txId: string,
    gasReverted: number | undefined,
    txFee: number,
    fromAddress: string,
    coin: {
        coinId: number,
        isToken: boolean,
        tokenType: string | null,
        coinSymbol: string,
        coinFamily: number,
        cmcId: number,
        tokenAddress: string,
        addedBy: string
    }
}

export interface coinData {
    coin_id: number,
    is_token: boolean,
    token_type: string | null,
    coin_symbol: string,
    coin_family: number,
    cmc_id: number,
    token_address: string,
    added_by: string
}