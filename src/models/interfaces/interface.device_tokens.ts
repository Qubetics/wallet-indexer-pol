export interface DeviceTokenInterface {
    id: number;
    device_token: string | string[];
    user_id: number;
    status: number;
    address: string;
    coin_family: number;
    push?: number;
}
