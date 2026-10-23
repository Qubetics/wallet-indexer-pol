export interface DeviceTokenInterface {
    id: number;
    device_token: string | string[];
    user_id: number;
    device_id: string;
    status: number;
    address: string;
    coin_family: number;
    push?: number;
}
