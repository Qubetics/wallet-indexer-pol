import redis from "redis";
import { config } from "./config";

class RedisServer {
  public client: redis.RedisClient;

  constructor() {
    this.initiateConnection();
  }

  public async initiateConnection(): Promise<void> {
    this.client = redis.createClient({ url: config.REDIS_CONN });
    this.client.on("connect", () => {
      console.log("Connected to redis client successfully");
    });
  }

  public async setRedisSting(key: string, value: number | string|any) {
    this.client.set(key, value.toString());
  }


  public async getRedisSting(key: string) {
    let value=this.client.get(key);
     if(value){
        return value;
     }
     return null;
  }

  public async del(key: any) {
    try {
      this.client.DEL(key)
    } catch (error: any) {
      console.error("delKey ERROR>>>>>>", error);
      return null;
    }
  }

}

const redisClient = new RedisServer().client;
export default redisClient;
