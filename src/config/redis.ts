import { createClient } from "redis";
import { config } from "./config";

class RedisServer {
  public client: ReturnType<typeof createClient>;

  constructor() {
    this.client = createClient({ url: config.REDIS_CONN });
    this.initiateConnection();
  }

  public initiateConnection(): void {
    this.client.on("connect", () => {
      console.log("Connected to redis client successfully");
    });
    // without an 'error' listener the client would crash the process on a connection error
    this.client.on("error", (err: any) => {
      console.error("Redis client error", err?.message || err);
    });
    // commands sent before the connection is ready are queued by the client
    this.client.connect().catch((err: any) => {
      console.error("Redis connect error", err?.message || err);
    });
  }

}

const redisClient = new RedisServer().client;
export default redisClient;
